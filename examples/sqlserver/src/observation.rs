//! Native observations, not promotion eligibility or an atomic DMV snapshot.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::executor::{QueryRow, SqlSession};
use crate::query::ReadQuery;
use crate::runtime_error::RuntimeError;
use crate::{
    AvailabilityGroupIdentity, AvailabilityGroupName, DatabaseIdentity, DatabaseLineage,
    DecimalProgress, Guid, NativeProgress, NativeRole, Observation, ObservationFailureKind,
    ReplicaIdentity, SUPPORTED_DATABASE_COUNT, SUPPORTED_ENGINE_MAJOR, SUPPORTED_REPLICA_COUNT,
    SUPPORTED_REQUIRED_SECONDARIES, ServerName, SqlIdentifier,
};

#[derive(Debug, Clone)]
pub struct ObservationTarget {
    pub availability_group: AvailabilityGroupName,
    pub expected_server_name: ServerName,
    pub replica: ReplicaIdentity,
}

/// An owned value from one bracketed attempt. No live connections or mutable
/// caches are retained. A fresh attempt does not make remote DMV reports fresh.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InstanceSnapshot {
    pub observed_at_unix_millis: u64,
    pub instance: InstanceMetadata,
    pub availability_group: Observation<AvailabilityGroupSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InstanceMetadata {
    pub server_name: ServerName,
    pub property_server_name: ServerName,
    pub product_version: String,
    pub product_major_version: u16,
    pub edition: String,
    pub engine_edition: u16,
    pub hadr_enabled: bool,
    pub host_platform: String,
    pub host_distribution: Option<String>,
    /// The SQL binary's advertised architecture, obtained from @@VERSION.
    pub architecture: String,
    /// SQL Server's local start time; this is not a UTC timestamp.
    pub sqlserver_start_time: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AvailabilityGroupSnapshot {
    pub identity: AvailabilityGroupIdentity,
    /// A nonnegative SQL bigint, serialized as a decimal string.
    pub configuration_sequence: DecimalProgress,
    pub cluster_type: String,
    pub required_synchronized_secondaries_to_commit: u32,
    pub basic_features: bool,
    pub is_distributed: bool,
    pub local_replica: LocalReplicaSnapshot,
    pub replicas: Vec<ReplicaSnapshot>,
    pub databases: Vec<DatabaseSnapshot>,
    /// Current-GUID-scoped history. An empty list is not proof of no seeding.
    pub automatic_seeding: Vec<AutomaticSeedingSnapshot>,
    /// Only processes attributable through an associated local database GUID.
    pub physical_seeding: Vec<PhysicalSeedingSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LocalReplicaSnapshot {
    pub identity: ReplicaIdentity,
    pub state_available: bool,
    /// NULL and missing DMV state are not converted to a healthy/joined role.
    pub role: Option<NativeRole>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeProvenance {
    Local,
    /// The primary's possibly delayed report about another replica.
    PrimaryRemote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplicaSnapshot {
    pub replica_id: Guid,
    pub server_name: ServerName,
    pub endpoint_url: Option<String>,
    pub availability_mode: String,
    pub failover_mode: String,
    pub seeding_mode: String,
    pub state: Option<ReplicaState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplicaState {
    pub provenance: NativeProvenance,
    pub role: Option<NativeRole>,
    pub operational_state: Option<String>,
    pub connected_state: Option<String>,
    pub recovery_health: Option<String>,
    pub synchronization_health: Option<String>,
    pub last_connect_error_number: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DatabaseSnapshot {
    pub identity: DatabaseIdentity,
    /// None means no GUID-associated local catalog row was visible. It does
    /// not prove that a same-named, not-yet-joined database does not exist.
    pub local: Option<LocalDatabaseSnapshot>,
    pub replicas: Vec<DatabaseReplicaSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LocalDatabaseSnapshot {
    pub database_id: u32,
    pub replica_id: Guid,
    pub state: Option<String>,
    pub recovery_model: Option<String>,
    /// Metadata may be unavailable for an offline or not-yet-started database.
    pub recovery: Option<LocalRecoveryMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LocalRecoveryMetadata {
    pub database_guid: Option<Guid>,
    pub family_guid: Option<Guid>,
    pub recovery_fork_guid: Option<Guid>,
    pub first_recovery_fork_guid: Option<Guid>,
    pub fork_point_lsn: Option<DecimalProgress>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RecoveryLineageObservation {
    Local {
        value: DatabaseLineage,
    },
    LocalUnavailable,
    /// No remote sys.database_recovery_status query was made.
    RemoteUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DatabaseReplicaSnapshot {
    pub group_database_id: Guid,
    pub replica_id: Guid,
    pub database_id: u32,
    pub provenance: NativeProvenance,
    pub lineage: RecoveryLineageObservation,
    pub is_primary_replica: Option<bool>,
    pub synchronization_state: Option<String>,
    pub synchronization_health: Option<String>,
    pub database_state: Option<String>,
    pub is_suspended: Option<bool>,
    pub suspend_reason: Option<String>,
    pub is_commit_participant: Option<bool>,
    /// Uninterpreted DMV positions. In particular, a synchronous primary's
    /// hardened value can be the minimum reported by its secondaries, rather
    /// than its own durable log position.
    pub progress: NativeProgress,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AutomaticSeedingSnapshot {
    pub group_database_id: Guid,
    pub remote_replica_id: Guid,
    pub operation_id: Guid,
    pub is_source: bool,
    pub current_state: Option<String>,
    pub performed_seeding: Option<bool>,
    pub failure_state: Option<i32>,
    pub error_code: Option<i32>,
    pub number_of_attempts: Option<u32>,
    /// These two DMV times are not documented as UTC.
    pub start_time: Option<String>,
    pub completion_time: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PhysicalSeedingSnapshot {
    pub group_database_id: Guid,
    pub local_replica_id: Guid,
    pub local_physical_seeding_id: Guid,
    pub remote_physical_seeding_id: Option<Guid>,
    pub local_database_id: u32,
    pub local_database_name: SqlIdentifier,
    /// Informational only: machine names are not native replica identities.
    pub remote_machine_name: Option<String>,
    pub role: Option<String>,
    pub internal_state: Option<String>,
    pub transfer_rate_bytes_per_second: Option<DecimalProgress>,
    pub transferred_size_bytes: Option<DecimalProgress>,
    pub database_size_bytes: Option<DecimalProgress>,
    pub failure_code: Option<i32>,
    pub is_compression_enabled: Option<bool>,
    pub start_time_utc: Option<String>,
    pub end_time_utc: Option<String>,
    pub estimate_time_complete_utc: Option<String>,
}

/// Permissions and identity/configuration anchors bracket every attempt,
/// including absence. Database/fork anchors additionally bracket progress.
///
/// These checks detect observed changes, not changes that happen and revert
/// between reads. Neither a SQL transaction nor this bracket makes DMVs atomic.
pub async fn observe_session(
    session: &mut dyn SqlSession,
    target: &ObservationTarget,
    observed_at_unix_millis: u64,
) -> Result<InstanceSnapshot, RuntimeError> {
    check_permissions(
        &session
            .query(ReadQuery::Permissions, &target.availability_group)
            .await?,
    )?;
    let before = parse_anchor(
        &session
            .query(ReadQuery::Anchor, &target.availability_group)
            .await?,
    )?;
    before.validate(target)?;

    let availability_group = if let Some(group) = &before.group {
        let replicas = parse_replicas(
            &session
                .query(ReadQuery::Replicas, &target.availability_group)
                .await?,
            &before,
            group,
        )?;
        let mut databases = parse_databases(
            &session
                .query(ReadQuery::Databases, &target.availability_group)
                .await?,
            group,
        )?;
        if databases.len() > usize::from(SUPPORTED_DATABASE_COUNT) {
            return Err(unsupported(
                "databases",
                "more than one managed database is unsupported",
            ));
        }
        add_database_states(
            &session
                .query(ReadQuery::DatabaseStates, &target.availability_group)
                .await?,
            group,
            &replicas,
            &mut databases,
        )?;
        let automatic_seeding = parse_automatic_seeding(
            &session
                .query(ReadQuery::AutomaticSeeding, &target.availability_group)
                .await?,
            group,
            &replicas,
            &databases,
        )?;
        let physical_seeding = parse_physical_seeding(
            &session
                .query(ReadQuery::PhysicalSeeding, &target.availability_group)
                .await?,
            group,
            &databases,
        )?;
        let after_databases = parse_databases(
            &session
                .query(ReadQuery::Databases, &target.availability_group)
                .await?,
            group,
        )?;
        if !database_anchors_match(&databases, &after_databases) {
            return Err(inconsistent(
                "databases",
                "database identity or local recovery lineage changed during observation",
            ));
        }
        let identity = match &group.local_replica_id {
            Some(native_id) => ReplicaIdentity::observed(
                target.replica.logical_id(),
                native_id.clone(),
                target.replica.incarnation(),
            )
            .map_err(|_| malformed("anchor", "invalid configured replica identity"))?,
            None => target.replica.clone(),
        };
        Observation::Present {
            value: AvailabilityGroupSnapshot {
                identity: group.identity.clone(),
                configuration_sequence: group.configuration_sequence,
                cluster_type: group.cluster_type_desc.clone(),
                required_synchronized_secondaries_to_commit: group.required_secondaries,
                basic_features: group.basic_features,
                is_distributed: group.is_distributed,
                local_replica: LocalReplicaSnapshot {
                    identity,
                    state_available: group.local_state_replica_id.is_some(),
                    role: group.local_role.clone(),
                },
                replicas: replicas.into_values().collect(),
                databases: databases.into_values().collect(),
                automatic_seeding,
                physical_seeding,
            },
            observed_at_unix_millis,
        }
    } else {
        Observation::Absent {
            observed_at_unix_millis,
        }
    };

    check_permissions(
        &session
            .query(ReadQuery::Permissions, &target.availability_group)
            .await?,
    )?;
    let after = parse_anchor(
        &session
            .query(ReadQuery::Anchor, &target.availability_group)
            .await?,
    )?;
    if before != after {
        return Err(inconsistent(
            "anchor",
            "instance, availability group, configuration, or local role changed during observation",
        ));
    }
    Ok(InstanceSnapshot {
        observed_at_unix_millis,
        instance: before.instance,
        availability_group,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Anchor {
    instance: InstanceMetadata,
    group: Option<GroupAnchor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GroupAnchor {
    identity: AvailabilityGroupIdentity,
    configuration_sequence: DecimalProgress,
    cluster_type: u8,
    cluster_type_desc: String,
    required_secondaries: u32,
    basic_features: bool,
    is_distributed: bool,
    local_replica_id: Option<Guid>,
    local_replica_server_name: Option<ServerName>,
    local_state_replica_id: Option<Guid>,
    local_role: Option<NativeRole>,
}

impl Anchor {
    fn validate(&self, target: &ObservationTarget) -> Result<(), RuntimeError> {
        let instance = &self.instance;
        if instance.server_name != target.expected_server_name
            || instance.property_server_name != target.expected_server_name
        {
            return Err(inconsistent(
                "anchor",
                "native server identity does not match the configured target",
            ));
        }
        if instance.product_major_version != SUPPORTED_ENGINE_MAJOR {
            return Err(unsupported(
                "anchor",
                "only SQL Server 2022 major version 16 is supported",
            ));
        }
        let edition = instance
            .edition
            .strip_suffix(" (64-bit)")
            .unwrap_or(&instance.edition);
        if instance.engine_edition != 3
            || !matches!(
                edition,
                "Developer"
                    | "Developer Edition"
                    | "Enterprise"
                    | "Enterprise Edition"
                    | "Enterprise Edition: Core-based Licensing"
            )
        {
            return Err(unsupported(
                "anchor",
                "only Developer or Enterprise edition with EngineEdition 3 is supported",
            ));
        }
        if !instance.hadr_enabled {
            return Err(unsupported(
                "anchor",
                "Always On availability groups must be enabled",
            ));
        }
        if instance.host_platform != "Linux" || instance.architecture != "x86_64" {
            return Err(unsupported(
                "anchor",
                "only a Linux x86-64 SQL Server engine is supported",
            ));
        }
        if let Some(group) = &self.group {
            if !group
                .identity
                .name
                .as_str()
                .eq_ignore_ascii_case(target.availability_group.as_str())
            {
                return Err(inconsistent(
                    "anchor",
                    "native availability group name does not match the requested group",
                ));
            }
            if group.cluster_type != 2 || !group.cluster_type_desc.eq_ignore_ascii_case("EXTERNAL")
            {
                return Err(unsupported(
                    "anchor",
                    "only EXTERNAL availability groups are supported",
                ));
            }
            if group.basic_features || group.is_distributed {
                return Err(unsupported(
                    "anchor",
                    "basic and distributed availability groups are unsupported",
                ));
            }
            if group.required_secondaries != u32::from(SUPPORTED_REQUIRED_SECONDARIES) {
                return Err(unsupported(
                    "anchor",
                    "exactly one synchronized secondary must be required for commit",
                ));
            }
            if group
                .local_replica_server_name
                .as_ref()
                .is_some_and(|name| name != &instance.server_name)
            {
                return Err(inconsistent(
                    "anchor",
                    "local replica configuration refers to another server",
                ));
            }
            if target
                .replica
                .native_replica_id()
                .is_some_and(|expected| group.local_replica_id.as_ref() != Some(expected))
            {
                return Err(inconsistent(
                    "anchor",
                    "native replica identity does not match the configured target",
                ));
            }
        }
        Ok(())
    }
}

fn check_permissions(rows: &[QueryRow]) -> Result<(), RuntimeError> {
    let row = single_row(rows, ReadQuery::Permissions)?;
    if row.unsigned::<u16>("product_major_version")? != SUPPORTED_ENGINE_MAJOR {
        return Err(unsupported(
            row.stage,
            "only SQL Server 2022 major version 16 is supported",
        ));
    }
    for permission in [
        "view_server_state",
        "view_server_performance_state",
        "view_any_definition",
        "view_any_database",
    ] {
        if row.optional_bool(permission)? != Some(true) {
            return Err(RuntimeError::new(
                ObservationFailureKind::PermissionDenied,
                row.stage,
                "required server-state, performance-state, definition, or database visibility permission is missing",
            ));
        }
    }
    Ok(())
}

const GROUP_COLUMNS: &[&str] = &[
    "group_name",
    "cluster_type",
    "cluster_type_desc",
    "sequence_number",
    "required_synchronized_secondaries_to_commit",
    "basic_features",
    "is_distributed",
    "local_replica_id",
    "local_replica_server_name",
    "local_state_group_id",
    "local_state_replica_id",
    "local_role_desc",
];

fn parse_anchor(rows: &[QueryRow]) -> Result<Anchor, RuntimeError> {
    let row = single_row(rows, ReadQuery::Anchor)?;
    let instance = InstanceMetadata {
        server_name: row.server_name("server_name")?,
        property_server_name: row.server_name("property_server_name")?,
        product_version: row.text("product_version", 128)?,
        product_major_version: row.unsigned("product_major_version")?,
        edition: row.text("edition", 128)?,
        engine_edition: row.unsigned("engine_edition")?,
        hadr_enabled: row.boolean("is_hadr_enabled")?,
        host_platform: row.text("host_platform", 256)?,
        host_distribution: row.optional_text("host_distribution", 256)?,
        architecture: row.text("architecture", 16)?,
        sqlserver_start_time: row
            .timestamp("sqlserver_start_time")?
            .ok_or_else(|| malformed(row.stage, "SQL Server start time is NULL"))?,
    };
    let version: Vec<&str> = instance.product_version.split('.').collect();
    if version.len() != 4
        || version
            .iter()
            .any(|part| !is_decimal(part) || part.parse::<u32>().is_err())
        || version[0].parse::<u16>().ok() != Some(instance.product_major_version)
    {
        return Err(malformed(
            row.stage,
            "SQL Server version properties are invalid or disagree",
        ));
    }
    let group = match row.optional_guid("group_id")? {
        None => {
            row.require_null(GROUP_COLUMNS)?;
            None
        }
        Some(group_id) => {
            let local_replica_id = row.optional_guid("local_replica_id")?;
            let local_replica_server_name =
                row.optional_server_name("local_replica_server_name")?;
            if local_replica_id.is_some() != local_replica_server_name.is_some() {
                return Err(malformed(
                    row.stage,
                    "partial local replica configuration identity",
                ));
            }
            let local_state_group_id = row.optional_guid("local_state_group_id")?;
            let local_state_replica_id = row.optional_guid("local_state_replica_id")?;
            let local_role = row.role("local_role_desc")?;
            match (&local_state_group_id, &local_state_replica_id) {
                (None, None) if local_role.is_none() => {}
                (Some(state_group), Some(state_replica)) => {
                    if state_group != &group_id || local_replica_id.as_ref() != Some(state_replica)
                    {
                        return Err(inconsistent(
                            row.stage,
                            "local replica state and native configuration identities disagree",
                        ));
                    }
                }
                _ => return Err(malformed(row.stage, "partial local replica state identity")),
            }
            Some(GroupAnchor {
                identity: AvailabilityGroupIdentity {
                    name: AvailabilityGroupName::new(row.required("group_name")?)
                        .map_err(|_| malformed(row.stage, "invalid availability group name"))?,
                    group_id,
                },
                configuration_sequence: row.bigint("sequence_number")?,
                cluster_type: row.unsigned("cluster_type")?,
                cluster_type_desc: row.text("cluster_type_desc", 60)?,
                required_secondaries: row
                    .nonnegative_int("required_synchronized_secondaries_to_commit")?,
                basic_features: row.boolean("basic_features")?,
                is_distributed: row.boolean("is_distributed")?,
                local_replica_id,
                local_replica_server_name,
                local_state_replica_id,
                local_role,
            })
        }
    };
    Ok(Anchor { instance, group })
}

const REPLICA_STATE_COLUMNS: &[&str] = &[
    "is_local",
    "role_desc",
    "operational_state_desc",
    "connected_state_desc",
    "recovery_health_desc",
    "synchronization_health_desc",
    "last_connect_error_number",
];

fn parse_replicas(
    rows: &[QueryRow],
    anchor: &Anchor,
    group: &GroupAnchor,
) -> Result<BTreeMap<Guid, ReplicaSnapshot>, RuntimeError> {
    let mut replicas = BTreeMap::new();
    let mut servers = BTreeSet::new();
    let mut local_config_id = None;
    let mut local_state_id = None;
    for result in rows {
        let row = Row::new(result, ReadQuery::Replicas);
        row.check_group(group)?;
        let replica_id = row.guid("replica_id")?;
        let server_name = row.server_name("replica_server_name")?;
        if !servers.insert(server_name.clone()) || replicas.contains_key(&replica_id) {
            return Err(malformed(
                row.stage,
                "duplicate replica GUID or server name",
            ));
        }
        let availability_mode = row.text("availability_mode_desc", 60)?;
        let failover_mode = row.text("failover_mode_desc", 60)?;
        let seeding_mode = row.text("seeding_mode_desc", 60)?;
        if row.unsigned::<u8>("availability_mode")? != 1
            || availability_mode != "SYNCHRONOUS_COMMIT"
        {
            return Err(unsupported(
                row.stage,
                "only synchronous-commit data replicas are supported",
            ));
        }
        if row.unsigned::<u8>("failover_mode")? != 2 || failover_mode != "EXTERNAL" {
            return Err(unsupported(
                row.stage,
                "only EXTERNAL replica failover mode is supported",
            ));
        }
        if row.unsigned::<u8>("seeding_mode")? != 0 || seeding_mode != "AUTOMATIC" {
            return Err(unsupported(
                row.stage,
                "only automatic seeding configuration is supported",
            ));
        }
        if server_name == anchor.instance.server_name {
            local_config_id = Some(replica_id.clone());
        }
        let state = match (
            row.optional_guid("state_group_id")?,
            row.optional_guid("state_replica_id")?,
        ) {
            (None, None) => {
                row.require_null(REPLICA_STATE_COLUMNS)?;
                None
            }
            (Some(state_group), Some(state_replica)) => {
                if state_group != group.identity.group_id || state_replica != replica_id {
                    return Err(inconsistent(
                        row.stage,
                        "replica catalog and state GUIDs disagree",
                    ));
                }
                let provenance = provenance(&row, group, &replica_id)?;
                let role = row.role("role_desc")?;
                if provenance == NativeProvenance::PrimaryRemote
                    && role == Some(NativeRole::Primary)
                {
                    return Err(inconsistent(
                        row.stage,
                        "remote primary role contradicts the observed local primary",
                    ));
                }
                if provenance == NativeProvenance::Local {
                    if local_state_id.is_some() || role != group.local_role {
                        return Err(inconsistent(
                            row.stage,
                            "local replica role or identity changed during observation",
                        ));
                    }
                    local_state_id = Some(replica_id.clone());
                }
                Some(ReplicaState {
                    provenance,
                    role,
                    operational_state: row.optional_text("operational_state_desc", 60)?,
                    connected_state: row.optional_text("connected_state_desc", 60)?,
                    recovery_health: row.optional_text("recovery_health_desc", 60)?,
                    synchronization_health: row.optional_text("synchronization_health_desc", 60)?,
                    last_connect_error_number: row.optional_int("last_connect_error_number")?,
                })
            }
            _ => return Err(malformed(row.stage, "partial replica state identity")),
        };
        replicas.insert(
            replica_id.clone(),
            ReplicaSnapshot {
                replica_id,
                server_name,
                endpoint_url: row.optional_text("endpoint_url", 256)?,
                availability_mode,
                failover_mode,
                seeding_mode,
                state,
            },
        );
    }
    if local_config_id != group.local_replica_id || local_state_id != group.local_state_replica_id {
        return Err(inconsistent(
            "replicas",
            "local replica configuration or state changed during observation",
        ));
    }
    if replicas.len() > usize::from(SUPPORTED_REPLICA_COUNT) {
        return Err(unsupported(
            "replicas",
            "more than three configured replicas are unsupported",
        ));
    }
    Ok(replicas)
}

fn provenance(
    row: &Row<'_>,
    group: &GroupAnchor,
    replica_id: &Guid,
) -> Result<NativeProvenance, RuntimeError> {
    if row.boolean("is_local")? {
        if group.local_state_replica_id.as_ref() != Some(replica_id) {
            return Err(inconsistent(
                row.stage,
                "local state refers to a different native replica",
            ));
        }
        Ok(NativeProvenance::Local)
    } else {
        if group.local_role != Some(NativeRole::Primary)
            || group.local_replica_id.as_ref() == Some(replica_id)
        {
            return Err(inconsistent(
                row.stage,
                "remote state is not attributable to the observed primary",
            ));
        }
        Ok(NativeProvenance::PrimaryRemote)
    }
}

const RECOVERY_COLUMNS: &[&str] = &[
    "database_guid",
    "family_guid",
    "recovery_fork_guid",
    "first_recovery_fork_guid",
    "fork_point_lsn",
];
const LOCAL_DATABASE_COLUMNS: &[&str] = &[
    "local_database_name",
    "local_group_database_id",
    "local_replica_id",
    "local_database_state_desc",
    "recovery_model_desc",
    "recovery_status_database_id",
    "database_guid",
    "family_guid",
    "recovery_fork_guid",
    "first_recovery_fork_guid",
    "fork_point_lsn",
];

fn parse_local_database(
    row: &Row<'_>,
    group: &GroupAnchor,
    database: &DatabaseIdentity,
) -> Result<Option<LocalDatabaseSnapshot>, RuntimeError> {
    let Some(database_id) = row.optional_database_id("local_database_id")? else {
        row.require_null(LOCAL_DATABASE_COLUMNS)?;
        return Ok(None);
    };
    let replica_id = row.guid("local_replica_id")?;
    if row.guid("local_group_database_id")? != database.group_database_id
        || group.local_replica_id.as_ref() != Some(&replica_id)
        || row.identifier("local_database_name")? != database.name
    {
        return Err(inconsistent(
            row.stage,
            "local database native identity disagrees with the availability group",
        ));
    }
    let recovery = match row.optional_database_id("recovery_status_database_id")? {
        None => {
            row.require_null(RECOVERY_COLUMNS)?;
            None
        }
        Some(recovery_database_id) => {
            if recovery_database_id != database_id {
                return Err(inconsistent(
                    row.stage,
                    "recovery metadata refers to a different local database",
                ));
            }
            Some(LocalRecoveryMetadata {
                database_guid: row.optional_guid("database_guid")?,
                family_guid: row.optional_guid("family_guid")?,
                recovery_fork_guid: row.optional_guid("recovery_fork_guid")?,
                first_recovery_fork_guid: row.optional_guid("first_recovery_fork_guid")?,
                fork_point_lsn: row.optional_progress("fork_point_lsn")?,
            })
        }
    };
    Ok(Some(LocalDatabaseSnapshot {
        database_id,
        replica_id,
        state: row.optional_text("local_database_state_desc", 60)?,
        recovery_model: row.optional_text("recovery_model_desc", 60)?,
        recovery,
    }))
}

fn parse_databases(
    rows: &[QueryRow],
    group: &GroupAnchor,
) -> Result<BTreeMap<Guid, DatabaseSnapshot>, RuntimeError> {
    let mut databases = BTreeMap::new();
    let mut names = BTreeSet::new();
    for result in rows {
        let row = Row::new(result, ReadQuery::Databases);
        row.check_group(group)?;
        let identity = DatabaseIdentity {
            name: row.identifier("database_name")?,
            group_database_id: row.guid("group_database_id")?,
        };
        if !names.insert(identity.name.clone())
            || databases.contains_key(&identity.group_database_id)
        {
            return Err(malformed(row.stage, "duplicate database GUID or name"));
        }
        let local = parse_local_database(&row, group, &identity)?;
        databases.insert(
            identity.group_database_id.clone(),
            DatabaseSnapshot {
                identity,
                local,
                replicas: Vec::new(),
            },
        );
    }
    Ok(databases)
}

fn local_identity_matches(
    left: &Option<LocalDatabaseSnapshot>,
    right: &Option<LocalDatabaseSnapshot>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            left.database_id == right.database_id
                && left.replica_id == right.replica_id
                && left.recovery == right.recovery
        }
        _ => false,
    }
}

fn database_anchors_match(
    before: &BTreeMap<Guid, DatabaseSnapshot>,
    after: &BTreeMap<Guid, DatabaseSnapshot>,
) -> bool {
    before.len() == after.len()
        && before.iter().all(|(id, database)| {
            after.get(id).is_some_and(|other| {
                database.identity == other.identity
                    && local_identity_matches(&database.local, &other.local)
            })
        })
}

fn add_database_states(
    rows: &[QueryRow],
    group: &GroupAnchor,
    replicas: &BTreeMap<Guid, ReplicaSnapshot>,
    databases: &mut BTreeMap<Guid, DatabaseSnapshot>,
) -> Result<(), RuntimeError> {
    let mut identities = BTreeSet::new();
    for result in rows {
        let row = Row::new(result, ReadQuery::DatabaseStates);
        row.check_group(group)?;
        let group_database_id = row.guid("group_database_id")?;
        let replica_id = row.guid("replica_id")?;
        if !identities.insert((group_database_id.clone(), replica_id.clone())) {
            return Err(malformed(row.stage, "duplicate database replica state"));
        }
        let replica = replicas.get(&replica_id).ok_or_else(|| {
            inconsistent(
                row.stage,
                "database state refers to an unknown replica GUID",
            )
        })?;
        let database = databases.get_mut(&group_database_id).ok_or_else(|| {
            inconsistent(
                row.stage,
                "database state refers to an unknown database GUID",
            )
        })?;
        if row.identifier("database_name")? != database.identity.name {
            return Err(inconsistent(
                row.stage,
                "database name changed during observation",
            ));
        }
        let provenance = provenance(&row, group, &replica_id)?;
        if replica
            .state
            .as_ref()
            .is_none_or(|state| state.provenance != provenance)
        {
            return Err(inconsistent(
                row.stage,
                "database state and replica state provenance disagree",
            ));
        }
        let database_id = row.database_id("database_id")?;
        let local = parse_local_database(&row, group, &database.identity)?;
        let lineage = match provenance {
            NativeProvenance::Local => {
                if !local_identity_matches(&database.local, &local)
                    || local
                        .as_ref()
                        .is_some_and(|local| local.database_id != database_id)
                {
                    return Err(inconsistent(
                        row.stage,
                        "local database identity or lineage changed during observation",
                    ));
                }
                match local
                    .as_ref()
                    .and_then(|local| local.recovery.as_ref())
                    .and_then(|recovery| recovery.recovery_fork_guid.as_ref())
                {
                    Some(fork) => RecoveryLineageObservation::Local {
                        value: DatabaseLineage {
                            database: database.identity.clone(),
                            recovery_fork_id: fork.clone(),
                        },
                    },
                    None => RecoveryLineageObservation::LocalUnavailable,
                }
            }
            NativeProvenance::PrimaryRemote => {
                if local.is_some() {
                    return Err(malformed(
                        row.stage,
                        "remote progress must not contain local recovery metadata",
                    ));
                }
                RecoveryLineageObservation::RemoteUnavailable
            }
        };
        let is_primary_replica = row.optional_bool("is_primary_replica")?;
        if (provenance == NativeProvenance::PrimaryRemote && is_primary_replica == Some(true))
            || (provenance == NativeProvenance::Local
                && matches!(
                    (&group.local_role, is_primary_replica),
                    (Some(NativeRole::Primary), Some(false))
                        | (Some(NativeRole::Secondary), Some(true))
                ))
        {
            return Err(inconsistent(
                row.stage,
                "database primary flag disagrees with the observed replica role",
            ));
        }
        database.replicas.push(DatabaseReplicaSnapshot {
            group_database_id,
            replica_id,
            database_id,
            provenance,
            lineage,
            is_primary_replica,
            synchronization_state: row.optional_text("synchronization_state_desc", 60)?,
            synchronization_health: row.optional_text("synchronization_health_desc", 60)?,
            database_state: row.optional_text("database_state_desc", 60)?,
            is_suspended: row.optional_bool("is_suspended")?,
            suspend_reason: row.optional_text("suspend_reason_desc", 60)?,
            is_commit_participant: row.optional_bool("is_commit_participant")?,
            progress: NativeProgress {
                hardened_block: row.optional_progress("last_hardened_lsn")?,
                redone_record: row.optional_progress("last_redone_lsn")?,
                committed_record: row.optional_progress("last_commit_lsn")?,
            },
        });
    }
    for database in databases.values_mut() {
        database
            .replicas
            .sort_by(|left, right| left.replica_id.cmp(&right.replica_id));
    }
    Ok(())
}

fn parse_automatic_seeding(
    rows: &[QueryRow],
    group: &GroupAnchor,
    replicas: &BTreeMap<Guid, ReplicaSnapshot>,
    databases: &BTreeMap<Guid, DatabaseSnapshot>,
) -> Result<Vec<AutomaticSeedingSnapshot>, RuntimeError> {
    let mut seeds = BTreeMap::new();
    for result in rows {
        let row = Row::new(result, ReadQuery::AutomaticSeeding);
        row.check_group(group)?;
        let group_database_id = row.guid("group_database_id")?;
        let remote_replica_id = row.guid("remote_replica_id")?;
        if !databases.contains_key(&group_database_id)
            || !replicas.contains_key(&remote_replica_id)
            || group.local_replica_id.as_ref() == Some(&remote_replica_id)
        {
            return Err(inconsistent(
                row.stage,
                "automatic seeding refers to an unrelated database or replica GUID",
            ));
        }
        let operation_id = row.guid("operation_id")?;
        if seeds.contains_key(&operation_id) {
            return Err(malformed(
                row.stage,
                "duplicate automatic seeding operation",
            ));
        }
        seeds.insert(
            operation_id.clone(),
            AutomaticSeedingSnapshot {
                group_database_id,
                remote_replica_id,
                operation_id,
                is_source: row.boolean("is_source")?,
                current_state: row.optional_text("current_state", 60)?,
                performed_seeding: row.optional_bool("performed_seeding")?,
                failure_state: row.optional_int("failure_state")?,
                error_code: row.optional_int("error_code")?,
                number_of_attempts: row.optional_nonnegative_int("number_of_attempts")?,
                start_time: row.timestamp("start_time")?,
                completion_time: row.timestamp("completion_time")?,
            },
        );
    }
    Ok(seeds.into_values().collect())
}

fn parse_physical_seeding(
    rows: &[QueryRow],
    group: &GroupAnchor,
    databases: &BTreeMap<Guid, DatabaseSnapshot>,
) -> Result<Vec<PhysicalSeedingSnapshot>, RuntimeError> {
    let mut seeds = BTreeMap::new();
    for result in rows {
        let row = Row::new(result, ReadQuery::PhysicalSeeding);
        row.check_group(group)?;
        let group_database_id = row.guid("group_database_id")?;
        let local_replica_id = row.guid("local_replica_id")?;
        let local_database_id = row.database_id("local_database_id")?;
        let local_database_name = row.identifier("local_database_name")?;
        let database = databases.get(&group_database_id).ok_or_else(|| {
            inconsistent(
                row.stage,
                "physical seeding refers to an unknown database GUID",
            )
        })?;
        if group.local_replica_id.as_ref() != Some(&local_replica_id)
            || database.identity.name != local_database_name
            || database.local.as_ref().is_none_or(|local| {
                local.database_id != local_database_id || local.replica_id != local_replica_id
            })
        {
            return Err(inconsistent(
                row.stage,
                "physical seeding is not associated with the observed local database",
            ));
        }
        let local_physical_seeding_id = row.guid("local_physical_seeding_id")?;
        if seeds.contains_key(&local_physical_seeding_id) {
            return Err(malformed(
                row.stage,
                "duplicate local physical seeding operation",
            ));
        }
        seeds.insert(
            local_physical_seeding_id.clone(),
            PhysicalSeedingSnapshot {
                group_database_id,
                local_replica_id,
                local_physical_seeding_id,
                remote_physical_seeding_id: row.optional_guid("remote_physical_seeding_id")?,
                local_database_id,
                local_database_name,
                remote_machine_name: row.optional_text("remote_machine_name", 256)?,
                role: row.optional_text("role_desc", 60)?,
                internal_state: row.optional_text("internal_state_desc", 256)?,
                transfer_rate_bytes_per_second: row
                    .optional_bigint("transfer_rate_bytes_per_second")?,
                transferred_size_bytes: row.optional_bigint("transferred_size_bytes")?,
                database_size_bytes: row.optional_bigint("database_size_bytes")?,
                failure_code: row.optional_int("failure_code")?,
                is_compression_enabled: row.optional_bool("is_compression_enabled")?,
                start_time_utc: row.timestamp("start_time_utc")?,
                end_time_utc: row.timestamp("end_time_utc")?,
                estimate_time_complete_utc: row.timestamp("estimate_time_complete_utc")?,
            },
        );
    }
    Ok(seeds.into_values().collect())
}

struct Row<'a> {
    values: &'a QueryRow,
    stage: &'static str,
}

impl<'a> Row<'a> {
    fn new(values: &'a QueryRow, query: ReadQuery) -> Self {
        Self {
            values,
            stage: query.label(),
        }
    }

    fn optional(&self, column: &str) -> Result<Option<&'a str>, RuntimeError> {
        self.values
            .get(column)
            .map(|value| value.as_deref())
            .ok_or_else(|| malformed(self.stage, "query result omitted an expected column"))
    }

    fn required(&self, column: &str) -> Result<&'a str, RuntimeError> {
        self.optional(column)?.ok_or_else(|| {
            malformed(
                self.stage,
                "query result contains NULL in a required column",
            )
        })
    }

    fn require_null(&self, columns: &[&str]) -> Result<(), RuntimeError> {
        for column in columns {
            if self.optional(column)?.is_some() {
                return Err(malformed(self.stage, "unassociated metadata must be NULL"));
            }
        }
        Ok(())
    }

    fn optional_text(
        &self,
        column: &str,
        max_utf16: usize,
    ) -> Result<Option<String>, RuntimeError> {
        self.optional(column)?
            .map(|value| {
                if value.is_empty()
                    || value.encode_utf16().count() > max_utf16
                    || value.chars().any(char::is_control)
                {
                    Err(malformed(self.stage, "invalid native metadata text"))
                } else {
                    Ok(value.to_owned())
                }
            })
            .transpose()
    }

    fn text(&self, column: &str, max_utf16: usize) -> Result<String, RuntimeError> {
        self.optional_text(column, max_utf16)?.ok_or_else(|| {
            malformed(
                self.stage,
                "query result contains NULL in required native metadata",
            )
        })
    }

    fn unsigned<T: std::str::FromStr>(&self, column: &str) -> Result<T, RuntimeError> {
        let value = self.required(column)?;
        if !is_decimal(value) {
            return Err(malformed(self.stage, "invalid native unsigned integer"));
        }
        value
            .parse()
            .map_err(|_| malformed(self.stage, "native unsigned integer is out of range"))
    }

    fn optional_int(&self, column: &str) -> Result<Option<i32>, RuntimeError> {
        self.optional(column)?
            .map(|value| {
                if !is_decimal(value.strip_prefix('-').unwrap_or(value)) {
                    return Err(malformed(self.stage, "invalid native integer"));
                }
                value
                    .parse()
                    .map_err(|_| malformed(self.stage, "native integer is out of range"))
            })
            .transpose()
    }

    fn optional_nonnegative_int(&self, column: &str) -> Result<Option<u32>, RuntimeError> {
        self.optional_int(column)?
            .map(|value| {
                u32::try_from(value).map_err(|_| malformed(self.stage, "native count is negative"))
            })
            .transpose()
    }

    fn nonnegative_int(&self, column: &str) -> Result<u32, RuntimeError> {
        self.optional_nonnegative_int(column)?
            .ok_or_else(|| malformed(self.stage, "required native count is NULL"))
    }

    fn optional_bool(&self, column: &str) -> Result<Option<bool>, RuntimeError> {
        match self.optional(column)? {
            None => Ok(None),
            Some("0") => Ok(Some(false)),
            Some("1") => Ok(Some(true)),
            _ => Err(malformed(self.stage, "invalid native Boolean")),
        }
    }

    fn boolean(&self, column: &str) -> Result<bool, RuntimeError> {
        self.optional_bool(column)?
            .ok_or_else(|| malformed(self.stage, "required native Boolean is NULL"))
    }

    fn optional_guid(&self, column: &str) -> Result<Option<Guid>, RuntimeError> {
        self.optional(column)?
            .map(|value| {
                Guid::parse("native GUID", value)
                    .map_err(|_| malformed(self.stage, "invalid or nil native GUID"))
            })
            .transpose()
    }

    fn guid(&self, column: &str) -> Result<Guid, RuntimeError> {
        self.optional_guid(column)?
            .ok_or_else(|| malformed(self.stage, "required native GUID is NULL"))
    }

    fn identifier(&self, column: &str) -> Result<SqlIdentifier, RuntimeError> {
        SqlIdentifier::new(self.required(column)?)
            .map_err(|_| malformed(self.stage, "invalid native database name"))
    }

    fn optional_server_name(&self, column: &str) -> Result<Option<ServerName>, RuntimeError> {
        self.optional(column)?
            .map(|value| {
                ServerName::new(value)
                    .map_err(|_| malformed(self.stage, "invalid native server name"))
            })
            .transpose()
    }

    fn server_name(&self, column: &str) -> Result<ServerName, RuntimeError> {
        self.optional_server_name(column)?
            .ok_or_else(|| malformed(self.stage, "required native server name is NULL"))
    }

    fn role(&self, column: &str) -> Result<Option<NativeRole>, RuntimeError> {
        self.optional_text(column, 60)?
            .map(|value| {
                NativeRole::parse(&value)
                    .map_err(|_| malformed(self.stage, "invalid native replica role"))
            })
            .transpose()
    }

    fn optional_progress(&self, column: &str) -> Result<Option<DecimalProgress>, RuntimeError> {
        self.optional(column)?
            .map(|value| {
                DecimalProgress::parse(value)
                    .map_err(|_| malformed(self.stage, "invalid native decimal progress"))
            })
            .transpose()
    }

    fn optional_bigint(&self, column: &str) -> Result<Option<DecimalProgress>, RuntimeError> {
        let value = self.optional_progress(column)?;
        if value.is_some_and(|value| value.value() > i64::MAX as u128) {
            return Err(malformed(
                self.stage,
                "native nonnegative bigint is out of range",
            ));
        }
        Ok(value)
    }

    fn bigint(&self, column: &str) -> Result<DecimalProgress, RuntimeError> {
        self.optional_bigint(column)?
            .ok_or_else(|| malformed(self.stage, "required native bigint is NULL"))
    }

    fn optional_database_id(&self, column: &str) -> Result<Option<u32>, RuntimeError> {
        let value = self.optional_nonnegative_int(column)?;
        if value.is_some_and(|id| id <= 4) {
            return Err(malformed(
                self.stage,
                "managed database identity is not a user database ID",
            ));
        }
        Ok(value)
    }

    fn database_id(&self, column: &str) -> Result<u32, RuntimeError> {
        self.optional_database_id(column)?
            .ok_or_else(|| malformed(self.stage, "required native database ID is NULL"))
    }

    fn timestamp(&self, column: &str) -> Result<Option<String>, RuntimeError> {
        self.optional_text(column, 33)?
            .map(|value| {
                if is_iso_timestamp(&value) {
                    Ok(value)
                } else {
                    Err(malformed(self.stage, "invalid native ISO timestamp"))
                }
            })
            .transpose()
    }

    fn check_group(&self, group: &GroupAnchor) -> Result<(), RuntimeError> {
        if self.guid("group_id")? != group.identity.group_id {
            Err(inconsistent(
                self.stage,
                "query result refers to a different availability group GUID",
            ))
        } else {
            Ok(())
        }
    }
}

fn single_row(rows: &[QueryRow], query: ReadQuery) -> Result<Row<'_>, RuntimeError> {
    match rows {
        [row] => Ok(Row::new(row, query)),
        _ => Err(malformed(
            query.label(),
            "expected exactly one metadata row",
        )),
    }
}

fn is_decimal(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_iso_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    if !(bytes.len() == 19 || (21..=27).contains(&bytes.len()))
        || !bytes.iter().enumerate().all(|(index, byte)| match index {
            4 | 7 => *byte == b'-',
            10 => *byte == b'T',
            13 | 16 => *byte == b':',
            19 => *byte == b'.',
            _ => byte.is_ascii_digit(),
        })
    {
        return false;
    }
    let year = value[..4].parse::<u32>().unwrap_or(0);
    let month = value[5..7].parse::<u32>().unwrap_or(0);
    let day = value[8..10].parse::<u32>().unwrap_or(0);
    let leap_year = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap_year => 29,
        2 => 28,
        _ => 0,
    };
    year != 0
        && (1..=days).contains(&day)
        && value[11..13].parse::<u8>().is_ok_and(|hour| hour < 24)
        && value[14..16].parse::<u8>().is_ok_and(|minute| minute < 60)
        && value[17..19].parse::<u8>().is_ok_and(|second| second < 60)
}

fn malformed(stage: &'static str, message: &'static str) -> RuntimeError {
    RuntimeError::new(ObservationFailureKind::Malformed, stage, message)
}

fn unsupported(stage: &'static str, message: &'static str) -> RuntimeError {
    RuntimeError::new(ObservationFailureKind::Unsupported, stage, message)
}

fn inconsistent(stage: &'static str, message: &'static str) -> RuntimeError {
    RuntimeError::new(ObservationFailureKind::Inconsistent, stage, message)
}
