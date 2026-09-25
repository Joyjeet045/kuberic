//! The complete read-only SQL surface of the observer.
//!
//! `@P1` is always a bound availability-group name, never an identifier or a
//! piece of SQL. Every projected value is `nvarchar` so neither TDS nor JSON
//! can silently narrow native `numeric(25,0)` progress.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadQuery {
    Permissions,
    Anchor,
    Replicas,
    Databases,
    DatabaseStates,
    AutomaticSeeding,
    PhysicalSeeding,
}

impl ReadQuery {
    pub const ALL: [Self; 7] = [
        Self::Permissions,
        Self::Anchor,
        Self::Replicas,
        Self::Databases,
        Self::DatabaseStates,
        Self::AutomaticSeeding,
        Self::PhysicalSeeding,
    ];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Permissions => "permissions",
            Self::Anchor => "anchor",
            Self::Replicas => "replicas",
            Self::Databases => "databases",
            Self::DatabaseStates => "database_states",
            Self::AutomaticSeeding => "automatic_seeding",
            Self::PhysicalSeeding => "physical_seeding",
        }
    }

    /// Projected aliases in wire order, including when a result set has no
    /// rows. The transport can validate metadata before accepting absence.
    pub const fn columns(self) -> &'static [&'static str] {
        match self {
            Self::Permissions => &[
                "product_major_version",
                "view_server_state",
                "view_server_performance_state",
                "view_any_definition",
                "view_any_database",
            ],
            Self::Anchor => &[
                "server_name",
                "property_server_name",
                "product_version",
                "product_major_version",
                "edition",
                "engine_edition",
                "is_hadr_enabled",
                "host_platform",
                "host_distribution",
                "architecture",
                "sqlserver_start_time",
                "group_id",
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
            ],
            Self::Replicas => &[
                "group_id",
                "replica_id",
                "replica_server_name",
                "endpoint_url",
                "availability_mode",
                "availability_mode_desc",
                "failover_mode",
                "failover_mode_desc",
                "seeding_mode",
                "seeding_mode_desc",
                "state_group_id",
                "state_replica_id",
                "is_local",
                "role_desc",
                "operational_state_desc",
                "connected_state_desc",
                "recovery_health_desc",
                "synchronization_health_desc",
                "last_connect_error_number",
            ],
            Self::Databases => &[
                "group_id",
                "group_database_id",
                "database_name",
                "local_database_id",
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
            ],
            Self::DatabaseStates => &[
                "group_id",
                "group_database_id",
                "database_name",
                "replica_id",
                "database_id",
                "is_local",
                "is_primary_replica",
                "synchronization_state_desc",
                "synchronization_health_desc",
                "database_state_desc",
                "is_suspended",
                "suspend_reason_desc",
                "is_commit_participant",
                "last_hardened_lsn",
                "last_redone_lsn",
                "last_commit_lsn",
                "local_database_id",
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
            ],
            Self::AutomaticSeeding => &[
                "group_id",
                "group_database_id",
                "remote_replica_id",
                "operation_id",
                "is_source",
                "current_state",
                "performed_seeding",
                "failure_state",
                "error_code",
                "number_of_attempts",
                "start_time",
                "completion_time",
            ],
            Self::PhysicalSeeding => &[
                "group_id",
                "group_database_id",
                "local_replica_id",
                "local_physical_seeding_id",
                "remote_physical_seeding_id",
                "local_database_id",
                "local_database_name",
                "remote_machine_name",
                "role_desc",
                "internal_state_desc",
                "transfer_rate_bytes_per_second",
                "transferred_size_bytes",
                "database_size_bytes",
                "failure_code",
                "is_compression_enabled",
                "start_time_utc",
                "end_time_utc",
                "estimate_time_complete_utc",
            ],
        }
    }

    pub const fn sql(self) -> &'static str {
        match self {
            // Server-level visibility must be established before an empty
            // catalog can mean absence. VIEW ANY DATABASE also covers
            // sys.availability_databases_cluster.
            Self::Permissions => {
                r#"SELECT
    CONVERT(nvarchar(10), SERVERPROPERTY(N'ProductMajorVersion')) AS product_major_version,
    CONVERT(nvarchar(1), HAS_PERMS_BY_NAME(NULL, NULL, N'VIEW SERVER STATE')) AS view_server_state,
    CONVERT(nvarchar(1), HAS_PERMS_BY_NAME(NULL, NULL, N'VIEW SERVER PERFORMANCE STATE')) AS view_server_performance_state,
    CONVERT(nvarchar(1), HAS_PERMS_BY_NAME(NULL, NULL, N'VIEW ANY DEFINITION')) AS view_any_definition,
    CONVERT(nvarchar(1), HAS_PERMS_BY_NAME(NULL, NULL, N'VIEW ANY DATABASE')) AS view_any_database
WHERE @P1 IS NOT NULL;"#
            }
            // The start time detects an engine restart even if all names are
            // unchanged. @@VERSION's explicit (X64) marker describes the SQL
            // binary, not the architecture of a Kubernetes node.
            Self::Anchor => {
                r#"SELECT
    CONVERT(nvarchar(128), @@SERVERNAME) AS server_name,
    CONVERT(nvarchar(128), SERVERPROPERTY(N'ServerName')) AS property_server_name,
    CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')) AS product_version,
    CONVERT(nvarchar(10), SERVERPROPERTY(N'ProductMajorVersion')) AS product_major_version,
    CONVERT(nvarchar(128), SERVERPROPERTY(N'Edition')) AS edition,
    CONVERT(nvarchar(10), SERVERPROPERTY(N'EngineEdition')) AS engine_edition,
    CONVERT(nvarchar(1), SERVERPROPERTY(N'IsHadrEnabled')) AS is_hadr_enabled,
    CONVERT(nvarchar(256), host.host_platform) AS host_platform,
    CONVERT(nvarchar(256), host.host_distribution) AS host_distribution,
    CONVERT(nvarchar(16), CASE
        WHEN CHARINDEX(N'(X64)', CONVERT(nvarchar(4000), @@VERSION)) > 0
        THEN N'x86_64' ELSE N'unknown' END) AS architecture,
    CONVERT(nvarchar(33), info.sqlserver_start_time, 126) AS sqlserver_start_time,
    CONVERT(nvarchar(36), ag.group_id) AS group_id,
    CONVERT(nvarchar(128), ag.name) AS group_name,
    CONVERT(nvarchar(3), ag.cluster_type) AS cluster_type,
    CONVERT(nvarchar(60), ag.cluster_type_desc) AS cluster_type_desc,
    CONVERT(nvarchar(20), ag.sequence_number) AS sequence_number,
    CONVERT(nvarchar(11), ag.required_synchronized_secondaries_to_commit) AS required_synchronized_secondaries_to_commit,
    CONVERT(nvarchar(1), ag.basic_features) AS basic_features,
    CONVERT(nvarchar(1), ag.is_distributed) AS is_distributed,
    CONVERT(nvarchar(36), local_config.replica_id) AS local_replica_id,
    CONVERT(nvarchar(256), local_config.replica_server_name) AS local_replica_server_name,
    CONVERT(nvarchar(36), local_state.group_id) AS local_state_group_id,
    CONVERT(nvarchar(36), local_state.replica_id) AS local_state_replica_id,
    CONVERT(nvarchar(60), local_state.role_desc) AS local_role_desc
FROM sys.dm_os_host_info AS host
CROSS JOIN sys.dm_os_sys_info AS info
LEFT JOIN sys.availability_groups AS ag ON ag.name = @P1
LEFT JOIN sys.availability_replicas AS local_config
    ON local_config.group_id = ag.group_id
    AND UPPER(local_config.replica_server_name) = UPPER(CONVERT(nvarchar(128), @@SERVERNAME))
LEFT JOIN sys.dm_hadr_availability_replica_states AS local_state
    ON local_state.group_id = ag.group_id AND local_state.is_local = 1;"#
            }
            Self::Replicas => {
                r#"SELECT
    CONVERT(nvarchar(36), ar.group_id) AS group_id,
    CONVERT(nvarchar(36), ar.replica_id) AS replica_id,
    CONVERT(nvarchar(256), ar.replica_server_name) AS replica_server_name,
    CONVERT(nvarchar(256), ar.endpoint_url) AS endpoint_url,
    CONVERT(nvarchar(3), ar.availability_mode) AS availability_mode,
    CONVERT(nvarchar(60), ar.availability_mode_desc) AS availability_mode_desc,
    CONVERT(nvarchar(3), ar.failover_mode) AS failover_mode,
    CONVERT(nvarchar(60), ar.failover_mode_desc) AS failover_mode_desc,
    CONVERT(nvarchar(3), ar.seeding_mode) AS seeding_mode,
    CONVERT(nvarchar(60), ar.seeding_mode_desc) AS seeding_mode_desc,
    CONVERT(nvarchar(36), ars.group_id) AS state_group_id,
    CONVERT(nvarchar(36), ars.replica_id) AS state_replica_id,
    CONVERT(nvarchar(1), ars.is_local) AS is_local,
    CONVERT(nvarchar(60), ars.role_desc) AS role_desc,
    CONVERT(nvarchar(60), ars.operational_state_desc) AS operational_state_desc,
    CONVERT(nvarchar(60), ars.connected_state_desc) AS connected_state_desc,
    CONVERT(nvarchar(60), ars.recovery_health_desc) AS recovery_health_desc,
    CONVERT(nvarchar(60), ars.synchronization_health_desc) AS synchronization_health_desc,
    CONVERT(nvarchar(11), ars.last_connect_error_number) AS last_connect_error_number
FROM sys.availability_replicas AS ar
INNER JOIN sys.availability_groups AS ag ON ag.group_id = ar.group_id
LEFT JOIN sys.dm_hadr_availability_replica_states AS ars
    ON ars.group_id = ar.group_id AND ars.replica_id = ar.replica_id
WHERE ag.name = @P1;"#
            }
            Self::Databases => {
                r#"SELECT
    CONVERT(nvarchar(36), adc.group_id) AS group_id,
    CONVERT(nvarchar(36), adc.group_database_id) AS group_database_id,
    CONVERT(nvarchar(128), adc.database_name) AS database_name,
    CONVERT(nvarchar(11), d.database_id) AS local_database_id,
    CONVERT(nvarchar(128), d.name) AS local_database_name,
    CONVERT(nvarchar(36), d.group_database_id) AS local_group_database_id,
    CONVERT(nvarchar(36), d.replica_id) AS local_replica_id,
    CONVERT(nvarchar(60), d.state_desc) AS local_database_state_desc,
    CONVERT(nvarchar(60), d.recovery_model_desc) AS recovery_model_desc,
    CONVERT(nvarchar(11), recovery.database_id) AS recovery_status_database_id,
    CONVERT(nvarchar(36), recovery.database_guid) AS database_guid,
    CONVERT(nvarchar(36), recovery.family_guid) AS family_guid,
    CONVERT(nvarchar(36), recovery.recovery_fork_guid) AS recovery_fork_guid,
    CONVERT(nvarchar(36), recovery.first_recovery_fork_guid) AS first_recovery_fork_guid,
    CONVERT(nvarchar(25), recovery.fork_point_lsn) AS fork_point_lsn
FROM sys.availability_databases_cluster AS adc
INNER JOIN sys.availability_groups AS ag ON ag.group_id = adc.group_id
LEFT JOIN sys.availability_replicas AS local_config
    ON local_config.group_id = adc.group_id
    AND UPPER(local_config.replica_server_name) = UPPER(CONVERT(nvarchar(128), @@SERVERNAME))
LEFT JOIN sys.databases AS d
    ON d.group_database_id = adc.group_database_id AND d.replica_id = local_config.replica_id
LEFT JOIN sys.database_recovery_status AS recovery ON recovery.database_id = d.database_id
WHERE ag.name = @P1;"#
            }
            // A primary's remote DMV row is not a query of that remote engine.
            // In particular its database_id must never join local recovery
            // metadata. Both local GUID associations and is_local are required.
            Self::DatabaseStates => {
                r#"SELECT
    CONVERT(nvarchar(36), drs.group_id) AS group_id,
    CONVERT(nvarchar(36), drs.group_database_id) AS group_database_id,
    CONVERT(nvarchar(128), adc.database_name) AS database_name,
    CONVERT(nvarchar(36), drs.replica_id) AS replica_id,
    CONVERT(nvarchar(11), drs.database_id) AS database_id,
    CONVERT(nvarchar(1), drs.is_local) AS is_local,
    CONVERT(nvarchar(1), drs.is_primary_replica) AS is_primary_replica,
    CONVERT(nvarchar(60), drs.synchronization_state_desc) AS synchronization_state_desc,
    CONVERT(nvarchar(60), drs.synchronization_health_desc) AS synchronization_health_desc,
    CONVERT(nvarchar(60), drs.database_state_desc) AS database_state_desc,
    CONVERT(nvarchar(1), drs.is_suspended) AS is_suspended,
    CONVERT(nvarchar(60), drs.suspend_reason_desc) AS suspend_reason_desc,
    CONVERT(nvarchar(1), drs.is_commit_participant) AS is_commit_participant,
    CONVERT(nvarchar(25), drs.last_hardened_lsn) AS last_hardened_lsn,
    CONVERT(nvarchar(25), drs.last_redone_lsn) AS last_redone_lsn,
    CONVERT(nvarchar(25), drs.last_commit_lsn) AS last_commit_lsn,
    CONVERT(nvarchar(11), d.database_id) AS local_database_id,
    CONVERT(nvarchar(128), d.name) AS local_database_name,
    CONVERT(nvarchar(36), d.group_database_id) AS local_group_database_id,
    CONVERT(nvarchar(36), d.replica_id) AS local_replica_id,
    CONVERT(nvarchar(60), d.state_desc) AS local_database_state_desc,
    CONVERT(nvarchar(60), d.recovery_model_desc) AS recovery_model_desc,
    CONVERT(nvarchar(11), recovery.database_id) AS recovery_status_database_id,
    CONVERT(nvarchar(36), recovery.database_guid) AS database_guid,
    CONVERT(nvarchar(36), recovery.family_guid) AS family_guid,
    CONVERT(nvarchar(36), recovery.recovery_fork_guid) AS recovery_fork_guid,
    CONVERT(nvarchar(36), recovery.first_recovery_fork_guid) AS first_recovery_fork_guid,
    CONVERT(nvarchar(25), recovery.fork_point_lsn) AS fork_point_lsn
FROM sys.dm_hadr_database_replica_states AS drs
INNER JOIN sys.availability_groups AS ag ON ag.group_id = drs.group_id
INNER JOIN sys.availability_replicas AS ar
    ON ar.group_id = drs.group_id AND ar.replica_id = drs.replica_id
INNER JOIN sys.availability_databases_cluster AS adc
    ON adc.group_id = drs.group_id AND adc.group_database_id = drs.group_database_id
LEFT JOIN sys.databases AS d
    ON drs.is_local = 1 AND d.database_id = drs.database_id
    AND d.group_database_id = drs.group_database_id AND d.replica_id = drs.replica_id
LEFT JOIN sys.database_recovery_status AS recovery
    ON drs.is_local = 1 AND recovery.database_id = d.database_id
WHERE ag.name = @P1;"#
            }
            // Old seeding attempts unrelated to the current native database
            // or replica GUIDs are deliberately excluded. Error numbers are
            // facts; server-supplied error messages are never projected.
            Self::AutomaticSeeding => {
                r#"SELECT
    CONVERT(nvarchar(36), seed.ag_id) AS group_id,
    CONVERT(nvarchar(36), seed.ag_db_id) AS group_database_id,
    CONVERT(nvarchar(36), seed.ag_remote_replica_id) AS remote_replica_id,
    CONVERT(nvarchar(36), seed.operation_id) AS operation_id,
    CONVERT(nvarchar(1), seed.is_source) AS is_source,
    CONVERT(nvarchar(60), seed.current_state) AS current_state,
    CONVERT(nvarchar(1), seed.performed_seeding) AS performed_seeding,
    CONVERT(nvarchar(11), seed.failure_state) AS failure_state,
    CONVERT(nvarchar(11), seed.error_code) AS error_code,
    CONVERT(nvarchar(11), seed.number_of_attempts) AS number_of_attempts,
    CONVERT(nvarchar(33), seed.start_time, 126) AS start_time,
    CONVERT(nvarchar(33), seed.completion_time, 126) AS completion_time
FROM sys.dm_hadr_automatic_seeding AS seed
INNER JOIN sys.availability_groups AS ag ON ag.group_id = seed.ag_id
INNER JOIN sys.availability_databases_cluster AS adc
    ON adc.group_id = seed.ag_id AND adc.group_database_id = seed.ag_db_id
INNER JOIN sys.availability_replicas AS ar
    ON ar.group_id = seed.ag_id AND ar.replica_id = seed.ag_remote_replica_id
WHERE ag.name = @P1;"#
            }
            // This DMV has no documented AG/replica GUID or automatic-seeding
            // operation-id relationship. Only an already-associated local
            // database provides a native GUID link. Destination processes not
            // yet associated with an AG may therefore be unavailable here.
            // remote_machine_name is informational, never a replica identity.
            Self::PhysicalSeeding => {
                r#"SELECT
    CONVERT(nvarchar(36), ag.group_id) AS group_id,
    CONVERT(nvarchar(36), adc.group_database_id) AS group_database_id,
    CONVERT(nvarchar(36), ar.replica_id) AS local_replica_id,
    CONVERT(nvarchar(36), seed.local_physical_seeding_id) AS local_physical_seeding_id,
    CONVERT(nvarchar(36), seed.remote_physical_seeding_id) AS remote_physical_seeding_id,
    CONVERT(nvarchar(11), seed.local_database_id) AS local_database_id,
    CONVERT(nvarchar(128), seed.local_database_name) AS local_database_name,
    CONVERT(nvarchar(256), seed.remote_machine_name) AS remote_machine_name,
    CONVERT(nvarchar(60), seed.role_desc) AS role_desc,
    CONVERT(nvarchar(256), seed.internal_state_desc) AS internal_state_desc,
    CONVERT(nvarchar(20), seed.transfer_rate_bytes_per_second) AS transfer_rate_bytes_per_second,
    CONVERT(nvarchar(20), seed.transferred_size_bytes) AS transferred_size_bytes,
    CONVERT(nvarchar(20), seed.database_size_bytes) AS database_size_bytes,
    CONVERT(nvarchar(11), seed.failure_code) AS failure_code,
    CONVERT(nvarchar(1), seed.is_compression_enabled) AS is_compression_enabled,
    CONVERT(nvarchar(33), seed.start_time_utc, 126) AS start_time_utc,
    CONVERT(nvarchar(33), seed.end_time_utc, 126) AS end_time_utc,
    CONVERT(nvarchar(33), seed.estimate_time_complete_utc, 126) AS estimate_time_complete_utc
FROM sys.dm_hadr_physical_seeding_stats AS seed
INNER JOIN sys.databases AS d ON d.database_id = seed.local_database_id
INNER JOIN sys.availability_databases_cluster AS adc ON adc.group_database_id = d.group_database_id
INNER JOIN sys.availability_groups AS ag ON ag.group_id = adc.group_id
INNER JOIN sys.availability_replicas AS ar
    ON ar.group_id = ag.group_id AND ar.replica_id = d.replica_id
WHERE ag.name = @P1
    AND UPPER(ar.replica_server_name) = UPPER(CONVERT(nvarchar(128), @@SERVERNAME));"#
            }
        }
    }
}
