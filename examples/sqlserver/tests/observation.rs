use std::collections::{BTreeSet, VecDeque};

use async_trait::async_trait;
use serde_json::Value;
use sqlserver_replicated::executor::{QueryRow, SqlSession};
use sqlserver_replicated::observation::{
    AvailabilityGroupSnapshot, InstanceSnapshot, NativeProvenance, ObservationTarget,
    RecoveryLineageObservation, observe_session,
};
use sqlserver_replicated::query::ReadQuery;
use sqlserver_replicated::runtime_error::RuntimeError;
use sqlserver_replicated::{
    AvailabilityGroupName, NativeRole, Observation, ObservationFailureKind, ReplicaIdentity,
    ServerName,
};

const AG: &str = "11111111-1111-4111-8111-111111111111";
const LOCAL: &str = "22222222-2222-4222-8222-222222222222";
const REMOTE: &str = "33333333-3333-4333-8333-333333333333";
const THIRD: &str = "44444444-4444-4444-8444-444444444444";
const DATABASE: &str = "55555555-5555-4555-8555-555555555555";
const DATABASE_GUID: &str = "66666666-6666-4666-8666-666666666666";
const FAMILY: &str = "77777777-7777-4777-8777-777777777777";
const FORK: &str = "88888888-8888-4888-8888-888888888888";
const DIFFERENT: &str = "99999999-9999-4999-8999-999999999999";
const AUTOMATIC: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const PHYSICAL: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const REMOTE_PHYSICAL: &str = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
const MAX_PROGRESS: &str = "9999999999999999999999999";
const ABOVE_I64: &str = "9223372036854775808";
const COMMITTED: &str = "9999999999999999999999998";
const CONFIG_SEQUENCE: &str = "9007199254740993";
const OBSERVED_AT: u64 = 42_000;

#[derive(Clone)]
struct Step {
    query: ReadQuery,
    result: Result<Vec<QueryRow>, RuntimeError>,
}

impl Step {
    fn rows(query: ReadQuery, rows: Vec<QueryRow>) -> Self {
        Self {
            query,
            result: Ok(rows),
        }
    }
}

struct ScriptedSession {
    steps: VecDeque<Step>,
    seen: Vec<ReadQuery>,
    expected_group: AvailabilityGroupName,
}

#[async_trait]
impl SqlSession for ScriptedSession {
    async fn query(
        &mut self,
        query: ReadQuery,
        availability_group: &AvailabilityGroupName,
    ) -> Result<Vec<QueryRow>, RuntimeError> {
        assert_eq!(
            availability_group, &self.expected_group,
            "the AG must remain a bound value"
        );
        let step = self.steps.pop_front().expect("unexpected additional query");
        assert_eq!(
            query, step.query,
            "query order is part of the observation bracket"
        );
        self.seen.push(query);
        step.result
    }
}

fn target() -> ObservationTarget {
    ObservationTarget {
        availability_group: AvailabilityGroupName::new("test-ag").unwrap(),
        expected_server_name: ServerName::new("sql-0").unwrap(),
        replica: ReplicaIdentity::desired("logical-0", "pod-uid-0").unwrap(),
    }
}

fn row(fields: &[(&str, Option<&str>)]) -> QueryRow {
    fields
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.map(str::to_owned)))
        .collect()
}

fn set(row: &mut QueryRow, name: &str, value: Option<&str>) {
    assert!(row.contains_key(name), "fixture omitted column {name}");
    row.insert(name.to_owned(), value.map(str::to_owned));
}

fn permissions() -> QueryRow {
    row(&[
        ("product_major_version", Some("16")),
        ("view_server_state", Some("1")),
        ("view_server_performance_state", Some("1")),
        ("view_any_definition", Some("1")),
        ("view_any_database", Some("1")),
    ])
}

const GROUP_COLUMNS: &[&str] = &[
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
];

fn anchor(present: bool) -> QueryRow {
    let mut result = row(&[
        ("server_name", Some("SQL-0")),
        ("property_server_name", Some("sql-0")),
        ("product_version", Some("16.0.4225.2")),
        ("product_major_version", Some("16")),
        ("edition", Some("Developer Edition (64-bit)")),
        ("engine_edition", Some("3")),
        ("is_hadr_enabled", Some("1")),
        ("host_platform", Some("Linux")),
        ("host_distribution", Some("Ubuntu")),
        ("architecture", Some("x86_64")),
        ("sqlserver_start_time", Some("2026-08-01T10:00:00")),
        ("group_id", Some(AG)),
        ("group_name", Some("test-ag")),
        ("cluster_type", Some("2")),
        ("cluster_type_desc", Some("EXTERNAL")),
        ("sequence_number", Some(CONFIG_SEQUENCE)),
        ("required_synchronized_secondaries_to_commit", Some("1")),
        ("basic_features", Some("0")),
        ("is_distributed", Some("0")),
        ("local_replica_id", Some(LOCAL)),
        ("local_replica_server_name", Some("sql-0")),
        ("local_state_group_id", Some(AG)),
        ("local_state_replica_id", Some(LOCAL)),
        ("local_role_desc", Some("PRIMARY")),
    ]);
    if !present {
        for name in GROUP_COLUMNS {
            set(&mut result, name, None);
        }
    }
    result
}

const REPLICA_STATE_COLUMNS: &[&str] = &[
    "state_group_id",
    "state_replica_id",
    "is_local",
    "role_desc",
    "operational_state_desc",
    "connected_state_desc",
    "recovery_health_desc",
    "synchronization_health_desc",
    "last_connect_error_number",
];

fn replica(id: &str, server: &str, is_local: Option<bool>) -> QueryRow {
    let mut result = row(&[
        ("group_id", Some(AG)),
        ("replica_id", Some(id)),
        ("replica_server_name", Some(server)),
        ("endpoint_url", Some("TCP://sql.example:5022")),
        ("availability_mode", Some("1")),
        ("availability_mode_desc", Some("SYNCHRONOUS_COMMIT")),
        ("failover_mode", Some("2")),
        ("failover_mode_desc", Some("EXTERNAL")),
        ("seeding_mode", Some("0")),
        ("seeding_mode_desc", Some("AUTOMATIC")),
        ("state_group_id", Some(AG)),
        ("state_replica_id", Some(id)),
        (
            "is_local",
            Some(if is_local == Some(true) { "1" } else { "0" }),
        ),
        (
            "role_desc",
            Some(if is_local == Some(true) {
                "PRIMARY"
            } else {
                "SECONDARY"
            }),
        ),
        (
            "operational_state_desc",
            if is_local == Some(true) {
                Some("ONLINE")
            } else {
                None
            },
        ),
        (
            "connected_state_desc",
            Some(if is_local == Some(true) {
                "CONNECTED"
            } else {
                "DISCONNECTED"
            }),
        ),
        (
            "recovery_health_desc",
            if is_local == Some(true) {
                Some("ONLINE")
            } else {
                None
            },
        ),
        (
            "synchronization_health_desc",
            Some(if is_local == Some(true) {
                "HEALTHY"
            } else {
                "NOT_HEALTHY"
            }),
        ),
        ("last_connect_error_number", Some("0")),
    ]);
    if is_local.is_none() {
        for name in REPLICA_STATE_COLUMNS {
            set(&mut result, name, None);
        }
    }
    result
}

const LOCAL_COLUMNS: &[&str] = &[
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
];

fn local_database_metadata(available: bool) -> QueryRow {
    let mut result = row(&[
        ("local_database_id", Some("5")),
        ("local_database_name", Some("app_db")),
        ("local_group_database_id", Some(DATABASE)),
        ("local_replica_id", Some(LOCAL)),
        ("local_database_state_desc", Some("ONLINE")),
        ("recovery_model_desc", Some("FULL")),
        ("recovery_status_database_id", Some("5")),
        ("database_guid", Some(DATABASE_GUID)),
        ("family_guid", Some(FAMILY)),
        ("recovery_fork_guid", Some(FORK)),
        ("first_recovery_fork_guid", Some(FORK)),
        ("fork_point_lsn", None),
    ]);
    if !available {
        for name in LOCAL_COLUMNS {
            set(&mut result, name, None);
        }
    }
    result
}

fn database() -> QueryRow {
    let mut result = row(&[
        ("group_id", Some(AG)),
        ("group_database_id", Some(DATABASE)),
        ("database_name", Some("app_db")),
    ]);
    result.extend(local_database_metadata(true));
    result
}

fn database_state(local: bool) -> QueryRow {
    let mut result = row(&[
        ("group_id", Some(AG)),
        ("group_database_id", Some(DATABASE)),
        ("database_name", Some("app_db")),
        ("replica_id", Some(if local { LOCAL } else { REMOTE })),
        // A remote row may report the same numeric database ID. That is not
        // permission to join it to this engine's recovery metadata.
        ("database_id", Some("5")),
        ("is_local", Some(if local { "1" } else { "0" })),
        ("is_primary_replica", Some(if local { "1" } else { "0" })),
        (
            "synchronization_state_desc",
            Some(if local {
                "SYNCHRONIZED"
            } else {
                "NOT SYNCHRONIZING"
            }),
        ),
        (
            "synchronization_health_desc",
            Some(if local { "HEALTHY" } else { "NOT_HEALTHY" }),
        ),
        (
            "database_state_desc",
            Some(if local { "ONLINE" } else { "RESTORING" }),
        ),
        ("is_suspended", Some(if local { "0" } else { "1" })),
        (
            "suspend_reason_desc",
            if local {
                None
            } else {
                Some("SUSPEND_FROM_USER")
            },
        ),
        ("is_commit_participant", Some(if local { "1" } else { "0" })),
        (
            "last_hardened_lsn",
            Some(if local {
                MAX_PROGRESS
            } else {
                "9007199254740993"
            }),
        ),
        (
            "last_redone_lsn",
            Some(if local { ABOVE_I64 } else { "9007199254740992" }),
        ),
        (
            "last_commit_lsn",
            Some(if local { COMMITTED } else { "9007199254740991" }),
        ),
    ]);
    result.extend(local_database_metadata(local));
    result
}

fn automatic_seed() -> QueryRow {
    row(&[
        ("group_id", Some(AG)),
        ("group_database_id", Some(DATABASE)),
        ("remote_replica_id", Some(REMOTE)),
        ("operation_id", Some(AUTOMATIC)),
        ("is_source", Some("1")),
        ("current_state", Some("FAILED")),
        ("performed_seeding", Some("0")),
        ("failure_state", Some("108")),
        ("error_code", Some("35250")),
        ("number_of_attempts", Some("2")),
        ("start_time", Some("2026-08-02T10:00:00.123")),
        ("completion_time", None),
    ])
}

fn physical_seed() -> QueryRow {
    row(&[
        ("group_id", Some(AG)),
        ("group_database_id", Some(DATABASE)),
        ("local_replica_id", Some(LOCAL)),
        ("local_physical_seeding_id", Some(PHYSICAL)),
        ("remote_physical_seeding_id", Some(REMOTE_PHYSICAL)),
        ("local_database_id", Some("5")),
        ("local_database_name", Some("app_db")),
        (
            "remote_machine_name",
            Some("remote-machine-not-an-instance-id"),
        ),
        ("role_desc", Some("Source")),
        ("internal_state_desc", Some("Preparing")),
        ("transfer_rate_bytes_per_second", Some("1024")),
        ("transferred_size_bytes", Some(CONFIG_SEQUENCE)),
        ("database_size_bytes", Some("9223372036854775807")),
        ("failure_code", Some("0")),
        ("is_compression_enabled", Some("1")),
        ("start_time_utc", Some("2026-08-02T10:00:01")),
        ("end_time_utc", None),
        ("estimate_time_complete_utc", None),
    ])
}

fn present_script() -> Vec<Step> {
    vec![
        Step::rows(ReadQuery::Permissions, vec![permissions()]),
        Step::rows(ReadQuery::Anchor, vec![anchor(true)]),
        Step::rows(
            ReadQuery::Replicas,
            vec![
                replica(LOCAL, "sql-0", Some(true)),
                replica(REMOTE, "sql-1", Some(false)),
                replica(THIRD, "sql-2", None),
            ],
        ),
        Step::rows(ReadQuery::Databases, vec![database()]),
        Step::rows(
            ReadQuery::DatabaseStates,
            vec![database_state(true), database_state(false)],
        ),
        Step::rows(ReadQuery::AutomaticSeeding, vec![automatic_seed()]),
        Step::rows(ReadQuery::PhysicalSeeding, vec![physical_seed()]),
        Step::rows(ReadQuery::Databases, vec![database()]),
        Step::rows(ReadQuery::Permissions, vec![permissions()]),
        Step::rows(ReadQuery::Anchor, vec![anchor(true)]),
    ]
}

fn absent_script() -> Vec<Step> {
    vec![
        Step::rows(ReadQuery::Permissions, vec![permissions()]),
        Step::rows(ReadQuery::Anchor, vec![anchor(false)]),
        Step::rows(ReadQuery::Permissions, vec![permissions()]),
        Step::rows(ReadQuery::Anchor, vec![anchor(false)]),
    ]
}

fn rows_mut(script: &mut [Step], query: ReadQuery, occurrence: usize) -> &mut Vec<QueryRow> {
    script
        .iter_mut()
        .filter(|step| step.query == query)
        .nth(occurrence)
        .unwrap()
        .result
        .as_mut()
        .unwrap()
}

fn change_rows(script: &mut [Step], query: ReadQuery, mut change: impl FnMut(&mut QueryRow)) {
    for step in script {
        if step.query == query {
            for row in step.result.as_mut().unwrap() {
                change(row);
            }
        }
    }
}

async fn execute(script: Vec<Step>) -> (Result<InstanceSnapshot, RuntimeError>, ScriptedSession) {
    let target = target();
    let mut session = ScriptedSession {
        steps: script.into(),
        seen: Vec::new(),
        expected_group: target.availability_group.clone(),
    };
    let result = observe_session(&mut session, &target, OBSERVED_AT).await;
    (result, session)
}

async fn success(script: Vec<Step>) -> InstanceSnapshot {
    let (result, session) = execute(script).await;
    let snapshot = result.expect("expected a supported, coherent observation");
    assert!(
        session.steps.is_empty(),
        "all closing brackets must be read"
    );
    assert_eq!(snapshot.observed_at_unix_millis, OBSERVED_AT);
    snapshot
}

async fn failure(script: Vec<Step>, kind: ObservationFailureKind) -> RuntimeError {
    let error = execute(script).await.0.expect_err("must fail closed");
    assert_eq!(error.kind, kind, "{error}");
    error
}

fn group(snapshot: &InstanceSnapshot) -> &AvailabilityGroupSnapshot {
    match &snapshot.availability_group {
        Observation::Present {
            value,
            observed_at_unix_millis,
        } => {
            assert_eq!(*observed_at_unix_millis, OBSERVED_AT);
            value
        }
        other => panic!("expected a present native group, got {other:?}"),
    }
}

#[tokio::test]
async fn observes_supported_instance_native_identities_and_unhealthy_replica_facts() {
    let snapshot = success(present_script()).await;
    assert_eq!(snapshot.instance.server_name.as_str(), "sql-0");
    assert_eq!(snapshot.instance.product_version, "16.0.4225.2");
    assert_eq!(snapshot.instance.edition, "Developer Edition (64-bit)");
    assert_eq!(snapshot.instance.host_platform, "Linux");
    assert_eq!(snapshot.instance.architecture, "x86_64");
    assert!(snapshot.instance.hadr_enabled);
    let group = group(&snapshot);
    assert_eq!(group.identity.group_id.as_str(), AG);
    assert_eq!(group.configuration_sequence.to_string(), CONFIG_SEQUENCE);
    assert_eq!(group.required_synchronized_secondaries_to_commit, 1);
    assert_eq!(group.local_replica.identity.logical_id(), "logical-0");
    assert_eq!(group.local_replica.identity.incarnation(), "pod-uid-0");
    assert_eq!(
        group
            .local_replica
            .identity
            .native_replica_id()
            .unwrap()
            .as_str(),
        LOCAL
    );
    assert!(group.local_replica.state_available);
    assert_eq!(group.local_replica.role, Some(NativeRole::Primary));
    assert_eq!(group.replicas.len(), 3);
    assert_eq!(
        group.replicas[1]
            .state
            .as_ref()
            .unwrap()
            .connected_state
            .as_deref(),
        Some("DISCONNECTED")
    );
    assert!(group.replicas[2].state.is_none());
    let database = &group.databases[0];
    assert_eq!(database.identity.group_database_id.as_str(), DATABASE);
    assert_eq!(
        database
            .local
            .as_ref()
            .unwrap()
            .recovery
            .as_ref()
            .unwrap()
            .family_guid
            .as_ref()
            .unwrap()
            .as_str(),
        FAMILY
    );
    let remote = &database.replicas[1];
    assert_eq!(remote.is_suspended, Some(true));
    assert_eq!(
        remote.synchronization_health.as_deref(),
        Some("NOT_HEALTHY")
    );
    assert_eq!(remote.suspend_reason.as_deref(), Some("SUSPEND_FROM_USER"));
}

#[tokio::test]
async fn observes_absence_only_after_permissions_capabilities_and_matching_anchors() {
    let snapshot = success(absent_script()).await;
    assert!(matches!(
        snapshot.availability_group,
        Observation::Absent {
            observed_at_unix_millis: OBSERVED_AT
        }
    ));
}

#[tokio::test]
async fn external_cluster_descriptors_accept_ascii_casing_and_preserve_native_evidence() {
    for descriptor in ["external", "EXTERNAL", "ExTeRnAl"] {
        let mut script = present_script();
        change_rows(&mut script, ReadQuery::Anchor, |row| {
            set(row, "cluster_type_desc", Some(descriptor));
        });
        let snapshot = success(script).await;
        assert_eq!(group(&snapshot).cluster_type, descriptor);
    }
}

#[tokio::test]
async fn external_cluster_descriptors_cannot_override_an_unsupported_numeric_type() {
    for cluster_type in ["0", "1", "3", "255"] {
        let mut script = present_script();
        change_rows(&mut script, ReadQuery::Anchor, |row| {
            set(row, "cluster_type", Some(cluster_type));
            set(row, "cluster_type_desc", Some("external"));
        });
        failure(script, ObservationFailureKind::Unsupported).await;
    }
}

#[tokio::test]
async fn supports_real_developer_and_enterprise_display_names_with_engine_edition_cross_check() {
    for edition in [
        "Developer",
        "Developer Edition",
        "Developer Edition (64-bit)",
        "Enterprise",
        "Enterprise Edition",
        "Enterprise Edition (64-bit)",
        "Enterprise Edition: Core-based Licensing (64-bit)",
    ] {
        let mut script = absent_script();
        change_rows(&mut script, ReadQuery::Anchor, |row| {
            set(row, "edition", Some(edition))
        });
        success(script).await;
    }
    for (edition, engine_edition) in [
        ("Standard Edition (64-bit)", "2"),
        ("Enterprise Evaluation Edition (64-bit)", "3"),
        ("Express Edition (64-bit)", "4"),
        ("Developer Edition (64-bit)", "2"),
        ("Enterprise Edition (64-bit)", "5"),
        ("Enterprise Edition unrecognized suffix", "3"),
    ] {
        let mut script = absent_script();
        set(
            &mut rows_mut(&mut script, ReadQuery::Anchor, 0)[0],
            "edition",
            Some(edition),
        );
        set(
            &mut rows_mut(&mut script, ReadQuery::Anchor, 0)[0],
            "engine_edition",
            Some(engine_edition),
        );
        failure(script, ObservationFailureKind::Unsupported).await;
    }
}

#[tokio::test]
async fn rejects_unsupported_capabilities_even_when_the_ag_is_absent() {
    for major in ["15", "17"] {
        let mut script = absent_script();
        set(
            &mut rows_mut(&mut script, ReadQuery::Permissions, 0)[0],
            "product_major_version",
            Some(major),
        );
        failure(script, ObservationFailureKind::Unsupported).await;
    }
    for (column, value) in [
        ("is_hadr_enabled", "0"),
        ("host_platform", "Windows"),
        ("architecture", "aarch64"),
        ("architecture", "unknown"),
    ] {
        let mut script = absent_script();
        set(
            &mut rows_mut(&mut script, ReadQuery::Anchor, 0)[0],
            column,
            Some(value),
        );
        failure(script, ObservationFailureKind::Unsupported).await;
    }
}

#[tokio::test]
async fn missing_or_null_permissions_stop_before_any_catalog_query() {
    for permission in [
        "view_server_state",
        "view_server_performance_state",
        "view_any_definition",
        "view_any_database",
    ] {
        for value in [Some("0"), None] {
            let mut result = permissions();
            set(&mut result, permission, value);
            let (observation, session) =
                execute(vec![Step::rows(ReadQuery::Permissions, vec![result])]).await;
            assert_eq!(
                observation.unwrap_err().kind,
                ObservationFailureKind::PermissionDenied
            );
            assert_eq!(session.seen, vec![ReadQuery::Permissions]);
        }
    }
}

#[tokio::test]
async fn permission_revocation_at_the_closing_bracket_is_not_absence() {
    for mut script in [absent_script(), present_script()] {
        set(
            &mut rows_mut(&mut script, ReadQuery::Permissions, 1)[0],
            "view_any_database",
            Some("0"),
        );
        failure(script, ObservationFailureKind::PermissionDenied).await;
    }
}

#[tokio::test]
async fn propagates_typed_unreachable_authentication_tls_and_timeout_failures() {
    for kind in [
        ObservationFailureKind::Unreachable,
        ObservationFailureKind::Authentication,
        ObservationFailureKind::Tls,
        ObservationFailureKind::TimedOut,
        ObservationFailureKind::PermissionDenied,
    ] {
        let mut expected = RuntimeError::new(kind, "transport", "adapter-owned test failure");
        expected.server_code = Some(18456);
        let actual = failure(
            vec![Step {
                query: ReadQuery::Permissions,
                result: Err(expected.clone()),
            }],
            kind,
        )
        .await;
        assert_eq!(actual, expected);
    }
    let mut script = present_script();
    script
        .iter_mut()
        .find(|step| step.query == ReadQuery::DatabaseStates)
        .unwrap()
        .result = Err(RuntimeError::new(
        ObservationFailureKind::Unreachable,
        "database_states",
        "connection ended",
    ));
    failure(script, ObservationFailureKind::Unreachable).await;
}

#[tokio::test]
async fn all_native_progress_and_bigint_json_values_are_lossless_strings() {
    let snapshot = success(present_script()).await;
    let local = &group(&snapshot).databases[0].replicas[0];
    assert_eq!(
        local.progress.hardened_block.unwrap().to_string(),
        MAX_PROGRESS
    );
    assert_eq!(local.progress.redone_record.unwrap().to_string(), ABOVE_I64);
    assert_eq!(
        local.progress.committed_record.unwrap().to_string(),
        COMMITTED
    );
    assert!(local.progress.redone_record.unwrap().value() > i64::MAX as u128);
    let json = serde_json::to_value(&snapshot).unwrap();
    let ag = &json["availability_group"]["value"];
    assert_eq!(
        ag["configuration_sequence"],
        Value::String(CONFIG_SEQUENCE.to_owned())
    );
    let progress = &ag["databases"][0]["replicas"][0]["progress"];
    assert_eq!(progress["hardened_block"], MAX_PROGRESS);
    assert_eq!(progress["redone_record"], ABOVE_I64);
    assert_eq!(progress["committed_record"], COMMITTED);
    assert_eq!(ag["local_replica"]["role"], "PRIMARY");
    assert_eq!(
        ag["physical_seeding"][0]["transferred_size_bytes"],
        CONFIG_SEQUENCE
    );
    assert_eq!(
        ag["physical_seeding"][0]["database_size_bytes"],
        "9223372036854775807"
    );
}

#[tokio::test]
async fn local_and_primary_remote_progress_have_distinct_provenance_and_lineage() {
    let snapshot = success(present_script()).await;
    let states = &group(&snapshot).databases[0].replicas;
    assert_eq!(states[0].database_id, states[1].database_id);
    assert_eq!(states[0].provenance, NativeProvenance::Local);
    match &states[0].lineage {
        RecoveryLineageObservation::Local { value } => {
            assert_eq!(value.database.group_database_id.as_str(), DATABASE);
            assert_eq!(value.recovery_fork_id.as_str(), FORK);
        }
        other => panic!("expected locally read lineage, got {other:?}"),
    }
    assert_eq!(states[1].provenance, NativeProvenance::PrimaryRemote);
    assert_eq!(
        states[1].lineage,
        RecoveryLineageObservation::RemoteUnavailable
    );
    let remote_json = serde_json::to_string(&states[1]).unwrap();
    assert!(!remote_json.contains(FORK));
    assert!(remote_json.contains("primary_remote"));
    assert!(remote_json.contains("remote_unavailable"));
}

#[tokio::test]
async fn missing_local_fork_is_explicit_and_never_replaced_with_a_remote_or_fabricated_fork() {
    for recovery_row_visible in [true, false] {
        let mut script = present_script();
        for query in [ReadQuery::Databases, ReadQuery::DatabaseStates] {
            change_rows(&mut script, query, |row| {
                if row["local_database_id"].is_some() {
                    for column in [
                        "database_guid",
                        "family_guid",
                        "recovery_fork_guid",
                        "first_recovery_fork_guid",
                        "fork_point_lsn",
                    ] {
                        set(row, column, None);
                    }
                    if !recovery_row_visible {
                        set(row, "recovery_status_database_id", None);
                    }
                }
            });
        }
        let snapshot = success(script).await;
        let database = &group(&snapshot).databases[0];
        assert_eq!(
            database.replicas[0].lineage,
            RecoveryLineageObservation::LocalUnavailable
        );
        assert_eq!(
            database.local.as_ref().unwrap().recovery.is_some(),
            recovery_row_visible
        );
        assert_eq!(
            database.replicas[1].lineage,
            RecoveryLineageObservation::RemoteUnavailable
        );
    }
}

#[tokio::test]
async fn rejects_local_recovery_metadata_attached_to_a_remote_progress_row() {
    let mut script = present_script();
    set(
        &mut rows_mut(&mut script, ReadQuery::DatabaseStates, 0)[1],
        "recovery_fork_guid",
        Some(FORK),
    );
    failure(script, ObservationFailureKind::Malformed).await;
    let mut script = present_script();
    rows_mut(&mut script, ReadQuery::DatabaseStates, 0)[1].extend(local_database_metadata(true));
    failure(script, ObservationFailureKind::Malformed).await;
}

fn make_local_only_states(script: &mut [Step], role: Option<&str>) {
    change_rows(script, ReadQuery::Anchor, |row| {
        set(row, "local_role_desc", role)
    });
    let replicas = rows_mut(script, ReadQuery::Replicas, 0);
    set(&mut replicas[0], "role_desc", role);
    for remote in &mut replicas[1..] {
        for column in REPLICA_STATE_COLUMNS {
            set(remote, column, None);
        }
    }
    let states = rows_mut(script, ReadQuery::DatabaseStates, 0);
    states.truncate(1);
    set(&mut states[0], "is_primary_replica", None);
}

#[tokio::test]
async fn unknown_null_and_resolving_roles_are_not_silently_promoted_to_primary() {
    for role in [Some("FUTURE_NATIVE_ROLE"), Some("RESOLVING"), None] {
        let mut script = present_script();
        make_local_only_states(&mut script, role);
        let snapshot = success(script).await;
        let observed = &group(&snapshot).local_replica;
        assert!(observed.state_available);
        assert_eq!(
            observed.role,
            role.map(|role| NativeRole::parse(role).unwrap())
        );
        assert_ne!(observed.role, Some(NativeRole::Primary));
        let json = serde_json::to_value(observed).unwrap();
        assert_eq!(
            json["role"],
            role.map_or(Value::Null, |role| Value::String(role.to_owned()))
        );
    }
}

#[tokio::test]
async fn secondary_observations_are_local_only() {
    let mut script = present_script();
    make_local_only_states(&mut script, Some("SECONDARY"));
    set(
        &mut rows_mut(&mut script, ReadQuery::DatabaseStates, 0)[0],
        "is_primary_replica",
        Some("0"),
    );
    let snapshot = success(script).await;
    assert_eq!(
        group(&snapshot).local_replica.role,
        Some(NativeRole::Secondary)
    );
    assert_eq!(group(&snapshot).databases[0].replicas.len(), 1);

    let mut script = present_script();
    change_rows(&mut script, ReadQuery::Anchor, |row| {
        set(row, "local_role_desc", Some("SECONDARY"))
    });
    set(
        &mut rows_mut(&mut script, ReadQuery::Replicas, 0)[0],
        "role_desc",
        Some("SECONDARY"),
    );
    failure(script, ObservationFailureKind::Inconsistent).await;
}

#[tokio::test]
async fn null_progress_and_health_fields_remain_unknown_not_zero_or_healthy() {
    let mut script = present_script();
    change_rows(&mut script, ReadQuery::DatabaseStates, |row| {
        for column in [
            "last_hardened_lsn",
            "last_redone_lsn",
            "last_commit_lsn",
            "is_primary_replica",
            "synchronization_state_desc",
            "synchronization_health_desc",
            "database_state_desc",
            "is_suspended",
            "suspend_reason_desc",
            "is_commit_participant",
        ] {
            set(row, column, None);
        }
    });
    let snapshot = success(script).await;
    for state in &group(&snapshot).databases[0].replicas {
        assert_eq!(state.progress.hardened_block, None);
        assert_eq!(state.progress.redone_record, None);
        assert_eq!(state.progress.committed_record, None);
        assert_eq!(state.is_suspended, None);
        assert_eq!(state.synchronization_health, None);
    }
}

#[tokio::test]
async fn preserves_unknown_native_state_descriptions_without_health_inference() {
    let mut script = present_script();
    change_rows(&mut script, ReadQuery::DatabaseStates, |row| {
        set(row, "synchronization_state_desc", Some("FUTURE_SYNC_STATE"));
        set(row, "synchronization_health_desc", Some("FUTURE_HEALTH"));
    });
    set(
        &mut rows_mut(&mut script, ReadQuery::Replicas, 0)[0],
        "connected_state_desc",
        Some("FUTURE_CONNECTIVITY"),
    );
    set(
        &mut rows_mut(&mut script, ReadQuery::AutomaticSeeding, 0)[0],
        "current_state",
        Some("FUTURE_SEEDING_STATE"),
    );
    let snapshot = success(script).await;
    let group = group(&snapshot);
    assert_eq!(
        group.databases[0].replicas[0]
            .synchronization_health
            .as_deref(),
        Some("FUTURE_HEALTH")
    );
    assert_eq!(
        group.replicas[0]
            .state
            .as_ref()
            .unwrap()
            .connected_state
            .as_deref(),
        Some("FUTURE_CONNECTIVITY")
    );
    assert_eq!(
        group.automatic_seeding[0].current_state.as_deref(),
        Some("FUTURE_SEEDING_STATE")
    );
}

#[tokio::test]
async fn partially_joined_and_empty_groups_are_represented_without_assuming_health() {
    for configured_local in [true, false] {
        let mut script = present_script();
        change_rows(&mut script, ReadQuery::Anchor, |row| {
            for column in [
                "local_state_group_id",
                "local_state_replica_id",
                "local_role_desc",
            ] {
                set(row, column, None);
            }
            if !configured_local {
                set(row, "local_replica_id", None);
                set(row, "local_replica_server_name", None);
            }
        });
        *rows_mut(&mut script, ReadQuery::Replicas, 0) = if configured_local {
            vec![replica(LOCAL, "sql-0", None)]
        } else {
            Vec::new()
        };
        change_rows(&mut script, ReadQuery::Databases, |row| {
            row.extend(local_database_metadata(false))
        });
        for query in [
            ReadQuery::DatabaseStates,
            ReadQuery::AutomaticSeeding,
            ReadQuery::PhysicalSeeding,
        ] {
            rows_mut(&mut script, query, 0).clear();
        }
        let snapshot = success(script).await;
        let group = group(&snapshot);
        assert!(!group.local_replica.state_available);
        assert_eq!(group.local_replica.role, None);
        assert_eq!(
            group.local_replica.identity.native_replica_id().is_some(),
            configured_local
        );
        assert!(group.databases[0].local.is_none());
        assert!(group.databases[0].replicas.is_empty());
    }
    let mut script = present_script();
    for query in [
        ReadQuery::Databases,
        ReadQuery::DatabaseStates,
        ReadQuery::AutomaticSeeding,
        ReadQuery::PhysicalSeeding,
    ] {
        for step in script.iter_mut().filter(|step| step.query == query) {
            step.result = Ok(Vec::new());
        }
    }
    assert!(group(&success(script).await).databases.is_empty());
}

#[tokio::test]
async fn progress_can_be_observed_with_local_catalog_and_lineage_unavailable() {
    let mut script = present_script();
    for query in [ReadQuery::Databases, ReadQuery::DatabaseStates] {
        change_rows(&mut script, query, |row| {
            row.extend(local_database_metadata(false))
        });
    }
    rows_mut(&mut script, ReadQuery::PhysicalSeeding, 0).clear();
    let snapshot = success(script).await;
    let database = &group(&snapshot).databases[0];
    assert!(database.local.is_none());
    assert_eq!(
        database.replicas[0].lineage,
        RecoveryLineageObservation::LocalUnavailable
    );
    assert_eq!(
        database.replicas[0]
            .progress
            .hardened_block
            .unwrap()
            .to_string(),
        MAX_PROGRESS
    );
}

#[tokio::test]
async fn returns_seeding_facts_without_conflating_automatic_and_physical_operations() {
    let snapshot = success(present_script()).await;
    let group = group(&snapshot);
    let automatic = &group.automatic_seeding[0];
    assert_eq!(automatic.operation_id.as_str(), AUTOMATIC);
    assert_eq!(automatic.remote_replica_id.as_str(), REMOTE);
    assert_eq!(automatic.current_state.as_deref(), Some("FAILED"));
    assert_eq!(automatic.performed_seeding, Some(false));
    assert_eq!(automatic.failure_state, Some(108));
    assert_eq!(automatic.error_code, Some(35250));
    assert_eq!(automatic.number_of_attempts, Some(2));
    let physical = &group.physical_seeding[0];
    assert_eq!(physical.local_physical_seeding_id.as_str(), PHYSICAL);
    assert_ne!(physical.local_physical_seeding_id, automatic.operation_id);
    assert_eq!(
        physical
            .remote_physical_seeding_id
            .as_ref()
            .unwrap()
            .as_str(),
        REMOTE_PHYSICAL
    );
    assert_eq!(physical.role.as_deref(), Some("Source"));
    assert_eq!(
        physical.remote_machine_name.as_deref(),
        Some("remote-machine-not-an-instance-id")
    );
    assert_eq!(physical.is_compression_enabled, Some(true));
}

#[tokio::test]
async fn every_anchor_change_including_role_server_restart_or_native_guid_is_inconsistent() {
    for (column, value) in [
        ("server_name", "sql-other"),
        ("property_server_name", "sql-other"),
        ("sqlserver_start_time", "2026-08-01T10:00:01"),
        ("product_version", "16.0.4225.3"),
        ("edition", "Enterprise Edition (64-bit)"),
        ("is_hadr_enabled", "0"),
        ("group_name", "renamed-ag"),
        ("sequence_number", "9007199254740994"),
        ("local_role_desc", "SECONDARY"),
        ("local_role_desc", "FUTURE_ROLE"),
        ("required_synchronized_secondaries_to_commit", "0"),
        ("cluster_type_desc", "NONE"),
    ] {
        let mut script = present_script();
        set(
            &mut rows_mut(&mut script, ReadQuery::Anchor, 1)[0],
            column,
            Some(value),
        );
        failure(script, ObservationFailureKind::Inconsistent).await;
    }
    let mut script = present_script();
    let closing = &mut rows_mut(&mut script, ReadQuery::Anchor, 1)[0];
    set(closing, "group_id", Some(DIFFERENT));
    set(closing, "local_state_group_id", Some(DIFFERENT));
    failure(script, ObservationFailureKind::Inconsistent).await;
    let mut script = present_script();
    let closing = &mut rows_mut(&mut script, ReadQuery::Anchor, 1)[0];
    set(closing, "local_replica_id", Some(DIFFERENT));
    set(closing, "local_state_replica_id", Some(DIFFERENT));
    failure(script, ObservationFailureKind::Inconsistent).await;
}

#[tokio::test]
async fn appearing_or_disappearing_ag_is_not_a_coherent_present_or_absent_snapshot() {
    let mut script = absent_script();
    rows_mut(&mut script, ReadQuery::Anchor, 1)[0] = anchor(true);
    failure(script, ObservationFailureKind::Inconsistent).await;
    let mut script = present_script();
    rows_mut(&mut script, ReadQuery::Anchor, 1)[0] = anchor(false);
    failure(script, ObservationFailureKind::Inconsistent).await;
}

#[tokio::test]
async fn detects_role_and_native_association_changes_inside_the_bracket() {
    for (query, index, column, value) in [
        (ReadQuery::Replicas, 0, "role_desc", "SECONDARY"),
        (ReadQuery::Replicas, 1, "role_desc", "PRIMARY"),
        (ReadQuery::Replicas, 0, "group_id", DIFFERENT),
        (ReadQuery::Replicas, 0, "state_group_id", DIFFERENT),
        (ReadQuery::Replicas, 0, "state_replica_id", DIFFERENT),
        (ReadQuery::Replicas, 1, "is_local", "1"),
        (ReadQuery::Databases, 0, "group_id", DIFFERENT),
        (
            ReadQuery::Databases,
            0,
            "local_group_database_id",
            DIFFERENT,
        ),
        (ReadQuery::Databases, 0, "local_replica_id", DIFFERENT),
        (ReadQuery::Databases, 0, "recovery_status_database_id", "6"),
        (ReadQuery::DatabaseStates, 0, "group_database_id", DIFFERENT),
        (ReadQuery::DatabaseStates, 0, "replica_id", DIFFERENT),
        (ReadQuery::DatabaseStates, 0, "database_id", "6"),
        (ReadQuery::DatabaseStates, 0, "is_primary_replica", "0"),
        (ReadQuery::DatabaseStates, 1, "is_primary_replica", "1"),
        (
            ReadQuery::AutomaticSeeding,
            0,
            "group_database_id",
            DIFFERENT,
        ),
        (
            ReadQuery::AutomaticSeeding,
            0,
            "remote_replica_id",
            DIFFERENT,
        ),
        (ReadQuery::AutomaticSeeding, 0, "remote_replica_id", LOCAL),
        (ReadQuery::PhysicalSeeding, 0, "group_id", DIFFERENT),
        (
            ReadQuery::PhysicalSeeding,
            0,
            "group_database_id",
            DIFFERENT,
        ),
        (ReadQuery::PhysicalSeeding, 0, "local_replica_id", REMOTE),
        (ReadQuery::PhysicalSeeding, 0, "local_database_id", "6"),
    ] {
        let mut script = present_script();
        set(
            &mut rows_mut(&mut script, query, 0)[index],
            column,
            Some(value),
        );
        failure(script, ObservationFailureKind::Inconsistent).await;
    }
}

#[tokio::test]
async fn detects_local_database_recreation_or_recovery_fork_changes_even_with_stable_ag_anchors() {
    for (column, value) in [
        ("database_guid", Some(DIFFERENT)),
        ("family_guid", Some(DIFFERENT)),
        ("recovery_fork_guid", Some(DIFFERENT)),
        ("recovery_fork_guid", None),
        ("first_recovery_fork_guid", Some(DIFFERENT)),
        ("fork_point_lsn", Some(ABOVE_I64)),
    ] {
        for query in [ReadQuery::DatabaseStates, ReadQuery::Databases] {
            let mut script = present_script();
            let occurrence = usize::from(query == ReadQuery::Databases);
            set(
                &mut rows_mut(&mut script, query, occurrence)[0],
                column,
                value,
            );
            failure(script, ObservationFailureKind::Inconsistent).await;
        }
    }
    let mut script = present_script();
    rows_mut(&mut script, ReadQuery::Databases, 1).clear();
    failure(script, ObservationFailureKind::Inconsistent).await;
}

#[tokio::test]
async fn health_and_progress_can_change_without_claiming_a_transactional_snapshot() {
    let mut script = present_script();
    set(
        &mut rows_mut(&mut script, ReadQuery::Databases, 1)[0],
        "local_database_state_desc",
        Some("RECOVERING"),
    );
    success(script).await;
}

#[tokio::test]
async fn wrong_expected_server_identity_is_rejected_before_detail_queries() {
    let mut script = absent_script();
    set(
        &mut rows_mut(&mut script, ReadQuery::Anchor, 0)[0],
        "server_name",
        Some("sql-other"),
    );
    let (result, session) = execute(script).await;
    assert_eq!(
        result.unwrap_err().kind,
        ObservationFailureKind::Inconsistent
    );
    assert_eq!(session.seen, [ReadQuery::Permissions, ReadQuery::Anchor]);
}

#[tokio::test]
async fn duplicate_rows_in_every_native_identity_domain_fail_closed() {
    for query in [
        ReadQuery::Permissions,
        ReadQuery::Anchor,
        ReadQuery::Replicas,
        ReadQuery::Databases,
        ReadQuery::DatabaseStates,
        ReadQuery::AutomaticSeeding,
        ReadQuery::PhysicalSeeding,
    ] {
        let mut script = present_script();
        let rows = rows_mut(&mut script, query, 0);
        rows.push(rows[0].clone());
        failure(script, ObservationFailureKind::Malformed).await;
    }
    let mut script = present_script();
    set(
        &mut rows_mut(&mut script, ReadQuery::Replicas, 0)[1],
        "replica_server_name",
        Some("SQL-0"),
    );
    failure(script, ObservationFailureKind::Malformed).await;
}

#[tokio::test]
async fn malformed_rows_null_required_fields_and_nil_guids_are_not_absence() {
    for query in [ReadQuery::Permissions, ReadQuery::Anchor] {
        let mut script = absent_script();
        rows_mut(&mut script, query, 0).clear();
        failure(script, ObservationFailureKind::Malformed).await;
    }
    for (query, column, value) in [
        (ReadQuery::Anchor, "server_name", None),
        (ReadQuery::Anchor, "group_id", Some("not-a-guid")),
        (
            ReadQuery::Anchor,
            "group_id",
            Some("00000000-0000-0000-0000-000000000000"),
        ),
        (
            ReadQuery::Anchor,
            "local_role_desc",
            Some("PRIMARY\nsecret"),
        ),
        (
            ReadQuery::Anchor,
            "product_version",
            Some("16.not-a-version"),
        ),
        (ReadQuery::Anchor, "product_major_version", Some("17")),
        (
            ReadQuery::Anchor,
            "sqlserver_start_time",
            Some("not-a-timestamp"),
        ),
        (
            ReadQuery::Anchor,
            "sqlserver_start_time",
            Some("2026-02-30T10:00:00"),
        ),
        (
            ReadQuery::Anchor,
            "sqlserver_start_time",
            Some("2026-08-02T25:00:00"),
        ),
        (
            ReadQuery::Anchor,
            "sequence_number",
            Some("9223372036854775808"),
        ),
        (ReadQuery::Anchor, "sequence_number", Some("-1")),
        (ReadQuery::Anchor, "sequence_number", None),
        (ReadQuery::Replicas, "state_group_id", None),
        (ReadQuery::Replicas, "is_local", None),
        (ReadQuery::Replicas, "is_local", Some("true")),
        (ReadQuery::Databases, "local_database_id", Some("0")),
        (
            ReadQuery::Databases,
            "local_database_id",
            Some("2147483648"),
        ),
        (
            ReadQuery::Databases,
            "recovery_fork_guid",
            Some("not-a-guid"),
        ),
        (ReadQuery::DatabaseStates, "is_local", None),
        (ReadQuery::DatabaseStates, "is_suspended", Some("2")),
        (ReadQuery::AutomaticSeeding, "operation_id", None),
        (
            ReadQuery::AutomaticSeeding,
            "number_of_attempts",
            Some("-1"),
        ),
        (
            ReadQuery::PhysicalSeeding,
            "database_size_bytes",
            Some(ABOVE_I64),
        ),
    ] {
        let mut script = present_script();
        set(&mut rows_mut(&mut script, query, 0)[0], column, value);
        failure(script, ObservationFailureKind::Malformed).await;
    }
    let mut script = absent_script();
    rows_mut(&mut script, ReadQuery::Permissions, 0)[0].remove("view_any_definition");
    failure(script, ObservationFailureKind::Malformed).await;
    let mut script = absent_script();
    set(
        &mut rows_mut(&mut script, ReadQuery::Anchor, 0)[0],
        "local_role_desc",
        Some("PRIMARY"),
    );
    failure(script, ObservationFailureKind::Malformed).await;
}

#[tokio::test]
async fn malformed_progress_cannot_be_rounded_narrowed_or_defaulted() {
    for value in [
        "",
        "-1",
        "+1",
        "1.0",
        "1e20",
        " 1",
        "1 ",
        "１２３",
        "10000000000000000000000000",
    ] {
        for column in ["last_hardened_lsn", "last_redone_lsn", "last_commit_lsn"] {
            let mut script = present_script();
            set(
                &mut rows_mut(&mut script, ReadQuery::DatabaseStates, 0)[0],
                column,
                Some(value),
            );
            failure(script, ObservationFailureKind::Malformed).await;
        }
    }
}

#[tokio::test]
async fn rejects_unsupported_ag_and_replica_configuration_without_inferred_health() {
    for (column, value) in [
        ("cluster_type", "0"),
        ("cluster_type", "1"),
        ("cluster_type_desc", "NONE"),
        ("cluster_type_desc", "none"),
        ("cluster_type_desc", "FUTURE_CLUSTER"),
        ("cluster_type_desc", "external_other"),
        ("required_synchronized_secondaries_to_commit", "0"),
        ("required_synchronized_secondaries_to_commit", "2"),
        ("basic_features", "1"),
        ("is_distributed", "1"),
    ] {
        let mut script = present_script();
        set(
            &mut rows_mut(&mut script, ReadQuery::Anchor, 0)[0],
            column,
            Some(value),
        );
        failure(script, ObservationFailureKind::Unsupported).await;
    }
    for (column, value) in [
        ("availability_mode", "0"),
        ("availability_mode", "4"),
        ("availability_mode_desc", "ASYNCHRONOUS_COMMIT"),
        ("failover_mode", "0"),
        ("failover_mode", "1"),
        ("failover_mode_desc", "MANUAL"),
        ("seeding_mode", "1"),
        ("seeding_mode_desc", "MANUAL"),
    ] {
        let mut script = present_script();
        set(
            &mut rows_mut(&mut script, ReadQuery::Replicas, 0)[2],
            column,
            Some(value),
        );
        failure(script, ObservationFailureKind::Unsupported).await;
    }
}

#[tokio::test]
async fn excessive_database_and_replica_counts_are_unsupported_not_silently_truncated() {
    let mut script = present_script();
    let mut extra = database();
    extra.extend(local_database_metadata(false));
    set(&mut extra, "group_database_id", Some(DIFFERENT));
    set(&mut extra, "database_name", Some("another_db"));
    rows_mut(&mut script, ReadQuery::Databases, 0).push(extra);
    failure(script, ObservationFailureKind::Unsupported).await;
    let mut script = present_script();
    rows_mut(&mut script, ReadQuery::Replicas, 0).push(replica(DIFFERENT, "sql-3", None));
    failure(script, ObservationFailureKind::Unsupported).await;
}

#[tokio::test]
async fn diagnostics_never_include_malformed_server_text_or_query_values() {
    const SENTINEL: &str = "sensitive-credential-and-raw-server-message";
    let mut script = present_script();
    set(
        &mut rows_mut(&mut script, ReadQuery::DatabaseStates, 0)[0],
        "last_hardened_lsn",
        Some(SENTINEL),
    );
    let error = failure(script, ObservationFailureKind::Malformed).await;
    assert!(!error.to_string().contains(SENTINEL));
    assert!(!format!("{error:?}").contains(SENTINEL));
    assert!(!error.to_string().contains(ReadQuery::DatabaseStates.sql()));
    let mut script = absent_script();
    set(
        &mut rows_mut(&mut script, ReadQuery::Anchor, 0)[0],
        "edition",
        Some(SENTINEL),
    );
    let error = failure(script, ObservationFailureKind::Unsupported).await;
    assert!(!error.to_string().contains(SENTINEL));
}

#[tokio::test]
async fn ag_names_with_sql_metacharacters_are_only_bound_parameters() {
    let mut target = target();
    target.availability_group = AvailabilityGroupName::new("ag']; SELECT secret;--").unwrap();
    let mut session = ScriptedSession {
        steps: absent_script().into(),
        seen: Vec::new(),
        expected_group: target.availability_group.clone(),
    };
    let snapshot = observe_session(&mut session, &target, OBSERVED_AT)
        .await
        .unwrap();
    assert!(matches!(
        snapshot.availability_group,
        Observation::Absent { .. }
    ));
    assert!(session.steps.is_empty());
    for query in ReadQuery::ALL {
        assert!(!query.sql().contains(target.availability_group.as_str()));
    }
}

// Only inspect projection boundaries, not SQL formatting or an entire query
// golden file. Nested CONVERT/HAS_PERMS calls contain non-projecting commas.
fn select_expressions(sql: &str) -> Vec<&str> {
    let projection = sql
        .strip_prefix("SELECT\n")
        .unwrap()
        .split("\nFROM ")
        .next()
        .unwrap()
        .split("\nWHERE ")
        .next()
        .unwrap();
    let mut expressions = Vec::new();
    let mut start = 0;
    let mut depth = 0usize;
    let mut quoted = false;
    for (index, byte) in projection.bytes().enumerate() {
        match byte {
            b'\'' => quoted = !quoted,
            b'(' if !quoted => depth += 1,
            b')' if !quoted => depth = depth.checked_sub(1).expect("balanced projection"),
            b',' if !quoted && depth == 0 => {
                expressions.push(projection[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }
    assert_eq!(depth, 0);
    assert!(!quoted);
    expressions.push(projection[start..].trim());
    expressions
}

fn selected_names(query: ReadQuery) -> BTreeSet<String> {
    select_expressions(query.sql())
        .iter()
        .map(|expression| {
            let (value, alias) = expression.rsplit_once(" AS ").expect("named projection");
            assert!(
                value.starts_with("CONVERT(nvarchar("),
                "every value must reach TDS as nvarchar"
            );
            assert!(
                alias
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            );
            alias.to_owned()
        })
        .collect()
}

#[test]
fn query_set_is_closed_parameterized_named_nvarchar_selects_without_side_effects() {
    let mut labels = BTreeSet::new();
    for query in ReadQuery::ALL {
        assert!(labels.insert(query.label()));
        let sql = query.sql();
        assert!(sql.trim_start().starts_with("SELECT"));
        assert_eq!(sql.matches(';').count(), 1, "one statement per query");
        assert!(sql.trim_end().ends_with(';'));
        assert!(sql.contains("@P1"));
        let tokens: BTreeSet<String> = sql
            .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
            .filter(|token| !token.is_empty())
            .map(str::to_ascii_uppercase)
            .collect();
        for forbidden in [
            "INSERT", "UPDATE", "DELETE", "MERGE", "EXEC", "EXECUTE", "CREATE", "DROP", "ALTER",
            "TRUNCATE", "USE", "BEGIN", "COMMIT", "ROLLBACK", "GRANT", "DENY", "REVOKE", "BACKUP",
            "RESTORE", "INTO",
        ] {
            assert!(
                !tokens.contains(forbidden),
                "unexpected SQL operation in {}",
                query.label()
            );
        }
        let expressions = select_expressions(sql);
        assert_eq!(
            selected_names(query).len(),
            expressions.len(),
            "aliases must be unique"
        );
        assert!(!sql.contains("failure_message"));
        assert!(!sql.contains("last_connect_error_description"));
        assert!(!sql.contains("write_lease"));
    }
}

#[test]
fn column_metadata_contract_matches_every_select_alias_in_wire_order() {
    for query in ReadQuery::ALL {
        let aliases: Vec<&str> = select_expressions(query.sql())
            .into_iter()
            .map(|expression| expression.rsplit_once(" AS ").unwrap().1)
            .collect();
        assert_eq!(
            query.columns(),
            aliases,
            "column metadata contract for {}",
            query.label()
        );
    }
}

#[test]
fn all_scripted_base_rows_match_the_predefined_query_projection() {
    for step in present_script() {
        let names = step
            .query
            .columns()
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<BTreeSet<_>>();
        for row in step.result.unwrap() {
            assert_eq!(
                row.keys().cloned().collect::<BTreeSet<_>>(),
                names,
                "fixture schema for {}",
                step.query.label()
            );
        }
    }
}

#[test]
fn progress_and_native_guid_joins_follow_the_documented_dmv_contract() {
    let sql = ReadQuery::DatabaseStates
        .sql()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    for column in ["last_hardened_lsn", "last_redone_lsn", "last_commit_lsn"] {
        assert!(sql.contains(&format!("CONVERT(nvarchar(25), drs.{column}) AS {column}")));
    }
    assert!(sql.contains("ar.group_id = drs.group_id AND ar.replica_id = drs.replica_id"));
    assert!(
        sql.contains(
            "adc.group_id = drs.group_id AND adc.group_database_id = drs.group_database_id"
        )
    );
    assert!(sql.contains("drs.is_local = 1 AND d.database_id = drs.database_id"));
    assert!(
        sql.contains(
            "d.group_database_id = drs.group_database_id AND d.replica_id = drs.replica_id"
        )
    );
    assert!(sql.contains("drs.is_local = 1 AND recovery.database_id = d.database_id"));
    assert!(sql.contains("recovery.recovery_fork_guid"));
    assert!(!sql.contains("drs.recovery_fork_guid"));
    let automatic = ReadQuery::AutomaticSeeding.sql();
    assert!(automatic.contains("adc.group_database_id = seed.ag_db_id"));
    assert!(automatic.contains("ar.replica_id = seed.ag_remote_replica_id"));
    let physical = ReadQuery::PhysicalSeeding.sql();
    assert!(!physical.contains("operation_id"));
    assert!(!physical.contains("sys.dm_hadr_automatic_seeding"));
    assert!(physical.contains("adc.group_database_id = d.group_database_id"));
}
