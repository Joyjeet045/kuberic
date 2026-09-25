use std::fs;

use bytes::Bytes;
use kuberic_agent::AgentError;
use kuberic_agent::provisioning::{
    InitializationAuthority, ObservedStorageIdentity, StorePresence, authorize_initialization,
    inspect_store, validate_established_identity,
};
use kuberic_agent::sqlite_store::SqliteStore;
use kuberic_agent::state::{AgentState, CoordinatorStage, ReconfigurationRecord, SCHEMA_VERSION};
use kuberic_agent::store::AgentStore;
use kuberic_protocol::command::{EnsureConfiguration, InitializeAgentStore};
use kuberic_protocol::types::{
    AgentGeneration, ConfigurationDescriptor, ConfigurationMember, EffectivePolicy, Epoch,
    OperationId, PodUid, ProvisioningIntent, PvcUid, ReplicaId, ReplicaIdentity, ReplicaInstanceId,
    ReplicaRole, ResourceUid, SwitchoverHandoff, SwitchoverRequestId, TransitionId,
    TransitionIntent, TransitionKind, derive_agent_generation, derive_initialization_id,
};
use kuberic_runtime_internal::authority::{
    AdmittedAuthority, AuthorityFence, DurableLocalWrite, LocalWriteJournal, LocalWritePhase,
    ReplicaAuthorityStore, ReplicationProgress, ReplicationProgressStore,
};
use rusqlite::Connection;
use serde_json::Value;
use tempfile::tempdir;

fn identity(replica_id: i64, instance: &str, generation: &str) -> ReplicaIdentity {
    ReplicaIdentity {
        replica_id: ReplicaId::new(replica_id),
        instance_id: ReplicaInstanceId::new(instance),
        agent_generation: AgentGeneration::new(generation),
    }
}

fn bootstrap_fixture() -> (
    InitializeAgentStore,
    ObservedStorageIdentity,
    TransitionIntent,
) {
    let policy = EffectivePolicy::fixed(1, 30).unwrap();
    let resource_uid = ResourceUid::new("resource-1");
    let pod_uid = PodUid::new("pod-instance-1");
    let pvc_uid = PvcUid::new("pvc-uid-1");
    let initialization_id =
        derive_initialization_id(&resource_uid, ReplicaId::new(1), &pod_uid, &pvc_uid);
    let local = identity(
        1,
        pod_uid.as_str(),
        derive_agent_generation(&initialization_id).as_str(),
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        local.replica_id,
        vec![ConfigurationMember {
            identity: local.clone(),
            role: ReplicaRole::Primary,
        }],
        policy.write_quorum,
    );
    let command = InitializeAgentStore {
        initialization_id,
        resource_uid,
        local_replica_id: local.replica_id,
        expected_instance_id: local.instance_id.clone(),
        expected_pod_uid: pod_uid,
        expected_pvc_uid: pvc_uid,
        assigned_agent_generation: local.agent_generation.clone(),
        effective_policy: policy.clone(),
        bootstrap_configuration: current.clone(),
        provisioning: None,
    };
    let observed = ObservedStorageIdentity {
        resource_uid: command.resource_uid.clone(),
        pod_uid: command.expected_pod_uid.clone(),
        pvc_uid: command.expected_pvc_uid.clone(),
        instance_id: command.expected_instance_id.clone(),
    };
    let transition = TransitionIntent {
        transition_id: TransitionId::new("bootstrap-1"),
        kind: TransitionKind::Bootstrap,
        spec_generation: 1,
        effective_policy: policy,
        previous_configuration_id: None,
        current_configuration: current,
        election_lsn: None,
        build_id: None,
        repair: None,
        switchover: None,
    };
    (command, observed, transition)
}

#[test]
fn store_presence_distinguishes_fresh_from_missing_established_metadata() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());

    assert_eq!(
        inspect_store(&path, false),
        StorePresence::FreshUninitialized
    );
    assert_eq!(
        inspect_store(&path, true),
        StorePresence::UnsafeMissingEstablished
    );
}

#[test]
fn fresh_bootstrap_store_requires_exact_persisted_authority() {
    let (command, observed, transition) = bootstrap_fixture();
    let storage_identity = authorize_initialization(
        &command,
        &observed,
        InitializationAuthority::Bootstrap(&transition),
    )
    .unwrap();
    assert_eq!(storage_identity.schema_version, SCHEMA_VERSION);

    let mut stale = command.clone();
    stale.expected_pvc_uid = PvcUid::new("other-pvc");
    assert!(matches!(
        authorize_initialization(
            &stale,
            &observed,
            InitializationAuthority::Bootstrap(&transition)
        ),
        Err(AgentError::InitializationNotAuthorized(_))
    ));
}

#[test]
fn fresh_replacement_store_requires_matching_provisioning_intent() {
    let (command, observed, _) = bootstrap_fixture();
    let provisioning = ProvisioningIntent {
        replaces: ReplicaIdentity {
            replica_id: command.local_replica_id,
            instance_id: ReplicaInstanceId::new("old-pod"),
            agent_generation: AgentGeneration::new("old-generation"),
        },
        pod_uid: command.expected_pod_uid.clone(),
        pvc_uid: command.expected_pvc_uid.clone(),
        operation_id: OperationId::new("replace-1"),
    };
    authorize_initialization(
        &command,
        &observed,
        InitializationAuthority::Replacement(&provisioning),
    )
    .unwrap();
}

#[tokio::test]
async fn sqlite_store_reopens_with_identity_authority_and_progress() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let (command, observed, transition) = bootstrap_fixture();
    let storage_identity = authorize_initialization(
        &command,
        &observed,
        InitializationAuthority::Bootstrap(&transition),
    )
    .unwrap();
    let state = AgentState::new(storage_identity.clone());
    let store = SqliteStore::create_authorized(&path, state).unwrap();

    let authority = AdmittedAuthority {
        local_identity: storage_identity.local_identity.clone(),
        transition_kind: Some(TransitionKind::Bootstrap),
        previous_configuration: None,
        current_configuration: transition.current_configuration.clone(),
        switchover_handoff: None,
    };
    store.admit(&authority).await.unwrap();
    let progress = ReplicationProgress {
        fence: AuthorityFence {
            epoch: transition.current_configuration.epoch,
            previous_configuration_id: None,
            current_configuration_id: transition.current_configuration.configuration_id.clone(),
        },
        verified_lsn: 7,
    };
    store.record_replication_progress(&progress).await.unwrap();
    let write = DurableLocalWrite {
        operation_id: OperationId::new("write-1"),
        lsn: 8,
        committed_lsn: 7,
        data: Bytes::from_static(b"value"),
        phase: LocalWritePhase::Registered,
    };
    store.record_local_write(&write).await.unwrap();
    drop(store);

    let reopened = SqliteStore::open_existing(&path, Some(&storage_identity)).unwrap();
    reopened
        .migrate_schema(SCHEMA_VERSION, SCHEMA_VERSION)
        .await
        .unwrap();
    assert_eq!(reopened.identity().await.unwrap(), storage_identity);
    assert_eq!(reopened.load().await.unwrap(), Some(authority));
    assert_eq!(
        reopened
            .load_replication_progress(&progress.fence)
            .await
            .unwrap(),
        Some(progress)
    );
    assert_eq!(
        reopened
            .load_local_write(&write.operation_id)
            .await
            .unwrap(),
        Some(write)
    );
}

#[test]
fn established_store_rejects_identity_schema_and_corruption_mismatches() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let (command, observed, transition) = bootstrap_fixture();
    let storage_identity = authorize_initialization(
        &command,
        &observed,
        InitializationAuthority::Bootstrap(&transition),
    )
    .unwrap();
    let store =
        SqliteStore::create_authorized(&path, AgentState::new(storage_identity.clone())).unwrap();
    drop(store);

    let mut other_identity = storage_identity.clone();
    other_identity.pod_uid = PodUid::new("different-pod");
    assert!(matches!(
        SqliteStore::open_existing(&path, Some(&other_identity)),
        Err(AgentError::IdentityMismatch(_))
    ));

    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.pragma_update(None, "user_version", 99).unwrap();
    }

    assert!(matches!(
        SqliteStore::open_existing(&path, Some(&storage_identity)),
        Err(AgentError::SchemaMismatch {
            expected: SCHEMA_VERSION,
            observed: 99
        })
    ));

    fs::write(&path, b"not a sqlite database").unwrap();
    assert!(matches!(
        SqliteStore::open_existing(&path, Some(&storage_identity)),
        Err(AgentError::Corrupt(_))
    ));
}

#[test]
fn established_identity_requires_the_same_observed_pod_and_pvc() {
    let (command, observed, transition) = bootstrap_fixture();
    let storage_identity = authorize_initialization(
        &command,
        &observed,
        InitializationAuthority::Bootstrap(&transition),
    )
    .unwrap();
    validate_established_identity(&storage_identity, &observed, ReplicaId::new(1)).unwrap();

    let mismatched = ObservedStorageIdentity {
        pod_uid: PodUid::new("replacement-pod"),
        instance_id: ReplicaInstanceId::new("replacement-pod"),
        ..observed
    };
    assert!(matches!(
        validate_established_identity(&storage_identity, &mismatched, ReplicaId::new(1)),
        Err(AgentError::IdentityMismatch(_))
    ));
}

#[tokio::test]
async fn current_only_completion_retires_exact_switchover_preparation() {
    let directory = tempdir().unwrap();
    let (initialize, observed, transition) = bootstrap_fixture();
    let storage_identity = authorize_initialization(
        &initialize,
        &observed,
        InitializationAuthority::Bootstrap(&transition),
    )
    .unwrap();
    let current = initialize.bootstrap_configuration.clone();
    let target = identity(2, "pod-2", "generation-2");
    let handoff = SwitchoverHandoff {
        preparation_generation: 1,
        preparation_operation_id: OperationId::new("prepare-1"),
        request_id: SwitchoverRequestId::new("request-1"),
        source: storage_identity.local_identity.clone(),
        target,
        starting_configuration_id: current.configuration_id.clone(),
        starting_epoch: current.epoch,
        handoff_lsn: 7,
    };
    let command = EnsureConfiguration {
        operation_id: OperationId::new("current-only-1"),
        previous_configuration: None,
        current_configuration: current.clone(),
        previous_epoch: None,
        current_epoch: current.epoch,
        effective_policy: storage_identity.effective_policy.clone(),
        local_replica_id: storage_identity.local_identity.replica_id,
        expected_instance_id: storage_identity.local_identity.instance_id.clone(),
        expected_agent_generation: storage_identity.local_identity.agent_generation.clone(),
        transition_kind: TransitionKind::PlannedSwitchover,
        failover_safe_lsn: None,
        primary_write_status: kuberic_protocol::types::AccessStatus::ReconfigurationPending,
        current_only: true,
        retire_build_ids: Vec::new(),
        switchover_handoff: Some(handoff.clone()),
        retire_switchover_preparation_ids: vec![handoff.preparation()],
    };
    let mut state = AgentState::new(storage_identity);
    state.highest_epoch = current.epoch;
    state.current_configuration = Some(current);
    state.role = ReplicaRole::Primary;
    state.prepared_switchover = Some(handoff);
    state.reconfiguration = Some(ReconfigurationRecord {
        command,
        stage: CoordinatorStage::Complete,
        observed_lsn: None,
    });
    let store = SqliteStore::create_authorized(
        SqliteStore::metadata_database_path(directory.path()),
        state,
    )
    .unwrap();

    store
        .complete_configuration(&OperationId::new("current-only-1"))
        .await
        .unwrap();
    assert!(
        store
            .load_state()
            .await
            .unwrap()
            .prepared_switchover
            .is_none()
    );
}

#[tokio::test]
async fn additive_handoff_fields_default_when_reopening_legacy_json() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let (initialize, observed, transition) = bootstrap_fixture();
    let storage_identity = authorize_initialization(
        &initialize,
        &observed,
        InitializationAuthority::Bootstrap(&transition),
    )
    .unwrap();
    let authority = AdmittedAuthority {
        local_identity: storage_identity.local_identity.clone(),
        transition_kind: Some(TransitionKind::Bootstrap),
        previous_configuration: None,
        current_configuration: initialize.bootstrap_configuration,
        switchover_handoff: None,
    };
    let store = SqliteStore::create_authorized(&path, AgentState::new(storage_identity)).unwrap();
    store.admit(&authority).await.unwrap();
    drop(store);

    let connection = Connection::open(&path).unwrap();
    for (table, column, removed_key) in [
        ("agent_state", "state_json", "preparedSwitchover"),
        ("replica_authority", "authority_json", "switchover_handoff"),
    ] {
        let mut json: Value = connection
            .query_row(
                &format!("SELECT {column} FROM {table} WHERE singleton = 1"),
                [],
                |row| row.get::<_, String>(0),
            )
            .map(|json| serde_json::from_str(&json).unwrap())
            .unwrap();
        let object = json.as_object_mut().unwrap();
        assert!(
            object.remove(removed_key).is_some(),
            "legacy fixture must remove {removed_key} from {table}"
        );
        connection
            .execute(
                &format!("UPDATE {table} SET {column} = ?1 WHERE singleton = 1"),
                [serde_json::to_string(&json).unwrap()],
            )
            .unwrap();
    }
    drop(connection);

    let reopened = SqliteStore::open_existing(&path, None).unwrap();
    assert!(
        reopened
            .load_state()
            .await
            .unwrap()
            .prepared_switchover
            .is_none()
    );
    assert_eq!(reopened.load().await.unwrap(), Some(authority));
}

#[test]
fn runtime_public_root_has_only_sf_shaped_modules() {
    let root = include_str!("../../kuberic-runtime/src/lib.rs");
    for forbidden in [
        "pub mod runtime",
        "pub mod authority",
        "pub mod effects",
        "PodRuntime",
        "RuntimeEffect",
        "AuthorityStore",
        "ManagedReplicator",
    ] {
        assert!(
            !root.contains(forbidden),
            "kuberic-runtime root exposes {forbidden}"
        );
    }
}
