use std::fs;

use bytes::Bytes;
use kuberic_agent::AgentError;
use kuberic_agent::provisioning::{
    InitializationAuthority, ObservedStorageIdentity, StorePresence, authorize_initialization,
    inspect_store, validate_established_identity,
};
use kuberic_agent::sqlite_store::SqliteStore;
use kuberic_agent::state::{AgentState, CoordinatorStage, ReconfigurationRecord, SCHEMA_VERSION};
use kuberic_agent::state::{EffectStage, PendingEffect, RetainedResult};
use kuberic_agent::store::{AgentStore, BeginConfiguration, BeginEffect};
use kuberic_protocol::command::AcceptSecondaryRemovalCommit;
use kuberic_protocol::command::{EnsureConfiguration, EnsureReplicaBuild, InitializeAgentStore};
use kuberic_protocol::types::{AccessStatus, SecondaryRemovalStage};
use kuberic_protocol::types::{
    AgentGeneration, ConfigurationDescriptor, ConfigurationMember, EffectivePolicy, Epoch,
    OperationId, PodUid, ProvisioningIntent, ProvisioningPurpose, PvcUid, ReplicaId,
    ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid, ScaleUpProvisioning,
    SwitchoverHandoff, SwitchoverRequestId, TransitionId, TransitionIntent, TransitionKind,
    derive_agent_generation, derive_initialization_id,
};
use kuberic_runtime_internal::authority::{
    AdmittedAuthority, AuthorityFence, BuildAuthority, BuildAuthorityKind, BuildAuthorityStore,
    BuildProgressStore, DurableBuildProgress, DurableLocalWrite, LocalWriteJournal,
    LocalWritePhase, ReplicaAuthorityStore, ReplicationProgress, ReplicationProgressStore,
};
use kuberic_runtime_internal::effects::{
    RuntimeEffect, RuntimeEffectAction, RuntimeEffectResult, RuntimePostcondition,
};
use rusqlite::Connection;
use serde_json::Value;
use tempfile::tempdir;

#[allow(dead_code)]
#[path = "../../kuberic-protocol/tests/support/secondary_scale_down.rs"]
mod removal_fixture;

fn pending_acceptance_fixture() -> (
    AgentState,
    RuntimeEffect,
    RuntimeEffect,
    RuntimeEffectResult,
) {
    let intent = removal_fixture::intent(&[1, 2, 3], 1);
    let committed = removal_fixture::cleanup(&intent);
    let local = intent.current_configuration.members[1].identity.clone();
    let (initialize, observed, transition) = bootstrap_fixture();
    let mut identity = authorize_initialization(
        &initialize,
        &observed,
        InitializationAuthority::Bootstrap(&transition),
    )
    .unwrap();
    identity.resource_uid = intent.resource_uid.clone();
    identity.local_identity = local.clone();
    identity.pod_uid = PodUid::new(local.instance_id.as_str());
    identity.effective_policy = intent.previous_policy.clone();
    let mut state = AgentState::new(identity);
    state.current_configuration = Some(intent.current_configuration.clone());
    state.highest_epoch = intent.current_configuration.epoch;
    state.admitted_policy = Some(intent.current_policy.clone());
    state.secondary_removal_evidence = Some(committed.evidence.clone());
    state.role = ReplicaRole::ActiveSecondary;
    state.next_effect_sequence = 7;
    let ordinary = RuntimeEffect {
        operation_id: intent.command_operation_id(SecondaryRemovalStage::AcceptCommit, &local),
        sequence: 7,
        action: RuntimeEffectAction::AcceptSecondaryRemovalCommit(Box::new(committed.clone())),
    };
    let historical = RuntimeEffect {
        action: RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(Box::new(
            AcceptSecondaryRemovalCommit {
                operation_id: ordinary.operation_id.clone(),
                target: local.clone(),
                committed: committed.clone(),
                local_recovery: true,
            },
        )),
        ..ordinary.clone()
    };
    state.pending_effect = Some(PendingEffect {
        effect: ordinary.clone(),
        stage: EffectStage::IntentCommitted,
    });
    let result = RuntimeEffectResult {
        operation_id: ordinary.operation_id.clone(),
        sequence: ordinary.sequence,
        postcondition: RuntimePostcondition {
            open: true,
            role: state.role,
            role_transition: None,
            read_status: state.read_status,
            write_status: state.write_status,
            authority: Some(AdmittedAuthority {
                local_identity: local,
                transition_kind: None,
                previous_configuration: None,
                current_configuration: intent.current_configuration,
                switchover_handoff: None,
                scale_up: None,
                secondary_removal: Some(committed.evidence.clone()),
            }),
            prepared_secondary_removal: None,
            retired_authority: None,
            accepted_secondary_removal: Some(committed),
            current_progress: 10,
            verified_replication_lsn: Some(10),
            committed_lsn: 10,
            current_configuration_quorum_progress: 0,
            catch_up_boundary: Some(10),
            catch_up_complete: false,
            builds: Vec::new(),
        },
    };
    (state, ordinary, historical, result)
}

#[tokio::test]
async fn pending_acceptance_conversion_is_atomic_one_way_and_preserves_stage_and_results() {
    for stage in [EffectStage::IntentCommitted, EffectStage::EffectApplied] {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let (mut state, ordinary, historical, result) = pending_acceptance_fixture();
        state.pending_effect.as_mut().unwrap().stage = stage;
        let mut retained = RetainedResult {
            operation_id: OperationId::new("previous-effect"),
            effect: ordinary.clone(),
            result: result.clone(),
        };
        retained.effect.operation_id = retained.operation_id.clone();
        retained.result.operation_id = retained.operation_id.clone();
        state.retained_result = Some(retained.clone());
        let store = SqliteStore::create_authorized(&path, state.clone()).unwrap();
        let authority = result.postcondition.authority.as_ref().unwrap();
        store.admit(authority).await.unwrap();
        store
            .record_replication_progress(&ReplicationProgress {
                fence: authority.fence(),
                verified_lsn: 10,
            })
            .await
            .unwrap();
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER reject_conversion BEFORE UPDATE ON agent_state
             BEGIN SELECT RAISE(FAIL, 'conversion write failed'); END;",
            )
            .unwrap();
        assert!(matches!(
            store.begin_effect(&historical).await,
            Err(AgentError::Sqlite(_))
        ));
        drop(store);
        let store = SqliteStore::open_existing(&path, Some(&state.identity)).unwrap();
        assert_eq!(store.load_state().await.unwrap(), state);
        connection
            .execute_batch("DROP TRIGGER reject_conversion;")
            .unwrap();
        assert_eq!(
            store.begin_effect(&historical).await.unwrap(),
            BeginEffect::Pending(historical.clone())
        );
        state.pending_effect.as_mut().unwrap().effect = historical.clone();
        assert_eq!(store.load_state().await.unwrap(), state);
        drop(store);
        let store = SqliteStore::open_existing(&path, Some(&state.identity)).unwrap();
        assert_eq!(store.load_state().await.unwrap(), state);
        assert_eq!(
            store.begin_effect(&historical).await.unwrap(),
            BeginEffect::Pending(historical.clone())
        );
        assert!(
            store.begin_effect(&ordinary).await.is_err(),
            "no widening back to live acceptance"
        );
        assert!(store.mark_effect_applied(&ordinary).await.is_err());
        assert!(store.cancel_effect(&ordinary).await.is_err());
        assert_eq!(store.load_state().await.unwrap(), state);
        store.mark_effect_applied(&historical).await.unwrap();
        store.complete_effect(&result).await.unwrap();
        drop(store);
        let store = SqliteStore::open_existing(&path, Some(&state.identity)).unwrap();
        let completed = store.load_state().await.unwrap();
        assert!(completed.pending_effect.is_none());
        assert_eq!(completed.next_effect_sequence, 8);
        assert_eq!(
            completed.removal_effects[&historical.operation_id].effect,
            historical
        );
        assert_eq!(
            completed.removal_effects[&historical.operation_id].result,
            result
        );
        assert_eq!(
            store.begin_effect(&historical).await.unwrap(),
            BeginEffect::Completed(Box::new(result))
        );
        assert!(store.begin_effect(&ordinary).await.is_err());
        assert_eq!(store.load_state().await.unwrap(), completed);
        assert!(
            store
                .load_secondary_removal_commit()
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn pending_acceptance_conversion_rejects_mutation_and_incompatible_durable_state() {
    for mutation in 0..21 {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let (mut state, ordinary, mut historical, result) = pending_acceptance_fixture();
        let mut authority = result.postcondition.authority.clone().unwrap();
        let mut progress = ReplicationProgress {
            fence: authority.fence(),
            verified_lsn: 10,
        };
        let RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(command) =
            &mut historical.action
        else {
            unreachable!()
        };
        match mutation {
            0 => historical.operation_id = OperationId::new("other-operation"),
            1 => historical.sequence += 1,
            2 => command.operation_id = OperationId::new("other-operation"),
            3 => command.committed.current_only_write_quorum[0].report_sequence += 1,
            4 => command.target.instance_id = ReplicaInstanceId::new("other-instance"),
            5 => command.target.agent_generation = AgentGeneration::new("other-generation"),
            6 => command.target = command.committed.evidence.preparation.intent.target.clone(),
            7 => command.local_recovery = false,
            8 => state.identity.resource_uid = ResourceUid::new("other-resource"),
            9 => state.highest_epoch.configuration_number -= 1,
            10 => state.highest_epoch.configuration_number += 1,
            11 => state.write_status = AccessStatus::Granted,
            12 => {
                state.previous_configuration = Some(
                    command
                        .committed
                        .evidence
                        .preparation
                        .intent
                        .previous_configuration
                        .clone(),
                )
            }
            13 => state.pending_effect.as_mut().unwrap().effect.action = RuntimeEffectAction::Close,
            14 => {
                let pending = state.pending_effect.as_mut().unwrap();
                pending.stage = EffectStage::EffectApplied;
                let RuntimeEffectAction::AcceptSecondaryRemovalCommit(committed) =
                    &mut pending.effect.action
                else {
                    unreachable!()
                };
                committed.current_only_write_quorum[0].report_sequence += 1;
            }
            15 => progress.verified_lsn = 9,
            16 => {
                authority.local_identity.agent_generation = AgentGeneration::new("other-generation")
            }
            17 => {
                state.retained_result = Some(RetainedResult {
                    operation_id: ordinary.operation_id.clone(),
                    effect: ordinary.clone(),
                    result: result.clone(),
                });
            }
            18 => {
                state.removal_effects.insert(
                    ordinary.operation_id.clone(),
                    RetainedResult {
                        operation_id: ordinary.operation_id.clone(),
                        effect: ordinary.clone(),
                        result: result.clone(),
                    },
                );
            }
            19 => {
                state.current_configuration = Some(
                    command
                        .committed
                        .evidence
                        .preparation
                        .intent
                        .previous_configuration
                        .clone(),
                )
            }
            _ => {
                command.committed.evidence.preparation.intent.cleanup.pvc =
                    kuberic_protocol::types::CleanupResourceIdentity::Present {
                        name: "changed-pvc".into(),
                        uid: "changed-uid".into(),
                    }
            }
        }
        let store = SqliteStore::create_authorized(&path, state.clone()).unwrap();
        // Seed incompatible durable runtime state directly, including states impossible
        // through normal authority admission, to verify conversion fails closed.
        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "INSERT INTO replica_authority(singleton, authority_json) VALUES(1, ?1)",
                [serde_json::to_string(&authority).unwrap()],
            )
            .unwrap();
        store.record_replication_progress(&progress).await.unwrap();
        assert!(
            store.begin_effect(&historical).await.is_err(),
            "mutation {mutation}"
        );
        drop(store);
        let store = SqliteStore::open_existing(&path, None).unwrap();
        assert_eq!(
            store.load_state().await.unwrap(),
            state,
            "mutation {mutation}"
        );
        assert_eq!(store.load().await.unwrap(), Some(authority));
    }
}

#[tokio::test]
async fn schema_two_is_rejected_without_migration_or_provenance_changes() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let (command, observed, transition) = bootstrap_fixture();
    let identity = authorize_initialization(
        &command,
        &observed,
        InitializationAuthority::Bootstrap(&transition),
    )
    .unwrap();
    let store = SqliteStore::create_authorized(&path, AgentState::new(identity.clone())).unwrap();
    assert!(store.migrate_schema(2, 3).await.is_err());
    drop(store);
    let connection = Connection::open(&path).unwrap();
    connection.pragma_update(None, "user_version", 2).unwrap();
    let original: String = connection
        .query_row("SELECT state_json FROM agent_state", [], |r| r.get(0))
        .unwrap();
    assert!(matches!(
        SqliteStore::open_existing(&path, None),
        Err(AgentError::SchemaMismatch {
            expected: 3,
            observed: 2
        })
    ));
    assert_eq!(
        connection
            .query_row("PRAGMA user_version", [], |r| r.get::<_, u32>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        connection
            .query_row("SELECT state_json FROM agent_state", [], |r| r
                .get::<_, String>(0))
            .unwrap(),
        original
    );
}

#[tokio::test]
async fn durable_retirement_rejects_active_authority_and_mutated_tombstones() {
    use kuberic_runtime_internal::authority::RetiredAuthority;
    let intent = removal_fixture::intent(&[1, 2], 1);
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let (command, observed, transition) = bootstrap_fixture();
    let mut identity = authorize_initialization(
        &command,
        &observed,
        InitializationAuthority::Bootstrap(&transition),
    )
    .unwrap();
    identity.resource_uid = intent.resource_uid.clone();
    identity.local_identity = intent.target.clone();
    identity.effective_policy = intent.previous_policy.clone();
    let store = SqliteStore::create_authorized(&path, AgentState::new(identity.clone())).unwrap();
    let active = AdmittedAuthority {
        local_identity: intent.target.clone(),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: intent.previous_configuration.clone(),
        switchover_handoff: None,
        scale_up: None,
        secondary_removal: None,
    };
    store.admit(&active).await.unwrap();
    let retired = RetiredAuthority {
        committed: removal_fixture::cleanup(&intent),
        report: removal_fixture::retirement(&intent),
    };
    let mut wrong_intent = intent.clone();
    wrong_intent.resource_uid = ResourceUid::new("another-resource");
    wrong_intent.operation_id = wrong_intent.expected_operation_id();
    let wrong_resource = RetiredAuthority {
        committed: removal_fixture::cleanup(&wrong_intent),
        report: removal_fixture::retirement(&wrong_intent),
    };
    assert!(
        store
            .record_retirement_started(&wrong_resource)
            .await
            .is_err()
    );
    assert!(store.load_retirement_started().await.unwrap().is_none());
    assert_eq!(store.load().await.unwrap(), Some(active.clone()));
    store.record_retirement_started(&retired).await.unwrap();
    store.record_retirement_started(&retired).await.unwrap();
    assert!(store.load_retired_authority().await.unwrap().is_none());
    assert!(store.admit(&active).await.is_err());
    drop(store);
    let store = SqliteStore::open_existing(&path, Some(&identity)).unwrap();
    assert_eq!(
        store.load_retirement_started().await.unwrap(),
        Some(retired.clone())
    );
    let mut conflict = retired.clone();
    conflict.report.process_session_id =
        kuberic_protocol::types::ProcessSessionId::new("conflicting-session");
    assert!(store.record_retirement_started(&conflict).await.is_err());
    assert!(store.retire(&conflict).await.is_err());
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_started_cleanup BEFORE DELETE ON runtime_lifecycle
         WHEN OLD.kind = 'retirement-started'
         BEGIN SELECT RAISE(FAIL, 'injected finalization failure'); END;",
        )
        .unwrap();
    assert!(store.retire(&retired).await.is_err());
    assert_eq!(store.load().await.unwrap(), Some(active.clone()));
    assert!(store.load_retired_authority().await.unwrap().is_none());
    assert_eq!(
        store.load_retirement_started().await.unwrap(),
        Some(retired.clone())
    );
    connection
        .execute_batch("DROP TRIGGER reject_started_cleanup;")
        .unwrap();
    store.retire(&retired).await.unwrap();
    drop(store);
    let store = SqliteStore::open_existing(&path, Some(&identity)).unwrap();
    assert_eq!(
        store.load_retired_authority().await.unwrap(),
        Some(retired.clone())
    );
    assert!(store.load().await.unwrap().is_none());
    assert!(store.load_retirement_started().await.unwrap().is_none());
    assert!(store.admit(&active).await.is_err());
    store.retire(&retired).await.unwrap();
    store.record_retirement_started(&retired).await.unwrap();
    assert!(store.load_retirement_started().await.unwrap().is_none());
    let mut changed = retired;
    changed.report.process_session_id =
        kuberic_protocol::types::ProcessSessionId::new("new-session");
    assert!(store.retire(&changed).await.is_err());
    assert_eq!(store.identity().await.unwrap(), identity);
}

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
        secondary_scale_down: None,
        secondary_removal_evidence: None,
        scale_up: None,
        scale_up_failover: None,
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
        purpose: kuberic_protocol::types::ProvisioningPurpose::replacement(ReplicaIdentity {
            replica_id: command.local_replica_id,
            instance_id: ReplicaInstanceId::new("old-pod"),
            agent_generation: AgentGeneration::new("old-generation"),
        }),
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

fn scale_up_initialization_fixture() -> (
    InitializeAgentStore,
    ObservedStorageIdentity,
    ProvisioningIntent,
) {
    let resource_uid = ResourceUid::new("scale-up-set");
    let previous_policy = EffectivePolicy::fixed(1, 30).unwrap();
    let current_policy = EffectivePolicy::fixed(2, 30).unwrap();
    let primary = identity(1, "primary-pod", "primary-generation");
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        primary.replica_id,
        vec![ConfigurationMember {
            identity: primary,
            role: ReplicaRole::Primary,
        }],
        previous_policy.write_quorum,
    );
    let mut provisioning = ProvisioningIntent {
        purpose: ProvisioningPurpose::scale_up(ScaleUpProvisioning {
            resource_uid: resource_uid.clone(),
            spec_generation: 2,
            desired_replicas: 2,
            previous_configuration: previous.clone(),
            previous_policy,
            current_policy: current_policy.clone(),
            target_replica_id: ReplicaId::new(2),
        }),
        pod_uid: PodUid::new("candidate-pod"),
        pvc_uid: PvcUid::new("candidate-pvc"),
        operation_id: OperationId::default(),
    };
    provisioning.operation_id = provisioning.expected_operation_id();
    let target = provisioning.target_identity(&resource_uid);
    let command = InitializeAgentStore {
        initialization_id: provisioning.initialization_id(&resource_uid),
        resource_uid: resource_uid.clone(),
        local_replica_id: target.replica_id,
        expected_instance_id: target.instance_id.clone(),
        expected_pod_uid: provisioning.pod_uid.clone(),
        expected_pvc_uid: provisioning.pvc_uid.clone(),
        assigned_agent_generation: target.agent_generation,
        effective_policy: current_policy,
        bootstrap_configuration: previous,
        provisioning: Some(provisioning.clone()),
    };
    let observed = ObservedStorageIdentity {
        resource_uid,
        pod_uid: command.expected_pod_uid.clone(),
        pvc_uid: command.expected_pvc_uid.clone(),
        instance_id: command.expected_instance_id.clone(),
    };
    (command, observed, provisioning)
}

#[test]
fn fresh_scale_up_store_requires_exact_frozen_authority() {
    let (command, observed, provisioning) = scale_up_initialization_fixture();
    let identity = authorize_initialization(
        &command,
        &observed,
        InitializationAuthority::ScaleUp(&provisioning),
    )
    .unwrap();
    assert_eq!(identity.schema_version, 3);

    for mutation in 0..6 {
        let mut stale = command.clone();
        match mutation {
            0 => stale.expected_pod_uid = PodUid::new("reused-pod"),
            1 => stale.expected_pvc_uid = PvcUid::new("reused-pvc"),
            2 => stale.local_replica_id = ReplicaId::new(3),
            3 => stale.effective_policy = EffectivePolicy::fixed(3, 30).unwrap(),
            4 => stale.bootstrap_configuration.epoch = Epoch::new(0, 2),
            _ => stale.assigned_agent_generation = AgentGeneration::new("retired-generation"),
        }
        assert!(matches!(
            authorize_initialization(
                &stale,
                &observed,
                InitializationAuthority::ScaleUp(&provisioning)
            ),
            Err(AgentError::InitializationNotAuthorized(_))
        ));
    }
}

#[tokio::test]
async fn scale_up_build_journal_replays_current_session_and_fences_retirement() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let (initialize, observed, provisioning) = scale_up_initialization_fixture();
    let identity = authorize_initialization(
        &initialize,
        &observed,
        InitializationAuthority::ScaleUp(&provisioning),
    )
    .unwrap();
    let mut state = AgentState::new(identity.clone());
    state.scale_up_initialization = Some(provisioning.clone());
    let store = SqliteStore::create_authorized(&path, state).unwrap();
    let source = initialize.bootstrap_configuration.members[0]
        .identity
        .clone();
    let authority = BuildAuthority {
        build_id: provisioning
            .scale_up_build_id(&initialize.resource_uid)
            .unwrap(),
        kind: BuildAuthorityKind::Provisioning,
        source,
        target: identity.local_identity.clone(),
        current_configuration: initialize.bootstrap_configuration.clone(),
        replication_boundary_lsn: 7,
    };
    let mut command = EnsureReplicaBuild {
        operation_id: authority.build_id.clone(),
        local_replica_id: identity.local_identity.replica_id,
        expected_instance_id: identity.local_identity.instance_id.clone(),
        expected_agent_generation: identity.local_identity.agent_generation.clone(),
        target: identity.local_identity.clone(),
        authority: Some(authority),
        source_session_id: Some(kuberic_protocol::types::ProcessSessionId::new("source-1")),
        retire: false,
    };
    store.journal_build(&command).await.unwrap();
    command.source_session_id = Some(kuberic_protocol::types::ProcessSessionId::new("source-2"));
    store.journal_build(&command).await.unwrap();
    drop(store);

    let reopened = SqliteStore::open_existing(&path, Some(&identity)).unwrap();
    assert_eq!(
        reopened
            .load_state()
            .await
            .unwrap()
            .build_commands
            .get(&command.operation_id),
        Some(&command)
    );
    let mut retired = reopened.load_state().await.unwrap();
    retired.retired_builds.insert(command.operation_id.clone());
    drop(reopened);
    let replacement_path = directory.path().join("retired-agent.db");
    let retired_store = SqliteStore::create_authorized(&replacement_path, retired).unwrap();
    assert!(retired_store.journal_build(&command).await.is_err());
}

#[tokio::test]
async fn source_build_abandonment_resolves_only_the_exact_pending_copy_effect() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let (initialize, observed, transition) = bootstrap_fixture();
    let identity = authorize_initialization(
        &initialize,
        &observed,
        InitializationAuthority::Bootstrap(&transition),
    )
    .unwrap();
    let store = SqliteStore::create_authorized(&path, AgentState::new(identity.clone())).unwrap();
    let target = ReplicaIdentity {
        replica_id: ReplicaId::new(2),
        instance_id: ReplicaInstanceId::new("candidate-pod"),
        agent_generation: AgentGeneration::new("candidate-generation"),
    };
    let build_id = OperationId::new("source-build");
    let build = EnsureReplicaBuild {
        operation_id: build_id.clone(),
        local_replica_id: identity.local_identity.replica_id,
        expected_instance_id: identity.local_identity.instance_id.clone(),
        expected_agent_generation: identity.local_identity.agent_generation.clone(),
        target: target.clone(),
        authority: None,
        source_session_id: None,
        retire: false,
    };
    store.journal_build(&build).await.unwrap();
    let pending = RuntimeEffect {
        operation_id: OperationId::new("source-build:build-replica"),
        sequence: 7,
        action: RuntimeEffectAction::BuildReplica {
            build_id: build_id.clone(),
            target: target.clone(),
            replication_address: String::new(),
        },
    };
    assert_eq!(
        store.begin_effect(&pending).await.unwrap(),
        BeginEffect::Execute(pending.clone())
    );
    let retirement = EnsureReplicaBuild {
        retire: true,
        ..build.clone()
    };
    store.abandon_build(&retirement).await.unwrap();
    let abandoned = store.load_state().await.unwrap();
    assert_eq!(
        abandoned
            .pending_effect
            .as_ref()
            .map(|pending| &pending.effect),
        Some(&pending)
    );
    assert!(abandoned.abandoned_builds.contains(&build_id));
    assert_eq!(abandoned.next_effect_sequence, 1);
    assert!(store.journal_build(&build).await.is_err());

    let unrelated_path = directory.path().join("unrelated.db");
    let unrelated =
        SqliteStore::create_authorized(&unrelated_path, AgentState::new(identity)).unwrap();
    unrelated.journal_build(&build).await.unwrap();
    let other = RuntimeEffect {
        operation_id: OperationId::new("unrelated"),
        sequence: 1,
        action: RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
    };
    unrelated.begin_effect(&other).await.unwrap();
    assert!(matches!(
        unrelated.abandon_build(&retirement).await,
        Err(AgentError::EffectConflict(_))
    ));
    let fenced = unrelated.load_state().await.unwrap();
    assert_eq!(
        fenced
            .pending_effect
            .as_ref()
            .map(|pending| &pending.effect),
        Some(&other)
    );
    assert!(!fenced.abandoned_builds.contains(&build_id));
}

#[tokio::test]
async fn durable_build_catch_up_boundary_is_write_once() {
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
    let authority = BuildAuthority {
        build_id: OperationId::new("boundary-build"),
        kind: BuildAuthorityKind::Provisioning,
        source: storage_identity.local_identity.clone(),
        target: identity(2, "target", "target-generation"),
        current_configuration: transition.current_configuration,
        replication_boundary_lsn: 0,
    };
    store.admit_build(&authority).await.unwrap();
    let progress = DurableBuildProgress {
        authority,
        last_sequence: 1,
        durable_lsn: 0,
        completed: true,
        catch_up_boundary_lsn: Some(2),
    };
    store.record_build_progress(&progress).await.unwrap();

    let mut advanced = progress.clone();
    advanced.last_sequence = 2;
    advanced.durable_lsn = 2;
    store.record_build_progress(&advanced).await.unwrap();
    assert_eq!(
        store
            .load_state()
            .await
            .unwrap()
            .build_progress
            .get(&advanced.authority.build_id),
        Some(&advanced)
    );

    let mut changed = advanced.clone();
    changed.catch_up_boundary_lsn = Some(3);
    assert!(store.record_build_progress(&changed).await.is_err());
    changed.catch_up_boundary_lsn = None;
    assert!(store.record_build_progress(&changed).await.is_err());
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
        scale_up: None,
        secondary_removal: None,
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
        previous_policy: None,
        secondary_removal_evidence: None,
        scale_up_evidence: None,
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
async fn sqlite_configuration_journal_replays_exact_installed_commands_and_rejects_mutation() {
    let directory = tempdir().unwrap();
    let (initialize, observed, transition) = bootstrap_fixture();
    let storage_identity = authorize_initialization(
        &initialize,
        &observed,
        InitializationAuthority::Bootstrap(&transition),
    )
    .unwrap();
    let previous = initialize.bootstrap_configuration.clone();
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        previous.primary_id,
        previous.members.clone(),
        previous.write_quorum,
    );
    let pc_cc = EnsureConfiguration {
        previous_policy: None,
        secondary_removal_evidence: None,
        scale_up_evidence: None,
        operation_id: OperationId::new("accepted-correction-pc-cc"),
        previous_configuration: Some(previous.clone()),
        current_configuration: current.clone(),
        previous_epoch: Some(previous.epoch),
        current_epoch: current.epoch,
        effective_policy: storage_identity.effective_policy.clone(),
        local_replica_id: storage_identity.local_identity.replica_id,
        expected_instance_id: storage_identity.local_identity.instance_id.clone(),
        expected_agent_generation: storage_identity.local_identity.agent_generation.clone(),
        transition_kind: TransitionKind::Failover,
        failover_safe_lsn: Some(12),
        primary_write_status: AccessStatus::ReconfigurationPending,
        current_only: false,
        retire_build_ids: Vec::new(),
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    };
    let store = SqliteStore::create_authorized(
        SqliteStore::metadata_database_path(directory.path()),
        AgentState::new(storage_identity.clone()),
    )
    .unwrap();
    assert!(matches!(
        store.begin_configuration(&pc_cc).await.unwrap(),
        BeginConfiguration::Execute(_)
    ));
    store
        .admit(&AdmittedAuthority {
            local_identity: storage_identity.local_identity.clone(),
            transition_kind: Some(TransitionKind::Failover),
            previous_configuration: Some(previous),
            current_configuration: current.clone(),
            switchover_handoff: None,
            secondary_removal: None,
            scale_up: None,
        })
        .await
        .unwrap();
    assert!(matches!(
        store.begin_configuration(&pc_cc).await.unwrap(),
        BeginConfiguration::Pending(record) if record.command == pc_cc
    ));
    let mut mutated = pc_cc.clone();
    mutated.failover_safe_lsn = Some(13);
    assert!(matches!(
        store.begin_configuration(&mutated).await,
        Err(AgentError::EffectConflict(_))
    ));

    let current_only = EnsureConfiguration {
        operation_id: OperationId::new("accepted-correction-current-only"),
        previous_configuration: None,
        previous_epoch: None,
        current_only: true,
        ..pc_cc
    };
    let mut installed = AgentState::new(storage_identity);
    installed.highest_epoch = current.epoch;
    installed.current_configuration = Some(current);
    installed.role = ReplicaRole::Primary;
    installed.reconfiguration = Some(ReconfigurationRecord {
        command: current_only.clone(),
        stage: CoordinatorStage::Activate,
        observed_lsn: Some(13),
    });
    fs::create_dir(directory.path().join("current-only")).unwrap();
    let current_only_store = SqliteStore::create_authorized(
        SqliteStore::metadata_database_path(&directory.path().join("current-only")),
        installed,
    )
    .unwrap();
    assert!(matches!(
        current_only_store
            .begin_configuration(&current_only)
            .await
            .unwrap(),
        BeginConfiguration::Pending(record) if record.command == current_only
    ));
    let mut mutated = current_only.clone();
    mutated.failover_safe_lsn = Some(13);
    assert!(matches!(
        current_only_store.begin_configuration(&mutated).await,
        Err(AgentError::EffectConflict(_))
    ));
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
        scale_up: None,
        secondary_removal: None,
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
