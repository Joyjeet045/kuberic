use std::fs;

use super::tempdir;
use crate::authority::{
    AdmittedAuthority, AuthorityFence, BuildAuthority, BuildAuthorityKind, BuildAuthorityStore,
    BuildProgressStore, BuildSelection, DurableBuildProgress, DurableLocalWrite, LocalWriteJournal,
    LocalWritePhase, ReplicaAuthorityStore, ReplicationProgress, ReplicationProgressStore,
};
use crate::effects::{
    AccessCompletion, AccessCompletionKind, BuildCompletion, BuildEffectState, BuildPostcondition,
    CatchUpCompletion, EpochCompletion, HistoricalSecondaryRemovalCompletion, RoleCompletion,
    RoleTransition, RuntimeEffect, RuntimeEffectAction, RuntimeEffectOutcome, RuntimeEffectResult,
};
use crate::host::provisioning::{
    InitializationAuthority, ObservedStorageIdentity, StorePresence, authorize_initialization,
    inspect_store, validate_established_identity,
};
use crate::host::sqlite_store::SqliteStore;
use crate::host::state::{AgentState, CoordinatorStage, ReconfigurationRecord, SCHEMA_VERSION};
use crate::host::state::{EffectStage, PendingEffect, RetainedResult};
use crate::host::store::{AgentStore, BeginConfiguration, BeginEffect};
use crate::protocol::command::AcceptSecondaryRemovalCommit;
use crate::protocol::command::{EnsureConfiguration, EnsureReplicaBuild, InitializeAgentStore};
use crate::protocol::types::{AccessStatus, SecondaryRemovalStage};
use crate::protocol::types::{
    AgentGeneration, ConfigurationDescriptor, ConfigurationMember, EffectivePolicy, Epoch,
    OperationId, PodUid, ProvisioningIntent, ProvisioningPurpose, PvcUid, ReplicaId,
    ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid, ScaleUpProvisioning,
    SwitchoverHandoff, SwitchoverRequestId, TransitionId, TransitionIntent, TransitionKind,
    derive_agent_generation, derive_initialization_id,
};
use crate::receipts::{NativeOperationToken, SecondaryRemovalReceipt, SwitchoverReceipt};
use bytes::Bytes;
use rusqlite::{Connection, OptionalExtension};
use serde_json::Value;

use crate::removal_fixture;

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
        applied_result: None,
    });
    let result = RuntimeEffectResult {
        operation_id: ordinary.operation_id.clone(),
        sequence: ordinary.sequence,
        outcome: RuntimeEffectOutcome::HistoricalSecondaryRemovalAccepted(
            HistoricalSecondaryRemovalCompletion {
                accepted_secondary_removal: committed.clone(),
                authority: Some(AdmittedAuthority {
                    local_identity: local,
                    transition_kind: None,
                    previous_configuration: None,
                    current_configuration: intent.current_configuration.clone(),
                    switchover_handoff: None,
                    scale_up: None,
                    secondary_removal: Some(committed.evidence.clone()),
                }),
                role: state.role,
                write_status: state.write_status,
                role_transition_clear: true,
                verified_replication_lsn: Some(10),
                receipt: None,
            },
        ),
    };
    (state, ordinary, historical, result)
}

#[tokio::test]
async fn exhausted_effect_sequence_persists_and_rejects_new_intents_after_reopen() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let (mut state, _, _, mut result) = pending_acceptance_fixture();
    state.pending_effect = None;
    state.next_effect_sequence = u64::MAX - 1;
    let effect = RuntimeEffect {
        sequence: u64::MAX - 1,
        operation_id: OperationId::new("last-effect"),
        action: RuntimeEffectAction::RefreshApplicationProgress,
    };
    result.sequence = effect.sequence;
    result.operation_id = effect.operation_id.clone();
    result.outcome = RuntimeEffectOutcome::ApplicationProgressRefreshed {
        current_progress: 10,
    };
    let store = SqliteStore::create_authorized(&path, state).unwrap();
    store.begin_effect(&effect).await.unwrap();
    store.mark_effect_applied(&effect, &result).await.unwrap();
    store.complete_effect(&result).await.unwrap();
    drop(store);
    let store = SqliteStore::open_existing(&path, None).unwrap();
    assert_eq!(
        store.load_state().await.unwrap().next_effect_sequence,
        u64::MAX
    );
    assert!(matches!(
        store.begin_effect(&effect).await.unwrap(),
        BeginEffect::Completed(_)
    ));
    for sequence in [u64::MAX, 0, 1] {
        let next = RuntimeEffect {
            sequence,
            operation_id: OperationId::new(format!("overflow-{sequence}")),
            action: RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        };
        assert!(matches!(store.begin_effect(&next).await,
            Err(crate::host::HostError::DurableEffectConflict(message)) if message.contains("exhausted")));
    }
    let state = store.load_state().await.unwrap();
    assert_eq!(state.next_effect_sequence, u64::MAX);
    assert!(state.pending_effect.is_none());
}

#[tokio::test]
async fn narrow_effect_completion_updates_only_declared_durable_domains() {
    #[derive(Clone, Copy)]
    enum OwnedDomain {
        Role,
        Epoch,
        ReadAccess,
        WriteAccess,
        CombinedAccess,
        None,
    }

    let (mut base, _, _, _) = pending_acceptance_fixture();
    base.pending_effect = None;
    base.retained_result = None;
    base.removal_effects.clear();
    base.prepared_secondary_removal = None;
    base.accepted_secondary_removal = None;
    base.secondary_removal_evidence = None;
    base.scale_up_evidence = None;
    base.role = ReplicaRole::IdleSecondary;
    base.read_status = AccessStatus::NotPrimary;
    base.write_status = AccessStatus::NotPrimary;
    base.highest_epoch = Epoch::default();
    base.next_effect_sequence = 20;

    let configuration = base.current_configuration.clone().unwrap();
    let authority = AdmittedAuthority {
        local_identity: base.identity.local_identity.clone(),
        transition_kind: None,
        previous_configuration: base.previous_configuration.clone(),
        current_configuration: configuration.clone(),
        switchover_handoff: None,
        scale_up: None,
        secondary_removal: None,
    };
    let target = configuration
        .members
        .iter()
        .find(|member| member.identity != base.identity.local_identity)
        .unwrap()
        .identity
        .clone();
    let build_authority = BuildAuthority {
        build_id: OperationId::new("owned-build"),
        kind: BuildAuthorityKind::Provisioning,
        source: base.identity.local_identity.clone(),
        target: target.clone(),
        current_configuration: configuration.clone(),
        replication_boundary_lsn: 7,
    };
    let build_progress = DurableBuildProgress {
        authority: build_authority.clone(),
        last_sequence: 3,
        durable_lsn: 9,
        completed: true,
        catch_up_boundary_lsn: Some(7),
    };
    base.build_progress
        .insert(build_authority.build_id.clone(), build_progress.clone());

    let cases = vec![
        (
            OwnedDomain::Role,
            RuntimeEffectAction::ChangeApplicationRole(ReplicaRole::Primary),
            RuntimeEffectOutcome::ApplicationRoleChanged {
                completion: RoleCompletion {
                    role: ReplicaRole::Primary,
                    role_transition: None,
                },
                receipt: None,
            },
        ),
        (
            OwnedDomain::Epoch,
            RuntimeEffectAction::UpdateEpoch,
            RuntimeEffectOutcome::EpochUpdated(EpochCompletion {
                epoch: configuration.epoch,
                role_transition: Some(RoleTransition {
                    completed_role: ReplicaRole::IdleSecondary,
                    target_role: ReplicaRole::Primary,
                    replicator_completed: true,
                    epoch_completed: true,
                    application_completed: false,
                }),
            }),
        ),
        (
            OwnedDomain::ReadAccess,
            RuntimeEffectAction::SetReadStatus(AccessStatus::Granted),
            RuntimeEffectOutcome::AccessChanged(AccessCompletion {
                kind: AccessCompletionKind::Read,
                read_status: AccessStatus::Granted,
                write_status: AccessStatus::NotPrimary,
                authority: Some(authority.clone()),
                role: ReplicaRole::Primary,
            }),
        ),
        (
            OwnedDomain::WriteAccess,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
            RuntimeEffectOutcome::AccessChanged(AccessCompletion {
                kind: AccessCompletionKind::Write,
                read_status: AccessStatus::NotPrimary,
                write_status: AccessStatus::Granted,
                authority: Some(authority.clone()),
                role: ReplicaRole::Primary,
            }),
        ),
        (
            OwnedDomain::CombinedAccess,
            RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::Granted,
            },
            RuntimeEffectOutcome::AccessChanged(AccessCompletion {
                kind: AccessCompletionKind::Combined,
                read_status: AccessStatus::Granted,
                write_status: AccessStatus::Granted,
                authority: Some(authority.clone()),
                role: ReplicaRole::Primary,
            }),
        ),
        (
            OwnedDomain::None,
            RuntimeEffectAction::WaitForCatchup,
            RuntimeEffectOutcome::CatchUpCompleted(CatchUpCompletion {
                authority: Some(authority.clone()),
                boundary_lsn: 9,
            }),
        ),
        (
            OwnedDomain::None,
            RuntimeEffectAction::BuildReplica {
                build_id: build_authority.build_id.clone(),
                target: target.clone(),
                replication_address: "in-process://target".into(),
            },
            RuntimeEffectOutcome::BuildReplica(BuildCompletion {
                build_id: build_authority.build_id.clone(),
                target,
                state: BuildEffectState::Completed(Box::new(BuildPostcondition {
                    authority: build_authority.clone(),
                    last_sequence: build_progress.last_sequence,
                    durable_lsn: build_progress.durable_lsn,
                    completed: true,
                    catch_up_boundary_lsn: build_progress.catch_up_boundary_lsn,
                })),
            }),
        ),
    ];

    for (index, (owned, action, outcome)) in cases.into_iter().enumerate() {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let before = base.clone();
        let store = SqliteStore::create_authorized(&path, before.clone()).unwrap();
        store.admit(&authority).await.unwrap();
        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "INSERT INTO build_authority(build_id, authority_json) VALUES(?1, ?2)",
                rusqlite::params![
                    build_authority.build_id.as_str(),
                    serde_json::to_string(&build_authority).unwrap()
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO build_progress(build_id, progress_json) VALUES(?1, ?2)",
                rusqlite::params![
                    build_authority.build_id.as_str(),
                    serde_json::to_string(&build_progress).unwrap()
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO runtime_lifecycle(kind, value_json) VALUES(?1, ?2)",
                rusqlite::params![
                    format!("build-selection/{}", build_authority.target.replica_id),
                    serde_json::to_string(&BuildSelection {
                        authority: build_authority.clone(),
                        generation: 1,
                    })
                    .unwrap()
                ],
            )
            .unwrap();
        let build_row_before: String = connection
            .query_row(
                "SELECT progress_json FROM build_progress WHERE build_id = ?1",
                [build_authority.build_id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        drop(connection);
        let effect = RuntimeEffect {
            operation_id: OperationId::new(format!("narrow-domain-{index}")),
            sequence: before.next_effect_sequence,
            action,
        };
        let result = RuntimeEffectResult {
            operation_id: effect.operation_id.clone(),
            sequence: effect.sequence,
            outcome,
        };
        store.begin_effect(&effect).await.unwrap();
        store.mark_effect_applied(&effect, &result).await.unwrap();
        store.complete_effect(&result).await.unwrap();
        let after = store.load_state().await.unwrap();
        let build_row_after: String = Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT progress_json FROM build_progress WHERE build_id = ?1",
                [build_authority.build_id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            build_row_after, build_row_before,
            "completion {index} changed dedicated build progress"
        );

        let mut normalized = after;
        normalized.pending_effect = before.pending_effect.clone();
        normalized.retained_result = before.retained_result.clone();
        normalized.next_effect_sequence = before.next_effect_sequence;
        normalized.removal_effects = before.removal_effects.clone();
        match owned {
            OwnedDomain::Role => normalized.role = before.role,
            OwnedDomain::Epoch => normalized.highest_epoch = before.highest_epoch,
            OwnedDomain::ReadAccess => normalized.read_status = before.read_status,
            OwnedDomain::WriteAccess => normalized.write_status = before.write_status,
            OwnedDomain::CombinedAccess => {
                normalized.read_status = before.read_status;
                normalized.write_status = before.write_status;
            }
            OwnedDomain::None => {}
        }
        assert_eq!(
            normalized, before,
            "completion {index} changed an undeclared durable domain"
        );
    }
}

#[tokio::test]
async fn applied_effect_rejects_a_changed_canonical_result_without_mutation() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let (mut state, _, _, _) = pending_acceptance_fixture();
    state.pending_effect = None;
    state.retained_result = None;
    state.removal_effects.clear();
    state.next_effect_sequence = 30;
    state.highest_epoch = Epoch::default();
    let expected_epoch = state.current_configuration.as_ref().unwrap().epoch;
    let store = SqliteStore::create_authorized(&path, state).unwrap();
    let effect = RuntimeEffect {
        operation_id: OperationId::new("epoch-result-baseline"),
        sequence: 30,
        action: RuntimeEffectAction::UpdateEpoch,
    };
    let transition = RoleTransition {
        completed_role: ReplicaRole::IdleSecondary,
        target_role: ReplicaRole::Primary,
        replicator_completed: true,
        epoch_completed: true,
        application_completed: false,
    };
    let result = RuntimeEffectResult {
        operation_id: effect.operation_id.clone(),
        sequence: effect.sequence,
        outcome: RuntimeEffectOutcome::EpochUpdated(EpochCompletion {
            epoch: expected_epoch,
            role_transition: Some(transition.clone()),
        }),
    };
    store.begin_effect(&effect).await.unwrap();
    store.mark_effect_applied(&effect, &result).await.unwrap();
    let applied = store.load_state().await.unwrap();

    let changed = RuntimeEffectResult {
        outcome: RuntimeEffectOutcome::EpochUpdated(EpochCompletion {
            epoch: Epoch::new(
                expected_epoch.data_loss_number,
                expected_epoch.configuration_number + 1,
            ),
            role_transition: Some(transition),
        }),
        ..result.clone()
    };
    assert!(store.complete_effect(&changed).await.is_err());
    assert_eq!(store.load_state().await.unwrap(), applied);
    store.complete_effect(&result).await.unwrap();
    let completed = store.load_state().await.unwrap();
    store.complete_effect(&result).await.unwrap();
    assert_eq!(store.load_state().await.unwrap(), completed);
}

#[tokio::test]
async fn late_catch_up_and_build_completion_reject_authority_replacement() {
    let (mut catch_up_state, _, _, _) = pending_acceptance_fixture();
    catch_up_state.pending_effect = None;
    catch_up_state.retained_result = None;
    catch_up_state.removal_effects.clear();
    catch_up_state.next_effect_sequence = 40;
    let authority = AdmittedAuthority {
        local_identity: catch_up_state.identity.local_identity.clone(),
        transition_kind: None,
        previous_configuration: catch_up_state.previous_configuration.clone(),
        current_configuration: catch_up_state.current_configuration.clone().unwrap(),
        switchover_handoff: None,
        scale_up: None,
        secondary_removal: None,
    };
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = SqliteStore::create_authorized(&path, catch_up_state).unwrap();
    store.admit(&authority).await.unwrap();
    let catch_up_effect = RuntimeEffect {
        operation_id: OperationId::new("late-catch-up"),
        sequence: 40,
        action: RuntimeEffectAction::WaitForCatchup,
    };
    let catch_up_result = RuntimeEffectResult {
        operation_id: catch_up_effect.operation_id.clone(),
        sequence: catch_up_effect.sequence,
        outcome: RuntimeEffectOutcome::CatchUpCompleted(CatchUpCompletion {
            authority: Some(authority.clone()),
            boundary_lsn: 9,
        }),
    };
    store.begin_effect(&catch_up_effect).await.unwrap();
    store
        .mark_effect_applied(&catch_up_effect, &catch_up_result)
        .await
        .unwrap();
    let catch_up_applied = store.load_state().await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute("DELETE FROM replica_authority WHERE singleton = 1", [])
        .unwrap();
    assert!(store.complete_effect(&catch_up_result).await.is_err());
    assert_eq!(store.load_state().await.unwrap(), catch_up_applied);
    drop(store);

    let (mut build_state, _, _, _) = pending_acceptance_fixture();
    build_state.pending_effect = None;
    build_state.retained_result = None;
    build_state.removal_effects.clear();
    build_state.next_effect_sequence = 50;
    let configuration = build_state.current_configuration.clone().unwrap();
    let target = configuration
        .members
        .iter()
        .find(|member| member.identity != build_state.identity.local_identity)
        .unwrap()
        .identity
        .clone();
    let build_authority = BuildAuthority {
        build_id: OperationId::new("late-build"),
        kind: BuildAuthorityKind::Provisioning,
        source: build_state.identity.local_identity.clone(),
        target: target.clone(),
        current_configuration: configuration,
        replication_boundary_lsn: 7,
    };
    let progress = DurableBuildProgress {
        authority: build_authority.clone(),
        last_sequence: 2,
        durable_lsn: 9,
        completed: true,
        catch_up_boundary_lsn: Some(7),
    };
    build_state
        .build_progress
        .insert(build_authority.build_id.clone(), progress.clone());
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = SqliteStore::create_authorized(&path, build_state).unwrap();
    let connection = Connection::open(&path).unwrap();
    connection
        .execute(
            "INSERT INTO build_authority(build_id, authority_json) VALUES(?1, ?2)",
            rusqlite::params![
                build_authority.build_id.as_str(),
                serde_json::to_string(&build_authority).unwrap()
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO build_progress(build_id, progress_json) VALUES(?1, ?2)",
            rusqlite::params![
                build_authority.build_id.as_str(),
                serde_json::to_string(&progress).unwrap()
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO runtime_lifecycle(kind, value_json) VALUES(?1, ?2)",
            rusqlite::params![
                format!("build-selection/{}", build_authority.target.replica_id),
                serde_json::to_string(&BuildSelection {
                    authority: build_authority.clone(),
                    generation: 1,
                })
                .unwrap()
            ],
        )
        .unwrap();
    drop(connection);
    let build_effect = RuntimeEffect {
        operation_id: OperationId::new("late-build-effect"),
        sequence: 50,
        action: RuntimeEffectAction::BuildReplica {
            build_id: build_authority.build_id.clone(),
            target: target.clone(),
            replication_address: "in-process://target".into(),
        },
    };
    let build_result = RuntimeEffectResult {
        operation_id: build_effect.operation_id.clone(),
        sequence: build_effect.sequence,
        outcome: RuntimeEffectOutcome::BuildReplica(BuildCompletion {
            build_id: build_authority.build_id.clone(),
            target,
            state: BuildEffectState::Completed(Box::new(BuildPostcondition {
                authority: build_authority.clone(),
                last_sequence: progress.last_sequence,
                durable_lsn: progress.durable_lsn,
                completed: true,
                catch_up_boundary_lsn: progress.catch_up_boundary_lsn,
            })),
        }),
    };
    store.begin_effect(&build_effect).await.unwrap();
    store
        .mark_effect_applied(&build_effect, &build_result)
        .await
        .unwrap();
    drop(store);
    let connection = Connection::open(&path).unwrap();
    let json: String = connection
        .query_row("SELECT state_json FROM agent_state", [], |row| row.get(0))
        .unwrap();
    let mut replaced: AgentState = serde_json::from_str(&json).unwrap();
    replaced
        .abandoned_builds
        .insert(build_authority.build_id.clone());
    connection
        .execute(
            "UPDATE agent_state SET state_json = ?1 WHERE singleton = 1",
            [serde_json::to_string(&replaced).unwrap()],
        )
        .unwrap();
    let mut replacement_authority = build_authority.clone();
    replacement_authority.build_id = OperationId::new("replacement-build");
    connection
        .execute(
            "UPDATE runtime_lifecycle SET value_json = ?1 WHERE kind = ?2",
            rusqlite::params![
                serde_json::to_string(&BuildSelection {
                    authority: replacement_authority,
                    generation: 2,
                })
                .unwrap(),
                format!("build-selection/{}", build_authority.target.replica_id)
            ],
        )
        .unwrap();
    let mut active_replacement = build_authority.clone();
    active_replacement.target = build_authority.source.clone();
    connection
        .execute(
            "UPDATE build_authority SET authority_json = ?1 WHERE build_id = ?2",
            rusqlite::params![
                serde_json::to_string(&active_replacement).unwrap(),
                build_authority.build_id.as_str()
            ],
        )
        .unwrap();
    let rows_before: (Option<String>, Option<String>, String) = (
        connection
            .query_row(
                "SELECT authority_json FROM build_authority WHERE build_id = ?1",
                [build_authority.build_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .unwrap(),
        connection
            .query_row(
                "SELECT progress_json FROM build_progress WHERE build_id = ?1",
                [build_authority.build_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .unwrap(),
        connection
            .query_row(
                "SELECT value_json FROM runtime_lifecycle WHERE kind = ?1",
                [format!(
                    "build-selection/{}",
                    build_authority.target.replica_id
                )],
                |row| row.get(0),
            )
            .unwrap(),
    );
    drop(connection);
    let store = SqliteStore::open_existing(&path, None).unwrap();
    let build_applied = store.load_state().await.unwrap();
    assert!(store.complete_effect(&build_result).await.is_err());
    assert_eq!(store.load_state().await.unwrap(), build_applied);
    let connection = Connection::open(&path).unwrap();
    let rows_after: (Option<String>, Option<String>, String) = (
        connection
            .query_row(
                "SELECT authority_json FROM build_authority WHERE build_id = ?1",
                [build_authority.build_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .unwrap(),
        connection
            .query_row(
                "SELECT progress_json FROM build_progress WHERE build_id = ?1",
                [build_authority.build_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .unwrap(),
        connection
            .query_row(
                "SELECT value_json FROM runtime_lifecycle WHERE kind = ?1",
                [format!(
                    "build-selection/{}",
                    build_authority.target.replica_id
                )],
                |row| row.get(0),
            )
            .unwrap(),
    );
    assert_eq!(rows_after, rows_before);
}

#[tokio::test]
async fn build_completion_fences_reject_independent_authority_selection_and_abandonment_changes() {
    for mutation in ["authority", "selection", "abandonment"] {
        let (mut state, _, _, _) = pending_acceptance_fixture();
        state.pending_effect = None;
        state.retained_result = None;
        state.removal_effects.clear();
        state.next_effect_sequence = 55;
        let configuration = state.current_configuration.clone().unwrap();
        let target = configuration
            .members
            .iter()
            .find(|member| member.identity != state.identity.local_identity)
            .unwrap()
            .identity
            .clone();
        let authority = BuildAuthority {
            build_id: OperationId::new(format!("independent-{mutation}")),
            kind: BuildAuthorityKind::Provisioning,
            source: state.identity.local_identity.clone(),
            target: target.clone(),
            current_configuration: configuration,
            replication_boundary_lsn: 7,
        };
        let progress = DurableBuildProgress {
            authority: authority.clone(),
            last_sequence: 2,
            durable_lsn: 9,
            completed: true,
            catch_up_boundary_lsn: Some(7),
        };
        state
            .build_progress
            .insert(authority.build_id.clone(), progress.clone());
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let store = SqliteStore::create_authorized(&path, state).unwrap();
        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "INSERT INTO build_authority(build_id, authority_json) VALUES(?1, ?2)",
                rusqlite::params![
                    authority.build_id.as_str(),
                    serde_json::to_string(&authority).unwrap()
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO build_progress(build_id, progress_json) VALUES(?1, ?2)",
                rusqlite::params![
                    authority.build_id.as_str(),
                    serde_json::to_string(&progress).unwrap()
                ],
            )
            .unwrap();
        let selection_key = format!("build-selection/{}", authority.target.replica_id);
        connection
            .execute(
                "INSERT INTO runtime_lifecycle(kind, value_json) VALUES(?1, ?2)",
                rusqlite::params![
                    &selection_key,
                    serde_json::to_string(&BuildSelection {
                        authority: authority.clone(),
                        generation: 1,
                    })
                    .unwrap()
                ],
            )
            .unwrap();
        drop(connection);

        let effect = RuntimeEffect {
            operation_id: OperationId::new(format!("independent-{mutation}:effect")),
            sequence: 55,
            action: RuntimeEffectAction::BuildReplica {
                build_id: authority.build_id.clone(),
                target: target.clone(),
                replication_address: "in-process://target".into(),
            },
        };
        let result = RuntimeEffectResult {
            operation_id: effect.operation_id.clone(),
            sequence: effect.sequence,
            outcome: RuntimeEffectOutcome::BuildReplica(BuildCompletion {
                build_id: authority.build_id.clone(),
                target,
                state: BuildEffectState::Completed(Box::new(BuildPostcondition {
                    authority: authority.clone(),
                    last_sequence: progress.last_sequence,
                    durable_lsn: progress.durable_lsn,
                    completed: true,
                    catch_up_boundary_lsn: progress.catch_up_boundary_lsn,
                })),
            }),
        };
        store.begin_effect(&effect).await.unwrap();
        store.mark_effect_applied(&effect, &result).await.unwrap();
        drop(store);

        let connection = Connection::open(&path).unwrap();
        match mutation {
            "authority" => {
                let mut replacement = authority.clone();
                replacement.target = replacement.source.clone();
                connection
                    .execute(
                        "UPDATE build_authority SET authority_json = ?1 WHERE build_id = ?2",
                        rusqlite::params![
                            serde_json::to_string(&replacement).unwrap(),
                            authority.build_id.as_str()
                        ],
                    )
                    .unwrap();
            }
            "selection" => {
                let mut replacement = authority.clone();
                replacement.build_id = OperationId::new("replacement-selection");
                connection
                    .execute(
                        "UPDATE runtime_lifecycle SET value_json = ?1 WHERE kind = ?2",
                        rusqlite::params![
                            serde_json::to_string(&BuildSelection {
                                authority: replacement,
                                generation: 2,
                            })
                            .unwrap(),
                            &selection_key
                        ],
                    )
                    .unwrap();
            }
            "abandonment" => {
                let json: String = connection
                    .query_row("SELECT state_json FROM agent_state", [], |row| row.get(0))
                    .unwrap();
                let mut abandoned: AgentState = serde_json::from_str(&json).unwrap();
                abandoned
                    .abandoned_builds
                    .insert(authority.build_id.clone());
                connection
                    .execute(
                        "UPDATE agent_state SET state_json = ?1 WHERE singleton = 1",
                        [serde_json::to_string(&abandoned).unwrap()],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let rows_before: (Option<String>, Option<String>, String) = (
            connection
                .query_row(
                    "SELECT authority_json FROM build_authority WHERE build_id = ?1",
                    [authority.build_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .unwrap(),
            connection
                .query_row(
                    "SELECT progress_json FROM build_progress WHERE build_id = ?1",
                    [authority.build_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .unwrap(),
            connection
                .query_row(
                    "SELECT value_json FROM runtime_lifecycle WHERE kind = ?1",
                    [&selection_key],
                    |row| row.get(0),
                )
                .unwrap(),
        );
        drop(connection);
        let store = SqliteStore::open_existing(&path, None).unwrap();
        let before = store.load_state().await.unwrap();
        assert!(store.complete_effect(&result).await.is_err(), "{mutation}");
        assert_eq!(store.load_state().await.unwrap(), before, "{mutation}");
        let connection = Connection::open(&path).unwrap();
        let rows_after: (Option<String>, Option<String>, String) = (
            connection
                .query_row(
                    "SELECT authority_json FROM build_authority WHERE build_id = ?1",
                    [authority.build_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .unwrap(),
            connection
                .query_row(
                    "SELECT progress_json FROM build_progress WHERE build_id = ?1",
                    [authority.build_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .unwrap(),
            connection
                .query_row(
                    "SELECT value_json FROM runtime_lifecycle WHERE kind = ?1",
                    [&selection_key],
                    |row| row.get(0),
                )
                .unwrap(),
        );
        assert_eq!(rows_after, rows_before, "{mutation}");
    }
}

#[tokio::test]
async fn token_bearing_topology_completion_rejects_replaced_durable_authority() {
    let (mut state, _, historical, mut result) = pending_acceptance_fixture();
    state.pending_effect = None;
    state.retained_result = None;
    state.removal_effects.clear();
    let authority = match &result.outcome {
        RuntimeEffectOutcome::HistoricalSecondaryRemovalAccepted(completion) => {
            completion.authority.clone().unwrap()
        }
        _ => unreachable!(),
    };
    let accepted = match &result.outcome {
        RuntimeEffectOutcome::HistoricalSecondaryRemovalAccepted(completion) => {
            completion.accepted_secondary_removal.clone()
        }
        _ => unreachable!(),
    };
    if let RuntimeEffectOutcome::HistoricalSecondaryRemovalAccepted(completion) =
        &mut result.outcome
    {
        completion.receipt = Some(Box::new(SecondaryRemovalReceipt {
            token: NativeOperationToken {
                authority: Some(authority.clone()),
                engine_session_id: "removal-engine".into(),
                engine_generation: 3,
            },
            preparation: None,
            witness: None,
            accepted: Some(accepted),
            verified_lsn: completion.verified_replication_lsn,
            committed_lsn: 10,
        }));
    }
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = SqliteStore::create_authorized(&path, state).unwrap();
    store.admit(&authority).await.unwrap();
    store.begin_effect(&historical).await.unwrap();
    store
        .mark_effect_applied(&historical, &result)
        .await
        .unwrap();
    let applied = store.load_state().await.unwrap();
    let mut replacement = authority.clone();
    replacement.current_configuration.epoch.configuration_number += 1;
    replacement.current_configuration.configuration_id =
        replacement.current_configuration.expected_id();
    Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE replica_authority SET authority_json = ?1 WHERE singleton = 1",
            [serde_json::to_string(&replacement).unwrap()],
        )
        .unwrap();
    assert!(store.complete_effect(&result).await.is_err());
    assert_eq!(store.load_state().await.unwrap(), applied);
    drop(store);

    let intent = removal_fixture::intent(&[1, 2, 3], 1);
    let (mut state, _, _, _) = pending_acceptance_fixture();
    state.identity.local_identity = intent.primary.clone();
    state.pending_effect = None;
    state.retained_result = None;
    state.removal_effects.clear();
    state.previous_configuration = None;
    state.current_configuration = Some(intent.previous_configuration.clone());
    state.highest_epoch = intent.previous_configuration.epoch;
    state.role = ReplicaRole::Primary;
    state.read_status = AccessStatus::ReconfigurationPending;
    state.write_status = AccessStatus::ReconfigurationPending;
    state.next_effect_sequence = 60;
    let authority = AdmittedAuthority {
        local_identity: intent.primary.clone(),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: intent.previous_configuration.clone(),
        switchover_handoff: None,
        scale_up: None,
        secondary_removal: None,
    };
    let target = intent
        .previous_configuration
        .members
        .iter()
        .find(|member| member.identity != intent.primary)
        .unwrap()
        .identity
        .clone();
    let request_id = SwitchoverRequestId::new("stale-authority-switchover");
    let effect = RuntimeEffect {
        operation_id: OperationId::new("stale-authority-switchover"),
        sequence: 60,
        action: RuntimeEffectAction::PrepareSwitchover {
            preparation_generation: 1,
            request_id: request_id.clone(),
            source: intent.primary.clone(),
            target: target.clone(),
            starting_configuration_id: intent.previous_configuration.configuration_id.clone(),
            starting_epoch: intent.previous_configuration.epoch,
        },
    };
    let result = RuntimeEffectResult {
        operation_id: effect.operation_id.clone(),
        sequence: effect.sequence,
        outcome: RuntimeEffectOutcome::SwitchoverPrepared(crate::effects::SwitchoverCompletion {
            authority: Some(authority.clone()),
            role: ReplicaRole::Primary,
            write_status: AccessStatus::ReconfigurationPending,
            current_progress: 5,
            committed_lsn: 4,
            receipt: Some(Box::new(SwitchoverReceipt {
                token: NativeOperationToken {
                    authority: Some(authority.clone()),
                    engine_session_id: "switchover-engine".into(),
                    engine_generation: 4,
                },
                preparation_generation: 1,
                request_id,
                source: intent.primary,
                target,
                starting_configuration_id: intent.previous_configuration.configuration_id.clone(),
                starting_epoch: intent.previous_configuration.epoch,
                handoff_lsn: 5,
                committed_lsn: 4,
            })),
        }),
    };
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = SqliteStore::create_authorized(&path, state).unwrap();
    store.admit(&authority).await.unwrap();
    store.begin_effect(&effect).await.unwrap();
    store.mark_effect_applied(&effect, &result).await.unwrap();
    let applied = store.load_state().await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute("DELETE FROM replica_authority WHERE singleton = 1", [])
        .unwrap();
    assert!(store.complete_effect(&result).await.is_err());
    assert_eq!(store.load_state().await.unwrap(), applied);
}

#[tokio::test]
async fn applied_completed_build_abandonment_clears_baseline_for_retirement() {
    let (mut state, _, _, _) = pending_acceptance_fixture();
    state.pending_effect = None;
    state.retained_result = None;
    state.removal_effects.clear();
    state.next_effect_sequence = 70;
    let configuration = state.current_configuration.clone().unwrap();
    let target = configuration
        .members
        .iter()
        .find(|member| member.identity != state.identity.local_identity)
        .unwrap()
        .identity
        .clone();
    let authority = BuildAuthority {
        build_id: OperationId::new("applied-abandoned-build"),
        kind: BuildAuthorityKind::Provisioning,
        source: state.identity.local_identity.clone(),
        target: target.clone(),
        current_configuration: configuration,
        replication_boundary_lsn: 7,
    };
    let progress = DurableBuildProgress {
        authority: authority.clone(),
        last_sequence: 2,
        durable_lsn: 9,
        completed: true,
        catch_up_boundary_lsn: Some(7),
    };
    state
        .build_progress
        .insert(authority.build_id.clone(), progress.clone());
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = SqliteStore::create_authorized(&path, state).unwrap();
    let effect = RuntimeEffect {
        operation_id: OperationId::new("applied-abandoned-build:build-replica"),
        sequence: 70,
        action: RuntimeEffectAction::BuildReplica {
            build_id: authority.build_id.clone(),
            target: target.clone(),
            replication_address: "in-process://target".into(),
        },
    };
    let result = RuntimeEffectResult {
        operation_id: effect.operation_id.clone(),
        sequence: effect.sequence,
        outcome: RuntimeEffectOutcome::BuildReplica(BuildCompletion {
            build_id: authority.build_id.clone(),
            target,
            state: BuildEffectState::Completed(Box::new(BuildPostcondition {
                authority: authority.clone(),
                last_sequence: progress.last_sequence,
                durable_lsn: progress.durable_lsn,
                completed: true,
                catch_up_boundary_lsn: progress.catch_up_boundary_lsn,
            })),
        }),
    };
    store.begin_effect(&effect).await.unwrap();
    store.mark_effect_applied(&effect, &result).await.unwrap();
    drop(store);

    let connection = Connection::open(&path).unwrap();
    let json: String = connection
        .query_row("SELECT state_json FROM agent_state", [], |row| row.get(0))
        .unwrap();
    let mut abandoned: AgentState = serde_json::from_str(&json).unwrap();
    abandoned
        .abandoned_builds
        .insert(authority.build_id.clone());
    connection
        .execute(
            "UPDATE agent_state SET state_json = ?1 WHERE singleton = 1",
            [serde_json::to_string(&abandoned).unwrap()],
        )
        .unwrap();
    drop(connection);

    let store = SqliteStore::open_existing(&path, None).unwrap();
    store.cancel_effect(&effect).await.unwrap();
    let cancelled = store.load_state().await.unwrap();
    assert!(cancelled.pending_effect.is_none());
    assert_eq!(cancelled.next_effect_sequence, 70);
    let retirement = RuntimeEffect {
        operation_id: OperationId::new("applied-abandoned-build:retire"),
        sequence: 70,
        action: RuntimeEffectAction::RetireBuild(authority.build_id),
    };
    assert_eq!(
        store.begin_effect(&retirement).await.unwrap(),
        BeginEffect::Execute(retirement)
    );
}

#[test]
fn schema_six_rejects_missing_nullable_canonical_fields_in_applied_results() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let (mut state, _, _, _) = pending_acceptance_fixture();
    state.pending_effect = Some(PendingEffect {
        effect: RuntimeEffect {
            operation_id: OperationId::new("strict-application-role"),
            sequence: state.next_effect_sequence,
            action: RuntimeEffectAction::ChangeApplicationRole(ReplicaRole::ActiveSecondary),
        },
        stage: EffectStage::EffectApplied,
        applied_result: Some(Box::new(RuntimeEffectResult {
            operation_id: OperationId::new("strict-application-role"),
            sequence: state.next_effect_sequence,
            outcome: RuntimeEffectOutcome::ApplicationRoleChanged {
                completion: RoleCompletion {
                    role: ReplicaRole::ActiveSecondary,
                    role_transition: None,
                },
                receipt: None,
            },
        })),
    });
    drop(SqliteStore::create_authorized(&path, state).unwrap());
    let connection = Connection::open(&path).unwrap();
    let json: String = connection
        .query_row("SELECT state_json FROM agent_state", [], |row| row.get(0))
        .unwrap();
    let mut value: Value = serde_json::from_str(&json).unwrap();
    let outcome = value
        .get_mut("pendingEffect")
        .and_then(Value::as_object_mut)
        .and_then(|pending| pending.get_mut("appliedResult"))
        .and_then(Value::as_object_mut)
        .and_then(|result| result.get_mut("outcome"))
        .and_then(Value::as_object_mut)
        .and_then(|outcome| outcome.get_mut("ApplicationRoleChanged"))
        .and_then(Value::as_object_mut)
        .expect("application-role outcome");
    outcome.remove("receipt");
    outcome
        .get_mut("completion")
        .and_then(Value::as_object_mut)
        .unwrap()
        .remove("role_transition");
    connection
        .execute(
            "UPDATE agent_state SET state_json = ?1 WHERE singleton = 1",
            [serde_json::to_string(&value).unwrap()],
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        SqliteStore::open_existing(&path, None),
        Err(crate::host::HostError::Serialization(_))
    ));
}

#[test]
fn schema_six_rejects_missing_nested_authority_fields() {
    for field in [
        "transition_kind",
        "previous_configuration",
        "switchover_handoff",
        "secondary_removal",
        "scale_up",
    ] {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let (mut state, _, _, result) = pending_acceptance_fixture();
        let authority = match result.outcome {
            RuntimeEffectOutcome::HistoricalSecondaryRemovalAccepted(completion) => {
                completion.authority.unwrap()
            }
            _ => unreachable!(),
        };
        state.pending_effect = Some(PendingEffect {
            effect: RuntimeEffect {
                operation_id: OperationId::new(format!("strict-authority-{field}")),
                sequence: state.next_effect_sequence,
                action: RuntimeEffectAction::WaitForCatchup,
            },
            stage: EffectStage::EffectApplied,
            applied_result: Some(Box::new(RuntimeEffectResult {
                operation_id: OperationId::new(format!("strict-authority-{field}")),
                sequence: state.next_effect_sequence,
                outcome: RuntimeEffectOutcome::CatchUpCompleted(CatchUpCompletion {
                    authority: Some(authority),
                    boundary_lsn: 9,
                }),
            })),
        });
        drop(SqliteStore::create_authorized(&path, state).unwrap());
        let connection = Connection::open(&path).unwrap();
        let json: String = connection
            .query_row("SELECT state_json FROM agent_state", [], |row| row.get(0))
            .unwrap();
        let mut value: Value = serde_json::from_str(&json).unwrap();
        value
            .get_mut("pendingEffect")
            .and_then(Value::as_object_mut)
            .and_then(|pending| pending.get_mut("appliedResult"))
            .and_then(Value::as_object_mut)
            .and_then(|result| result.get_mut("outcome"))
            .and_then(Value::as_object_mut)
            .and_then(|outcome| outcome.get_mut("CatchUpCompleted"))
            .and_then(Value::as_object_mut)
            .and_then(|completion| completion.get_mut("authority"))
            .and_then(Value::as_object_mut)
            .expect("catch-up authority")
            .remove(field);
        connection
            .execute(
                "UPDATE agent_state SET state_json = ?1 WHERE singleton = 1",
                [serde_json::to_string(&value).unwrap()],
            )
            .unwrap();
        drop(connection);
        assert!(
            matches!(
                SqliteStore::open_existing(&path, None),
                Err(crate::host::HostError::Serialization(_))
            ),
            "missing {field} was accepted"
        );
    }
}

#[test]
fn schema_six_rejects_missing_topology_witness_fields_at_applied_and_retained_cutpoints() {
    let intent = removal_fixture::intent(&[1, 2, 3], 1);
    let witness = removal_fixture::cleanup(&intent)
        .evidence
        .previous_read_quorum[0]
        .clone();
    for cutpoint in ["applied", "retained"] {
        for field in [
            "previousConfigurationId",
            "pendingOperationId",
            "retainedOperationId",
        ] {
            let directory = tempdir().unwrap();
            let path = SqliteStore::metadata_database_path(directory.path());
            let (mut state, _, _, _) = pending_acceptance_fixture();
            state.pending_effect = None;
            state.retained_result = None;
            state.removal_effects.clear();
            let effect = RuntimeEffect {
                operation_id: OperationId::new(format!("strict-witness-{cutpoint}-{field}")),
                sequence: state.next_effect_sequence,
                action: RuntimeEffectAction::ObserveSecondaryRemovalWitness(Box::new(
                    witness.clone(),
                )),
            };
            let result = RuntimeEffectResult {
                operation_id: effect.operation_id.clone(),
                sequence: effect.sequence,
                outcome: RuntimeEffectOutcome::SecondaryRemovalWitnessObserved {
                    witness: Box::new(witness.clone()),
                    receipt: None,
                },
            };
            if cutpoint == "applied" {
                state.pending_effect = Some(PendingEffect {
                    effect: effect.clone(),
                    stage: EffectStage::EffectApplied,
                    applied_result: Some(Box::new(result.clone())),
                });
            } else {
                state.retained_result = Some(RetainedResult {
                    operation_id: effect.operation_id.clone(),
                    record: crate::effects::RecordedEffect {
                        effect: effect.clone(),
                        result: result.clone(),
                    },
                });
            }
            drop(SqliteStore::create_authorized(&path, state).unwrap());
            let connection = Connection::open(&path).unwrap();
            let json: String = connection
                .query_row("SELECT state_json FROM agent_state", [], |row| row.get(0))
                .unwrap();
            let mut value: Value = serde_json::from_str(&json).unwrap();
            let result = if cutpoint == "applied" {
                value
                    .get_mut("pendingEffect")
                    .and_then(Value::as_object_mut)
                    .and_then(|pending| pending.get_mut("appliedResult"))
            } else {
                value
                    .get_mut("retainedResult")
                    .and_then(Value::as_object_mut)
                    .and_then(|retained| retained.get_mut("result"))
            };
            let witness_object = result
                .and_then(Value::as_object_mut)
                .and_then(|result| result.get_mut("outcome"))
                .and_then(Value::as_object_mut)
                .and_then(|outcome| outcome.get_mut("SecondaryRemovalWitnessObserved"))
                .and_then(Value::as_object_mut)
                .and_then(|completion| completion.get_mut("witness"))
                .and_then(Value::as_object_mut)
                .expect("secondary-removal witness");
            assert!(
                witness_object.remove(field).is_some(),
                "missing fixture key {field}; keys: {:?}",
                witness_object.keys().collect::<Vec<_>>()
            );
            connection
                .execute(
                    "UPDATE agent_state SET state_json = ?1 WHERE singleton = 1",
                    [serde_json::to_string(&value).unwrap()],
                )
                .unwrap();
            drop(connection);
            assert!(
                matches!(
                    SqliteStore::open_existing(&path, None),
                    Err(crate::host::HostError::Serialization(_))
                ),
                "{cutpoint} accepted missing {field}"
            );
        }
    }
}

#[test]
fn schema_six_rejects_missing_removal_evidence_fields_at_applied_and_retained_cutpoints() {
    for cutpoint in ["applied", "retained"] {
        for field in ["reducedWriteQuorum", "retirement"] {
            let directory = tempdir().unwrap();
            let path = SqliteStore::metadata_database_path(directory.path());
            let (mut state, _, effect, result) = pending_acceptance_fixture();
            state.pending_effect = None;
            state.retained_result = None;
            state.removal_effects.clear();
            if cutpoint == "applied" {
                state.pending_effect = Some(PendingEffect {
                    effect: effect.clone(),
                    stage: EffectStage::EffectApplied,
                    applied_result: Some(Box::new(result.clone())),
                });
            } else {
                state.retained_result = Some(RetainedResult {
                    operation_id: effect.operation_id.clone(),
                    record: crate::effects::RecordedEffect {
                        effect: effect.clone(),
                        result: result.clone(),
                    },
                });
            }
            drop(SqliteStore::create_authorized(&path, state).unwrap());
            let connection = Connection::open(&path).unwrap();
            let json: String = connection
                .query_row("SELECT state_json FROM agent_state", [], |row| row.get(0))
                .unwrap();
            let mut value: Value = serde_json::from_str(&json).unwrap();
            let result = if cutpoint == "applied" {
                value
                    .get_mut("pendingEffect")
                    .and_then(Value::as_object_mut)
                    .and_then(|pending| pending.get_mut("appliedResult"))
            } else {
                value
                    .get_mut("retainedResult")
                    .and_then(Value::as_object_mut)
                    .and_then(|retained| retained.get_mut("result"))
            };
            let cleanup = result
                .and_then(Value::as_object_mut)
                .and_then(|result| result.get_mut("outcome"))
                .and_then(Value::as_object_mut)
                .and_then(|outcome| outcome.get_mut("HistoricalSecondaryRemovalAccepted"))
                .and_then(Value::as_object_mut)
                .and_then(|completion| completion.get_mut("accepted_secondary_removal"))
                .and_then(Value::as_object_mut)
                .expect("historical cleanup");
            let removed = if field == "retirement" {
                cleanup.remove(field)
            } else {
                cleanup
                    .get_mut("evidence")
                    .and_then(Value::as_object_mut)
                    .unwrap()
                    .remove(field)
            };
            assert!(removed.is_some(), "missing fixture key {field}");
            connection
                .execute(
                    "UPDATE agent_state SET state_json = ?1 WHERE singleton = 1",
                    [serde_json::to_string(&value).unwrap()],
                )
                .unwrap();
            drop(connection);
            assert!(
                matches!(
                    SqliteStore::open_existing(&path, None),
                    Err(crate::host::HostError::Serialization(_))
                ),
                "{cutpoint} accepted missing {field}"
            );
        }
    }
}

#[tokio::test]
async fn pending_acceptance_conversion_is_atomic_one_way_and_resets_canonical_baseline() {
    for stage in [EffectStage::IntentCommitted, EffectStage::EffectApplied] {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let (mut state, ordinary, historical, result) = pending_acceptance_fixture();
        state.pending_effect.as_mut().unwrap().stage = stage;
        if stage == EffectStage::EffectApplied {
            state.pending_effect.as_mut().unwrap().applied_result = Some(Box::new(result.clone()));
        }
        let mut retained = RetainedResult {
            operation_id: OperationId::new("previous-effect"),
            record: crate::effects::RecordedEffect {
                effect: ordinary.clone(),
                result: result.clone(),
            },
        };
        retained.effect.operation_id = retained.operation_id.clone();
        retained.result.operation_id = retained.operation_id.clone();
        state.retained_result = Some(retained.clone());
        let store = SqliteStore::create_authorized(&path, state.clone()).unwrap();
        let authority = match &result.outcome {
            RuntimeEffectOutcome::HistoricalSecondaryRemovalAccepted(completion) => {
                completion.authority.as_ref().unwrap()
            }
            _ => unreachable!(),
        };
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
            Err(crate::host::HostError::Sqlite(_))
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
        state.pending_effect.as_mut().unwrap().stage = EffectStage::IntentCommitted;
        state.pending_effect.as_mut().unwrap().applied_result = None;
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
        assert!(store.mark_effect_applied(&ordinary, &result).await.is_err());
        assert!(store.cancel_effect(&ordinary).await.is_err());
        assert_eq!(store.load_state().await.unwrap(), state);
        store
            .mark_effect_applied(&historical, &result)
            .await
            .unwrap();
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
        let mut authority = match &result.outcome {
            RuntimeEffectOutcome::HistoricalSecondaryRemovalAccepted(completion) => {
                completion.authority.clone().unwrap()
            }
            _ => unreachable!(),
        };
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
                    record: crate::effects::RecordedEffect {
                        effect: ordinary.clone(),
                        result: result.clone(),
                    },
                });
            }
            18 => {
                state.removal_effects.insert(
                    ordinary.operation_id.clone(),
                    RetainedResult {
                        operation_id: ordinary.operation_id.clone(),
                        record: crate::effects::RecordedEffect {
                            effect: ordinary.clone(),
                            result: result.clone(),
                        },
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
                    crate::protocol::types::CleanupResourceIdentity::Present {
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
        Err(crate::host::HostError::SchemaMismatch {
            expected: 6,
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
    use crate::authority::RetiredAuthority;
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
        crate::protocol::types::ProcessSessionId::new("conflicting-session");
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
        crate::protocol::types::ProcessSessionId::new("new-session");
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
        Err(crate::host::HostError::InitializationNotAuthorized(_))
    ));
}

#[test]
fn fresh_replacement_store_requires_matching_provisioning_intent() {
    let (command, observed, _) = bootstrap_fixture();
    let provisioning = ProvisioningIntent {
        purpose: crate::protocol::types::ProvisioningPurpose::replacement(ReplicaIdentity {
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
    assert_eq!(identity.schema_version, 6);

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
            Err(crate::host::HostError::InitializationNotAuthorized(_))
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
        source_session_id: Some(crate::protocol::types::ProcessSessionId::new("source-1")),
        retire: false,
    };
    store.journal_build(&command).await.unwrap();
    command.source_session_id = Some(crate::protocol::types::ProcessSessionId::new("source-2"));
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
    let authority = BuildAuthority {
        build_id: build_id.clone(),
        kind: BuildAuthorityKind::Provisioning,
        source: identity.local_identity.clone(),
        target: target.clone(),
        current_configuration: initialize.bootstrap_configuration.clone(),
        replication_boundary_lsn: 0,
    };
    authority.validate().unwrap();
    assert!(store.admit_build(&authority).await.is_err());
    assert!(store.load_build(&build_id).await.unwrap().is_none());
    assert!(store.load_builds().await.unwrap().is_empty());

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
        Err(crate::host::HostError::DurableEffectConflict(_))
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
async fn custom_build_selection_fences_superseded_and_replacement_receipts_durably() {
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
    let a = BuildAuthority {
        build_id: OperationId::new("selected-a"),
        kind: BuildAuthorityKind::Provisioning,
        source: storage_identity.local_identity.clone(),
        target: identity(2, "target", "target-generation"),
        current_configuration: transition.current_configuration,
        replication_boundary_lsn: 0,
    };
    store.admit_build(&a).await.unwrap();
    let selected_a = store.select_build(&a).await.unwrap();
    let receipt_a = DurableBuildProgress {
        authority: a.clone(),
        last_sequence: 1,
        durable_lsn: 0,
        completed: true,
        catch_up_boundary_lsn: Some(0),
    };
    store
        .record_selected_build_progress(&selected_a, &receipt_a)
        .await
        .unwrap();
    let b = BuildAuthority {
        build_id: OperationId::new("selected-b"),
        ..a.clone()
    };
    store.admit_build(&b).await.unwrap();
    let selected_b = store.select_build(&b).await.unwrap();
    assert!(selected_b.generation > selected_a.generation);
    assert!(store.load_build(&a.build_id).await.unwrap().is_none());
    assert!(store.select_build(&a).await.is_err());
    assert!(
        store
            .record_selected_build_progress(&selected_a, &receipt_a)
            .await
            .is_err()
    );
    assert!(store.record_build_progress(&receipt_a).await.is_err());
    drop(store);
    let store = SqliteStore::open_existing(&path, Some(&storage_identity)).unwrap();
    assert_eq!(
        store.load_build_selection(&b.target).await.unwrap(),
        Some(selected_b.clone())
    );
    let receipt_b = DurableBuildProgress {
        authority: b.clone(),
        ..receipt_a.clone()
    };
    store
        .record_selected_build_progress(&selected_b, &receipt_b)
        .await
        .unwrap();
    let replacement = BuildAuthority {
        build_id: OperationId::new("selected-replacement"),
        target: identity(2, "replacement", "new-generation"),
        ..b.clone()
    };
    store.admit_build(&replacement).await.unwrap();
    store.select_build(&replacement).await.unwrap();
    assert!(
        store
            .record_selected_build_progress(&selected_b, &receipt_b)
            .await
            .is_err()
    );
    assert!(store.admit_build(&b).await.is_err());
}

#[tokio::test]
async fn committed_snapshot_boundary_is_durable_write_once_before_first_chunk() {
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
        build_id: OperationId::new("frozen-snapshot"),
        kind: BuildAuthorityKind::Provisioning,
        source: storage_identity.local_identity.clone(),
        target: identity(2, "target", "target-generation"),
        current_configuration: transition.current_configuration,
        replication_boundary_lsn: 9,
    };
    store.admit_build(&authority).await.unwrap();
    let progress = DurableBuildProgress {
        authority,
        last_sequence: 0,
        durable_lsn: 0,
        completed: false,
        catch_up_boundary_lsn: None,
    };
    store.record_build_progress(&progress).await.unwrap();
    drop(store);
    let store = SqliteStore::open_existing(&path, None).unwrap();
    assert_eq!(
        store
            .load_build_progress(&progress.authority.build_id)
            .await
            .unwrap(),
        Some(progress.clone())
    );
    for boundary in [-1, 8, 10, 11] {
        let changed = BuildAuthority {
            replication_boundary_lsn: boundary,
            ..progress.authority.clone()
        };
        assert!(store.admit_build(&changed).await.is_err());
    }
    store.record_build_progress(&progress).await.unwrap();
}

#[tokio::test]
async fn application_creation_permission_is_durable_one_way_and_opt_in() {
    use crate::host::state::ApplicationStorageBinding;
    use std::collections::BTreeMap;

    for opt_in in [false, true] {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let (command, observed, transition) = bootstrap_fixture();
        let identity = authorize_initialization(
            &command,
            &observed,
            InitializationAuthority::Bootstrap(&transition),
        )
        .unwrap();
        let mut state = AgentState::new(identity.clone());
        state.application_storage = opt_in.then(|| ApplicationStorageBinding {
            paths: BTreeMap::from([("database".into(), directory.path().join("application"))]),
            initializing: true,
        });
        let store = SqliteStore::create_authorized(&path, state.clone()).unwrap();
        drop(store);
        let store = SqliteStore::open_existing(&path, Some(&identity)).unwrap();
        assert_eq!(store.load_state().await.unwrap(), state);
        store.complete_application_initialization().await.unwrap();
        store.complete_application_initialization().await.unwrap();
        if let Some(binding) = &mut state.application_storage {
            binding.initializing = false;
        }
        drop(store);
        let store = SqliteStore::open_existing(&path, Some(&identity)).unwrap();
        assert_eq!(store.load_state().await.unwrap(), state);
        assert!(!directory.path().join("application").exists());
    }
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
        Err(crate::host::HostError::IdentityMismatch(_))
    ));

    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.pragma_update(None, "user_version", 99).unwrap();
    }

    assert!(matches!(
        SqliteStore::open_existing(&path, Some(&storage_identity)),
        Err(crate::host::HostError::SchemaMismatch {
            expected: SCHEMA_VERSION,
            observed: 99
        })
    ));

    fs::write(&path, b"not a sqlite database").unwrap();
    assert!(matches!(
        SqliteStore::open_existing(&path, Some(&storage_identity)),
        Err(crate::host::HostError::Corrupt(_))
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
        Err(crate::host::HostError::IdentityMismatch(_))
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
        primary_write_status: crate::protocol::types::AccessStatus::ReconfigurationPending,
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
        Err(crate::host::HostError::DurableEffectConflict(_))
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
        Err(crate::host::HostError::DurableEffectConflict(_))
    ));
}

#[tokio::test]
async fn diagnostic_handoff_defaults_but_canonical_authority_is_strict() {
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
    let mut json: Value = connection
        .query_row(
            "SELECT state_json FROM agent_state WHERE singleton = 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .map(|json| serde_json::from_str(&json).unwrap())
        .unwrap();
    assert!(
        json.as_object_mut()
            .unwrap()
            .remove("preparedSwitchover")
            .is_some()
    );
    connection
        .execute(
            "UPDATE agent_state SET state_json = ?1 WHERE singleton = 1",
            [serde_json::to_string(&json).unwrap()],
        )
        .unwrap();
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
    drop(reopened);

    let connection = Connection::open(&path).unwrap();
    let mut json: Value = connection
        .query_row(
            "SELECT authority_json FROM replica_authority WHERE singleton = 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .map(|json| serde_json::from_str(&json).unwrap())
        .unwrap();
    assert!(
        json.as_object_mut()
            .unwrap()
            .remove("switchover_handoff")
            .is_some()
    );
    connection
        .execute(
            "UPDATE replica_authority SET authority_json = ?1 WHERE singleton = 1",
            [serde_json::to_string(&json).unwrap()],
        )
        .unwrap();
    drop(connection);
    let reopened = SqliteStore::open_existing(&path, None).unwrap();
    assert!(reopened.load().await.is_err());
}
