use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kuberic_agent::command::admit_configuration;
use kuberic_agent::coordinator::Coordinator;
use kuberic_agent::runtime_adapter::{RuntimeAdapter, RuntimeEffectExecutor};
use kuberic_agent::sqlite_store::SqliteStore;
use kuberic_agent::state::{AgentState, SCHEMA_VERSION, StorageIdentity};
use kuberic_agent::store::{AgentStore, BeginConfiguration};
use kuberic_agent::{AgentError, Result};
use kuberic_protocol::command::{EnsureConfiguration, EnsureReplicaBuild};
use kuberic_protocol::types::{
    AccessStatus, AgentGeneration, BuildAuthority, BuildAuthorityKind, ConfigurationDescriptor,
    ConfigurationMember, EffectivePolicy, Epoch, InitializationId, OperationId, PodUid,
    ProcessSessionId, PvcUid, ReplicaId, ReplicaIdentity, ReplicaInstanceId, ReplicaRole,
    ResourceUid, TransitionKind,
};
use kuberic_runtime::RuntimeError;
use kuberic_runtime_internal::authority::AdmittedAuthority;
use kuberic_runtime_internal::effects::{
    RoleTransition, RuntimeEffect, RuntimeEffectAction, RuntimeEffectResult, RuntimePostcondition,
};
use tempfile::tempdir;

struct FakeRuntime {
    state: Mutex<RuntimePostcondition>,
    calls: Mutex<Vec<&'static str>>,
    fail_once: Mutex<Option<&'static str>>,
}

impl FakeRuntime {
    fn new() -> Self {
        Self {
            state: Mutex::new(RuntimePostcondition {
                open: true,
                role: ReplicaRole::None,
                role_transition: None,
                read_status: AccessStatus::NotPrimary,
                write_status: AccessStatus::NotPrimary,
                authority: None,
                current_progress: 7,
                verified_replication_lsn: Some(7),
                committed_lsn: 7,
                current_configuration_quorum_progress: 7,
                catch_up_boundary: None,
                catch_up_complete: true,
                builds: Vec::new(),
            }),
            calls: Mutex::new(Vec::new()),
            fail_once: Mutex::new(None),
        }
    }

    fn fail_once(&self, stage: &'static str) {
        *self.fail_once.lock().unwrap() = Some(stage);
    }
}

#[async_trait]
impl RuntimeEffectExecutor for FakeRuntime {
    async fn apply_runtime_effect(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        let stage = match &effect.action {
            RuntimeEffectAction::AdmitAuthority(_) => "admit",
            RuntimeEffectAction::AuthorizeFailoverPrefix(_) => "failover-prefix",
            RuntimeEffectAction::SetReadStatus(_) => "read",
            RuntimeEffectAction::SetAccessStatus { .. } => "access",
            RuntimeEffectAction::RefreshApplicationProgress => "get-lsn",
            RuntimeEffectAction::WaitForCatchup => "catchup",
            RuntimeEffectAction::SetWriteStatus(_) => "write",
            RuntimeEffectAction::ChangeReplicatorRole(_) => "replicator-role",
            RuntimeEffectAction::UpdateEpoch => "epoch",
            RuntimeEffectAction::ChangeApplicationRole(_) => "application-role",
            RuntimeEffectAction::AdmitBuildAuthority(_) => "admit-build",
            RuntimeEffectAction::BuildReplica { .. } => "build-replica",
            RuntimeEffectAction::RetireBuild(_) => "retire-build",
            action => panic!("unexpected coordinator action {action:?}"),
        };
        self.calls.lock().unwrap().push(stage);
        let mut fail_once = self.fail_once.lock().unwrap();
        if *fail_once == Some(stage) {
            *fail_once = None;
            return Err(RuntimeError::Application("injected failure".into()).into());
        }
        drop(fail_once);
        let mut state = self.state.lock().unwrap();
        match effect.action {
            RuntimeEffectAction::AdmitAuthority(authority) => {
                state.authority = Some(*authority);
                state.read_status = AccessStatus::ReconfigurationPending;
                state.write_status = AccessStatus::ReconfigurationPending;
            }
            RuntimeEffectAction::AuthorizeFailoverPrefix(lsn) => {
                state.verified_replication_lsn = Some(lsn);
            }
            RuntimeEffectAction::SetReadStatus(status) => state.read_status = status,
            RuntimeEffectAction::SetAccessStatus { read, write } => {
                state.read_status = read;
                state.write_status = write;
            }
            RuntimeEffectAction::SetWriteStatus(status) => state.write_status = status,
            RuntimeEffectAction::RefreshApplicationProgress
            | RuntimeEffectAction::WaitForCatchup => {}
            RuntimeEffectAction::ChangeReplicatorRole(role) => {
                state.role_transition = Some(RoleTransition {
                    completed_role: state.role,
                    target_role: role,
                    replicator_completed: true,
                    epoch_completed: role != ReplicaRole::Primary,
                    application_completed: false,
                });
            }
            RuntimeEffectAction::UpdateEpoch => {
                state
                    .role_transition
                    .as_mut()
                    .expect("replicator role stage")
                    .epoch_completed = true;
            }
            RuntimeEffectAction::ChangeApplicationRole(role) => {
                state.role = role;
                state.role_transition = None;
            }
            RuntimeEffectAction::AdmitBuildAuthority(authority) => {
                state
                    .builds
                    .push(kuberic_runtime_internal::effects::BuildPostcondition {
                        authority: *authority,
                        last_sequence: 0,
                        durable_lsn: 0,
                        completed: false,
                    });
            }
            RuntimeEffectAction::BuildReplica {
                build_id, target, ..
            } => {
                let current_progress = state.current_progress;
                state
                    .builds
                    .push(kuberic_runtime_internal::effects::BuildPostcondition {
                        authority: BuildAuthority {
                            build_id,
                            kind: BuildAuthorityKind::Provisioning,
                            source: identity(),
                            target,
                            current_configuration: command("build-authority", Epoch::new(0, 1))
                                .current_configuration,
                            replication_boundary_lsn: current_progress,
                        },
                        last_sequence: 1,
                        durable_lsn: current_progress,
                        completed: true,
                    });
            }
            RuntimeEffectAction::RetireBuild(_) => {}
            _ => unreachable!(),
        }
        Ok(RuntimeEffectResult {
            operation_id: effect.operation_id,
            sequence: effect.sequence,
            postcondition: state.clone(),
        })
    }
}

fn identity() -> ReplicaIdentity {
    ReplicaIdentity {
        replica_id: ReplicaId::new(1),
        instance_id: ReplicaInstanceId::new("pod-1"),
        agent_generation: AgentGeneration::new("generation-1"),
    }
}

fn storage_identity() -> StorageIdentity {
    StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: ResourceUid::new("resource-1"),
        pod_uid: PodUid::new("pod-1"),
        pvc_uid: PvcUid::new("pvc-1"),
        initialization_id: InitializationId::new("init-1"),
        local_identity: identity(),
        effective_policy: EffectivePolicy::fixed(1, 30).unwrap(),
    }
}

fn command(operation_id: &str, epoch: Epoch) -> EnsureConfiguration {
    let local = identity();
    let policy = EffectivePolicy::fixed(1, 30).unwrap();
    let current_configuration = ConfigurationDescriptor::new(
        epoch,
        local.replica_id,
        vec![ConfigurationMember {
            identity: local.clone(),
            role: ReplicaRole::Primary,
        }],
        policy.write_quorum,
    );
    EnsureConfiguration {
        operation_id: OperationId::new(operation_id),
        previous_configuration: None,
        current_configuration,
        previous_epoch: None,
        current_epoch: epoch,
        effective_policy: policy,
        local_replica_id: local.replica_id,
        expected_instance_id: local.instance_id,
        expected_agent_generation: local.agent_generation,
        transition_kind: TransitionKind::Bootstrap,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::ReconfigurationPending,
        current_only: false,
        retire_build_ids: Vec::new(),
    }
}

fn grant_command(operation_id: &str, epoch: Epoch) -> EnsureConfiguration {
    EnsureConfiguration {
        primary_write_status: AccessStatus::Granted,
        ..command(operation_id, epoch)
    }
}

fn store() -> (tempfile::TempDir, Arc<SqliteStore>) {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = SqliteStore::create_authorized(path, AgentState::new(storage_identity())).unwrap();
    (directory, Arc::new(store))
}

#[tokio::test]
async fn replacement_build_target_is_admitted_as_idle_secondary() {
    let (_directory, store) = store();
    let runtime = Arc::new(FakeRuntime::new());
    let coordinator = Coordinator::new(store.clone(), runtime);
    let target = identity();
    let source = ReplicaIdentity {
        replica_id: ReplicaId::new(2),
        instance_id: ReplicaInstanceId::new("source-pod"),
        agent_generation: AgentGeneration::new("source-generation"),
    };
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        source.replica_id,
        vec![ConfigurationMember {
            identity: source.clone(),
            role: ReplicaRole::Primary,
        }],
        1,
    );
    coordinator
        .ensure_build(EnsureReplicaBuild {
            operation_id: OperationId::new("replacement-build"),
            local_replica_id: target.replica_id,
            expected_instance_id: target.instance_id.clone(),
            expected_agent_generation: target.agent_generation.clone(),
            target: target.clone(),
            authority: Some(BuildAuthority {
                build_id: OperationId::new("replacement-build"),
                kind: BuildAuthorityKind::Provisioning,
                source,
                target,
                current_configuration: current,
                replication_boundary_lsn: 7,
            }),
            source_session_id: Some(ProcessSessionId::new("source-session")),
        })
        .await
        .unwrap();
    let state = store.load_state().await.unwrap();
    assert_eq!(state.role, ReplicaRole::IdleSecondary);
    assert!(state.retained_result.is_some_and(|result| {
        matches!(
            result.effect.action,
            RuntimeEffectAction::AdmitBuildAuthority(_)
        )
    }));
}

#[tokio::test]
async fn coordinator_converges_duplicates_and_retains_terminal_result() {
    let (_directory, store) = store();
    let runtime = Arc::new(FakeRuntime::new());
    let coordinator = Coordinator::new(store.clone(), runtime.clone());
    let command = command("configuration-1", Epoch::new(0, 1));

    coordinator
        .ensure_configuration(command.clone())
        .await
        .unwrap();
    let transition_calls = runtime.calls.lock().unwrap().len();
    let grant = grant_command("configuration-1-grant", Epoch::new(0, 1));
    let first = coordinator
        .ensure_configuration(grant.clone())
        .await
        .unwrap();
    assert_eq!(
        &runtime.calls.lock().unwrap()[transition_calls..],
        ["access"],
        "same-authority access changes must not re-admit or catch up authority"
    );
    let calls = runtime.calls.lock().unwrap().clone();
    let duplicate = coordinator.ensure_configuration(grant).await.unwrap();
    assert_eq!(duplicate, first);
    assert_eq!(*runtime.calls.lock().unwrap(), calls);

    let no_quorum = EnsureConfiguration {
        operation_id: OperationId::new("configuration-1-no-quorum"),
        primary_write_status: AccessStatus::NoWriteQuorum,
        ..grant_command("unused", Epoch::new(0, 1))
    };
    coordinator.ensure_configuration(no_quorum).await.unwrap();
    assert_eq!(runtime.calls.lock().unwrap().last(), Some(&"access"));
    let state = store.load_state().await.unwrap();
    assert!(state.reconfiguration.is_none());
    assert_eq!(state.read_status, AccessStatus::Granted);
    assert_eq!(state.write_status, AccessStatus::NoWriteQuorum);
}

#[tokio::test]
async fn failover_updates_epoch_before_get_lsn_and_can_publish_no_write_quorum() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let local = identity();
    let second = ReplicaIdentity {
        replica_id: ReplicaId::new(2),
        instance_id: ReplicaInstanceId::new("pod-2"),
        agent_generation: AgentGeneration::new("generation-2"),
    };
    let third = ReplicaIdentity {
        replica_id: ReplicaId::new(3),
        instance_id: ReplicaInstanceId::new("pod-3"),
        agent_generation: AgentGeneration::new("generation-3"),
    };
    let policy = EffectivePolicy::fixed(3, 30).unwrap();
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        second.replica_id,
        vec![
            ConfigurationMember {
                identity: local.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: second.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: third.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        policy.write_quorum,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        local.replica_id,
        vec![
            ConfigurationMember {
                identity: local.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: second,
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: third,
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        policy.write_quorum,
    );
    let mut durable = AgentState::new(StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: ResourceUid::new("resource-1"),
        pod_uid: PodUid::new("pod-1"),
        pvc_uid: PvcUid::new("pvc-1"),
        initialization_id: InitializationId::new("init-1"),
        local_identity: local.clone(),
        effective_policy: policy.clone(),
    });
    durable.highest_epoch = previous.epoch;
    durable.current_configuration = Some(previous.clone());
    durable.role = ReplicaRole::ActiveSecondary;
    durable.read_status = AccessStatus::Granted;
    durable.write_status = AccessStatus::NotPrimary;
    let store = Arc::new(SqliteStore::create_authorized(path, durable).unwrap());
    let runtime = Arc::new(FakeRuntime::new());
    {
        let mut state = runtime.state.lock().unwrap();
        state.role = ReplicaRole::ActiveSecondary;
        state.read_status = AccessStatus::Granted;
        state.write_status = AccessStatus::NotPrimary;
        state.authority = Some(AdmittedAuthority {
            local_identity: local.clone(),
            transition_kind: None,
            previous_configuration: None,
            current_configuration: previous.clone(),
        });
    }
    let coordinator = Coordinator::new(store.clone(), runtime.clone());
    coordinator
        .ensure_configuration(EnsureConfiguration {
            operation_id: OperationId::new("failover-no-quorum"),
            previous_configuration: Some(previous.clone()),
            current_configuration: current.clone(),
            previous_epoch: Some(previous.epoch),
            current_epoch: current.epoch,
            effective_policy: policy,
            local_replica_id: local.replica_id,
            expected_instance_id: local.instance_id,
            expected_agent_generation: local.agent_generation,
            transition_kind: TransitionKind::Failover,
            failover_safe_lsn: Some(7),
            primary_write_status: AccessStatus::NoWriteQuorum,
            current_only: false,
            retire_build_ids: Vec::new(),
        })
        .await
        .unwrap();

    let calls = runtime.calls.lock().unwrap().clone();
    let admit = calls.iter().position(|stage| *stage == "admit").unwrap();
    let prefix = calls
        .iter()
        .position(|stage| *stage == "failover-prefix")
        .unwrap();
    let epoch = calls.iter().position(|stage| *stage == "epoch").unwrap();
    let get_lsn = calls.iter().position(|stage| *stage == "get-lsn").unwrap();
    assert!(admit < prefix && prefix < epoch && epoch < get_lsn);
    let state = store.load_state().await.unwrap();
    assert_eq!(state.highest_epoch, current.epoch);
    assert_eq!(state.role, ReplicaRole::Primary);
    assert_eq!(state.read_status, AccessStatus::ReconfigurationPending);
    assert_eq!(state.write_status, AccessStatus::NoWriteQuorum);
    assert_eq!(
        state.deactivation.as_ref().map(|value| value.epoch),
        Some(current.epoch)
    );
}

#[tokio::test]
async fn coordinator_rejects_operation_reuse_and_stale_authority() {
    let (_directory, store) = store();
    let runtime = Arc::new(FakeRuntime::new());
    let coordinator = Coordinator::new(store.clone(), runtime);
    coordinator
        .ensure_configuration(command("same-operation", Epoch::new(0, 2)))
        .await
        .unwrap();

    assert!(matches!(
        coordinator
            .ensure_configuration(command("same-operation", Epoch::new(0, 3)))
            .await,
        Err(AgentError::EffectConflict(_))
    ));
    assert!(matches!(
        coordinator
            .ensure_configuration(command("stale-operation", Epoch::new(0, 1)))
            .await,
        Err(AgentError::CommandRejected(_))
    ));
}

#[tokio::test]
async fn bootstrap_resumes_pending_effect_without_repeating_completed_effects() {
    let (directory, store) = store();
    let runtime = Arc::new(FakeRuntime::new());
    runtime.fail_once("epoch");
    let command = command("restart-operation", Epoch::new(0, 1));
    let coordinator = Coordinator::new(store.clone(), runtime.clone());
    assert!(
        coordinator
            .ensure_configuration(command.clone())
            .await
            .is_err()
    );
    let calls_before = runtime.calls.lock().unwrap().clone();
    assert_eq!(
        calls_before,
        [
            "admit",
            "read",
            "get-lsn",
            "write",
            "replicator-role",
            "epoch"
        ]
    );
    drop(coordinator);
    drop(store);

    let reopened = Arc::new(
        SqliteStore::open_existing(
            SqliteStore::metadata_database_path(directory.path()),
            Some(&storage_identity()),
        )
        .unwrap(),
    );
    let resumed = Coordinator::new(reopened, runtime.clone());
    resumed.ensure_configuration(command).await.unwrap();
    assert_eq!(
        runtime.calls.lock().unwrap().as_slice(),
        [
            "admit",
            "read",
            "get-lsn",
            "write",
            "replicator-role",
            "epoch",
            "epoch",
            "application-role",
            "access",
        ]
    );
}

#[tokio::test]
async fn bootstrap_observes_effect_complete_before_stage_advance() {
    let (_directory, store) = store();
    let runtime = Arc::new(FakeRuntime::new());
    let command = command("lost-stage-advance", Epoch::new(0, 1));
    store.begin_configuration(&command).await.unwrap();
    let authority = admit_configuration(&command, &store.load_state().await.unwrap()).unwrap();
    RuntimeAdapter::new(store.clone(), runtime.clone())
        .execute(RuntimeEffect {
            operation_id: OperationId::new("lost-stage-advance:admit-authority"),
            sequence: 1,
            action: RuntimeEffectAction::AdmitAuthority(Box::new(authority)),
        })
        .await
        .unwrap();

    Coordinator::new(store, runtime.clone())
        .ensure_configuration(command)
        .await
        .unwrap();
    assert_eq!(
        runtime
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|stage| **stage == "admit")
            .count(),
        1
    );
}

#[tokio::test]
async fn newer_epoch_supersedes_pending_command_and_its_wait_effect() {
    let (_directory, store) = store();
    let old = grant_command("old-command", Epoch::new(0, 1));
    assert!(matches!(
        store.begin_configuration(&old).await.unwrap(),
        BeginConfiguration::Execute(_)
    ));
    store
        .begin_effect(&RuntimeEffect {
            operation_id: OperationId::new("old-command:catchup"),
            sequence: 1,
            action: RuntimeEffectAction::WaitForCatchup,
        })
        .await
        .unwrap();

    let newer = grant_command("new-command", Epoch::new(0, 2));
    assert!(matches!(
        store.begin_configuration(&newer).await.unwrap(),
        BeginConfiguration::Superseded(_)
    ));
    let state = store.load_state().await.unwrap();
    assert_eq!(
        state.reconfiguration.as_ref().map(|record| &record.command),
        Some(&newer)
    );
    assert!(state.pending_effect.is_none());
}

#[tokio::test]
async fn bootstrap_resumes_the_enclosing_configuration_after_restart() {
    let (directory, store) = store();
    let runtime = Arc::new(FakeRuntime::new());
    runtime.fail_once("access");
    let command = command("resume-configuration", Epoch::new(0, 1));
    let coordinator = Coordinator::new(store.clone(), runtime.clone());
    assert!(
        coordinator
            .ensure_configuration(command.clone())
            .await
            .is_err()
    );
    assert_eq!(
        store
            .load_state()
            .await
            .unwrap()
            .reconfiguration
            .unwrap()
            .stage,
        kuberic_agent::state::CoordinatorStage::Activate
    );
    drop(coordinator);
    drop(store);

    let reopened = Arc::new(
        SqliteStore::open_existing(
            SqliteStore::metadata_database_path(directory.path()),
            Some(&storage_identity()),
        )
        .unwrap(),
    );
    let resumed = Coordinator::new(reopened.clone(), runtime);
    let result = resumed.resume_configuration().await.unwrap().unwrap();
    assert_eq!(result.command, command);
    let state = reopened.load_state().await.unwrap();
    assert!(state.reconfiguration.is_none());
    assert_eq!(
        state
            .retained_command
            .unwrap()
            .command
            .operation_id
            .as_str(),
        "resume-configuration"
    );
}

#[tokio::test]
async fn current_only_replay_resumes_after_durable_pc_removal() {
    let directory = tempdir().unwrap();
    let local = identity();
    let policy = EffectivePolicy::fixed(3, 30).unwrap();
    let secondary = |id: i64, instance: &str| ConfigurationMember {
        identity: ReplicaIdentity {
            replica_id: ReplicaId::new(id),
            instance_id: ReplicaInstanceId::new(instance),
            agent_generation: AgentGeneration::new(format!("generation-{instance}")),
        },
        role: ReplicaRole::ActiveSecondary,
    };
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        local.replica_id,
        vec![
            ConfigurationMember {
                identity: local.clone(),
                role: ReplicaRole::Primary,
            },
            secondary(2, "secondary"),
            secondary(3, "old"),
        ],
        policy.write_quorum,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        local.replica_id,
        vec![
            ConfigurationMember {
                identity: local.clone(),
                role: ReplicaRole::Primary,
            },
            secondary(2, "secondary"),
            secondary(3, "replacement"),
        ],
        policy.write_quorum,
    );
    let mut state = AgentState::new(StorageIdentity {
        effective_policy: policy.clone(),
        ..storage_identity()
    });
    state.role = ReplicaRole::Primary;
    state.highest_epoch = current.epoch;
    state.previous_configuration = Some(previous.clone());
    state.current_configuration = Some(current.clone());
    let store = Arc::new(
        SqliteStore::create_authorized(
            SqliteStore::metadata_database_path(directory.path()),
            state,
        )
        .unwrap(),
    );
    let runtime = Arc::new(FakeRuntime::new());
    {
        let mut runtime_state = runtime.state.lock().unwrap();
        runtime_state.role = ReplicaRole::Primary;
        runtime_state.authority = Some(AdmittedAuthority {
            local_identity: local.clone(),
            transition_kind: Some(TransitionKind::Replacement),
            previous_configuration: Some(previous),
            current_configuration: current.clone(),
        });
    }
    let command = EnsureConfiguration {
        operation_id: OperationId::new("replacement-current-only"),
        previous_configuration: None,
        current_configuration: current.clone(),
        previous_epoch: None,
        current_epoch: current.epoch,
        effective_policy: policy,
        local_replica_id: local.replica_id,
        expected_instance_id: local.instance_id,
        expected_agent_generation: local.agent_generation,
        transition_kind: TransitionKind::Replacement,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::ReconfigurationPending,
        current_only: true,
        retire_build_ids: vec![OperationId::new("replacement-build")],
    };
    runtime.fail_once("read");
    let coordinator = Coordinator::new(store.clone(), runtime.clone());
    assert!(
        coordinator
            .ensure_configuration(command.clone())
            .await
            .is_err()
    );
    let interrupted = store.load_state().await.unwrap();
    assert!(interrupted.previous_configuration.is_none());
    assert!(interrupted.reconfiguration.is_some());

    coordinator
        .ensure_configuration(command.clone())
        .await
        .unwrap();
    let calls = runtime.calls.lock().unwrap().clone();
    coordinator.ensure_configuration(command).await.unwrap();
    assert_eq!(*runtime.calls.lock().unwrap(), calls);
}

#[test]
fn same_epoch_new_operation_cannot_replace_durable_membership() {
    let local = identity();
    let policy = EffectivePolicy::fixed(3, 30).unwrap();
    let member = |id: i64, instance: &str, role: ReplicaRole| ConfigurationMember {
        identity: ReplicaIdentity {
            replica_id: ReplicaId::new(id),
            instance_id: ReplicaInstanceId::new(instance),
            agent_generation: AgentGeneration::new(format!("generation-{instance}")),
        },
        role,
    };
    let existing = ConfigurationDescriptor::new(
        Epoch::new(0, 5),
        local.replica_id,
        vec![
            ConfigurationMember {
                identity: local.clone(),
                role: ReplicaRole::Primary,
            },
            member(2, "secondary", ReplicaRole::ActiveSecondary),
            member(3, "old", ReplicaRole::ActiveSecondary),
        ],
        policy.write_quorum,
    );
    let conflicting = ConfigurationDescriptor::new(
        existing.epoch,
        local.replica_id,
        vec![
            ConfigurationMember {
                identity: local.clone(),
                role: ReplicaRole::Primary,
            },
            member(2, "secondary", ReplicaRole::ActiveSecondary),
            member(3, "replacement", ReplicaRole::ActiveSecondary),
        ],
        policy.write_quorum,
    );
    let mut state = AgentState::new(StorageIdentity {
        effective_policy: policy.clone(),
        ..storage_identity()
    });
    state.highest_epoch = existing.epoch;
    state.current_configuration = Some(existing);
    let command = EnsureConfiguration {
        operation_id: OperationId::new("conflicting-same-epoch"),
        previous_configuration: None,
        current_configuration: conflicting,
        previous_epoch: None,
        current_epoch: Epoch::new(0, 5),
        effective_policy: policy,
        local_replica_id: local.replica_id,
        expected_instance_id: local.instance_id,
        expected_agent_generation: local.agent_generation,
        transition_kind: TransitionKind::Bootstrap,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::ReconfigurationPending,
        current_only: false,
        retire_build_ids: Vec::new(),
    };
    assert!(matches!(
        admit_configuration(&command, &state),
        Err(AgentError::CommandRejected(_))
    ));
}

#[tokio::test]
async fn concurrent_matching_commands_coalesce_without_panicking() {
    let (_directory, store) = store();
    let runtime = Arc::new(FakeRuntime::new());
    let coordinator = Arc::new(Coordinator::new(store, runtime.clone()));
    let command = command("concurrent-command", Epoch::new(0, 1));
    let first_coordinator = coordinator.clone();
    let first_command = command.clone();
    let first =
        tokio::spawn(async move { first_coordinator.ensure_configuration(first_command).await });
    let second_coordinator = coordinator.clone();
    let second =
        tokio::spawn(async move { second_coordinator.ensure_configuration(command).await });
    assert_eq!(
        first.await.unwrap().unwrap(),
        second.await.unwrap().unwrap()
    );
    assert_eq!(
        runtime
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|stage| **stage == "admit")
            .count(),
        1
    );
}
