use crate::protocol::command::ProtocolCommand;
use crate::protocol::observation::*;
use crate::protocol::types::ConfigurationId;
use crate::test_controller::Plan;
#[allow(dead_code)]
#[path = "support/scale_up_model.rs"]
pub(super) mod scale_up_model;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use super::tempdir;
use crate::RuntimeError;
use crate::authority::{AdmittedAuthority, BuildAuthorityStore, BuildProgressStore};
use crate::effects::{
    RoleTransition, RuntimeEffect, RuntimeEffectAction, RuntimeEffectResult, RuntimePostcondition,
};
use crate::host::Result;
use crate::host::command::{admit_configuration, admit_persisted_configuration};
use crate::host::coordinator::Coordinator;
use crate::host::runtime_adapter::{RuntimeAdapter, RuntimeEffectExecutor};
use crate::host::sqlite_store::SqliteStore;
use crate::host::state::{
    AgentState, CoordinatorStage, ReconfigurationRecord, RetainedCommandResult, SCHEMA_VERSION,
    StorageIdentity,
};
use crate::host::store::{AgentStore, BeginConfiguration};
use crate::protocol::command::{EnsureConfiguration, EnsureReplicaBuild, PrepareSwitchover};
use crate::protocol::types::{
    AccessStatus, AgentGeneration, BuildAuthority, BuildAuthorityKind, ConfigurationDescriptor,
    ConfigurationMember, EffectivePolicy, Epoch, InitializationId, OperationId, PodUid,
    ProcessSessionId, ProvisioningIntent, ProvisioningPurpose, PvcUid, ReplicaId, ReplicaIdentity,
    ReplicaInstanceId, ReplicaRole, ResourceUid, ScaleUpConfigurationEvidence,
    ScaleUpFailoverEvidence, ScaleUpIntent, ScaleUpProvisioning, ScaleUpStage, ScaleUpWitness,
    SwitchoverHandoff, SwitchoverRequestId, TransitionKind,
};
use async_trait::async_trait;

use crate::removal_fixture;

fn removal_state(
    intent: &crate::protocol::types::SecondaryScaleDownIntent,
    target: bool,
) -> AgentState {
    let local = if target {
        &intent.target
    } else {
        &intent.primary
    };
    let mut state = AgentState::new(StorageIdentity {
        resource_uid: intent.resource_uid.clone(),
        local_identity: local.clone(),
        pod_uid: PodUid::new(local.instance_id.as_str()),
        effective_policy: intent.previous_policy.clone(),
        ..storage_identity()
    });
    state.current_configuration = Some(intent.previous_configuration.clone());
    state.highest_epoch = intent.previous_configuration.epoch;
    state.role = if target {
        ReplicaRole::ActiveSecondary
    } else {
        ReplicaRole::Primary
    };
    state.read_status = AccessStatus::Granted;
    state.write_status = if target {
        AccessStatus::NotPrimary
    } else {
        AccessStatus::Granted
    };
    state
}

fn removal_runtime(state: &AgentState) -> Arc<FakeRuntime> {
    let runtime = Arc::new(FakeRuntime::new());
    {
        let mut snapshot = runtime.state.lock().unwrap();
        snapshot.role = state.role;
        snapshot.read_status = state.read_status;
        snapshot.write_status = state.write_status;
        snapshot.authority = Some(AdmittedAuthority {
            local_identity: state.identity.local_identity.clone(),
            transition_kind: None,
            previous_configuration: state.previous_configuration.clone(),
            current_configuration: state.current_configuration.clone().unwrap(),
            switchover_handoff: None,
            scale_up: None,
            secondary_removal: state.secondary_removal_evidence.clone(),
        });
        snapshot.prepared_secondary_removal = state.prepared_secondary_removal.clone();
    }
    runtime
}

#[test]
fn accepted_removal_access_command_preserves_admitted_evidence() {
    let intent = removal_fixture::intent(&[1, 2], 1);
    let mut state = removal_state(&intent, false);
    state.current_configuration = Some(intent.current_configuration.clone());
    state.highest_epoch = intent.current_configuration.epoch;
    state.admitted_policy = Some(intent.current_policy.clone());
    state.secondary_removal_evidence = Some(removal_fixture::evidence(&intent));
    let mut grant = removal_fixture::configuration_command(&intent, true);
    grant.operation_id = OperationId::new("stable-grant");
    grant.transition_kind = TransitionKind::Bootstrap;
    grant.current_only = false;
    grant.previous_policy = None;
    grant.secondary_removal_evidence = None;
    grant.primary_write_status = AccessStatus::Granted;
    assert!(admit_configuration(&grant, &state).is_err());
    state.accepted_secondary_removal = Some(removal_fixture::cleanup(&intent));
    let admitted = admit_configuration(&grant, &state).unwrap();
    assert_eq!(admitted.secondary_removal, state.secondary_removal_evidence);
    assert_eq!(admitted.previous_configuration, None);
    assert_eq!(admitted.current_configuration, intent.current_configuration);
}

#[tokio::test]
async fn older_excluded_target_cannot_admit_exact_previous_authority_retirement() {
    let intent = removal_fixture::intent(&[1, 2, 3, 4], 1);
    let mut state = removal_state(&intent, true);
    let previous = &intent.previous_configuration;
    let older = ConfigurationDescriptor::new(
        Epoch::new(
            previous.epoch.data_loss_number,
            previous.epoch.configuration_number - 1,
        ),
        previous.primary_id,
        previous.members.clone(),
        previous.write_quorum,
    );
    state.highest_epoch = older.epoch;
    state.current_configuration = Some(older);
    let directory = tempdir().unwrap();
    let store = Arc::new(
        SqliteStore::create_authorized(directory.path().join("agent.db"), state.clone()).unwrap(),
    );
    let runtime = removal_runtime(&state);
    let result = Coordinator::new(store.clone(), runtime)
        .ensure_replica_retired(
            removal_fixture::retire_command(&intent),
            crate::protocol::types::ProcessSessionId::new("returned"),
            1,
        )
        .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("exact previous secondary authority")
    );
    assert_eq!(store.load_state().await.unwrap(), state);
}

#[tokio::test]
async fn reduction_coordinates_retained_secondaries_but_never_admits_the_excluded_target() {
    let intent = removal_fixture::intent(&[1, 2, 3], 1);
    for local in [
        &intent.current_configuration.members[1].identity,
        &intent.target,
    ] {
        let mut state = removal_state(&intent, false);
        state.identity.local_identity = local.clone();
        state.identity.pod_uid = PodUid::new(local.instance_id.as_str());
        state.role = ReplicaRole::ActiveSecondary;
        state.write_status = AccessStatus::NotPrimary;
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let store = Arc::new(SqliteStore::create_authorized(&path, state.clone()).unwrap());
        let runtime = removal_runtime(&state);
        let coordinator = Coordinator::new(store.clone(), runtime.clone());
        for current_only in [false, true] {
            let mut command = removal_fixture::configuration_command(&intent, current_only);
            command.local_replica_id = local.replica_id;
            command.expected_instance_id = local.instance_id.clone();
            command.expected_agent_generation = local.agent_generation.clone();
            command.operation_id = intent.command_operation_id(
                if current_only {
                    crate::protocol::types::SecondaryRemovalStage::CurrentOnly
                } else {
                    crate::protocol::types::SecondaryRemovalStage::PreviousCurrent
                },
                local,
            );
            let result = coordinator.ensure_configuration(command.clone()).await;
            if local == &intent.target {
                assert!(result.is_err());
                assert_eq!(store.load_state().await.unwrap(), state);
                assert!(runtime.calls.lock().unwrap().is_empty());
            } else {
                result.unwrap();
                let reopened =
                    Arc::new(SqliteStore::open_existing(&path, Some(&state.identity)).unwrap());
                Coordinator::new(reopened, runtime.clone())
                    .ensure_configuration(command)
                    .await
                    .unwrap();
                let reduced = store.load_state().await.unwrap();
                assert_eq!(reduced.role, ReplicaRole::ActiveSecondary);
                assert_eq!(reduced.write_status, AccessStatus::NotPrimary);
                assert_eq!(reduced.admitted_policy, Some(intent.current_policy.clone()));
                assert!(reduced.prepared_secondary_removal.is_none());
            }
        }
    }
}

#[tokio::test]
async fn removal_commands_reject_identity_generation_primary_and_epoch_mutations_before_effects() {
    let intent = removal_fixture::intent(&[1, 2, 3], 1);
    for target in [false, true] {
        let state = removal_state(&intent, target);
        let directory = tempdir().unwrap();
        let store = Arc::new(
            SqliteStore::create_authorized(
                SqliteStore::metadata_database_path(directory.path()),
                state.clone(),
            )
            .unwrap(),
        );
        let runtime = removal_runtime(&state);
        let coordinator = Coordinator::new(store.clone(), runtime.clone());
        let session = ProcessSessionId::new("session-1");
        for mutation in 0..5 {
            if target {
                let mut command = removal_fixture::retire_command(&intent);
                match mutation {
                    0 => command.expected_agent_generation = AgentGeneration::new("stale"),
                    1 => command.expected_instance_id = ReplicaInstanceId::new("replaced"),
                    2 => command.local_replica_id = intent.primary.replica_id,
                    3 => {
                        command.committed.evidence.preparation.intent.primary =
                            intent.target.clone()
                    }
                    _ => {
                        command
                            .committed
                            .evidence
                            .preparation
                            .intent
                            .current_configuration
                            .epoch = Epoch::new(0, 1)
                    }
                }
                assert!(
                    coordinator
                        .ensure_replica_retired(command, session.clone(), 1)
                        .await
                        .is_err()
                );
            } else {
                let mut command = removal_fixture::prepare_command(&intent);
                match mutation {
                    0 => command.expected_agent_generation = AgentGeneration::new("stale"),
                    1 => command.expected_instance_id = ReplicaInstanceId::new("replaced"),
                    2 => command.local_replica_id = intent.target.replica_id,
                    3 => command.intent.primary = intent.target.clone(),
                    _ => command.intent.previous_configuration.epoch = Epoch::new(0, 1),
                }
                assert!(
                    coordinator
                        .ensure_secondary_removal_prepared(command, session.clone(), 1)
                        .await
                        .is_err()
                );
            }
            assert_eq!(store.load_state().await.unwrap(), state);
            assert!(runtime.calls.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn secondary_removal_replays_every_coordinator_stage_with_independent_policy() {
    for size in [2, 3] {
        for failure in [
            "admit",
            "read",
            "get-lsn",
            "write",
            "replicator-role",
            "epoch",
            "application-role",
            "access",
        ] {
            for current_only in [false, true] {
                let intent = removal_fixture::intent(&(1..=size).collect::<Vec<_>>(), 1);
                let state = removal_state(&intent, false);
                let provenance = state.identity.clone();
                let directory = tempdir().unwrap();
                let path = SqliteStore::metadata_database_path(directory.path());
                let store = Arc::new(SqliteStore::create_authorized(&path, state.clone()).unwrap());
                let runtime = removal_runtime(&state);
                let coordinator = Coordinator::new(store.clone(), runtime.clone());
                let preparation = coordinator
                    .ensure_secondary_removal_prepared(
                        removal_fixture::prepare_command(&intent),
                        ProcessSessionId::new("session-1"),
                        1,
                    )
                    .await
                    .unwrap();
                let mut joint = removal_fixture::configuration_command(&intent, false);
                joint
                    .secondary_removal_evidence
                    .as_mut()
                    .unwrap()
                    .preparation = preparation;
                joint
                    .secondary_removal_evidence
                    .as_mut()
                    .unwrap()
                    .reduced_write_quorum
                    .clear();
                let mut reduced = removal_fixture::configuration_command(&intent, true);
                reduced
                    .secondary_removal_evidence
                    .as_mut()
                    .unwrap()
                    .preparation = joint
                    .secondary_removal_evidence
                    .as_ref()
                    .unwrap()
                    .preparation
                    .clone();
                if current_only {
                    coordinator
                        .ensure_configuration(joint.clone())
                        .await
                        .unwrap();
                }
                let command = if current_only {
                    reduced.clone()
                } else {
                    joint.clone()
                };
                runtime.fail_once(failure);
                assert!(
                    coordinator
                        .ensure_configuration(command.clone())
                        .await
                        .is_err(),
                    "{failure}"
                );
                let interrupted = store.load_state().await.unwrap();
                assert_eq!(interrupted.identity, provenance);
                assert!(interrupted.pending_effect.is_some());
                drop(coordinator);
                drop(store);
                let store = Arc::new(SqliteStore::open_existing(&path, Some(&provenance)).unwrap());
                let coordinator = Coordinator::new(store.clone(), runtime);
                coordinator.resume_configuration().await.unwrap();
                coordinator
                    .ensure_configuration(joint.clone())
                    .await
                    .unwrap();
                coordinator
                    .ensure_configuration(reduced.clone())
                    .await
                    .unwrap();
                let final_state = store.load_state().await.unwrap();
                assert_eq!(final_state.identity, provenance);
                assert_eq!(
                    final_state.admitted_policy,
                    Some(intent.current_policy.clone())
                );
                assert!(final_state.previous_policy.is_none());
                assert!(final_state.previous_configuration.is_none());
                assert_ne!(final_state.write_status, AccessStatus::Granted);
                let before = final_state.clone();
                coordinator
                    .ensure_configuration(reduced.clone())
                    .await
                    .unwrap();
                assert_eq!(store.load_state().await.unwrap(), before);
                let mut mutation = reduced.clone();
                mutation
                    .secondary_removal_evidence
                    .as_mut()
                    .unwrap()
                    .previous_read_quorum[0]
                    .report_sequence += 1;
                assert!(coordinator.ensure_configuration(mutation).await.is_err());
                let mut wrong_policy = reduced.clone();
                wrong_policy.previous_policy = Some(intent.current_policy.clone());
                assert!(
                    coordinator
                        .ensure_configuration(wrong_policy)
                        .await
                        .is_err()
                );
                let mut grant = reduced;
                grant.primary_write_status = AccessStatus::Granted;
                assert!(coordinator.ensure_configuration(grant).await.is_err());
            }
        }
    }
}

#[tokio::test]
async fn secondary_removal_preparation_and_retirement_keep_exact_restart_receipts() {
    let intent = removal_fixture::intent(&[1, 2, 3], 1);
    for target in [false, true] {
        let state = removal_state(&intent, target);
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let store = Arc::new(SqliteStore::create_authorized(&path, state.clone()).unwrap());
        let runtime = removal_runtime(&state);
        let coordinator = Coordinator::new(store.clone(), runtime.clone());
        runtime.fail_once(if target { "retire" } else { "prepare-removal" });
        let session = ProcessSessionId::new("first-session");
        if target {
            assert!(
                coordinator
                    .ensure_replica_retired(
                        removal_fixture::retire_command(&intent),
                        session.clone(),
                        3
                    )
                    .await
                    .is_err()
            );
        } else {
            assert!(
                coordinator
                    .ensure_secondary_removal_prepared(
                        removal_fixture::prepare_command(&intent),
                        session.clone(),
                        3
                    )
                    .await
                    .is_err()
            );
        }
        let pending = store.load_state().await.unwrap().pending_effect.unwrap();
        drop(coordinator);
        drop(store);
        let store = Arc::new(SqliteStore::open_existing(&path, Some(&state.identity)).unwrap());
        let coordinator = Coordinator::new(store.clone(), runtime.clone());
        if target {
            let receipt = coordinator
                .ensure_replica_retired(
                    removal_fixture::retire_command(&intent),
                    ProcessSessionId::new("second-session"),
                    1,
                )
                .await
                .unwrap();
            assert_eq!(receipt.process_session_id, session);
            assert_eq!(receipt.report_sequence, 3);
            assert_eq!(receipt.role, ReplicaRole::None);
            assert!(receipt.application_closed && receipt.peers_fenced);
            let mut conflict = removal_fixture::retire_command(&intent);
            conflict.committed.current_only_write_quorum[0].report_sequence += 1;
            assert!(
                coordinator
                    .ensure_replica_retired(conflict, session.clone(), 4)
                    .await
                    .is_err()
            );
            assert!(
                coordinator
                    .ensure_configuration(removal_fixture::configuration_command(&intent, false))
                    .await
                    .is_err()
            );
        } else {
            let receipt = coordinator
                .ensure_secondary_removal_prepared(
                    removal_fixture::prepare_command(&intent),
                    ProcessSessionId::new("second-session"),
                    1,
                )
                .await
                .unwrap();
            assert_eq!(receipt.process_session_id, session);
            assert_eq!(receipt.report_sequence, 3);
            assert_eq!(receipt.boundary_lsn, 7);
            let mut conflict = removal_fixture::prepare_command(&intent);
            conflict.intent.cleanup.pvc = crate::protocol::types::CleanupResourceIdentity::Absent {
                name: "changed".into(),
            };
            assert!(
                coordinator
                    .ensure_secondary_removal_prepared(conflict, session.clone(), 4)
                    .await
                    .is_err()
            );
        }
        let terminal = store.load_state().await.unwrap();
        assert_eq!(
            terminal.removal_effects[&pending.effect.operation_id].effect,
            pending.effect
        );
        let calls = runtime.calls.lock().unwrap().len();
        if target {
            coordinator
                .ensure_replica_retired(
                    removal_fixture::retire_command(&intent),
                    session.clone(),
                    5,
                )
                .await
                .unwrap();
        } else {
            coordinator
                .ensure_secondary_removal_prepared(
                    removal_fixture::prepare_command(&intent),
                    session.clone(),
                    5,
                )
                .await
                .unwrap();
        }
        assert_eq!(runtime.calls.lock().unwrap().len(), calls);
        assert_eq!(store.load_state().await.unwrap(), terminal);
    }
}

struct FakeRuntime {
    state: Mutex<RuntimePostcondition>,
    calls: Mutex<Vec<&'static str>>,
    fail_once: Mutex<Option<&'static str>>,
}

impl FakeRuntime {
    fn new() -> Self {
        Self {
            state: Mutex::new(RuntimePostcondition {
                prepared_secondary_removal: None,
                retired_authority: None,
                accepted_secondary_removal: None,
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
            RuntimeEffectAction::PrepareSwitchover { .. } => "prepare-switchover",
            RuntimeEffectAction::PrepareSecondaryRemoval { .. } => "prepare-removal",
            RuntimeEffectAction::RetireReplica(_) => "retire",
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
                let preserve_scale_up = authority.scale_up.as_deref().is_some_and(|evidence| {
                    matches!(evidence, ScaleUpConfigurationEvidence::Admission { .. })
                        && state.role == ReplicaRole::Primary
                        && state.read_status == AccessStatus::Granted
                        && state.write_status == AccessStatus::Granted
                        && authority.local_role() == ReplicaRole::Primary
                });
                state.authority = Some(*authority);
                if !preserve_scale_up {
                    state.read_status = AccessStatus::ReconfigurationPending;
                    state.write_status = AccessStatus::ReconfigurationPending;
                }
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
            RuntimeEffectAction::PrepareSwitchover { .. } => {
                state.write_status = AccessStatus::ReconfigurationPending;
            }
            RuntimeEffectAction::PrepareSecondaryRemoval {
                intent,
                process_session_id,
                report_sequence,
            } => {
                state.read_status = AccessStatus::ReconfigurationPending;
                state.write_status = AccessStatus::ReconfigurationPending;
                state.prepared_secondary_removal =
                    Some(crate::protocol::types::SecondaryRemovalPreparation {
                        operation_id: effect.operation_id.clone(),
                        intent: *intent,
                        process_session_id,
                        report_sequence,
                        boundary_lsn: state.current_progress,
                    });
            }
            RuntimeEffectAction::RetireReplica(retired) => {
                state.open = false;
                state.role = ReplicaRole::None;
                state.read_status = AccessStatus::NotPrimary;
                state.write_status = AccessStatus::NotPrimary;
                state.authority = None;
                state.retired_authority = Some(*retired);
            }
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
                state.builds.push(crate::effects::BuildPostcondition {
                    authority: *authority,
                    last_sequence: 0,
                    durable_lsn: 0,
                    completed: false,
                    catch_up_boundary_lsn: None,
                });
            }
            RuntimeEffectAction::BuildReplica {
                build_id, target, ..
            } => {
                let current_progress = state.current_progress;
                state.builds.push(crate::effects::BuildPostcondition {
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
                    catch_up_boundary_lsn: None,
                });
            }
            RuntimeEffectAction::RetireBuild(build_id) => {
                state
                    .builds
                    .retain(|build| build.authority.build_id != build_id);
            }
            _ => unreachable!(),
        }
        Ok(RuntimeEffectResult {
            operation_id: effect.operation_id,
            sequence: effect.sequence,
            topology_receipt: None,
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
        previous_policy: None,
        secondary_removal_evidence: None,
        scale_up_evidence: None,
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
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
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

fn apply_evaluator_initialization(
    states: &mut BTreeMap<i64, AgentState>,
    command: &crate::protocol::command::InitializeAgentStore,
) {
    let identity = ReplicaIdentity {
        replica_id: command.local_replica_id,
        instance_id: command.expected_instance_id.clone(),
        agent_generation: command.assigned_agent_generation.clone(),
    };
    let mut state = AgentState::new(StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: command.resource_uid.clone(),
        pod_uid: command.expected_pod_uid.clone(),
        pvc_uid: command.expected_pvc_uid.clone(),
        initialization_id: command.initialization_id.clone(),
        local_identity: identity,
        effective_policy: command.effective_policy.clone(),
    });
    state.scale_up_initialization = command.provisioning.clone();
    states.insert(command.local_replica_id.value(), state);
}

fn apply_evaluator_configuration(
    states: &mut BTreeMap<i64, AgentState>,
    command: &EnsureConfiguration,
) {
    let state = states
        .get_mut(&command.local_replica_id.value())
        .expect("evaluator command target state");
    let evidence = command
        .scale_up_evidence
        .as_deref()
        .expect("scale-up command evidence");
    let intent = evidence.intent();
    if state.identity.local_identity == intent.target
        && !state.build_commands.contains_key(&intent.build_id)
    {
        let authority = BuildAuthority {
            build_id: intent.build_id.clone(),
            kind: BuildAuthorityKind::Provisioning,
            source: intent.primary.clone(),
            target: intent.target.clone(),
            current_configuration: intent.previous_configuration.clone(),
            replication_boundary_lsn: intent.snapshot_boundary_lsn,
        };
        state.build_commands.insert(
            intent.build_id.clone(),
            EnsureReplicaBuild {
                operation_id: intent.build_id.clone(),
                local_replica_id: intent.target.replica_id,
                expected_instance_id: intent.target.instance_id.clone(),
                expected_agent_generation: intent.target.agent_generation.clone(),
                target: intent.target.clone(),
                authority: Some(authority.clone()),
                source_session_id: Some(ProcessSessionId::new("evaluator-source-session")),
                retire: false,
            },
        );
        state.build_progress.insert(
            intent.build_id.clone(),
            crate::authority::DurableBuildProgress {
                authority,
                last_sequence: 3,
                durable_lsn: intent.catch_up_boundary_lsn,
                completed: true,
                catch_up_boundary_lsn: Some(intent.catch_up_boundary_lsn),
            },
        );
        state.role = ReplicaRole::IdleSecondary;
    }
    if state.identity.local_identity == intent.target {
        if matches!(evidence, ScaleUpConfigurationEvidence::Failover { .. }) {
            if command.current_only {
                assert_eq!(
                    state.scale_up_evidence.as_deref(),
                    command.scale_up_evidence.as_deref()
                );
            } else {
                let first_fenced_epoch = matches!(
                    state.scale_up_evidence.as_deref(),
                    Some(ScaleUpConfigurationEvidence::Admission {
                        intent: durable_intent
                    }) if durable_intent == intent
                ) && state.current_configuration.as_ref()
                    == Some(&intent.current_configuration);
                let finalized_epoch = matches!(
                    (
                        state.scale_up_evidence.as_deref(),
                        command.scale_up_evidence.as_deref(),
                    ),
                    (
                        Some(ScaleUpConfigurationEvidence::Failover {
                            evidence: installed
                        }),
                        Some(ScaleUpConfigurationEvidence::Failover {
                            evidence: commanded
                        }),
                    ) if installed.same_provisional_authority(commanded)
                ) && state
                    .current_configuration
                    .as_ref()
                    .is_some_and(|current| current.epoch < command.current_epoch);
                assert!(first_fenced_epoch || finalized_epoch);
            }
            if command.current_only {
                assert_eq!(
                    state.current_configuration.as_ref(),
                    Some(&command.current_configuration)
                );
            }
        }
        let build = state.build_commands.get(&intent.build_id).unwrap();
        let authority = build.authority.as_ref().unwrap();
        let progress = state.build_progress.get(&intent.build_id).unwrap();
        assert_eq!(build.target, intent.target);
        assert_eq!(authority.build_id, intent.build_id);
        assert_eq!(authority.source, intent.primary);
        assert_eq!(authority.target, intent.target);
        assert_eq!(
            authority.current_configuration,
            intent.previous_configuration
        );
        assert_eq!(
            authority.replication_boundary_lsn,
            intent.snapshot_boundary_lsn
        );
        assert_eq!(progress.authority, *authority);
        assert!(progress.completed);
        assert_eq!(
            progress.catch_up_boundary_lsn,
            Some(intent.catch_up_boundary_lsn)
        );
        assert!(progress.durable_lsn >= intent.catch_up_boundary_lsn);
    }
    let authority = admit_configuration(command, state).unwrap_or_else(|error| {
        panic!(
            "evaluator-generated command {} rejected for replica {}: {error}",
            command.operation_id, command.local_replica_id
        )
    });
    state.previous_configuration = authority.previous_configuration.clone();
    state.current_configuration = Some(authority.current_configuration.clone());
    state.highest_epoch = authority.current_configuration.epoch;
    state.scale_up_evidence = authority.scale_up.clone();
    state.admitted_policy = Some(intent.current_policy.clone());
    state.previous_policy = authority
        .previous_configuration
        .as_ref()
        .map(|_| intent.previous_policy.clone());
    state.role = authority.local_role();
    state.read_status = AccessStatus::Granted;
    state.write_status = if state.role == ReplicaRole::Primary {
        command.primary_write_status
    } else {
        AccessStatus::NotPrimary
    };
    if command.current_only {
        state.retired_builds.insert(intent.build_id.clone());
    }
    let retained = RetainedCommandResult {
        command: command.clone(),
        role: state.role,
        epoch: state.highest_epoch,
    };
    if command.current_only {
        state.completed_scale_up = Some(Box::new(retained.clone()));
    }
    state.retained_command = Some(retained);
}

fn apply_access_restoration(state: &mut AgentState) {
    let current = state.current_configuration.clone().unwrap();
    let identity = state.identity.local_identity.clone();
    let command = EnsureConfiguration {
        operation_id: OperationId::new(format!("post-scale-up-access-{}", identity.replica_id)),
        previous_configuration: None,
        current_configuration: current.clone(),
        previous_epoch: None,
        current_epoch: current.epoch,
        effective_policy: state.admitted_policy.clone().unwrap(),
        previous_policy: None,
        secondary_removal_evidence: None,
        scale_up_evidence: None,
        local_replica_id: identity.replica_id,
        expected_instance_id: identity.instance_id,
        expected_agent_generation: identity.agent_generation,
        transition_kind: TransitionKind::Bootstrap,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::Granted,
        current_only: false,
        retire_build_ids: Vec::new(),
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    };
    let authority = admit_configuration(&command, state).unwrap();
    assert!(authority.scale_up.is_some());
    state.retained_command = Some(RetainedCommandResult {
        command,
        role: state.role,
        epoch: state.highest_epoch,
    });
    assert!(state.completed_scale_up.is_some());
}

fn scale_up_fixture(current_only: bool) -> (AgentState, EnsureConfiguration, AdmittedAuthority) {
    let primary = identity();
    let candidate = ReplicaIdentity {
        replica_id: ReplicaId::new(2),
        instance_id: ReplicaInstanceId::new("candidate-pod"),
        agent_generation: AgentGeneration::new("candidate-generation"),
    };
    let previous_policy = EffectivePolicy::fixed(1, 30).unwrap();
    let current_policy = EffectivePolicy::fixed(2, 30).unwrap();
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        primary.replica_id,
        vec![ConfigurationMember {
            identity: primary.clone(),
            role: ReplicaRole::Primary,
        }],
        previous_policy.write_quorum,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        primary.replica_id,
        vec![
            ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: candidate.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        current_policy.write_quorum,
    );
    let mut intent = ScaleUpIntent {
        operation_id: OperationId::default(),
        resource_uid: ResourceUid::new("resource-1"),
        spec_generation: 2,
        desired_replicas: 2,
        previous_configuration: previous.clone(),
        current_configuration: current.clone(),
        previous_policy: previous_policy.clone(),
        current_policy: current_policy.clone(),
        primary: primary.clone(),
        target: candidate,
        build_id: OperationId::new("scale-up-build"),
        snapshot_boundary_lsn: 7,
        catch_up_boundary_lsn: 9,
    };
    intent.operation_id = intent.expected_operation_id();
    let evidence = ScaleUpConfigurationEvidence::Admission {
        intent: intent.clone(),
    };
    let mut state = AgentState::new(StorageIdentity {
        effective_policy: previous_policy.clone(),
        ..storage_identity()
    });
    state.admitted_policy = Some(if current_only {
        current_policy.clone()
    } else {
        previous_policy.clone()
    });
    state.highest_epoch = if current_only {
        current.epoch
    } else {
        previous.epoch
    };
    state.previous_configuration = current_only.then(|| previous.clone());
    state.current_configuration = Some(if current_only {
        current.clone()
    } else {
        previous.clone()
    });
    state.role = ReplicaRole::Primary;
    state.read_status = AccessStatus::Granted;
    state.write_status = AccessStatus::Granted;
    if current_only {
        state.scale_up_evidence = Some(Box::new(evidence.clone()));
    }
    let stage = if current_only {
        ScaleUpStage::CurrentOnly
    } else {
        ScaleUpStage::PreviousCurrent
    };
    let command = EnsureConfiguration {
        operation_id: intent.command_operation_id(stage, &primary, &current),
        previous_configuration: (!current_only).then_some(previous.clone()),
        current_configuration: current.clone(),
        previous_epoch: (!current_only).then_some(previous.epoch),
        current_epoch: current.epoch,
        effective_policy: current_policy,
        previous_policy: Some(previous_policy),
        secondary_removal_evidence: None,
        scale_up_evidence: Some(Box::new(evidence.clone())),
        local_replica_id: primary.replica_id,
        expected_instance_id: primary.instance_id.clone(),
        expected_agent_generation: primary.agent_generation.clone(),
        transition_kind: TransitionKind::ScaleUp,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::Granted,
        current_only,
        retire_build_ids: current_only
            .then_some(vec![intent.build_id.clone()])
            .unwrap_or_default(),
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    };
    let authority = AdmittedAuthority {
        local_identity: primary,
        transition_kind: (!current_only).then_some(TransitionKind::ScaleUp),
        previous_configuration: (!current_only).then_some(previous),
        current_configuration: current,
        switchover_handoff: None,
        secondary_removal: None,
        scale_up: Some(Box::new(evidence)),
    };
    (state, command, authority)
}

#[test]
fn exact_pending_scale_up_current_only_is_admitted_before_and_after_authority_install() {
    let (mut state, command, expected) = scale_up_fixture(true);
    assert_eq!(admit_configuration(&command, &state).unwrap(), expected);

    state.reconfiguration = Some(ReconfigurationRecord {
        command: command.clone(),
        stage: CoordinatorStage::AdmitAuthority,
        observed_lsn: None,
    });
    assert_eq!(
        admit_persisted_configuration(&command, &state).unwrap(),
        expected,
        "the exact journaled current-only command must remain admissible before authority install"
    );

    state.previous_configuration = None;
    state.current_configuration = Some(command.current_configuration.clone());
    state.highest_epoch = command.current_epoch;
    state.admitted_policy = Some(command.effective_policy.clone());
    state.scale_up_evidence = command.scale_up_evidence.clone();
    assert_eq!(
        admit_persisted_configuration(&command, &state).unwrap(),
        expected,
        "the exact journaled current-only command must remain admissible after authority install"
    );

    let mut mutated = command.clone();
    mutated.retire_build_ids = vec![OperationId::new("different-build")];
    assert!(admit_persisted_configuration(&mutated, &state).is_err());
    let mut unrelated = command;
    unrelated.operation_id = OperationId::new("unrelated-current-only");
    assert!(admit_persisted_configuration(&unrelated, &state).is_err());
}

fn candidate_admission_fixture() -> (
    AgentState,
    EnsureConfiguration,
    crate::authority::DurableBuildProgress,
) {
    let resource_uid = ResourceUid::new("resource-1");
    let primary = identity();
    let previous_policy = EffectivePolicy::fixed(1, 30).unwrap();
    let current_policy = EffectivePolicy::fixed(2, 30).unwrap();
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        primary.replica_id,
        vec![ConfigurationMember {
            identity: primary.clone(),
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
            previous_policy: previous_policy.clone(),
            current_policy: current_policy.clone(),
            target_replica_id: ReplicaId::new(2),
        }),
        pod_uid: PodUid::new("candidate-pod"),
        pvc_uid: PvcUid::new("candidate-pvc"),
        operation_id: OperationId::default(),
    };
    provisioning.operation_id = provisioning.expected_operation_id();
    let candidate = provisioning.target_identity(&resource_uid);
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        primary.replica_id,
        vec![
            ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: candidate.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        current_policy.write_quorum,
    );
    let authority = BuildAuthority {
        build_id: provisioning.scale_up_build_id(&resource_uid).unwrap(),
        kind: BuildAuthorityKind::Provisioning,
        source: primary.clone(),
        target: candidate.clone(),
        current_configuration: previous.clone(),
        replication_boundary_lsn: 4,
    };
    let progress = crate::authority::DurableBuildProgress {
        authority: authority.clone(),
        last_sequence: 2,
        durable_lsn: 9,
        completed: true,
        catch_up_boundary_lsn: Some(9),
    };
    let mut intent = ScaleUpIntent {
        operation_id: OperationId::default(),
        resource_uid: resource_uid.clone(),
        spec_generation: 2,
        desired_replicas: 2,
        previous_configuration: previous.clone(),
        current_configuration: current.clone(),
        previous_policy: previous_policy.clone(),
        current_policy: current_policy.clone(),
        primary,
        target: candidate.clone(),
        build_id: authority.build_id.clone(),
        snapshot_boundary_lsn: 4,
        catch_up_boundary_lsn: 9,
    };
    intent.operation_id = intent.expected_operation_id();
    let command = EnsureConfiguration {
        operation_id: intent.command_operation_id(
            ScaleUpStage::PreviousCurrent,
            &candidate,
            &current,
        ),
        previous_configuration: Some(previous.clone()),
        current_configuration: current,
        previous_epoch: Some(previous.epoch),
        current_epoch: Epoch::new(0, 2),
        effective_policy: current_policy.clone(),
        previous_policy: Some(previous_policy),
        secondary_removal_evidence: None,
        scale_up_evidence: Some(Box::new(ScaleUpConfigurationEvidence::Admission { intent })),
        local_replica_id: candidate.replica_id,
        expected_instance_id: candidate.instance_id.clone(),
        expected_agent_generation: candidate.agent_generation.clone(),
        transition_kind: TransitionKind::ScaleUp,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::Granted,
        current_only: false,
        retire_build_ids: Vec::new(),
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    };
    let mut state = AgentState::new(StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid,
        pod_uid: provisioning.pod_uid.clone(),
        pvc_uid: provisioning.pvc_uid.clone(),
        initialization_id: provisioning.initialization_id(&ResourceUid::new("resource-1")),
        local_identity: candidate.clone(),
        effective_policy: current_policy,
    });
    state.scale_up_initialization = Some(provisioning);
    state.role = ReplicaRole::IdleSecondary;
    state.build_commands.insert(
        authority.build_id.clone(),
        EnsureReplicaBuild {
            operation_id: authority.build_id.clone(),
            local_replica_id: candidate.replica_id,
            expected_instance_id: candidate.instance_id,
            expected_agent_generation: candidate.agent_generation,
            target: authority.target.clone(),
            authority: Some(authority),
            source_session_id: Some(ProcessSessionId::new("source-session")),
            retire: false,
        },
    );
    (state, command, progress)
}

#[tokio::test]
async fn sqlite_coordinator_replays_exact_pending_scale_up_commands_before_and_after_install() {
    for current_only in [false, true] {
        for fail_stage in ["admit", "access"] {
            let (state, command, _) = scale_up_fixture(current_only);
            let directory = tempdir().unwrap();
            let path = SqliteStore::metadata_database_path(directory.path());
            let store = Arc::new(SqliteStore::create_authorized(&path, state).unwrap());
            let runtime = Arc::new(FakeRuntime::new());
            {
                let mut runtime_state = runtime.state.lock().unwrap();
                runtime_state.role = ReplicaRole::Primary;
                runtime_state.read_status = AccessStatus::Granted;
                runtime_state.write_status = AccessStatus::Granted;
            }
            runtime.fail_once(fail_stage);
            let coordinator = Coordinator::new(store.clone(), runtime.clone());
            assert!(
                coordinator
                    .ensure_configuration(command.clone())
                    .await
                    .is_err(),
                "current_only={current_only} fail_stage={fail_stage}"
            );
            let interrupted = store.load_state().await.unwrap();
            assert_eq!(
                interrupted
                    .reconfiguration
                    .as_ref()
                    .map(|record| &record.command),
                Some(&command)
            );
            if fail_stage == "access" {
                assert_eq!(
                    interrupted.current_configuration.as_ref(),
                    Some(&command.current_configuration),
                    "authority must be installed before the interrupted completion cut"
                );
            }

            let mut mutated = command.clone();
            mutated.primary_write_status = AccessStatus::ReconfigurationPending;
            assert!(
                Coordinator::new(store.clone(), runtime.clone())
                    .ensure_configuration(mutated)
                    .await
                    .is_err(),
                "mutated pending payload must be rejected"
            );
            let completed = Coordinator::new(store.clone(), runtime.clone())
                .ensure_configuration(command.clone())
                .await
                .unwrap();
            assert_eq!(completed.command, command);
            let terminal = store.load_state().await.unwrap();
            assert!(terminal.reconfiguration.is_none());
            assert_eq!(
                terminal
                    .retained_command
                    .as_ref()
                    .map(|retained| &retained.command),
                Some(&completed.command)
            );
        }
    }
}

#[tokio::test]
async fn scale_up_same_primary_never_runs_the_ordinary_write_fence() {
    let (state, command, previous_authority) = scale_up_fixture(false);
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = Arc::new(SqliteStore::create_authorized(&path, state.clone()).unwrap());
    let runtime = Arc::new(FakeRuntime::new());
    {
        let mut runtime_state = runtime.state.lock().unwrap();
        runtime_state.role = ReplicaRole::Primary;
        runtime_state.read_status = AccessStatus::Granted;
        runtime_state.write_status = AccessStatus::Granted;
        runtime_state.authority = Some(AdmittedAuthority {
            current_configuration: state.current_configuration.clone().unwrap(),
            previous_configuration: None,
            transition_kind: None,
            scale_up: None,
            ..previous_authority.clone()
        });
    }
    let coordinator = Coordinator::new(store.clone(), runtime.clone());
    coordinator.ensure_configuration(command).await.unwrap();
    assert_eq!(
        *runtime.calls.lock().unwrap(),
        vec!["admit", "access"],
        "same-primary PC/CC must not execute read/write reconfiguration fences"
    );
    let admitted = store.load_state().await.unwrap();
    assert_eq!(admitted.read_status, AccessStatus::Granted);
    assert_eq!(admitted.write_status, AccessStatus::Granted);
    assert!(admitted.scale_up_evidence.is_some());

    let (_, current_only, _) = scale_up_fixture(true);
    coordinator
        .ensure_configuration(current_only.clone())
        .await
        .unwrap();
    assert_eq!(
        &runtime.calls.lock().unwrap()[2..],
        ["admit", "access", "retire-build"]
    );
    let completed = store.load_state().await.unwrap();
    assert_eq!(completed.previous_configuration, None);
    assert_eq!(
        completed.current_configuration,
        Some(current_only.current_configuration.clone())
    );
    assert_eq!(completed.write_status, AccessStatus::Granted);
    assert!(
        completed
            .retired_builds
            .contains(&OperationId::new("scale-up-build"))
    );
    assert_eq!(
        completed
            .retained_command
            .as_ref()
            .map(|retained| &retained.command),
        Some(&current_only)
    );
}

#[tokio::test]
async fn scale_up_candidate_activates_only_after_exact_build_and_retires_it_on_completion() {
    let resource_uid = ResourceUid::new("resource-1");
    let primary = identity();
    let previous_policy = EffectivePolicy::fixed(1, 30).unwrap();
    let current_policy = EffectivePolicy::fixed(2, 30).unwrap();
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        primary.replica_id,
        vec![ConfigurationMember {
            identity: primary.clone(),
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
            previous_policy: previous_policy.clone(),
            current_policy: current_policy.clone(),
            target_replica_id: ReplicaId::new(2),
        }),
        pod_uid: PodUid::new("candidate-pod"),
        pvc_uid: PvcUid::new("candidate-pvc"),
        operation_id: OperationId::default(),
    };
    provisioning.operation_id = provisioning.expected_operation_id();
    let candidate = provisioning.target_identity(&resource_uid);
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        primary.replica_id,
        vec![
            ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: candidate.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        current_policy.write_quorum,
    );
    let build_id = provisioning.scale_up_build_id(&resource_uid).unwrap();
    let authority = BuildAuthority {
        build_id: build_id.clone(),
        kind: BuildAuthorityKind::Provisioning,
        source: primary.clone(),
        target: candidate.clone(),
        current_configuration: previous.clone(),
        replication_boundary_lsn: 4,
    };
    let mut intent = ScaleUpIntent {
        operation_id: OperationId::default(),
        resource_uid: resource_uid.clone(),
        spec_generation: 2,
        desired_replicas: 2,
        previous_configuration: previous.clone(),
        current_configuration: current.clone(),
        previous_policy: previous_policy.clone(),
        current_policy: current_policy.clone(),
        primary,
        target: candidate.clone(),
        build_id: build_id.clone(),
        snapshot_boundary_lsn: 4,
        catch_up_boundary_lsn: 9,
    };
    intent.operation_id = intent.expected_operation_id();
    let evidence = ScaleUpConfigurationEvidence::Admission {
        intent: intent.clone(),
    };
    let mut state = AgentState::new(StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: resource_uid.clone(),
        pod_uid: provisioning.pod_uid.clone(),
        pvc_uid: provisioning.pvc_uid.clone(),
        initialization_id: provisioning.initialization_id(&resource_uid),
        local_identity: candidate.clone(),
        effective_policy: current_policy.clone(),
    });
    state.scale_up_initialization = Some(provisioning);
    state.role = ReplicaRole::IdleSecondary;
    state.read_status = AccessStatus::NotPrimary;
    state.write_status = AccessStatus::NotPrimary;
    state.build_commands.insert(
        build_id.clone(),
        EnsureReplicaBuild {
            operation_id: build_id.clone(),
            local_replica_id: candidate.replica_id,
            expected_instance_id: candidate.instance_id.clone(),
            expected_agent_generation: candidate.agent_generation.clone(),
            target: candidate.clone(),
            authority: Some(authority.clone()),
            source_session_id: Some(ProcessSessionId::new("source-session")),
            retire: false,
        },
    );
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = Arc::new(SqliteStore::create_authorized(&path, state.clone()).unwrap());
    store.admit_build(&authority).await.unwrap();
    store
        .record_build_progress(&crate::authority::DurableBuildProgress {
            authority: authority.clone(),
            last_sequence: 2,
            durable_lsn: 9,
            completed: true,
            catch_up_boundary_lsn: Some(9),
        })
        .await
        .unwrap();
    let runtime = Arc::new(FakeRuntime::new());
    {
        let mut runtime_state = runtime.state.lock().unwrap();
        runtime_state.role = ReplicaRole::IdleSecondary;
        runtime_state.builds = vec![crate::effects::BuildPostcondition {
            authority,
            last_sequence: 2,
            durable_lsn: 9,
            completed: true,
            catch_up_boundary_lsn: Some(9),
        }];
    }
    let coordinator = Coordinator::new(store.clone(), runtime);
    let configuration = |current_only: bool| EnsureConfiguration {
        operation_id: intent.command_operation_id(
            if current_only {
                ScaleUpStage::CurrentOnly
            } else {
                ScaleUpStage::PreviousCurrent
            },
            &candidate,
            &current,
        ),
        previous_configuration: (!current_only).then_some(previous.clone()),
        current_configuration: current.clone(),
        previous_epoch: (!current_only).then_some(previous.epoch),
        current_epoch: current.epoch,
        effective_policy: current_policy.clone(),
        previous_policy: Some(previous_policy.clone()),
        secondary_removal_evidence: None,
        scale_up_evidence: Some(Box::new(evidence.clone())),
        local_replica_id: candidate.replica_id,
        expected_instance_id: candidate.instance_id.clone(),
        expected_agent_generation: candidate.agent_generation.clone(),
        transition_kind: TransitionKind::ScaleUp,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::Granted,
        current_only,
        retire_build_ids: current_only
            .then_some(vec![build_id.clone()])
            .unwrap_or_default(),
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    };
    coordinator
        .ensure_configuration(configuration(false))
        .await
        .unwrap();
    let pc_cc = store.load_state().await.unwrap();
    assert_eq!(pc_cc.role, ReplicaRole::ActiveSecondary);
    assert_eq!(pc_cc.write_status, AccessStatus::NotPrimary);
    assert_eq!(pc_cc.previous_configuration, Some(previous.clone()));
    assert!(!pc_cc.retired_builds.contains(&build_id));

    coordinator
        .ensure_configuration(configuration(true))
        .await
        .unwrap();
    let completed = store.load_state().await.unwrap();
    assert_eq!(completed.role, ReplicaRole::ActiveSecondary);
    assert_eq!(completed.previous_configuration, None);
    assert_eq!(completed.current_configuration, Some(current.clone()));
    assert!(completed.retired_builds.contains(&build_id));
    assert_eq!(
        completed
            .retained_command
            .as_ref()
            .map(|retained| &retained.command),
        Some(&configuration(true))
    );
}

#[test]
fn scale_up_candidate_pc_cc_requires_exact_completed_durable_build_progress() {
    let (mut state, command, progress) = candidate_admission_fixture();
    assert!(admit_configuration(&command, &state).is_err());

    let build_id = progress.authority.build_id.clone();
    let mut incomplete = progress.clone();
    incomplete.completed = false;
    state.build_progress.insert(build_id.clone(), incomplete);
    assert!(admit_configuration(&command, &state).is_err());

    let mut mutated_boundary = progress.clone();
    mutated_boundary.catch_up_boundary_lsn = Some(10);
    state
        .build_progress
        .insert(build_id.clone(), mutated_boundary);
    assert!(admit_configuration(&command, &state).is_err());

    let mut below_boundary = progress.clone();
    below_boundary.durable_lsn = 8;
    state
        .build_progress
        .insert(build_id.clone(), below_boundary);
    assert!(admit_configuration(&command, &state).is_err());

    let mut wrong_authority = progress.clone();
    wrong_authority.authority.replication_boundary_lsn = 5;
    state
        .build_progress
        .insert(build_id.clone(), wrong_authority);
    assert!(admit_configuration(&command, &state).is_err());

    state.build_progress.insert(build_id, progress);
    assert!(admit_configuration(&command, &state).is_ok());
}

#[test]
fn scale_up_failover_requires_durable_pc_cc_and_the_new_primary_witness() {
    let identities = (1..=3)
        .map(|id| ReplicaIdentity {
            replica_id: ReplicaId::new(id),
            instance_id: ReplicaInstanceId::new(format!("pod-{id}")),
            agent_generation: AgentGeneration::new(format!("generation-{id}")),
        })
        .collect::<Vec<_>>();
    let previous_policy = EffectivePolicy::fixed(2, 30).unwrap();
    let current_policy = EffectivePolicy::fixed(3, 30).unwrap();
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        identities[0].replica_id,
        vec![
            ConfigurationMember {
                identity: identities[0].clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: identities[1].clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        previous_policy.write_quorum,
    );
    let expanded = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        identities[0].replica_id,
        vec![
            ConfigurationMember {
                identity: identities[0].clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: identities[1].clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: identities[2].clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        current_policy.write_quorum,
    );
    let mut intent = ScaleUpIntent {
        operation_id: OperationId::default(),
        resource_uid: ResourceUid::new("resource-1"),
        spec_generation: 2,
        desired_replicas: 3,
        previous_configuration: previous.clone(),
        current_configuration: expanded.clone(),
        previous_policy: previous_policy.clone(),
        current_policy: current_policy.clone(),
        primary: identities[0].clone(),
        target: identities[2].clone(),
        build_id: OperationId::new("scale-up-failover-build"),
        snapshot_boundary_lsn: 4,
        catch_up_boundary_lsn: 9,
    };
    intent.operation_id = intent.expected_operation_id();
    let witness = |identity: ReplicaIdentity, sequence: u64| ScaleUpWitness {
        resource_uid: intent.resource_uid.clone(),
        role: expanded
            .members
            .iter()
            .find(|member| member.identity == identity)
            .unwrap()
            .role,
        process_session_id: ProcessSessionId::new(format!("session-{sequence}")),
        report_sequence: sequence,
        epoch: expanded.epoch,
        previous_configuration_id: Some(previous.configuration_id.clone()),
        current_configuration_id: expanded.configuration_id.clone(),
        verified_replication_lsn: 9,
        write_status: AccessStatus::ReconfigurationPending,
        pending_operation_id: None,
        retained_operation_id: Some(intent.command_operation_id(
            ScaleUpStage::PreviousCurrent,
            &identity,
            &expanded,
        )),
        identity,
    };
    let failover = ConfigurationDescriptor::new(
        Epoch::new(0, 3),
        identities[1].replica_id,
        vec![
            ConfigurationMember {
                identity: identities[0].clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: identities[1].clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: identities[2].clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        current_policy.write_quorum,
    );
    let evidence = ScaleUpFailoverEvidence {
        intent: intent.clone(),
        provisional_configuration: failover.clone(),
        previous_read_quorum: vec![witness(identities[0].clone(), 1)],
        current_read_quorum: vec![
            witness(identities[1].clone(), 2),
            witness(identities[2].clone(), 3),
        ],
        final_election: None,
    };
    let mut state = AgentState::new(StorageIdentity {
        resource_uid: intent.resource_uid.clone(),
        local_identity: identities[1].clone(),
        pod_uid: PodUid::new(identities[1].instance_id.as_str()),
        effective_policy: current_policy.clone(),
        ..storage_identity()
    });
    state.admitted_policy = Some(current_policy.clone());
    state.previous_policy = Some(previous_policy.clone());
    state.highest_epoch = expanded.epoch;
    state.previous_configuration = Some(previous.clone());
    state.current_configuration = Some(expanded.clone());
    state.scale_up_evidence = Some(Box::new(ScaleUpConfigurationEvidence::Admission {
        intent: intent.clone(),
    }));
    state.role = ReplicaRole::ActiveSecondary;
    state.read_status = AccessStatus::Granted;
    state.write_status = AccessStatus::NotPrimary;
    let command = EnsureConfiguration {
        operation_id: intent.command_operation_id(
            ScaleUpStage::PreviousCurrent,
            &identities[1],
            &failover,
        ),
        previous_configuration: Some(previous.clone()),
        current_configuration: failover.clone(),
        previous_epoch: Some(previous.epoch),
        current_epoch: failover.epoch,
        effective_policy: current_policy,
        previous_policy: Some(previous_policy),
        secondary_removal_evidence: None,
        scale_up_evidence: Some(Box::new(ScaleUpConfigurationEvidence::Failover {
            evidence: evidence.clone(),
        })),
        local_replica_id: identities[1].replica_id,
        expected_instance_id: identities[1].instance_id.clone(),
        expected_agent_generation: identities[1].agent_generation.clone(),
        transition_kind: TransitionKind::Failover,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::ReconfigurationPending,
        current_only: false,
        retire_build_ids: Vec::new(),
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    };
    let admitted = admit_configuration(&command, &state).unwrap();
    assert_eq!(admitted.current_configuration, failover);
    assert_eq!(admitted.local_role(), ReplicaRole::Primary);

    let mut installed = state.clone();
    installed.highest_epoch = command.current_epoch;
    installed.previous_configuration = command.previous_configuration.clone();
    installed.current_configuration = Some(command.current_configuration.clone());
    installed.scale_up_evidence = command.scale_up_evidence.clone();
    installed.role = ReplicaRole::Primary;
    installed.read_status = AccessStatus::ReconfigurationPending;
    installed.write_status = AccessStatus::ReconfigurationPending;
    assert!(admit_persisted_configuration(&command, &installed).is_ok());

    let mut missing_new_primary = command.clone();
    let ScaleUpConfigurationEvidence::Failover { evidence } = missing_new_primary
        .scale_up_evidence
        .as_deref_mut()
        .unwrap()
    else {
        unreachable!()
    };
    evidence.current_read_quorum = vec![
        witness(identities[0].clone(), 4),
        witness(identities[2].clone(), 5),
    ];
    assert!(admit_configuration(&missing_new_primary, &state).is_err());

    let mut insufficient_previous = command.clone();
    let ScaleUpConfigurationEvidence::Failover { evidence } = insufficient_previous
        .scale_up_evidence
        .as_deref_mut()
        .unwrap()
    else {
        unreachable!()
    };
    evidence.previous_read_quorum.clear();
    assert!(admit_configuration(&insufficient_previous, &state).is_err());

    let mut insufficient_current = command.clone();
    let ScaleUpConfigurationEvidence::Failover { evidence } = insufficient_current
        .scale_up_evidence
        .as_deref_mut()
        .unwrap()
    else {
        unreachable!()
    };
    evidence.current_read_quorum.truncate(1);
    assert!(admit_configuration(&insufficient_current, &state).is_err());

    let mut no_durable_pc_cc = state;
    no_durable_pc_cc.scale_up_evidence = None;
    assert!(admit_configuration(&command, &no_durable_pc_cc).is_err());
}

#[test]
fn planned_switchover_admission_binds_starting_authority_and_retirement() {
    let source = identity();
    let target = ReplicaIdentity {
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
        source.replica_id,
        vec![
            ConfigurationMember {
                identity: source.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: target.clone(),
                role: ReplicaRole::ActiveSecondary,
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
        target.replica_id,
        vec![
            ConfigurationMember {
                identity: source.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: target.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: third,
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        policy.write_quorum,
    );
    let handoff = SwitchoverHandoff {
        preparation_generation: 1,
        preparation_operation_id: OperationId::new("prepare-1"),
        request_id: SwitchoverRequestId::new("request-1"),
        source: source.clone(),
        target,
        starting_configuration_id: previous.configuration_id.clone(),
        starting_epoch: previous.epoch,
        handoff_lsn: 7,
    };
    let mut state = AgentState::new(StorageIdentity {
        effective_policy: policy.clone(),
        ..storage_identity()
    });
    state.highest_epoch = previous.epoch;
    state.current_configuration = Some(previous.clone());
    state.role = ReplicaRole::Primary;
    let command = EnsureConfiguration {
        previous_policy: None,
        secondary_removal_evidence: None,
        scale_up_evidence: None,
        operation_id: OperationId::new("install-1"),
        previous_configuration: Some(previous.clone()),
        current_configuration: current.clone(),
        previous_epoch: Some(previous.epoch),
        current_epoch: current.epoch,
        effective_policy: policy.clone(),
        local_replica_id: source.replica_id,
        expected_instance_id: source.instance_id.clone(),
        expected_agent_generation: source.agent_generation.clone(),
        transition_kind: TransitionKind::PlannedSwitchover,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::ReconfigurationPending,
        current_only: false,
        retire_build_ids: Vec::new(),
        switchover_handoff: Some(handoff.clone()),
        retire_switchover_preparation_ids: Vec::new(),
    };
    state.prepared_switchover = Some(handoff.clone());
    assert!(admit_configuration(&command, &state).is_ok());

    let mut wrong_start = command.clone();
    wrong_start
        .switchover_handoff
        .as_mut()
        .unwrap()
        .starting_configuration_id = crate::protocol::types::ConfigurationId::new("unrelated");
    assert!(admit_configuration(&wrong_start, &state).is_err());

    state.highest_epoch = current.epoch;
    state.previous_configuration = Some(previous.clone());
    state.current_configuration = Some(current.clone());
    state.role = ReplicaRole::ActiveSecondary;
    let mut current_only = command;
    current_only.operation_id = OperationId::new("current-only-1");
    current_only.previous_configuration = None;
    current_only.previous_epoch = None;
    current_only.current_only = true;
    current_only.current_configuration = current;
    current_only.retire_switchover_preparation_ids =
        vec![crate::protocol::types::SwitchoverPreparationId {
            operation_id: OperationId::new(""),
            generation: 1,
        }];
    assert!(admit_configuration(&current_only, &state).is_err());
    current_only.retire_switchover_preparation_ids = vec![handoff.preparation()];
    assert!(admit_configuration(&current_only, &state).is_ok());
    let mut changed_certificate = current_only.clone();
    changed_certificate
        .switchover_handoff
        .as_mut()
        .unwrap()
        .handoff_lsn += 1;
    assert!(admit_configuration(&changed_certificate, &state).is_err());

    let mut target_state = AgentState::new(StorageIdentity {
        local_identity: handoff.target.clone(),
        effective_policy: state.identity.effective_policy.clone(),
        ..storage_identity()
    });
    target_state.highest_epoch = current_only.current_epoch;
    target_state.previous_configuration = Some(previous);
    target_state.current_configuration = Some(current_only.current_configuration.clone());
    target_state.role = ReplicaRole::Primary;
    target_state.read_status = AccessStatus::ReconfigurationPending;
    target_state.write_status = AccessStatus::ReconfigurationPending;
    let mut target_current_only = current_only.clone();
    target_current_only.operation_id = OperationId::new("target-current-only");
    target_current_only.local_replica_id = handoff.target.replica_id;
    target_current_only.expected_instance_id = handoff.target.instance_id.clone();
    target_current_only.expected_agent_generation = handoff.target.agent_generation.clone();
    target_current_only
        .retire_switchover_preparation_ids
        .clear();
    assert!(admit_configuration(&target_current_only, &target_state).is_ok());

    state.prepared_switchover = None;
    state.previous_configuration = None;
    state.reconfiguration = Some(ReconfigurationRecord {
        command: current_only.clone(),
        stage: CoordinatorStage::Complete,
        observed_lsn: Some(handoff.handoff_lsn),
    });
    assert!(admit_persisted_configuration(&current_only, &state).is_ok());

    let mut changed_replay = current_only.clone();
    changed_replay
        .switchover_handoff
        .as_mut()
        .unwrap()
        .starting_configuration_id = crate::protocol::types::ConfigurationId::new("changed");
    assert!(admit_persisted_configuration(&changed_replay, &state).is_err());

    state.reconfiguration = None;
    state.retained_command = Some(RetainedCommandResult {
        command: current_only.clone(),
        role: ReplicaRole::ActiveSecondary,
        epoch: current_only.current_epoch,
    });
    assert!(admit_persisted_configuration(&current_only, &state).is_ok());
}

#[tokio::test]
async fn planned_switchover_preparation_is_durable_idempotent_and_restart_visible() {
    let directory = tempdir().unwrap();
    let source = identity();
    let target = ReplicaIdentity {
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
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        source.replica_id,
        vec![
            ConfigurationMember {
                identity: source.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: target.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: third,
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        policy.write_quorum,
    );
    let mut state = AgentState::new(StorageIdentity {
        effective_policy: policy,
        ..storage_identity()
    });
    state.highest_epoch = current.epoch;
    state.current_configuration = Some(current.clone());
    state.role = ReplicaRole::Primary;
    state.read_status = AccessStatus::Granted;
    state.write_status = AccessStatus::Granted;
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
        runtime_state.read_status = AccessStatus::Granted;
        runtime_state.write_status = AccessStatus::Granted;
        runtime_state.current_progress = 7;
        runtime_state.authority = Some(AdmittedAuthority {
            scale_up: None,
            secondary_removal: None,
            local_identity: source.clone(),
            transition_kind: None,
            previous_configuration: None,
            current_configuration: current.clone(),
            switchover_handoff: None,
        });
    }
    let command = PrepareSwitchover {
        preparation_generation: 1,
        operation_id: crate::protocol::types::derive_switchover_preparation_operation_id(
            &storage_identity().resource_uid,
            &SwitchoverRequestId::new("request-1"),
            1,
            &current.configuration_id,
            &source,
            &target,
        ),
        request_id: SwitchoverRequestId::new("request-1"),
        local_replica_id: source.replica_id,
        expected_instance_id: source.instance_id.clone(),
        expected_agent_generation: source.agent_generation.clone(),
        source,
        target: target.clone(),
        current_configuration: current,
    };
    let coordinator = Coordinator::new(store.clone(), runtime.clone());
    runtime.fail_once("prepare-switchover");
    assert!(
        coordinator
            .ensure_switchover_prepared(command.clone())
            .await
            .is_err()
    );
    let interrupted = store.load_state().await.unwrap();
    assert!(interrupted.pending_effect.is_some());
    assert!(interrupted.prepared_switchover.is_none());

    let reopened = Arc::new(
        SqliteStore::open_existing(SqliteStore::metadata_database_path(directory.path()), None)
            .unwrap(),
    );
    let restarted_runtime = Arc::new(FakeRuntime::new());
    {
        let mut runtime_state = restarted_runtime.state.lock().unwrap();
        runtime_state.role = ReplicaRole::Primary;
        runtime_state.read_status = AccessStatus::Granted;
        runtime_state.write_status = AccessStatus::Granted;
        runtime_state.current_progress = 7;
        runtime_state.authority = Some(AdmittedAuthority {
            scale_up: None,
            secondary_removal: None,
            local_identity: command.source.clone(),
            transition_kind: None,
            previous_configuration: None,
            current_configuration: command.current_configuration.clone(),
            switchover_handoff: None,
        });
    }
    let restarted = Coordinator::new(reopened.clone(), restarted_runtime.clone());
    let prepared = restarted
        .ensure_switchover_prepared(command.clone())
        .await
        .unwrap();
    assert_eq!(prepared.handoff_lsn, 7);
    assert_eq!(
        reopened.load_state().await.unwrap().prepared_switchover,
        Some(prepared.clone())
    );
    let calls = restarted_runtime.calls.lock().unwrap().clone();
    assert_eq!(
        restarted
            .ensure_switchover_prepared(command.clone())
            .await
            .unwrap(),
        prepared
    );
    assert_eq!(*restarted_runtime.calls.lock().unwrap(), calls);
    assert_eq!(
        restarted
            .ensure_switchover_prepared(command.clone())
            .await
            .unwrap(),
        prepared
    );

    let mut changed = command.clone();
    changed.target = target;
    changed.request_id = SwitchoverRequestId::new("changed");
    assert!(restarted.ensure_switchover_prepared(changed).await.is_err());

    let mut earlier = Vec::new();
    for generation in 1..=4 {
        let mut next = command.clone();
        next.preparation_generation = generation;
        if generation > 1 {
            next.request_id = SwitchoverRequestId::new(format!("request-{generation}"));
            next.operation_id = crate::protocol::types::derive_switchover_preparation_operation_id(
                &storage_identity().resource_uid,
                &next.request_id,
                generation,
                &next.current_configuration.configuration_id,
                &next.source,
                &next.target,
            );
        }
        // The last request is retired without ever observing preparation.
        let handoff = if generation < 4 {
            Some(
                restarted
                    .ensure_switchover_prepared(next.clone())
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        let restore = EnsureConfiguration {
            previous_policy: None,
            secondary_removal_evidence: None,
            scale_up_evidence: None,
            operation_id: OperationId::new("same-authority-restore"),
            previous_configuration: None,
            current_configuration: next.current_configuration.clone(),
            previous_epoch: None,
            current_epoch: next.current_configuration.epoch,
            effective_policy: reopened
                .load_state()
                .await
                .unwrap()
                .identity
                .effective_policy,
            local_replica_id: next.local_replica_id,
            expected_instance_id: next.expected_instance_id.clone(),
            expected_agent_generation: next.expected_agent_generation.clone(),
            transition_kind: TransitionKind::PlannedSwitchover,
            failover_safe_lsn: None,
            primary_write_status: AccessStatus::ReconfigurationPending,
            current_only: false,
            retire_build_ids: Vec::new(),
            switchover_handoff: handoff,
            retire_switchover_preparation_ids: vec![
                crate::protocol::types::SwitchoverPreparationId {
                    generation,
                    operation_id: next.operation_id.clone(),
                },
            ],
        };
        restarted
            .ensure_configuration(restore.clone())
            .await
            .unwrap();
        let grant = EnsureConfiguration {
            operation_id: OperationId::new("same-authority-grant"),
            transition_kind: TransitionKind::Bootstrap,
            primary_write_status: AccessStatus::Granted,
            switchover_handoff: None,
            retire_switchover_preparation_ids: Vec::new(),
            ..restore
        };
        restarted.ensure_configuration(grant).await.unwrap();
        earlier.push(next);
        let state = reopened.load_state().await.unwrap();
        let calls = restarted_runtime.calls.lock().unwrap().clone();
        assert_eq!(state.write_status, AccessStatus::Granted);
        assert_eq!(
            state.preparation_retirement.as_ref().unwrap().generation,
            generation
        );
        for old in &earlier {
            assert!(
                restarted
                    .ensure_switchover_prepared(old.clone())
                    .await
                    .is_err()
            );
            let mut altered = old.clone();
            altered.operation_id = OperationId::new("changed-id");
            assert!(restarted.ensure_switchover_prepared(altered).await.is_err());
            let mut reused = old.clone();
            reused.preparation_generation = generation + 1;
            assert!(restarted.ensure_switchover_prepared(reused).await.is_err());
        }
        assert_eq!(reopened.load_state().await.unwrap(), state);
        assert_eq!(*restarted_runtime.calls.lock().unwrap(), calls);

        let cold_store = Arc::new(
            SqliteStore::open_existing(SqliteStore::metadata_database_path(directory.path()), None)
                .unwrap(),
        );
        let cold_runtime = Arc::new(FakeRuntime::new());
        *cold_runtime.state.lock().unwrap() = restarted_runtime.state.lock().unwrap().clone();
        let cold = Coordinator::new(cold_store.clone(), cold_runtime.clone());
        for old in &earlier {
            assert!(cold.ensure_switchover_prepared(old.clone()).await.is_err());
        }
        assert_eq!(cold_store.load_state().await.unwrap(), state);
        assert!(cold_runtime.calls.lock().unwrap().is_empty());
        assert_eq!(
            cold_runtime.state.lock().unwrap().write_status,
            AccessStatus::Granted
        );
    }
}

#[tokio::test]
async fn planned_switchover_sequences_source_target_and_uninvolved_through_current_only() {
    let source = identity();
    let target = ReplicaIdentity {
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
        source.replica_id,
        vec![
            ConfigurationMember {
                identity: source.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: target.clone(),
                role: ReplicaRole::ActiveSecondary,
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
        target.replica_id,
        vec![
            ConfigurationMember {
                identity: source.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: target.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: third.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        policy.write_quorum,
    );
    let handoff = SwitchoverHandoff {
        preparation_generation: 1,
        preparation_operation_id: OperationId::new("prepare-1"),
        request_id: SwitchoverRequestId::new("request-1"),
        source: source.clone(),
        target: target.clone(),
        starting_configuration_id: previous.configuration_id.clone(),
        starting_epoch: previous.epoch,
        handoff_lsn: 7,
    };
    for (local, role, is_source) in [
        (source.clone(), ReplicaRole::ActiveSecondary, true),
        (target.clone(), ReplicaRole::Primary, false),
        (third, ReplicaRole::ActiveSecondary, false),
    ] {
        let directory = tempdir().unwrap();
        let mut state = AgentState::new(StorageIdentity {
            local_identity: local.clone(),
            effective_policy: policy.clone(),
            ..storage_identity()
        });
        let starting_role = if is_source {
            ReplicaRole::Primary
        } else {
            ReplicaRole::ActiveSecondary
        };
        state.highest_epoch = previous.epoch;
        state.current_configuration = Some(previous.clone());
        state.role = starting_role;
        state.read_status = AccessStatus::ReconfigurationPending;
        state.write_status = AccessStatus::ReconfigurationPending;
        state.prepared_switchover = is_source.then(|| handoff.clone());
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
            runtime_state.role = starting_role;
            runtime_state.read_status = AccessStatus::ReconfigurationPending;
            runtime_state.write_status = AccessStatus::ReconfigurationPending;
            runtime_state.current_progress = 7;
            runtime_state.authority = Some(AdmittedAuthority {
                scale_up: None,
                secondary_removal: None,
                local_identity: local.clone(),
                transition_kind: None,
                previous_configuration: None,
                current_configuration: previous.clone(),
                switchover_handoff: None,
            });
        }
        let command = EnsureConfiguration {
            previous_policy: None,
            secondary_removal_evidence: None,
            scale_up_evidence: None,
            operation_id: OperationId::new(format!("current-only-{}", local.replica_id)),
            previous_configuration: None,
            current_configuration: current.clone(),
            previous_epoch: None,
            current_epoch: current.epoch,
            effective_policy: policy.clone(),
            local_replica_id: local.replica_id,
            expected_instance_id: local.instance_id,
            expected_agent_generation: local.agent_generation,
            transition_kind: TransitionKind::PlannedSwitchover,
            failover_safe_lsn: None,
            primary_write_status: AccessStatus::ReconfigurationPending,
            current_only: true,
            retire_build_ids: Vec::new(),
            switchover_handoff: Some(handoff.clone()),
            retire_switchover_preparation_ids: if is_source {
                vec![handoff.preparation()]
            } else {
                Vec::new()
            },
        };

        let install = EnsureConfiguration {
            operation_id: OperationId::new(format!("pc-cc-{}", local.replica_id)),
            previous_configuration: Some(previous.clone()),
            previous_epoch: Some(previous.epoch),
            current_only: false,
            retire_switchover_preparation_ids: Vec::new(),
            ..command.clone()
        };
        if is_source {
            let restore_directory = tempdir().unwrap();
            let restore_path = SqliteStore::metadata_database_path(restore_directory.path());
            let restore_store = Arc::new(
                SqliteStore::create_authorized(&restore_path, store.load_state().await.unwrap())
                    .unwrap(),
            );
            let restore_runtime = Arc::new(FakeRuntime::new());
            *restore_runtime.state.lock().unwrap() = runtime.state.lock().unwrap().clone();
            let restore = EnsureConfiguration {
                operation_id: OperationId::new("restore-source"),
                current_configuration: previous.clone(),
                current_epoch: previous.epoch,
                current_only: false,
                ..command.clone()
            };
            assert!(restore.is_switchover_restoration());
            let mut delayed_grant = restore.clone();
            delayed_grant.transition_kind = TransitionKind::Bootstrap;
            delayed_grant.switchover_handoff = None;
            delayed_grant.retire_switchover_preparation_ids.clear();
            delayed_grant.primary_write_status = AccessStatus::Granted;
            assert!(
                admit_configuration(&delayed_grant, &restore_store.load_state().await.unwrap())
                    .is_err()
            );
            let restored = Coordinator::new(restore_store.clone(), restore_runtime.clone());
            restored
                .ensure_configuration(restore.clone())
                .await
                .unwrap();
            assert_eq!(*restore_runtime.calls.lock().unwrap(), ["access"]);
            let state = restore_store.load_state().await.unwrap();
            assert_eq!(state.highest_epoch, previous.epoch);
            assert_eq!(state.role, ReplicaRole::Primary);
            assert!(state.prepared_switchover.is_none());
            assert_eq!(state.retired_switchover, Some(handoff.clone()));
            assert_ne!(state.write_status, AccessStatus::Granted);
            let reopened = Arc::new(SqliteStore::open_existing(&restore_path, None).unwrap());
            Coordinator::new(reopened.clone(), restore_runtime)
                .ensure_configuration(restore)
                .await
                .unwrap();
            let replay = PrepareSwitchover {
                preparation_generation: handoff.preparation_generation,
                operation_id: handoff.preparation_operation_id.clone(),
                request_id: handoff.request_id.clone(),
                local_replica_id: source.replica_id,
                expected_instance_id: source.instance_id.clone(),
                expected_agent_generation: source.agent_generation.clone(),
                source: source.clone(),
                target: target.clone(),
                current_configuration: previous.clone(),
            };
            assert!(
                crate::host::command::admit_switchover_preparation(
                    &replay,
                    &reopened.load_state().await.unwrap()
                )
                .is_err()
            );
        }
        let mut premature_grant = install.clone();
        premature_grant.primary_write_status = AccessStatus::Granted;
        assert!(admit_configuration(&premature_grant, &store.load_state().await.unwrap()).is_err());
        if is_source {
            let interrupted_directory = tempdir().unwrap();
            let interrupted_store = Arc::new(
                SqliteStore::create_authorized(
                    SqliteStore::metadata_database_path(interrupted_directory.path()),
                    store.load_state().await.unwrap(),
                )
                .unwrap(),
            );
            let interrupted_runtime = Arc::new(FakeRuntime::new());
            *interrupted_runtime.state.lock().unwrap() = runtime.state.lock().unwrap().clone();
            interrupted_runtime.fail_once("read");
            let interrupted = Coordinator::new(interrupted_store.clone(), interrupted_runtime);
            assert!(
                interrupted
                    .ensure_configuration(install.clone())
                    .await
                    .is_err()
            );
            let admitted = interrupted_store.load_state().await.unwrap();
            assert_eq!(admitted.highest_epoch, current.epoch);
            assert_eq!(admitted.role, ReplicaRole::Primary); // authority admitted, role not demoted
            let compensation = ConfigurationDescriptor::new(
                Epoch::new(0, 3),
                source.replica_id,
                previous.members.clone(),
                previous.write_quorum,
            );
            let compensate = EnsureConfiguration {
                operation_id: OperationId::new("supersede-admitted-request"),
                previous_configuration: Some(current.clone()),
                previous_epoch: Some(current.epoch),
                current_configuration: compensation.clone(),
                current_epoch: compensation.epoch,
                ..install.clone()
            };
            interrupted.ensure_configuration(compensate).await.unwrap();
            let recovered = interrupted_store.load_state().await.unwrap();
            assert_eq!(recovered.highest_epoch, Epoch::new(0, 3));
            assert_eq!(recovered.role, ReplicaRole::Primary);
            assert_eq!(recovered.prepared_switchover, Some(handoff.clone()));
            assert_ne!(recovered.write_status, AccessStatus::Granted);
            assert!(
                interrupted
                    .ensure_configuration(install.clone())
                    .await
                    .is_err()
            );
        }
        let coordinator = Coordinator::new(store.clone(), runtime.clone());
        coordinator.ensure_configuration(install).await.unwrap();
        let mut expected = vec!["admit", "read", "get-lsn", "write", "replicator-role"];
        if role == ReplicaRole::Primary {
            expected.push("epoch");
        }
        expected.push("application-role");
        if role == ReplicaRole::Primary {
            expected.push("catchup");
        }
        expected.push("access");
        assert_eq!(*runtime.calls.lock().unwrap(), expected);
        let installed = store.load_state().await.unwrap();
        assert_eq!(installed.role, role);
        assert_ne!(installed.write_status, AccessStatus::Granted);
        if role == ReplicaRole::Primary {
            assert_eq!(installed.read_status, AccessStatus::ReconfigurationPending);
        }
        assert_eq!(
            installed.prepared_switchover,
            is_source.then(|| handoff.clone())
        );
        runtime.calls.lock().unwrap().clear();
        coordinator
            .ensure_configuration(command.clone())
            .await
            .unwrap();
        let completed = store.load_state().await.unwrap();
        assert!(completed.reconfiguration.is_none());
        assert!(completed.prepared_switchover.is_none());
        assert_ne!(completed.write_status, AccessStatus::Granted);
        if role == ReplicaRole::Primary {
            assert_eq!(completed.read_status, AccessStatus::ReconfigurationPending);
        }
        assert_eq!(*runtime.calls.lock().unwrap(), expected);
        assert_eq!(
            completed.retained_command.as_ref().unwrap().command,
            command
        );
        coordinator
            .ensure_configuration(command.clone())
            .await
            .unwrap();
        let compensation = ConfigurationDescriptor::new(
            Epoch::new(0, 3),
            source.replica_id,
            previous.members.clone(),
            previous.write_quorum,
        );
        let compensate = EnsureConfiguration {
            operation_id: OperationId::new(format!("compensate-{}", local.replica_id)),
            previous_configuration: Some(current.clone()),
            previous_epoch: Some(current.epoch),
            current_configuration: compensation.clone(),
            current_epoch: compensation.epoch,
            current_only: false,
            retire_switchover_preparation_ids: Vec::new(),
            ..command.clone()
        };
        if is_source {
            for wrong in ["prefix", "request", "target"] {
                let mut changed = compensate.clone();
                let certificate = changed.switchover_handoff.as_mut().unwrap();
                match wrong {
                    "prefix" => certificate.handoff_lsn -= 1,
                    "request" => certificate.request_id = SwitchoverRequestId::new("other"),
                    "target" => certificate.target.agent_generation = AgentGeneration::new("other"),
                    _ => unreachable!(),
                }
                assert!(admit_configuration(&changed, &store.load_state().await.unwrap()).is_err());
            }
        }
        coordinator
            .ensure_configuration(compensate.clone())
            .await
            .unwrap();
        let complete_compensation = EnsureConfiguration {
            operation_id: OperationId::new(format!(
                "compensation-current-only-{}",
                local.replica_id
            )),
            previous_configuration: None,
            previous_epoch: None,
            current_only: true,
            retire_switchover_preparation_ids: command.retire_switchover_preparation_ids.clone(),
            ..compensate.clone()
        };
        coordinator
            .ensure_configuration(complete_compensation)
            .await
            .unwrap();
        let completed = store.load_state().await.unwrap();
        assert_eq!(
            completed.role,
            if is_source {
                ReplicaRole::Primary
            } else {
                ReplicaRole::ActiveSecondary
            }
        );
        assert_eq!(completed.highest_epoch, Epoch::new(0, 3));
        assert!(completed.previous_configuration.is_none());
        assert!(completed.prepared_switchover.is_none());
        assert_ne!(completed.write_status, AccessStatus::Granted);
        assert!(coordinator.ensure_configuration(command).await.is_err());
        if is_source {
            let grant = EnsureConfiguration {
                operation_id: OperationId::new("compensated-stable-grant"),
                previous_configuration: None,
                previous_epoch: None,
                primary_write_status: AccessStatus::Granted,
                transition_kind: TransitionKind::Bootstrap,
                switchover_handoff: None,
                ..compensate
            };
            coordinator.ensure_configuration(grant).await.unwrap();
            assert_eq!(
                store.load_state().await.unwrap().write_status,
                AccessStatus::Granted
            );
        }
    }
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
            retire: false,
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
        state.current_progress = 13;
        state.verified_replication_lsn = Some(13);
        state.committed_lsn = 13;
        state.current_configuration_quorum_progress = 13;
        state.authority = Some(AdmittedAuthority {
            scale_up: None,
            secondary_removal: None,
            local_identity: local.clone(),
            transition_kind: None,
            previous_configuration: None,
            current_configuration: previous.clone(),
            switchover_handoff: None,
        });
    }
    let coordinator = Coordinator::new(store.clone(), runtime.clone());
    let command = EnsureConfiguration {
        previous_policy: None,
        secondary_removal_evidence: None,
        scale_up_evidence: None,
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
        failover_safe_lsn: Some(12),
        primary_write_status: AccessStatus::NoWriteQuorum,
        current_only: false,
        retire_build_ids: Vec::new(),
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    };
    coordinator
        .ensure_configuration(command.clone())
        .await
        .unwrap();
    assert_eq!(
        coordinator
            .ensure_configuration(command.clone())
            .await
            .unwrap()
            .command,
        command
    );
    let mut mutated = command.clone();
    mutated.failover_safe_lsn = Some(13);
    assert!(matches!(
        coordinator.ensure_configuration(mutated).await,
        Err(crate::host::HostError::DurableEffectConflict(_))
    ));

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
        Err(crate::host::HostError::DurableEffectConflict(_))
    ));
    assert!(matches!(
        coordinator
            .ensure_configuration(command("stale-operation", Epoch::new(0, 1)))
            .await,
        Err(crate::host::HostError::CommandRejected(_))
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
        crate::host::state::CoordinatorStage::Activate
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
            scale_up: None,
            secondary_removal: None,
            local_identity: local.clone(),
            transition_kind: Some(TransitionKind::Replacement),
            previous_configuration: Some(previous),
            current_configuration: current.clone(),
            switchover_handoff: None,
        });
    }
    let command = EnsureConfiguration {
        previous_policy: None,
        secondary_removal_evidence: None,
        scale_up_evidence: None,
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
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
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
    coordinator
        .ensure_configuration(command.clone())
        .await
        .unwrap();
    assert_eq!(*runtime.calls.lock().unwrap(), calls);
    let mut mutated = command;
    mutated.retire_build_ids = vec![OperationId::new("different-build")];
    assert!(matches!(
        coordinator.ensure_configuration(mutated).await,
        Err(crate::host::HostError::DurableEffectConflict(_))
    ));
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
        previous_policy: None,
        secondary_removal_evidence: None,
        scale_up_evidence: None,
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
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    };
    assert!(matches!(
        admit_configuration(&command, &state),
        Err(crate::host::HostError::CommandRejected(_))
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

fn evaluator_agent_states(model: &scale_up_model::Model) -> BTreeMap<i64, AgentState> {
    let configuration = &model
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration;
    let policy = model.snapshot.status.effective_policy.clone().unwrap();
    configuration
        .members
        .iter()
        .map(|member| {
            let mut state = AgentState::new(StorageIdentity {
                schema_version: SCHEMA_VERSION,
                resource_uid: model.snapshot.resource_uid.clone(),
                pod_uid: PodUid::new(member.identity.instance_id.as_str()),
                pvc_uid: PvcUid::new(format!("pvc-{}", member.identity.replica_id)),
                initialization_id: InitializationId::new(format!(
                    "init-{}",
                    member.identity.replica_id
                )),
                local_identity: member.identity.clone(),
                effective_policy: policy.clone(),
            });
            state.admitted_policy = Some(policy.clone());
            state.highest_epoch = configuration.epoch;
            state.current_configuration = Some(configuration.clone());
            state.role = member.role;
            state.read_status = AccessStatus::Granted;
            state.write_status = if member.role == ReplicaRole::Primary {
                AccessStatus::Granted
            } else {
                AccessStatus::NotPrimary
            };
            (member.identity.replica_id.value(), state)
        })
        .collect()
}

fn step_evaluator_with_agent_admission(
    model: &mut scale_up_model::Model,
    states: &mut BTreeMap<i64, AgentState>,
) {
    match model.plan() {
        Plan::Apply { changes } => {
            for change in changes {
                if let crate::protocol::command::KubernetesChange::PersistStatus { status } =
                    &change
                {
                    crate::protocol::validation::validate_status(status).unwrap();
                }
                model.apply(change);
            }
        }
        Plan::Execute { command } => {
            match &command {
                ProtocolCommand::InitializeAgentStore(command) => {
                    apply_evaluator_initialization(states, command)
                }
                ProtocolCommand::EnsureConfiguration(command) => {
                    apply_evaluator_configuration(states, command)
                }
                _ => {}
            }
            model.execute(command);
        }
        Plan::Wait { status, .. } => model.apply_wait(status),
        Plan::Stable { status, .. } => model.snapshot.status = status,
        Plan::Unsafe { reason, .. } => panic!("evaluator admission trace unsafe: {reason:?}"),
    }
}

#[test]
fn evaluator_generated_sequential_scale_up_commands_pass_real_agent_admission() {
    let mut model = scale_up_model::Model::new(1, 3);
    let mut states = evaluator_agent_states(&model);
    let mut restored_access = false;
    for _ in 0..240 {
        if model.accepted_count() == 2
            && model.snapshot.status.transition.is_none()
            && !restored_access
        {
            let primary = states
                .values_mut()
                .find(|state| state.role == ReplicaRole::Primary)
                .unwrap();
            apply_access_restoration(primary);
            restored_access = true;
        }
        step_evaluator_with_agent_admission(&mut model, &mut states);
        if model.accepted_count() == 3
            && model.snapshot.status.transition.is_none()
            && matches!(model.plan(), Plan::Stable { .. })
        {
            break;
        }
    }
    assert_eq!(model.accepted_history, vec![1, 2, 3]);
    assert!(restored_access);
    let accepted = &model
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration;
    for member in &accepted.members {
        let state = states.get(&member.identity.replica_id.value()).unwrap();
        assert_eq!(state.current_configuration.as_ref(), Some(accepted));
        assert!(state.previous_configuration.is_none());
        assert_eq!(state.role, member.role);
        assert!(
            state
                .retained_command
                .as_ref()
                .is_some_and(|retained| retained.command.current_only)
        );
    }
}

#[test]
fn evaluator_generated_carried_failover_commands_pass_real_agent_admission() {
    let mut model = scale_up_model::Model::new(2, 3);
    let mut states = evaluator_agent_states(&model);
    loop {
        let candidate_current_only = model
            .snapshot
            .status
            .transition
            .as_ref()
            .and_then(|transition| transition.scale_up.as_deref())
            .is_some_and(|intent| {
                model
                    .snapshot
                    .observation_for_identity(&intent.target)
                    .and_then(|observation| match &observation.agent {
                        AgentObservation::Report(report) => Some(report),
                        _ => None,
                    })
                    .is_some_and(|report| {
                        report.previous_configuration.is_none()
                            && report.current_configuration.as_ref()
                                == Some(&intent.current_configuration)
                    })
            });
        if candidate_current_only {
            break;
        }
        step_evaluator_with_agent_admission(&mut model, &mut states);
    }
    model.report_mut(1).reported_fault = Some(crate::protocol::types::FaultType::Permanent);
    model.report_mut(1).write_status = AccessStatus::ReconfigurationPending;
    for _ in 0..160 {
        step_evaluator_with_agent_admission(&mut model, &mut states);
        if model.accepted_count() == 3 && model.snapshot.status.transition.is_none() {
            break;
        }
    }
    assert_eq!(model.accepted_count(), 3);
    let receipt = model
        .snapshot
        .status
        .last_scale_up
        .as_ref()
        .unwrap()
        .clone();
    assert!(receipt.failover_evidence.is_some());
    let accepted = receipt.accepted_configuration.clone();
    for member in accepted
        .members
        .iter()
        .filter(|member| member.identity.replica_id != ReplicaId::new(1))
    {
        let state = states.get(&member.identity.replica_id.value()).unwrap();
        assert_eq!(state.current_configuration.as_ref(), Some(&accepted));
        assert!(state.previous_configuration.is_none());
        assert!(matches!(
            state.scale_up_evidence.as_deref(),
            Some(ScaleUpConfigurationEvidence::Failover { .. })
        ));
    }

    let intent = receipt.intent.clone();
    let old_primary = intent.primary.clone();
    let original_member = intent
        .current_configuration
        .members
        .iter()
        .find(|member| member.identity == old_primary)
        .unwrap();
    let original_current_only = EnsureConfiguration {
        operation_id: intent.command_operation_id(
            ScaleUpStage::CurrentOnly,
            &old_primary,
            &intent.current_configuration,
        ),
        previous_configuration: None,
        current_configuration: intent.current_configuration.clone(),
        previous_epoch: None,
        current_epoch: intent.current_configuration.epoch,
        effective_policy: intent.current_policy.clone(),
        previous_policy: Some(intent.previous_policy.clone()),
        secondary_removal_evidence: None,
        scale_up_evidence: Some(Box::new(ScaleUpConfigurationEvidence::Admission {
            intent: intent.clone(),
        })),
        local_replica_id: old_primary.replica_id,
        expected_instance_id: old_primary.instance_id.clone(),
        expected_agent_generation: old_primary.agent_generation.clone(),
        transition_kind: TransitionKind::ScaleUp,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::Granted,
        current_only: true,
        retire_build_ids: vec![intent.build_id.clone()],
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    };
    {
        let state = states.get_mut(&old_primary.replica_id.value()).unwrap();
        state.previous_configuration = None;
        state.current_configuration = Some(intent.current_configuration.clone());
        state.highest_epoch = intent.current_configuration.epoch;
        state.scale_up_evidence = original_current_only.scale_up_evidence.clone();
        state.admitted_policy = Some(intent.current_policy.clone());
        state.previous_policy = None;
        state.role = original_member.role;
        state.read_status = AccessStatus::Granted;
        state.write_status = AccessStatus::Granted;
        state.retired_builds.insert(intent.build_id.clone());
        state.retained_command = Some(RetainedCommandResult {
            command: original_current_only.clone(),
            role: state.role,
            epoch: state.highest_epoch,
        });
        state.completed_scale_up = state.retained_command.clone().map(Box::new);
    }
    {
        let report = model.report_mut(old_primary.replica_id.value());
        report.healthy = true;
        report.reported_fault = None;
        report.role = original_member.role;
        report.read_status = AccessStatus::Granted;
        report.write_status = AccessStatus::Granted;
        report.epoch = intent.current_configuration.epoch;
        report.previous_configuration = None;
        report.current_configuration = Some(intent.current_configuration.clone());
        report.verified_replication_lsn = Some(intent.catch_up_boundary_lsn);
        report.retained_operation_id = Some(original_current_only.operation_id.clone());
        report.scale_up_intent = Some(Box::new(intent.clone()));
        report.report_sequence += 1;
    }
    let mut corrections = 0;
    for _ in 0..10 {
        match model.plan() {
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } if command.local_replica_id == old_primary.replica_id => {
                apply_evaluator_configuration(&mut states, &command);
                model.execute(ProtocolCommand::EnsureConfiguration(command));
                corrections += 1;
                if corrections == 2 {
                    break;
                }
            }
            Plan::Wait { status, .. } => model.apply_wait(status),
            other => panic!("late failover correction stalled: {other:?}"),
        }
    }
    assert_eq!(corrections, 2);
    let recovered = states.get(&old_primary.replica_id.value()).unwrap();
    assert_eq!(recovered.current_configuration.as_ref(), Some(&accepted));
    assert!(recovered.previous_configuration.is_none());
}

#[tokio::test]
async fn final_reselection_and_provisional_receipt_recovery_pass_real_agent_admission() {
    let mut model = scale_up_model::Model::new(4, 5);
    model.truncate_durable_history(8);
    let mut states = evaluator_agent_states(&model);
    loop {
        let pc_cc_complete = model
            .snapshot
            .status
            .transition
            .as_ref()
            .and_then(|transition| transition.scale_up.as_deref())
            .is_some_and(|intent| {
                intent.current_configuration.members.iter().all(|member| {
                    model
                        .snapshot
                        .observation_for_identity(&member.identity)
                        .and_then(|observation| match &observation.agent {
                            AgentObservation::Report(report) => Some(report.as_ref()),
                            _ => None,
                        })
                        .is_some_and(|report| {
                            report.previous_configuration.as_ref()
                                == Some(&intent.previous_configuration)
                                && report.current_configuration.as_ref()
                                    == Some(&intent.current_configuration)
                                && report.pending_operation_id.is_none()
                        })
                })
            });
        if pc_cc_complete {
            break;
        }
        step_evaluator_with_agent_admission(&mut model, &mut states);
    }

    let replica3_key = model
        .snapshot
        .replicas
        .iter()
        .find_map(|(key, observation)| match &observation.agent {
            AgentObservation::Report(report) if report.identity.replica_id == ReplicaId::new(3) => {
                Some(key.clone())
            }
            _ => None,
        })
        .unwrap();
    let returning_replica3 = model.snapshot.replicas[&replica3_key].clone();
    model
        .snapshot
        .replicas
        .get_mut(&replica3_key)
        .unwrap()
        .agent = AgentObservation::Absent;
    model.report_mut(1).reported_fault = Some(crate::protocol::types::FaultType::Permanent);
    model.report_mut(1).write_status = AccessStatus::ReconfigurationPending;
    for _ in 0..8 {
        step_evaluator_with_agent_admission(&mut model, &mut states);
        if model
            .snapshot
            .status
            .transition
            .as_ref()
            .is_some_and(|transition| transition.scale_up_failover.is_some())
        {
            break;
        }
    }
    let provisional_transition = model.snapshot.status.transition.as_ref().unwrap().clone();
    let provisional_evidence = provisional_transition.scale_up_failover.as_deref().unwrap();
    assert_eq!(
        provisional_transition.current_configuration.primary_id,
        ReplicaId::new(2)
    );
    assert_eq!(
        provisional_evidence
            .current_read_quorum
            .iter()
            .map(|witness| witness.identity.replica_id.value())
            .collect::<Vec<_>>(),
        vec![2, 4, 5]
    );
    assert!(provisional_evidence.final_election.is_none());

    model
        .snapshot
        .replicas
        .insert(replica3_key.clone(), returning_replica3);
    let acknowledged = model.acknowledge_old_scale_up_authority_write(91, 7, &[1, 3, 4]);
    assert_eq!(acknowledged, 11);

    let mut provisional_replica2 = None;
    let mut provisional_replica2_report = None;
    let mut admitted_final_primary = false;
    for _ in 0..240 {
        let plan = model.plan();
        match plan {
            Plan::Apply { changes } => {
                for change in changes {
                    if let crate::protocol::command::KubernetesChange::PersistStatus { status } =
                        &change
                        && let Some(transition) = status.transition.as_ref()
                        && transition.election_lsn == Some(11)
                    {
                        let evidence = transition
                            .scale_up_failover
                            .as_deref()
                            .expect("carried scale-up failover evidence");
                        let final_election = evidence
                            .final_election
                            .as_deref()
                            .expect("final post-fence election evidence");
                        assert_eq!(
                            final_election.selected_primary_replica_id,
                            ReplicaId::new(3)
                        );
                        assert_eq!(
                            final_election
                                .final_configuration(&evidence.provisional_configuration)
                                .as_ref(),
                            Some(&transition.current_configuration)
                        );
                        assert_eq!(final_election.safe_lsn(), Some(11));
                        assert!(
                            final_election
                                .current_read_quorum
                                .contains(&ReplicaId::new(3))
                        );
                        assert!(
                            final_election
                                .witness(ReplicaId::new(3))
                                .is_some_and(|witness| {
                                    witness.replica_id == ReplicaId::new(3)
                                        && witness.current_progress == 11
                                        && witness.deactivated_lsn == 11
                                })
                        );
                    }
                    model.apply(change);
                }
            }
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } => {
                if command.scale_up_evidence.is_none() {
                    let state = states
                        .get_mut(&command.local_replica_id.value())
                        .expect("access-restoration target state");
                    let authority = admit_configuration(&command, state).unwrap();
                    let role = authority.local_role();
                    state.previous_configuration = authority.previous_configuration.clone();
                    state.current_configuration = Some(authority.current_configuration.clone());
                    state.highest_epoch = authority.current_configuration.epoch;
                    state.scale_up_evidence = authority.scale_up;
                    state.role = role;
                    state.read_status = AccessStatus::Granted;
                    state.write_status = if state.role == ReplicaRole::Primary {
                        command.primary_write_status
                    } else {
                        AccessStatus::NotPrimary
                    };
                    state.retained_command = Some(RetainedCommandResult {
                        command: command.as_ref().clone(),
                        role: state.role,
                        epoch: state.highest_epoch,
                    });
                    model.execute(ProtocolCommand::EnsureConfiguration(command));
                    continue;
                }
                if command.failover_safe_lsn.is_some()
                    && command.local_replica_id == ReplicaId::new(3)
                {
                    let mut missing_final = command.as_ref().clone();
                    let Some(ScaleUpConfigurationEvidence::Failover { evidence }) =
                        missing_final.scale_up_evidence.as_deref_mut()
                    else {
                        unreachable!()
                    };
                    evidence.final_election = None;
                    assert!(
                        admit_configuration(
                            &missing_final,
                            states.get(&3).expect("replica 3 state"),
                        )
                        .is_err(),
                        "final primary must reject a command without final election evidence"
                    );
                    let coordinator_state = states[&3].clone();
                    let directory = tempdir().unwrap();
                    let store = Arc::new(
                        SqliteStore::create_authorized(
                            directory.path().join("final-primary.db"),
                            coordinator_state.clone(),
                        )
                        .unwrap(),
                    );
                    let runtime = Arc::new(FakeRuntime::new());
                    {
                        let mut postcondition = runtime.state.lock().unwrap();
                        postcondition.role = coordinator_state.role;
                        postcondition.read_status = coordinator_state.read_status;
                        postcondition.write_status = coordinator_state.write_status;
                        postcondition.current_progress = 11;
                        postcondition.verified_replication_lsn = Some(11);
                        postcondition.committed_lsn = 11;
                        postcondition.current_configuration_quorum_progress = 11;
                        postcondition.authority = Some(AdmittedAuthority {
                            local_identity: coordinator_state.identity.local_identity.clone(),
                            transition_kind: Some(TransitionKind::Failover),
                            previous_configuration: coordinator_state
                                .previous_configuration
                                .clone(),
                            current_configuration: coordinator_state
                                .current_configuration
                                .clone()
                                .unwrap(),
                            switchover_handoff: None,
                            secondary_removal: None,
                            scale_up: coordinator_state.scale_up_evidence.clone(),
                        });
                    }
                    Coordinator::new(store.clone(), runtime)
                        .ensure_configuration(command.as_ref().clone())
                        .await
                        .expect("real coordinator must admit the final reselected primary");
                    assert_eq!(
                        store.load_state().await.unwrap().current_configuration,
                        Some(command.current_configuration.clone())
                    );
                    admitted_final_primary = true;
                }
                let provisional_replica2_command = command.failover_safe_lsn.is_none()
                    && command.local_replica_id == ReplicaId::new(2);
                apply_evaluator_configuration(&mut states, &command);
                model.execute(ProtocolCommand::EnsureConfiguration(command));
                if provisional_replica2_command {
                    provisional_replica2 = Some(states[&2].clone());
                    let replica2_key = model
                        .snapshot
                        .replicas
                        .iter()
                        .find_map(|(key, observation)| match &observation.agent {
                            AgentObservation::Report(report)
                                if report.identity.replica_id == ReplicaId::new(2) =>
                            {
                                Some(key.clone())
                            }
                            _ => None,
                        })
                        .unwrap();
                    provisional_replica2_report =
                        Some(model.snapshot.replicas[&replica2_key].clone());
                }
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Wait { status, .. } => model.apply_wait(status),
            Plan::Stable { status, .. } => model.snapshot.status = status,
            Plan::Unsafe { reason, .. } => panic!("final reselection became unsafe: {reason:?}"),
        }
        if model.accepted_count() == 5
            && model.snapshot.status.transition.is_none()
            && matches!(model.plan(), Plan::Stable { .. })
        {
            break;
        }
    }
    assert!(admitted_final_primary);
    let receipt = model.snapshot.status.last_scale_up.clone().unwrap();
    assert_eq!(receipt.accepted_configuration.primary_id, ReplicaId::new(3));
    assert_eq!(receipt.failover_safe_lsn, Some(11));
    let final_primary = receipt
        .accepted_configuration
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap();
    assert!(
        model
            .validate_exact_durable_history(&final_primary.identity, acknowledged)
            .unwrap()
            >= acknowledged
    );
    {
        let settled = model.report_mut(1);
        settled.healthy = true;
        settled.reported_fault = None;
        settled.role = receipt
            .accepted_configuration
            .members
            .iter()
            .find(|member| member.identity.replica_id == ReplicaId::new(1))
            .unwrap()
            .role;
        settled.write_status = AccessStatus::NotPrimary;
        settled.epoch = receipt.accepted_configuration.epoch;
        settled.previous_configuration = None;
        settled.current_configuration = Some(receipt.accepted_configuration.clone());
        settled.verified_replication_lsn = Some(acknowledged);
        settled.pending_operation_id = None;
        settled.retained_operation_id = Some(receipt.intent.command_operation_id(
            ScaleUpStage::CurrentOnly,
            &settled.identity,
            &receipt.accepted_configuration,
        ));
        settled.scale_up_intent = Some(Box::new(receipt.intent.clone()));
        settled.report_sequence += 1;
    }

    let replica2_key = model
        .snapshot
        .replicas
        .iter()
        .find_map(|(key, observation)| match &observation.agent {
            AgentObservation::Report(report) if report.identity.replica_id == ReplicaId::new(2) => {
                Some(key.clone())
            }
            _ => None,
        })
        .unwrap();
    states.insert(
        2,
        provisional_replica2.expect("saved provisional replica 2 state"),
    );
    model.snapshot.replicas.insert(
        replica2_key.clone(),
        provisional_replica2_report.expect("saved provisional replica 2 report"),
    );

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(correction),
    } = model.plan()
    else {
        panic!("returning provisional member must receive final PC/CC correction")
    };
    assert_eq!(correction.local_replica_id, ReplicaId::new(2));
    assert!(!correction.current_only);
    assert!(
        correction.scale_up_evidence.is_some(),
        "unexpected correction command: {correction:#?}"
    );
    assert_eq!(correction.failover_safe_lsn, Some(11));
    assert!(admit_configuration(&correction, &states[&2]).is_ok());

    let mut unrelated = states[&2].clone();
    unrelated
        .current_configuration
        .as_mut()
        .unwrap()
        .configuration_id = ConfigurationId::new("other-provisional");
    assert!(admit_configuration(&correction, &unrelated).is_err());

    {
        let report = model.report_mut(2);
        report.pending_operation_id = Some(correction.operation_id.clone());
        report.pending_configuration = Some(correction.clone());
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(correction.clone())
        }
    );
    {
        let report = model.report_mut(2);
        report.epoch = correction.current_epoch;
        report.previous_configuration = correction.previous_configuration.clone();
        report.current_configuration = Some(correction.current_configuration.clone());
        report.role = correction
            .current_configuration
            .members
            .iter()
            .find(|member| member.identity.replica_id == ReplicaId::new(2))
            .unwrap()
            .role;
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(correction.clone())
        },
        "installed final PC/CC must replay byte-identically while pending"
    );
    {
        let report = model.report_mut(2);
        report.pending_operation_id = None;
        report.pending_configuration = None;
    }
    apply_evaluator_configuration(&mut states, &correction);
    model.execute(ProtocolCommand::EnsureConfiguration(correction));

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(current_only),
    } = model.plan()
    else {
        panic!("corrected provisional member must advance to final current-only")
    };
    assert!(current_only.current_only);
    apply_evaluator_configuration(&mut states, &current_only);
    model.execute(ProtocolCommand::EnsureConfiguration(current_only));

    for _ in 0..20 {
        step_evaluator_with_agent_admission(&mut model, &mut states);
        if matches!(model.plan(), Plan::Stable { .. }) {
            break;
        }
    }
    assert!(matches!(model.plan(), Plan::Stable { .. }));
    assert_eq!(model.snapshot.status.last_scale_up.as_ref(), Some(&receipt));
}

#[test]
fn active_carried_failover_repairs_returning_provisional_member_before_current_only() {
    let mut model = scale_up_model::Model::new(4, 5);
    model.truncate_durable_history(8);
    let mut states = evaluator_agent_states(&model);
    loop {
        let pc_cc_complete = model
            .snapshot
            .status
            .transition
            .as_ref()
            .and_then(|transition| transition.scale_up.as_deref())
            .is_some_and(|intent| {
                intent.current_configuration.members.iter().all(|member| {
                    model
                        .snapshot
                        .observation_for_identity(&member.identity)
                        .and_then(|observation| match &observation.agent {
                            AgentObservation::Report(report) => Some(report.as_ref()),
                            _ => None,
                        })
                        .is_some_and(|report| {
                            report.previous_configuration.as_ref()
                                == Some(&intent.previous_configuration)
                                && report.current_configuration.as_ref()
                                    == Some(&intent.current_configuration)
                                && report.pending_operation_id.is_none()
                        })
                })
            });
        if pc_cc_complete {
            break;
        }
        step_evaluator_with_agent_admission(&mut model, &mut states);
    }

    model.report_mut(1).reported_fault = Some(crate::protocol::types::FaultType::Permanent);
    model.report_mut(1).write_status = AccessStatus::ReconfigurationPending;
    while model
        .snapshot
        .status
        .transition
        .as_ref()
        .is_none_or(|transition| transition.scale_up_failover.is_none())
    {
        step_evaluator_with_agent_admission(&mut model, &mut states);
    }
    let acknowledged = model.acknowledge_old_scale_up_authority_write(101, 7, &[1, 3, 4]);
    assert_eq!(acknowledged, 11);

    let replica4_key = model
        .snapshot
        .replicas
        .iter()
        .find_map(|(key, observation)| match &observation.agent {
            AgentObservation::Report(report) if report.identity.replica_id == ReplicaId::new(4) => {
                Some(key.clone())
            }
            _ => None,
        })
        .unwrap();
    let mut provisional_replica4 = None;
    for _ in 0..240 {
        match model.plan() {
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } => {
                let installs_replica4_provisional = command.failover_safe_lsn.is_none()
                    && command.local_replica_id == ReplicaId::new(4);
                let completes_final_primary = command.failover_safe_lsn == Some(acknowledged)
                    && command.current_only
                    && command.local_replica_id == ReplicaId::new(3);
                if completes_final_primary {
                    let saved = provisional_replica4
                        .clone()
                        .expect("replica 4 provisional report");
                    model.snapshot.replicas.insert(replica4_key.clone(), saved);
                    let Plan::Execute {
                        command: ProtocolCommand::EnsureConfiguration(rechecked),
                    } = model.plan()
                    else {
                        panic!("returning provisional member preempted final primary completion")
                    };
                    assert_eq!(rechecked.local_replica_id, ReplicaId::new(3));
                    assert!(rechecked.current_only);
                    apply_evaluator_configuration(&mut states, &rechecked);
                    model.execute(ProtocolCommand::EnsureConfiguration(rechecked));
                    break;
                }
                apply_evaluator_configuration(&mut states, &command);
                model.execute(ProtocolCommand::EnsureConfiguration(command));
                if installs_replica4_provisional {
                    provisional_replica4 = Some(model.snapshot.replicas[&replica4_key].clone());
                    model
                        .snapshot
                        .replicas
                        .get_mut(&replica4_key)
                        .unwrap()
                        .agent = AgentObservation::Absent;
                }
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Wait { status, .. } => model.apply_wait(status),
            Plan::Stable { status, .. } => model.snapshot.status = status,
            Plan::Unsafe { reason, .. } => panic!("active carried failover unsafe: {reason:?}"),
        }
    }

    let transition = model
        .snapshot
        .status
        .transition
        .as_ref()
        .expect("active final carried failover");
    assert_eq!(
        transition.current_configuration.primary_id,
        ReplicaId::new(3)
    );
    assert_eq!(transition.election_lsn, Some(acknowledged));
    assert!(model.snapshot.status.last_scale_up.is_none());

    let assert_fenced = |broken: &scale_up_model::Model, description: &str| {
        assert!(
            matches!(broken.plan(), Plan::Unsafe { .. }),
            "{description}: {:?}",
            broken.plan()
        );
    };
    let mut wrong_epoch = model.fork();
    wrong_epoch.report_mut(4).epoch.configuration_number += 1;
    assert_fenced(&wrong_epoch, "wrong provisional epoch");

    let mut wrong_configuration = model.fork();
    wrong_configuration
        .report_mut(4)
        .current_configuration
        .as_mut()
        .unwrap()
        .configuration_id = ConfigurationId::new("wrong-provisional");
    assert_fenced(&wrong_configuration, "wrong provisional configuration");

    let mut wrong_session = model.fork();
    wrong_session.report_mut(4).process_session_id =
        ProcessSessionId::new("wrong-provisional-session");
    assert_fenced(&wrong_session, "wrong provisional process session");

    let mut wrong_attempt = model.fork();
    wrong_attempt.report_mut(4).scale_up_intent = None;
    assert_fenced(&wrong_attempt, "wrong provisional attempt");

    let mut wrong_final_witness = model.fork();
    let final_election = wrong_final_witness
        .snapshot
        .status
        .transition
        .as_mut()
        .unwrap()
        .scale_up_failover
        .as_mut()
        .unwrap()
        .final_election
        .as_mut()
        .unwrap();
    let witness_replica_id = final_election.current_read_quorum[0];
    final_election
        .witnesses
        .iter_mut()
        .find(|witness| witness.replica_id == witness_replica_id)
        .unwrap()
        .fence_operation_id = OperationId::new("wrong-final-fence");
    assert_fenced(&wrong_final_witness, "wrong final election witness");

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(pc_cc),
    } = model.plan()
    else {
        panic!("returning provisional member must receive final PC/CC before current-only")
    };
    assert_eq!(pc_cc.local_replica_id, ReplicaId::new(4));
    assert!(!pc_cc.current_only);
    assert_eq!(pc_cc.failover_safe_lsn, Some(acknowledged));
    assert!(admit_configuration(&pc_cc, &states[&4]).is_ok());

    {
        let report = model.report_mut(4);
        report.pending_operation_id = Some(pc_cc.operation_id.clone());
        report.pending_configuration = Some(pc_cc.clone());
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(pc_cc.clone())
        },
        "pending final PC/CC must replay byte-identically before installation"
    );
    let mut mutated_pending = model.fork();
    mutated_pending
        .report_mut(4)
        .pending_configuration
        .as_mut()
        .unwrap()
        .failover_safe_lsn = Some(acknowledged + 1);
    assert_fenced(&mutated_pending, "mutated pending final PC/CC");
    {
        let report = model.report_mut(4);
        report.epoch = pc_cc.current_epoch;
        report.previous_configuration = pc_cc.previous_configuration.clone();
        report.current_configuration = Some(pc_cc.current_configuration.clone());
        report.role = pc_cc
            .current_configuration
            .members
            .iter()
            .find(|member| member.identity.replica_id == ReplicaId::new(4))
            .unwrap()
            .role;
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(pc_cc.clone())
        },
        "pending final PC/CC must replay byte-identically after installation"
    );
    {
        let report = model.report_mut(4);
        report.pending_operation_id = None;
        report.pending_configuration = None;
    }
    apply_evaluator_configuration(&mut states, &pc_cc);
    model.execute(ProtocolCommand::EnsureConfiguration(pc_cc));

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(current_only),
    } = model.plan()
    else {
        panic!("corrected provisional member must receive final current-only")
    };
    assert_eq!(current_only.local_replica_id, ReplicaId::new(4));
    assert!(current_only.current_only);
    assert!(admit_configuration(&current_only, &states[&4]).is_ok());
    {
        let report = model.report_mut(4);
        report.pending_operation_id = Some(current_only.operation_id.clone());
        report.pending_configuration = Some(current_only.clone());
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(current_only.clone())
        },
        "pending final current-only must replay byte-identically before installation"
    );
    {
        let report = model.report_mut(4);
        report.previous_configuration = None;
        report.current_configuration = Some(current_only.current_configuration.clone());
        report.epoch = current_only.current_epoch;
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(current_only.clone())
        },
        "pending final current-only must replay byte-identically after installation"
    );
    {
        let report = model.report_mut(4);
        report.pending_operation_id = None;
        report.pending_configuration = None;
    }
    apply_evaluator_configuration(&mut states, &current_only);
    model.execute(ProtocolCommand::EnsureConfiguration(current_only));

    for _ in 0..40 {
        match model.plan() {
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } if command.scale_up_evidence.is_none() => {
                let state = states
                    .get_mut(&command.local_replica_id.value())
                    .expect("access-restoration target state");
                let authority = admit_configuration(&command, state).unwrap();
                let role = authority.local_role();
                state.previous_configuration = authority.previous_configuration.clone();
                state.current_configuration = Some(authority.current_configuration.clone());
                state.highest_epoch = authority.current_configuration.epoch;
                state.scale_up_evidence = authority.scale_up;
                state.role = role;
                state.read_status = AccessStatus::Granted;
                state.write_status = if state.role == ReplicaRole::Primary {
                    command.primary_write_status
                } else {
                    AccessStatus::NotPrimary
                };
                model.execute(ProtocolCommand::EnsureConfiguration(command));
            }
            Plan::Execute { command } => {
                if let ProtocolCommand::EnsureConfiguration(command) = &command {
                    apply_evaluator_configuration(&mut states, command);
                }
                model.execute(command);
            }
            Plan::Wait { status, .. } => model.apply_wait(status),
            Plan::Stable { status, .. } => model.snapshot.status = status,
            Plan::Unsafe { reason, .. } => panic!("final convergence unsafe: {reason:?}"),
        }
        if model.snapshot.status.transition.is_none()
            && let Some(receipt) = model.snapshot.status.last_scale_up.clone()
        {
            let accepted_member = receipt
                .accepted_configuration
                .members
                .iter()
                .find(|member| member.identity.replica_id == ReplicaId::new(1))
                .unwrap();
            let report = model.report_mut(1);
            report.healthy = true;
            report.reported_fault = None;
            report.role = accepted_member.role;
            report.read_status = AccessStatus::Granted;
            report.write_status = AccessStatus::NotPrimary;
            report.epoch = receipt.accepted_configuration.epoch;
            report.previous_configuration = None;
            report.current_configuration = Some(receipt.accepted_configuration.clone());
            report.current_progress = report.current_progress.max(acknowledged);
            report.verified_replication_lsn = Some(report.current_progress);
            report.committed_lsn = report.committed_lsn.max(acknowledged);
            report.pending_operation_id = None;
            report.pending_configuration = None;
            report.retained_operation_id = Some(receipt.intent.command_operation_id(
                ScaleUpStage::CurrentOnly,
                &report.identity,
                &receipt.accepted_configuration,
            ));
            report.scale_up_intent = Some(Box::new(receipt.intent.clone()));
            report.report_sequence += 1;
        }
        if model.snapshot.status.transition.is_none() && matches!(model.plan(), Plan::Stable { .. })
        {
            break;
        }
    }
    assert_eq!(model.accepted_count(), 5);
    assert!(model.snapshot.status.transition.is_none());
    assert!(model.snapshot.status.last_scale_up.is_some());
    assert!(
        matches!(model.plan(), Plan::Stable { .. }),
        "final plan: {:?}",
        model.plan()
    );
}

#[test]
fn evaluator_returning_member_gets_pc_cc_before_current_only_via_real_admission() {
    let mut model = scale_up_model::Model::new(3, 4);
    let mut states = evaluator_agent_states(&model);
    while model.snapshot.status.scale_up_admission_started.is_none() {
        step_evaluator_with_agent_admission(&mut model, &mut states);
    }
    let key = model
        .snapshot
        .replicas
        .iter()
        .find_map(|(key, observation)| match &observation.agent {
            AgentObservation::Report(report) if report.identity.replica_id == ReplicaId::new(2) => {
                Some(key.clone())
            }
            _ => None,
        })
        .unwrap();
    let saved = model.snapshot.replicas.get(&key).unwrap().clone();
    model.snapshot.replicas.get_mut(&key).unwrap().agent = AgentObservation::Absent;
    loop {
        let another_current_only = model.snapshot.replicas.values().any(|observation| {
            matches!(&observation.agent,
                AgentObservation::Report(report)
                    if report.identity.replica_id != ReplicaId::new(2)
                        && report.previous_configuration.is_none()
                        && report.scale_up_intent.is_some())
        });
        if another_current_only {
            break;
        }
        step_evaluator_with_agent_admission(&mut model, &mut states);
    }
    assert_eq!(model.accepted_count(), 3);
    model.snapshot.replicas.insert(key, saved);

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(pc_cc),
    } = model.plan()
    else {
        panic!("returning retained member must receive PC/CC before commitment")
    };
    assert_eq!(pc_cc.local_replica_id, ReplicaId::new(2));
    assert!(!pc_cc.current_only);
    apply_evaluator_configuration(&mut states, &pc_cc);
    model.execute(ProtocolCommand::EnsureConfiguration(pc_cc));

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(current_only),
    } = model.plan()
    else {
        panic!("returning retained member must receive current-only after PC/CC")
    };
    assert_eq!(current_only.local_replica_id, ReplicaId::new(2));
    assert!(current_only.current_only);
    apply_evaluator_configuration(&mut states, &current_only);
    model.execute(ProtocolCommand::EnsureConfiguration(current_only));

    for _ in 0..80 {
        step_evaluator_with_agent_admission(&mut model, &mut states);
        if model.accepted_count() == 4 && model.snapshot.status.transition.is_none() {
            break;
        }
    }
    assert_eq!(model.accepted_count(), 4);
    assert!(model.snapshot.status.transition.is_none());
}

#[test]
fn evaluator_zero_dispatch_failover_recovers_original_primary_via_real_admission() {
    let mut model = scale_up_model::Model::new(2, 3);
    let mut states = evaluator_agent_states(&model);
    while model.snapshot.status.scale_up_admission_started.is_none() {
        step_evaluator_with_agent_admission(&mut model, &mut states);
    }
    let key = model
        .snapshot
        .replicas
        .iter()
        .find_map(|(key, observation)| match &observation.agent {
            AgentObservation::Report(report) if report.identity.replica_id == ReplicaId::new(1) => {
                Some(key.clone())
            }
            _ => None,
        })
        .unwrap();
    let original_report = model.snapshot.replicas.get(&key).unwrap().clone();
    model.report_mut(1).reported_fault = Some(crate::protocol::types::FaultType::Permanent);
    model.report_mut(1).write_status = AccessStatus::ReconfigurationPending;
    for _ in 0..160 {
        step_evaluator_with_agent_admission(&mut model, &mut states);
        if model.accepted_count() == 3 && model.snapshot.status.transition.is_none() {
            break;
        }
    }
    assert_eq!(model.accepted_count(), 3);
    model.snapshot.replicas.insert(key, original_report);

    let mut corrections = 0;
    for _ in 0..20 {
        match model.plan() {
            Plan::Apply { changes } => {
                for change in changes {
                    if let crate::protocol::command::KubernetesChange::PersistStatus { status } =
                        &change
                    {
                        crate::protocol::validation::validate_status(status).unwrap();
                    }
                    model.apply(change);
                }
            }
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } if command.local_replica_id == ReplicaId::new(1) => {
                apply_evaluator_configuration(&mut states, &command);
                model.execute(ProtocolCommand::EnsureConfiguration(command));
                corrections += 1;
                if corrections == 2 {
                    break;
                }
            }
            Plan::Wait { status, .. } => model.apply_wait(status),
            other => panic!("zero-dispatch late-primary recovery stalled: {other:?}"),
        }
    }
    assert_eq!(corrections, 2);
    let accepted = &model
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration;
    let state = states.get(&1).unwrap();
    assert_eq!(state.current_configuration.as_ref(), Some(accepted));
    assert!(state.previous_configuration.is_none());
}
