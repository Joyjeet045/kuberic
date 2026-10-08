use super::*;
use crate::application::OpenMode;
use crate::authority::LocalWriteJournal;
use crate::host::state::{ReconfigurationRecord, RetainedCommandResult, RetainedResult};
use crate::protocol::types::{FaultType, LoadMetric};
use crate::protocol::types::{
    ProcessSessionId, SecondaryRemovalPreparation, SecondaryScaleDownIntent,
};

#[allow(dead_code)]
#[path = "scale_down_model.rs"]
mod model;
use model::fixture;

#[path = "removal_fault_store.rs"]
mod fault_store;
use fault_store::FaultAuthorityStore;

#[path = "removal_quorum_crashes.rs"]
mod quorum_crashes;

struct CrashStore {
    inner: Arc<SqliteStore>,
    boundary: String,
}

impl CrashStore {
    fn hit(&self, point: &str) {
        if self.boundary == point {
            std::process::exit(73);
        }
    }

    fn effect_hit(&self, point: &str, operation: &OperationId) {
        self.hit(point);
        self.hit(&format!(
            "{point}-{}",
            operation.as_str().rsplit(':').next().unwrap()
        ));
    }
}

#[async_trait]
impl AgentStore for CrashStore {
    async fn identity(&self) -> Result<StorageIdentity> {
        self.inner.identity().await
    }
    async fn load_state(&self) -> Result<AgentState> {
        self.inner.load_state().await
    }

    async fn load_admitted_authority(&self) -> Result<Option<AdmittedAuthority>> {
        Ok(self.inner.load().await?)
    }
    async fn begin_effect(&self, effect: &RuntimeEffect) -> Result<BeginEffect> {
        let result = self.inner.begin_effect(effect).await?;
        if matches!(
            effect.action,
            RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(_)
        ) {
            self.hit("historical-intent");
        }
        self.effect_hit("intent", &effect.operation_id);
        Ok(result)
    }
    async fn mark_effect_applied(&self, effect: &RuntimeEffect) -> Result<()> {
        self.inner.mark_effect_applied(effect).await?;
        if matches!(
            effect.action,
            RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(_)
        ) {
            self.hit("historical-applied");
        }
        self.effect_hit("applied", &effect.operation_id);
        Ok(())
    }
    async fn complete_effect(&self, result: &RuntimeEffectResult) -> Result<()> {
        self.inner.complete_effect(result).await?;
        self.effect_hit("receipt", &result.operation_id);
        Ok(())
    }
    async fn cancel_effect(&self, effect: &RuntimeEffect) -> Result<()> {
        self.inner.cancel_effect(effect).await
    }
    async fn begin_configuration(
        &self,
        command: &EnsureConfiguration,
    ) -> Result<BeginConfiguration> {
        let result = self.inner.begin_configuration(command).await?;
        self.hit("command");
        Ok(result)
    }
    async fn journal_build(
        &self,
        command: &crate::protocol::command::EnsureReplicaBuild,
    ) -> Result<crate::protocol::command::EnsureReplicaBuild> {
        self.inner.journal_build(command).await
    }
    async fn abandon_build(
        &self,
        command: &crate::protocol::command::EnsureReplicaBuild,
    ) -> Result<()> {
        self.inner.abandon_build(command).await
    }
    async fn advance_configuration(
        &self,
        id: &OperationId,
        expected: CoordinatorStage,
        next: CoordinatorStage,
        lsn: Option<i64>,
    ) -> Result<ReconfigurationRecord> {
        let result = self
            .inner
            .advance_configuration(id, expected, next, lsn)
            .await?;
        self.hit("stage");
        self.hit(&format!("stage-{expected:?}"));
        Ok(result)
    }
    async fn complete_configuration(&self, id: &OperationId) -> Result<RetainedCommandResult> {
        let result = self.inner.complete_configuration(id).await?;
        self.hit("terminal");
        Ok(result)
    }
    async fn retained_result(&self) -> Result<Option<RetainedResult>> {
        self.inner.retained_result().await
    }
    async fn set_reconfiguration(&self, data: Option<String>) -> Result<()> {
        self.inner.set_reconfiguration(data).await
    }
    async fn clear_reconfiguration(&self) -> Result<()> {
        self.inner.clear_reconfiguration().await
    }
    async fn migrate_schema(&self, a: u32, b: u32) -> Result<()> {
        self.inner.migrate_schema(a, b).await
    }
    async fn record_partition_reports(
        &self,
        metrics: Vec<LoadMetric>,
        fault: Option<FaultType>,
    ) -> Result<()> {
        self.inner.record_partition_reports(metrics, fault).await
    }
    async fn complete_application_initialization(&self) -> Result<()> {
        self.inner.complete_application_initialization().await
    }
}

struct CrashRuntime {
    runtime: Arc<PodRuntime>,
    boundary: String,
}

#[async_trait]
impl RuntimeEffectExecutor for CrashRuntime {
    async fn apply_runtime_effect(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        let historical = matches!(
            effect.action,
            RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(_)
        );
        let stage = effect
            .operation_id
            .as_str()
            .rsplit(':')
            .next()
            .unwrap()
            .to_string();
        let result = self.runtime.apply_effect(effect).await;
        if self.boundary == "closed-before-tombstone" {
            let error = result.unwrap_err();
            assert!(
                error.to_string().contains("crash-before-tombstone"),
                "{error}"
            );
            let snapshot = self.runtime.snapshot().await;
            assert!(!snapshot.open);
            assert_eq!(snapshot.role, ReplicaRole::None);
            assert_eq!(snapshot.read_status, AccessStatus::NotPrimary);
            assert_eq!(snapshot.write_status, AccessStatus::NotPrimary);
            assert!(snapshot.authority.is_none());
            std::process::exit(73);
        }
        let result = result?;
        if historical && self.boundary == "historical-runtime" {
            std::process::exit(73);
        }
        if self.boundary == "runtime" || self.boundary == format!("runtime-{stage}") {
            std::process::exit(73);
        }
        Ok(result)
    }
}

fn state(intent: &SecondaryScaleDownIntent, retiring: bool) -> AgentState {
    let local = if retiring {
        &intent.target
    } else {
        &intent.primary
    };
    let mut state = AgentState::new(StorageIdentity {
        resource_uid: intent.resource_uid.clone(),
        local_identity: local.clone(),
        pod_uid: PodUid::new(local.instance_id.as_str()),
        pvc_uid: PvcUid::new(format!("pvc-{}", local.replica_id)),
        effective_policy: intent.previous_policy.clone(),
        ..storage_identity()
    });
    state.highest_epoch = intent.previous_configuration.epoch;
    state.current_configuration = Some(intent.previous_configuration.clone());
    state.role = if retiring {
        ReplicaRole::ActiveSecondary
    } else {
        ReplicaRole::Primary
    };
    state.read_status = AccessStatus::Granted;
    state.write_status = if retiring {
        AccessStatus::NotPrimary
    } else {
        AccessStatus::Granted
    };
    state
}

fn command(prepared: &SecondaryRemovalPreparation, current_only: bool) -> EnsureConfiguration {
    let mut command = fixture::configuration_command(&prepared.intent, current_only);
    let evidence = command.secondary_removal_evidence.as_mut().unwrap();
    evidence.preparation = prepared.clone();
    for witness in evidence
        .previous_read_quorum
        .iter_mut()
        .chain(&mut evidence.reduced_write_quorum)
    {
        witness.verified_replication_lsn = prepared.boundary_lsn;
    }
    if !current_only {
        evidence.reduced_write_quorum.clear();
    }
    command
}

#[tokio::test]
async fn cancellation_keeps_removal_intent_and_rejects_unrelated_authority() {
    let intent = fixture::intent(&[1, 2], 1);
    for retiring in [false, true] {
        let state = state(&intent, retiring);
        let directory = tempdir().unwrap();
        let store = Arc::new(
            SqliteStore::create_authorized(
                SqliteStore::metadata_database_path(directory.path()),
                state.clone(),
            )
            .unwrap(),
        );
        let action = if retiring {
            RuntimeEffectAction::RetireReplica(Box::new(crate::authority::RetiredAuthority {
                committed: fixture::cleanup(&intent),
                report: fixture::retirement(&intent),
            }))
        } else {
            RuntimeEffectAction::PrepareSecondaryRemoval {
                intent: Box::new(intent.clone()),
                process_session_id: ProcessSessionId::new("session-1"),
                report_sequence: 1,
            }
        };
        let operation_id = if retiring {
            fixture::retire_command(&intent).operation_id
        } else {
            fixture::prepare_command(&intent).operation_id
        };
        let effect = RuntimeEffect {
            operation_id,
            sequence: 1,
            action,
        };
        assert!(
            RuntimeAdapter::new(store.clone(), Arc::new(CancelledRuntime))
                .execute(effect.clone())
                .await
                .is_err()
        );
        let state = store.load_state().await.unwrap();
        assert_eq!(state.pending_effect.as_ref().unwrap().effect, effect);
        let mut unrelated = configuration_command();
        unrelated.current_configuration = intent.previous_configuration.clone();
        unrelated.current_epoch = intent.previous_configuration.epoch;
        unrelated.effective_policy = intent.previous_policy.clone();
        unrelated.local_replica_id = state.identity.local_identity.replica_id;
        unrelated.expected_instance_id = state.identity.local_identity.instance_id.clone();
        unrelated.expected_agent_generation =
            state.identity.local_identity.agent_generation.clone();
        assert!(admit_configuration(&unrelated, &state).is_err());
    }
}

#[tokio::test]
#[ignore]
async fn writer() {
    let path = PathBuf::from(env::var("KUBERIC_REMOVAL_PATH").unwrap());
    let boundary = env::var("KUBERIC_REMOVAL_BOUNDARY").unwrap();
    let (phase, point) = boundary.split_once(':').unwrap();
    let intent = fixture::intent(&[1, 2], 1);
    let state = state(&intent, phase == "retire");
    let store = Arc::new(SqliteStore::create_authorized(&path, state.clone()).unwrap());
    let authority = AdmittedAuthority {
        local_identity: state.identity.local_identity.clone(),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: intent.previous_configuration.clone(),
        switchover_handoff: None,
        scale_up: None,
        secondary_removal: None,
    };
    store.admit(&authority).await.unwrap();
    let application = Arc::new(CrashState::open(crash_application_path(&path)));
    application.apply(seeded_operation()).await.unwrap();
    store
        .record_replication_progress(&ReplicationProgress {
            fence: authority.fence(),
            verified_lsn: 1,
        })
        .await
        .unwrap();
    let fault_store = Arc::new(FaultAuthorityStore {
        inner: store.clone(),
        boundary: boundary.clone(),
        interrupted: Mutex::new(None),
        candidate_path: path.with_extension("preparation-candidate.json"),
    });
    let runtime = Arc::new(PodRuntime::new(
        state.identity.local_identity,
        application,
        fault_store.clone(),
    ));
    runtime
        .reconstruct(
            OpenMode::Existing,
            state.role,
            state.read_status,
            state.write_status,
            None,
        )
        .await
        .unwrap();
    let coordinator = Coordinator::new(store.clone(), runtime.clone());
    if phase == "prepare" {
        let pending = runtime
            .data_plane()
            .begin_write(crate::application::ClientWrite {
                operation_id: OperationId::new("interrupted-local-write"),
                data: Bytes::from_static(b"unacknowledged-before-preparation"),
            })
            .await
            .unwrap();
        *fault_store.interrupted.lock().unwrap() = Some(pending);
    }
    let preparation = if phase == "joint" || phase == "current" {
        Some(
            coordinator
                .ensure_secondary_removal_prepared(
                    fixture::prepare_command(&intent),
                    ProcessSessionId::new("session-1"),
                    1,
                )
                .await
                .unwrap(),
        )
    } else {
        None
    };
    if phase == "current" {
        coordinator
            .ensure_configuration(command(preparation.as_ref().unwrap(), false))
            .await
            .unwrap();
    }
    if point == "closed-before-tombstone" {
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER crash_before_tombstone
                 BEFORE INSERT ON runtime_lifecycle WHEN NEW.kind = 'retired'
                 BEGIN SELECT RAISE(FAIL, 'crash-before-tombstone'); END;",
            )
            .unwrap();
    }
    let coordinator = Coordinator::new(
        Arc::new(CrashStore {
            inner: store,
            boundary: point.into(),
        }),
        Arc::new(CrashRuntime {
            runtime,
            boundary: point.into(),
        }),
    );
    match phase {
        "prepare" => {
            coordinator
                .ensure_secondary_removal_prepared(
                    fixture::prepare_command(&intent),
                    ProcessSessionId::new("session-1"),
                    1,
                )
                .await
                .unwrap();
        }
        "retire" => {
            coordinator
                .ensure_replica_retired(
                    fixture::retire_command(&intent),
                    ProcessSessionId::new("target-session-1"),
                    5,
                )
                .await
                .unwrap();
        }
        "joint" | "current" => {
            coordinator
                .ensure_configuration(command(preparation.as_ref().unwrap(), phase == "current"))
                .await
                .unwrap();
        }
        _ => unreachable!(),
    }
    assert_eq!(point, "reply");
    std::process::exit(73);
}

#[test]
fn real_process_removal_boundaries_preserve_exact_authority_and_values() {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(run_removal_boundary_matrix)
        .unwrap()
        .join()
        .unwrap();
}

fn run_removal_boundary_matrix() {
    let mut boundaries = [
        "prepare:intent",
        "prepare:closed-before-boundary",
        "prepare:runtime",
        "prepare:applied",
        "prepare:receipt",
        "prepare:reply",
        "joint:command",
        "joint:intent",
        "joint:runtime",
        "joint:applied",
        "joint:receipt",
        "joint:stage",
        "joint:terminal",
        "joint:reply",
        "current:command",
        "current:intent",
        "current:runtime",
        "current:applied",
        "current:receipt",
        "current:stage",
        "current:terminal",
        "current:reply",
        "retire:intent",
        "retire:started-before-role-none",
        "retire:role-none-before-close",
        "retire:closed-before-tombstone",
        "retire:application-close",
        "retire:runtime",
        "retire:applied",
        "retire:receipt",
        "retire:reply",
    ]
    .map(str::to_string)
    .to_vec();
    for phase in ["joint", "current"] {
        for (effect, stage) in [
            ("admit-authority", "AdmitAuthority"),
            ("demote", "Demote"),
            ("get-lsn", "GetLsn"),
            ("deactivate", "Deactivate"),
            ("replicator-role", "ReplicatorRole"),
            ("epoch", "Epoch"),
            ("application-role", "ApplicationRole"),
            ("activate", "Activate"),
        ] {
            for point in ["intent", "runtime", "applied", "receipt"] {
                boundaries.push(format!("{phase}:{point}-{effect}"));
            }
            boundaries.push(format!("{phase}:stage-{stage}"));
        }
    }
    if let Ok(boundary) = env::var("KUBERIC_VERIFY_REMOVAL_BOUNDARY") {
        assert!(boundaries.contains(&boundary));
        boundaries = vec![boundary];
    }
    for boundary in boundaries {
        eprintln!("recovering {boundary}");
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "host::tests::crash_boundaries::removal_crashes::writer",
                "--nocapture",
            ])
            .env("KUBERIC_REMOVAL_PATH", &path)
            .env("KUBERIC_REMOVAL_BOUNDARY", &boundary)
            .env("RUST_MIN_STACK", "16777216")
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(73),
            "{boundary}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if boundary == "retire:closed-before-tombstone" {
            rusqlite::Connection::open(&path)
                .unwrap()
                .execute_batch("DROP TRIGGER crash_before_tombstone;")
                .unwrap();
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(Box::pin(async {
                tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    Box::pin(async {
                        let intent = fixture::intent(&[1, 2], 1);
                        let retiring = boundary.starts_with("retire:");
                        let provenance = state(&intent, retiring).identity;
                        for changed_pod in [true, false] {
                            let mut wrong = provenance.clone();
                            if changed_pod {
                                wrong.pod_uid = PodUid::new("replacement-pod");
                            } else {
                                wrong.pvc_uid = PvcUid::new("replacement-pvc");
                            }
                            assert!(
                                SqliteStore::open_existing(&path, Some(&wrong)).is_err(),
                                "{boundary}: cannot reopen another incarnation's storage"
                            );
                        }
                        let store =
                            Arc::new(SqliteStore::open_existing(&path, Some(&provenance)).unwrap());
                        let before = store.load_state().await.unwrap();
                        let tombstone = store.load_retired_authority().await.unwrap();
                        let started = store.load_retirement_started().await.unwrap();
                        let application = Arc::new(CrashState::open(crash_application_path(&path)));
                        if boundary == "retire:closed-before-tombstone" {
                            assert_eq!(application.state.lock().unwrap().close_completions, 1);
                            assert!(tombstone.is_none());
                            assert!(before.retired_authority.is_none());
                            assert_eq!(
                                before.pending_effect.as_ref().unwrap().stage,
                                EffectStage::IntentCommitted
                            );
                        }
                        if matches!(
                            boundary.as_str(),
                            "retire:started-before-role-none"
                                | "retire:role-none-before-close"
                                | "retire:closed-before-tombstone"
                        ) {
                            assert!(started.is_some());
                            assert!(tombstone.is_none());
                            let app = application.state.lock().unwrap();
                            assert_eq!(
                                app.last_role,
                                Some(if boundary == "retire:started-before-role-none" {
                                    ReplicaRole::ActiveSecondary
                                } else {
                                    ReplicaRole::None
                                })
                            );
                            if boundary != "retire:closed-before-tombstone" {
                                assert_eq!(app.close_completions, 0);
                            }
                        }
                        if boundary == "prepare:closed-before-boundary" {
                            assert!(store.load_secondary_removal().await.unwrap().is_none());
                            assert!(before.prepared_secondary_removal.is_none());
                            assert_eq!(application.state.lock().unwrap().applied_lsn, 2);
                        }
                        let runtime = Arc::new(PodRuntime::new(
                            provenance.local_identity.clone(),
                            application.clone(),
                            store.clone(),
                        ));
                        let service = crate::host::service::AgentService::new(
                            store.clone(),
                            runtime.clone(),
                            runtime.clone(),
                            "token",
                        )
                        .unwrap();
                        assert_ne!(service.sessions().local_session().as_str(), "session-1");
                        assert_ne!(
                            service.sessions().local_session().as_str(),
                            "target-session-1"
                        );
                        service
                            .reconstruct_runtime()
                            .await
                            .unwrap_or_else(|e| panic!("{boundary}: {e}"));
                        eprintln!("reconstructed {boundary}");
                        if tombstone.is_some() || started.is_some() {
                            assert_eq!(
                                application.opens.load(Ordering::SeqCst),
                                0,
                                "{boundary} must not Open a tombstone"
                            );
                        }
                        if boundary == "prepare:closed-before-boundary" {
                            assert_ne!(
                                runtime.snapshot().await.write_status,
                                AccessStatus::Granted
                            );
                            assert!(
                                runtime
                                    .data_plane()
                                    .begin_write(crate::application::ClientWrite {
                                        operation_id: OperationId::new(
                                            "forbidden-after-preparation-cut"
                                        ),
                                        data: Bytes::new(),
                                    })
                                    .await
                                    .is_err()
                            );
                        }
                        let coordinator = Coordinator::new(store.clone(), runtime.clone());
                        coordinator
                            .resume_configuration()
                            .await
                            .unwrap_or_else(|e| panic!("{boundary}: {e}"));
                        if retiring {
                            let receipt = coordinator
                                .ensure_replica_retired(
                                    fixture::retire_command(&intent),
                                    service.sessions().local_session().clone(),
                                    1,
                                )
                                .await
                                .unwrap();
                            assert_eq!(
                                receipt.process_session_id,
                                ProcessSessionId::new("target-session-1")
                            );
                            assert!(receipt.application_closed && receipt.peers_fenced);
                            assert_eq!(runtime.snapshot().await.role, ReplicaRole::None);
                            assert!(!runtime.snapshot().await.open);
                            assert!(store.load().await.unwrap().is_none());
                            assert!(store.load_retirement_started().await.unwrap().is_none());
                            assert_eq!(
                                store
                                    .load_retired_authority()
                                    .await
                                    .unwrap()
                                    .unwrap()
                                    .report,
                                receipt
                            );
                            let durable = store.load_state().await.unwrap();
                            let duplicate = coordinator
                                .ensure_replica_retired(
                                    fixture::retire_command(&intent),
                                    service.sessions().local_session().clone(),
                                    99,
                                )
                                .await
                                .unwrap();
                            assert_eq!(receipt, duplicate);
                            assert_eq!(store.load_state().await.unwrap(), durable);
                            let mut conflicting = fixture::retire_command(&intent);
                            conflicting.committed.evidence.preparation.boundary_lsn += 1;
                            assert!(
                                coordinator
                                    .ensure_replica_retired(
                                        conflicting,
                                        service.sessions().local_session().clone(),
                                        100,
                                    )
                                    .await
                                    .is_err()
                            );
                            assert_eq!(store.load_state().await.unwrap(), durable);
                        } else {
                            let prepared = if boundary.starts_with("prepare:") {
                                coordinator
                                    .ensure_secondary_removal_prepared(
                                        fixture::prepare_command(&intent),
                                        service.sessions().local_session().clone(),
                                        1,
                                    )
                                    .await
                                    .unwrap()
                            } else {
                                store
                                    .load_state()
                                    .await
                                    .unwrap()
                                    .prepared_secondary_removal
                                    .unwrap()
                            };
                            assert_eq!(
                                prepared.process_session_id,
                                ProcessSessionId::new("session-1")
                            );
                            assert_eq!(
                                prepared.boundary_lsn,
                                if boundary.starts_with("prepare:") {
                                    2
                                } else {
                                    1
                                }
                            );
                            if boundary == "prepare:closed-before-boundary" {
                                let candidate: SecondaryRemovalPreparation =
                                    serde_json::from_slice(
                                        &std::fs::read(
                                            path.with_extension("preparation-candidate.json"),
                                        )
                                        .unwrap(),
                                    )
                                    .unwrap();
                                assert_eq!(prepared, candidate);
                                assert_eq!(store.load_local_writes().await.unwrap().len(), 1);
                                assert_eq!(application.state.lock().unwrap().operations.len(), 2);
                            }
                            if boundary.starts_with("prepare:") {
                                let durable = store.load_state().await.unwrap();
                                assert_eq!(
                                    coordinator
                                        .ensure_secondary_removal_prepared(
                                            fixture::prepare_command(&intent),
                                            service.sessions().local_session().clone(),
                                            99,
                                        )
                                        .await
                                        .unwrap(),
                                    prepared
                                );
                                assert_eq!(store.load_state().await.unwrap(), durable);
                                let mut conflicting = fixture::prepare_command(&intent);
                                conflicting.intent.spec_generation += 1;
                                assert!(
                                    coordinator
                                        .ensure_secondary_removal_prepared(
                                            conflicting,
                                            service.sessions().local_session().clone(),
                                            100,
                                        )
                                        .await
                                        .is_err()
                                );
                                assert_eq!(store.load_state().await.unwrap(), durable);
                            }
                            coordinator
                                .ensure_configuration(command(&prepared, false))
                                .await
                                .unwrap();
                            coordinator
                                .ensure_configuration(command(&prepared, true))
                                .await
                                .unwrap();
                            let durable = store.load_state().await.unwrap();
                            let duplicate = coordinator
                                .ensure_configuration(command(&prepared, true))
                                .await
                                .unwrap();
                            assert_eq!(Some(duplicate), durable.retained_command);
                            assert_eq!(store.load_state().await.unwrap(), durable);
                            let mut conflicting = command(&prepared, true);
                            conflicting.primary_write_status = AccessStatus::Granted;
                            assert!(coordinator.ensure_configuration(conflicting).await.is_err());
                            assert_eq!(store.load_state().await.unwrap(), durable);
                            if boundary.starts_with("prepare:") {
                                let original = store
                                    .load_local_write(&OperationId::new("interrupted-local-write"))
                                    .await
                                    .unwrap()
                                    .unwrap();
                                assert_eq!(original.lsn, 2);
                                assert_eq!(
                                    original.data.as_ref(),
                                    b"unacknowledged-before-preparation"
                                );
                            }
                            assert_eq!(
                                store.load_state().await.unwrap().admitted_policy,
                                Some(intent.current_policy.clone())
                            );
                            assert_eq!(
                                application.state.lock().unwrap().operations[&1],
                                b"acknowledged-before-crash"
                            );
                            assert!(
                                runtime
                                    .data_plane()
                                    .begin_write(crate::application::ClientWrite {
                                        operation_id: OperationId::new(
                                            "forbidden-before-acceptance"
                                        ),
                                        data: Bytes::new(),
                                    })
                                    .await
                                    .is_err()
                            );
                        }
                        let after = store.load_state().await.unwrap();
                        assert_eq!(after.identity, provenance);
                        assert!(after.pending_effect.is_none());
                        assert!(after.reconfiguration.is_none());
                        assert_ne!(after.write_status, AccessStatus::Granted);
                        if let Some(pending) = before.pending_effect
                            && matches!(
                                pending.effect.action,
                                RuntimeEffectAction::PrepareSecondaryRemoval { .. }
                                    | RuntimeEffectAction::RetireReplica(_)
                            )
                        {
                            assert_eq!(
                                after.removal_effects[&pending.effect.operation_id].effect,
                                pending.effect
                            );
                        }
                        let reporter = crate::host::report::AgentReporter::new(store.clone());
                        let report = reporter.report(&runtime.report_runtime()).await.unwrap();
                        crate::control::normalize_agent_status_report(report).unwrap();
                        if boundary == "current:reply" {
                            let state = store.load_state().await.unwrap();
                            let mut committed = fixture::cleanup(&intent);
                            committed.evidence = state.secondary_removal_evidence.unwrap();
                            committed.current_only_write_quorum[0].verified_replication_lsn = 1;
                            let adapter = RuntimeAdapter::new(store.clone(), runtime.clone());
                            for (offset, action) in [
                                RuntimeEffectAction::AcceptSecondaryRemovalCommit(Box::new(
                                    committed,
                                )),
                                RuntimeEffectAction::SetAccessStatus {
                                    read: AccessStatus::Granted,
                                    write: AccessStatus::Granted,
                                },
                            ]
                            .into_iter()
                            .enumerate()
                            {
                                adapter
                                    .execute(RuntimeEffect {
                                        operation_id: OperationId::new(format!(
                                            "accepted-singleton-{offset}"
                                        )),
                                        sequence: state.next_effect_sequence + offset as u64,
                                        action,
                                    })
                                    .await
                                    .unwrap();
                            }
                            runtime
                                .data_plane()
                                .begin_write(crate::application::ClientWrite {
                                    operation_id: OperationId::new("fresh-singleton-value"),
                                    data: Bytes::from_static(b"fresh-value"),
                                })
                                .await
                                .unwrap()
                                .committed()
                                .await
                                .unwrap();
                            let reporter = crate::host::report::AgentReporter::new(store.clone());
                            crate::control::normalize_agent_status_report(
                                reporter.report(&runtime.report_runtime()).await.unwrap(),
                            )
                            .unwrap();
                            runtime.abort();
                            let reopened = Arc::new(
                                SqliteStore::open_existing(&path, Some(&provenance)).unwrap(),
                            );
                            let app = Arc::new(CrashState::open(crash_application_path(&path)));
                            assert_eq!(app.state.lock().unwrap().operations[&2], b"fresh-value");
                            let host = Arc::new(PodRuntime::new(
                                provenance.local_identity.clone(),
                                app,
                                reopened.clone(),
                            ));
                            let service = crate::host::service::AgentService::new(
                                reopened.clone(),
                                host.clone(),
                                host.clone(),
                                "token",
                            )
                            .unwrap();
                            service.reconstruct_runtime().await.unwrap();
                            assert_eq!(host.snapshot().await.write_status, AccessStatus::Granted);
                            assert_eq!(
                                host.data_plane()
                                    .begin_write(crate::application::ClientWrite {
                                        operation_id: OperationId::new(
                                            "fresh-after-second-restart"
                                        ),
                                        data: Bytes::from_static(b"next"),
                                    })
                                    .await
                                    .unwrap()
                                    .committed()
                                    .await
                                    .unwrap()
                                    .lsn,
                                3
                            );
                            assert_eq!(reopened.identity().await.unwrap(), provenance);
                            assert_eq!(
                                reopened.load_state().await.unwrap().admitted_policy,
                                Some(intent.current_policy)
                            );
                        }
                    }),
                )
                .await
                .unwrap_or_else(|_| panic!("recovery timed out at {boundary}"));
            }));
    }
}
