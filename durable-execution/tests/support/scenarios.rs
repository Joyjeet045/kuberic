use async_trait::async_trait;
use futures::{FutureExt, future::poll_fn, join, task::AtomicWaker};
use kuberic_durable_execution::{
    ActivityName, ActivityObservation, ActivitySequence, ActivitySpec, ActivityState, AttemptId,
    CHECKPOINT_FORMAT_VERSION, CasOutcome, CheckpointEnvelope, CheckpointError, CheckpointLimits,
    CheckpointPayload, CheckpointState, CheckpointStore, DispatchPermit, DurableHost, ExactBytes,
    ExecutionContract, ExecutionId, ExecutionSpec, HostEpoch, HostOutcome, InMemoryCheckpointStore,
    InMemoryFault, LogicalActivityId, Nondeterminism, ObservationRejection, PersistenceBoundary,
    PreparedActivityError, PreparedActivityResolver, ReloadReason, StorageRevision, StoreError,
    StoreErrorKind, StoreOperation, StoredCheckpoint, TerminalCheckpointStatus, TerminalOutcome,
    Workflow, WorkflowContext,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScenarioId {
    RestartBeforeSchedulePersistence,
    RestartAfterSchedulePersistence,
    RestartAfterDispatchExposure,
    LostReplyFollowedByObservation,
    DuplicateSchedulePollAndConflict,
    DuplicateExposurePollAndConflict,
    ChangedActivityOrder,
    ChangedActivityName,
    ChangedExactInput,
    RefreshedAttemptStableLogicalIdentity,
    AmbiguityQuarantine,
    DeterministicCompletedReplay,
    UnsupportedCheckpointFormat,
    ScheduleOutcomeUnknownAfterApply,
    ExposureOutcomeUnknownAfterApply,
    ObservationOutcomeUnknownAfterApply,
    MismatchedObservation,
    CompetingObservations,
    QuarantineResolution,
    QuarantineBlocksAllProgress,
    LoadAbsenceAndProviderFailures,
    OpaqueStorageRevisions,
    OutcomeUnknownApplyStateHidden,
    ChangedResultBound,
    ActivityCountAndGrowingHistory,
    EncodedByteReservation,
    OversizedObservation,
    Base64ExactBytes,
    ActiveToTerminalCompaction,
    TerminalReloadWithoutWorkflowPoll,
    ZeroActivityTerminalization,
    TerminalOutcomeBounds,
    TerminalCapacityAdmission,
    CompletionConflict,
    CompletionOutcomeUnknownAfterApply,
    CompletionOutcomeUnknownWithoutApply,
    CompletionStoreFailures,
    ExecutionContractValidation,
    FusedScheduleExposure,
    FusedScheduleExposureFaults,
    FusedObservationNextExposure,
    FusedObservationTerminal,
    FusedObservationFaults,
    FusedTerminalFaults,
    FusedCapacityReservation,
}

impl ScenarioId {
    pub const ALL: [Self; 45] = [
        Self::RestartBeforeSchedulePersistence,
        Self::RestartAfterSchedulePersistence,
        Self::RestartAfterDispatchExposure,
        Self::LostReplyFollowedByObservation,
        Self::DuplicateSchedulePollAndConflict,
        Self::DuplicateExposurePollAndConflict,
        Self::ChangedActivityOrder,
        Self::ChangedActivityName,
        Self::ChangedExactInput,
        Self::RefreshedAttemptStableLogicalIdentity,
        Self::AmbiguityQuarantine,
        Self::DeterministicCompletedReplay,
        Self::UnsupportedCheckpointFormat,
        Self::ScheduleOutcomeUnknownAfterApply,
        Self::ExposureOutcomeUnknownAfterApply,
        Self::ObservationOutcomeUnknownAfterApply,
        Self::MismatchedObservation,
        Self::CompetingObservations,
        Self::QuarantineResolution,
        Self::QuarantineBlocksAllProgress,
        Self::LoadAbsenceAndProviderFailures,
        Self::OpaqueStorageRevisions,
        Self::OutcomeUnknownApplyStateHidden,
        Self::ChangedResultBound,
        Self::ActivityCountAndGrowingHistory,
        Self::EncodedByteReservation,
        Self::OversizedObservation,
        Self::Base64ExactBytes,
        Self::ActiveToTerminalCompaction,
        Self::TerminalReloadWithoutWorkflowPoll,
        Self::ZeroActivityTerminalization,
        Self::TerminalOutcomeBounds,
        Self::TerminalCapacityAdmission,
        Self::CompletionConflict,
        Self::CompletionOutcomeUnknownAfterApply,
        Self::CompletionOutcomeUnknownWithoutApply,
        Self::CompletionStoreFailures,
        Self::ExecutionContractValidation,
        Self::FusedScheduleExposure,
        Self::FusedScheduleExposureFaults,
        Self::FusedObservationNextExposure,
        Self::FusedObservationTerminal,
        Self::FusedObservationFaults,
        Self::FusedTerminalFaults,
        Self::FusedCapacityReservation,
    ];

    pub const fn stable_id(self) -> &'static str {
        match self {
            Self::RestartBeforeSchedulePersistence => "FR-013-01",
            Self::RestartAfterSchedulePersistence => "FR-013-02",
            Self::RestartAfterDispatchExposure => "FR-013-03",
            Self::LostReplyFollowedByObservation => "FR-013-04",
            Self::DuplicateSchedulePollAndConflict => "FR-013-05",
            Self::DuplicateExposurePollAndConflict => "FR-013-06",
            Self::ChangedActivityOrder => "FR-013-07",
            Self::ChangedActivityName => "FR-013-08",
            Self::ChangedExactInput => "FR-013-09",
            Self::RefreshedAttemptStableLogicalIdentity => "FR-013-10",
            Self::AmbiguityQuarantine => "FR-013-11",
            Self::DeterministicCompletedReplay => "FR-013-12",
            Self::UnsupportedCheckpointFormat => "FR-013-13",
            Self::ScheduleOutcomeUnknownAfterApply => "FR-013-14",
            Self::ExposureOutcomeUnknownAfterApply => "FR-013-15",
            Self::ObservationOutcomeUnknownAfterApply => "FR-013-16",
            Self::MismatchedObservation => "FR-013-17",
            Self::CompetingObservations => "FR-013-18",
            Self::QuarantineResolution => "FR-013-19",
            Self::QuarantineBlocksAllProgress => "FR-013-20",
            Self::LoadAbsenceAndProviderFailures => "FR-013-21",
            Self::OpaqueStorageRevisions => "FR-013-22",
            Self::OutcomeUnknownApplyStateHidden => "FR-013-23",
            Self::ChangedResultBound => "FR-013-24",
            Self::ActivityCountAndGrowingHistory => "FR-013-25",
            Self::EncodedByteReservation => "FR-013-26",
            Self::OversizedObservation => "FR-013-27",
            Self::Base64ExactBytes => "FR-013-28",
            Self::ActiveToTerminalCompaction => "FR-013-29",
            Self::TerminalReloadWithoutWorkflowPoll => "FR-013-30",
            Self::ZeroActivityTerminalization => "FR-013-31",
            Self::TerminalOutcomeBounds => "FR-013-32",
            Self::TerminalCapacityAdmission => "FR-013-33",
            Self::CompletionConflict => "FR-013-34",
            Self::CompletionOutcomeUnknownAfterApply => "FR-013-35",
            Self::CompletionOutcomeUnknownWithoutApply => "FR-013-36",
            Self::CompletionStoreFailures => "FR-013-37",
            Self::ExecutionContractValidation => "FR-013-38",
            Self::FusedScheduleExposure => "FR-013-39",
            Self::FusedScheduleExposureFaults => "FR-013-40",
            Self::FusedObservationNextExposure => "FR-013-41",
            Self::FusedObservationTerminal => "FR-013-42",
            Self::FusedObservationFaults => "FR-013-43",
            Self::FusedTerminalFaults => "FR-013-44",
            Self::FusedCapacityReservation => "FR-013-45",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssertionEvidence {
    pub assertion: &'static str,
    pub passed: bool,
    pub safety_or_determinism: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScenarioEvidence {
    pub id: ScenarioId,
    pub setup: &'static str,
    pub assertions: Vec<AssertionEvidence>,
}

impl ScenarioEvidence {
    pub fn passed(&self) -> bool {
        self.assertions.iter().all(|assertion| assertion.passed)
    }

    fn new(
        id: ScenarioId,
        setup: &'static str,
        assertions: impl IntoIterator<Item = (&'static str, bool)>,
    ) -> Self {
        Self {
            id,
            setup,
            assertions: assertions
                .into_iter()
                .map(|(assertion, passed)| AssertionEvidence {
                    assertion,
                    passed,
                    safety_or_determinism: true,
                })
                .collect(),
        }
    }

    fn mark_conformance_only(mut self, assertion: &'static str) -> Self {
        let evidence = self
            .assertions
            .iter_mut()
            .find(|evidence| evidence.assertion == assertion)
            .expect("conformance-only assertion must exist in the scenario");
        evidence.safety_or_determinism = false;
        self
    }
}

pub async fn run_conformance_matrix() -> Vec<ScenarioEvidence> {
    let mut evidence = Vec::with_capacity(ScenarioId::ALL.len());
    for id in ScenarioId::ALL {
        evidence.push(run_scenario(id).await);
    }
    evidence
}

async fn run_scenario(id: ScenarioId) -> ScenarioEvidence {
    AssertUnwindSafe(run_scenario_inner(id))
        .catch_unwind()
        .await
        .unwrap_or_else(|_| {
            ScenarioEvidence::new(
                id,
                "scenario setup or execution panicked",
                [("scenario completed and emitted structured evidence", false)],
            )
        })
}

async fn run_scenario_inner(id: ScenarioId) -> ScenarioEvidence {
    match id {
        ScenarioId::RestartBeforeSchedulePersistence => restart_before_schedule(id).await,
        ScenarioId::RestartAfterSchedulePersistence => restart_after_schedule(id).await,
        ScenarioId::RestartAfterDispatchExposure => restart_after_exposure(id).await,
        ScenarioId::LostReplyFollowedByObservation => lost_reply_then_observation(id).await,
        ScenarioId::DuplicateSchedulePollAndConflict => schedule_conflict(id).await,
        ScenarioId::DuplicateExposurePollAndConflict => exposure_conflict(id).await,
        ScenarioId::ChangedActivityOrder => changed_order(id).await,
        ScenarioId::ChangedActivityName => changed_name(id).await,
        ScenarioId::ChangedExactInput => changed_input(id).await,
        ScenarioId::RefreshedAttemptStableLogicalIdentity => refreshed_attempt(id).await,
        ScenarioId::AmbiguityQuarantine => quarantine(id).await,
        ScenarioId::DeterministicCompletedReplay => completed_replay(id).await,
        ScenarioId::UnsupportedCheckpointFormat => unsupported_format(id).await,
        ScenarioId::ScheduleOutcomeUnknownAfterApply => {
            schedule_outcome_unknown_after_apply(id).await
        }
        ScenarioId::ExposureOutcomeUnknownAfterApply => {
            exposure_outcome_unknown_after_apply(id).await
        }
        ScenarioId::ObservationOutcomeUnknownAfterApply => {
            observation_outcome_unknown_after_apply(id).await
        }
        ScenarioId::MismatchedObservation => mismatched_observation(id).await,
        ScenarioId::CompetingObservations => competing_observations(id).await,
        ScenarioId::QuarantineResolution => quarantine_resolution(id).await,
        ScenarioId::QuarantineBlocksAllProgress => quarantine_blocks_progress(id).await,
        ScenarioId::LoadAbsenceAndProviderFailures => load_absence_and_provider_failures(id).await,
        ScenarioId::OpaqueStorageRevisions => opaque_storage_revisions(id).await,
        ScenarioId::OutcomeUnknownApplyStateHidden => outcome_unknown_apply_state_hidden(id).await,
        ScenarioId::ChangedResultBound => changed_result_bound(id).await,
        ScenarioId::ActivityCountAndGrowingHistory => activity_count_and_growing_history(id).await,
        ScenarioId::EncodedByteReservation => encoded_byte_reservation(id).await,
        ScenarioId::OversizedObservation => oversized_observation(id).await,
        ScenarioId::Base64ExactBytes => base64_exact_bytes(id).await,
        ScenarioId::ActiveToTerminalCompaction => active_to_terminal_compaction(id).await,
        ScenarioId::TerminalReloadWithoutWorkflowPoll => terminal_reload_without_poll(id).await,
        ScenarioId::ZeroActivityTerminalization => zero_activity_terminalization(id).await,
        ScenarioId::TerminalOutcomeBounds => terminal_outcome_bounds(id).await,
        ScenarioId::TerminalCapacityAdmission => terminal_capacity_admission(id).await,
        ScenarioId::CompletionConflict => completion_conflict(id).await,
        ScenarioId::CompletionOutcomeUnknownAfterApply => {
            completion_outcome_unknown_after_apply(id).await
        }
        ScenarioId::CompletionOutcomeUnknownWithoutApply => {
            completion_outcome_unknown_without_apply(id).await
        }
        ScenarioId::CompletionStoreFailures => completion_store_failures(id).await,
        ScenarioId::ExecutionContractValidation => execution_contract_validation(id).await,
        ScenarioId::FusedScheduleExposure => fused_schedule_exposure(id).await,
        ScenarioId::FusedScheduleExposureFaults => fused_schedule_exposure_faults(id).await,
        ScenarioId::FusedObservationNextExposure => fused_observation_next_exposure(id).await,
        ScenarioId::FusedObservationTerminal => fused_observation_terminal(id).await,
        ScenarioId::FusedObservationFaults => fused_observation_faults(id).await,
        ScenarioId::FusedTerminalFaults => fused_terminal_faults(id).await,
        ScenarioId::FusedCapacityReservation => fused_capacity_reservation(id).await,
    }
}

#[derive(Default)]
struct AsyncBarrier {
    arrivals: AtomicUsize,
    waiter: AtomicWaker,
}

impl AsyncBarrier {
    async fn wait(&self) {
        let mut arrived = false;
        poll_fn(|context| {
            if !arrived {
                arrived = true;
                if self.arrivals.fetch_add(1, Ordering::AcqRel) + 1 == 2 {
                    self.waiter.wake();
                    return Poll::Ready(());
                }
            }
            if self.arrivals.load(Ordering::Acquire) >= 2 {
                Poll::Ready(())
            } else {
                self.waiter.register(context.waker());
                if self.arrivals.load(Ordering::Acquire) >= 2 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
        })
        .await;
    }
}

#[derive(Clone)]
struct ContendedStore {
    inner: InMemoryCheckpointStore,
    compare_barrier: Arc<AsyncBarrier>,
}

impl ContendedStore {
    fn pair(inner: InMemoryCheckpointStore) -> (Self, Self) {
        let compare_barrier = Arc::new(AsyncBarrier::default());
        let first = Self {
            inner: inner.clone(),
            compare_barrier: compare_barrier.clone(),
        };
        let second = Self {
            inner,
            compare_barrier,
        };
        (first, second)
    }
}

#[async_trait]
impl CheckpointStore for ContendedStore {
    async fn load(
        &self,
        execution_id: ExecutionId,
    ) -> Result<Option<StoredCheckpoint>, StoreError> {
        self.inner.load(execution_id).await
    }

    async fn compare_and_swap(
        &self,
        execution_id: ExecutionId,
        expected: Option<StorageRevision>,
        checkpoint: CheckpointEnvelope,
    ) -> Result<CasOutcome, StoreError> {
        self.compare_barrier.wait().await;
        self.inner
            .compare_and_swap(execution_id, expected, checkpoint)
            .await
    }
}

async fn contending_turns(
    store: InMemoryCheckpointStore,
    workflow: LinearWorkflow,
    execution_id: ExecutionId,
    input: ExactBytes,
) -> [HostOutcome; 2] {
    let (first_store, second_store) = ContendedStore::pair(store);
    let mut first_host = DurableHost::new(first_store, epoch(201), generous_limits());
    let mut second_host = DurableHost::new(second_store, epoch(202), generous_limits());
    let first_workflow = workflow.clone();
    let first_input = input.clone();
    let (first, second) = join!(
        first_host.turn(&first_workflow, execution_spec(execution_id, first_input)),
        second_host.turn(&workflow, execution_spec(execution_id, input))
    );
    [first, second]
}

#[derive(Clone)]
pub struct LinearWorkflow {
    activities: Vec<ActivitySpec>,
}

struct PreparedPrefixResolver;

impl PreparedActivityResolver for PreparedPrefixResolver {
    fn resolve(
        &self,
        logical: &ActivitySpec,
        _recorded: Option<&ActivitySpec>,
    ) -> Result<ActivitySpec, PreparedActivityError> {
        let mut input = b"prepared:".to_vec();
        input.extend_from_slice(logical.input().as_slice());
        Ok(ActivitySpec::new(
            logical.name().clone(),
            ExactBytes::new(input),
            logical.max_result_bytes(),
        ))
    }
}

impl LinearWorkflow {
    pub fn one(name: &str, version: u32, input: &[u8]) -> Self {
        Self::one_with_bound(name, version, input, 1024)
    }

    fn one_with_bound(name: &str, version: u32, input: &[u8], max_result_bytes: u64) -> Self {
        Self {
            activities: vec![activity_spec(name, version, input, max_result_bytes)],
        }
    }

    fn two() -> Self {
        Self {
            activities: vec![
                activity_spec("first", 1, b"A", 1024),
                activity_spec("second", 1, b"B", 1024),
            ],
        }
    }
}

#[async_trait]
impl Workflow for LinearWorkflow {
    async fn run(&self, context: &mut WorkflowContext<'_>, _input: ExactBytes) -> TerminalOutcome {
        let mut result = Vec::new();
        for spec in &self.activities {
            result.extend(context.activity(spec.clone()).await.as_slice());
        }
        TerminalOutcome::succeeded(ExactBytes::new(result))
    }
}

#[derive(Clone)]
struct TerminalWorkflow {
    outcome: TerminalOutcome,
}

#[async_trait]
impl Workflow for TerminalWorkflow {
    async fn run(&self, _context: &mut WorkflowContext<'_>, _input: ExactBytes) -> TerminalOutcome {
        self.outcome.clone()
    }
}

struct PollSentinelWorkflow<'a> {
    polls: &'a Cell,
}

#[async_trait]
impl Workflow for PollSentinelWorkflow<'_> {
    async fn run(&self, _context: &mut WorkflowContext<'_>, _input: ExactBytes) -> TerminalOutcome {
        self.polls.set(self.polls.get() + 1);
        TerminalOutcome::failed(bytes(b"workflow was polled"))
    }
}

fn execution(value: u8) -> ExecutionId {
    ExecutionId::from_bytes([value; 16])
}

fn epoch(value: u8) -> HostEpoch {
    HostEpoch::from_bytes([value; 16])
}

fn bytes(value: &[u8]) -> ExactBytes {
    ExactBytes::new(value)
}

const MAX_TERMINAL_PAYLOAD_BYTES: u64 = 4096;

fn execution_spec(execution_id: ExecutionId, input: ExactBytes) -> ExecutionSpec {
    ExecutionSpec::new(execution_id, input, MAX_TERMINAL_PAYLOAD_BYTES)
}

fn activity_name(value: &str, version: u32) -> ActivityName {
    ActivityName::new(value, version).unwrap()
}

fn activity_spec(name: &str, version: u32, input: &[u8], max_result_bytes: u64) -> ActivitySpec {
    ActivitySpec::new(activity_name(name, version), bytes(input), max_result_bytes)
}

fn generous_limits() -> CheckpointLimits {
    CheckpointLimits::new(128, 1_000_000, 1_000_000).unwrap()
}

fn host(store: InMemoryCheckpointStore, epoch_value: u8) -> DurableHost<InMemoryCheckpointStore> {
    DurableHost::new(store, epoch(epoch_value), generous_limits())
}

async fn schedule(
    host: &mut DurableHost<InMemoryCheckpointStore>,
    workflow: &LinearWorkflow,
    execution_id: ExecutionId,
    input: &ExactBytes,
) -> Option<LogicalActivityId> {
    match host
        .turn(workflow, execution_spec(execution_id, input.clone()))
        .await
    {
        HostOutcome::ScheduleAccepted { activity, .. } => Some(activity),
        _ => None,
    }
}

async fn expose(
    host: &mut DurableHost<InMemoryCheckpointStore>,
    workflow: &LinearWorkflow,
    execution_id: ExecutionId,
    input: &ExactBytes,
) -> Option<DispatchPermit> {
    match host
        .turn(workflow, execution_spec(execution_id, input.clone()))
        .await
    {
        HostOutcome::DispatchPermitted { permit, .. } => Some(permit),
        _ => None,
    }
}

async fn prepared_one(
    value: u8,
) -> (
    InMemoryCheckpointStore,
    DurableHost<InMemoryCheckpointStore>,
    LinearWorkflow,
    ExecutionId,
    ExactBytes,
    LogicalActivityId,
    DispatchPermit,
) {
    let store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::one("effect", 1, b"activity");
    let execution_id = execution(value);
    let input = bytes(b"workflow");
    let mut durable_host = host(store.clone(), value);
    let logical = schedule(&mut durable_host, &workflow, execution_id, &input)
        .await
        .unwrap();
    let permit = expose(&mut durable_host, &workflow, execution_id, &input)
        .await
        .unwrap();
    (
        store,
        durable_host,
        workflow,
        execution_id,
        input,
        logical,
        permit,
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SyntheticEffectCall {
    activity: LogicalActivityId,
    attempt_id: AttemptId,
}

#[derive(Default)]
struct SyntheticEffect {
    calls: Vec<SyntheticEffectCall>,
}

impl SyntheticEffect {
    fn invoke(&mut self, permit: DispatchPermit) -> ExactBytes {
        self.calls.push(SyntheticEffectCall {
            activity: permit.activity().clone(),
            attempt_id: permit.attempt_id(),
        });
        bytes(b"effect-result")
    }
}

async fn restart_before_schedule(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::one("effect", 1, b"A");
    let execution_id = execution(1);
    let input = bytes(b"workflow");
    let mut first = host(store.clone(), 1);
    store.fail_next_compare_and_swap(InMemoryFault::FailBeforeRequest(
        StoreErrorKind::Unavailable,
    ));
    let rejected = first
        .turn(&workflow, execution_spec(execution_id, input.clone()))
        .await;
    let absent = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail")
        .is_none();
    let mut restarted = host(store.clone(), 2);
    let accepted = restarted
        .turn(&workflow, execution_spec(execution_id, input))
        .await;
    ScenarioEvidence::new(
        id,
        "fail schedule before request, discard host, and replay",
        [
            (
                "rejected schedule requires reload and grants no permit",
                matches!(
                    rejected,
                    HostOutcome::StoreFailed {
                        operation: kuberic_durable_execution::StoreOperation::CompareAndSwap(
                            PersistenceBoundary::Schedule
                        ),
                        ..
                    }
                ),
            ),
            ("no checkpoint was persisted", absent),
            (
                "restarted host accepts the same schedule",
                matches!(accepted, HostOutcome::ScheduleAccepted { .. }),
            ),
        ],
    )
}

async fn restart_after_schedule(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::one("effect", 1, b"A");
    let execution_id = execution(2);
    let input = bytes(b"workflow");
    let mut first = host(store.clone(), 1);
    let (logical, schedule_revision) = match first
        .turn(&workflow, execution_spec(execution_id, input.clone()))
        .await
    {
        HostOutcome::ScheduleAccepted { activity, revision } => (Some(activity), Some(revision)),
        _ => (None, None),
    };
    let mut restarted = host(store, 2);
    let (permit, exposure_revision) = match restarted
        .turn(&workflow, execution_spec(execution_id, input.clone()))
        .await
    {
        HostOutcome::DispatchPermitted {
            permit, revision, ..
        } => (Some(permit), Some(revision)),
        _ => (None, None),
    };
    ScenarioEvidence::new(
        id,
        "persist schedule, discard host, and prepare exposure in a new epoch",
        [
            ("schedule was accepted", logical.is_some()),
            (
                "restart grants a permit only after exposure acceptance",
                permit.is_some(),
            ),
            (
                "schedule and exposure are separate accepted revisions",
                schedule_revision
                    .zip(exposure_revision)
                    .is_some_and(|(schedule, exposure)| schedule != exposure),
            ),
            (
                "logical identity survives restart",
                permit
                    .as_ref()
                    .zip(logical.as_ref())
                    .is_some_and(|(permit, logical)| permit.activity() == logical),
            ),
        ],
    )
}

async fn restart_after_exposure(id: ScenarioId) -> ScenarioEvidence {
    let (store, _, workflow, execution_id, input, logical, permit) = prepared_one(3).await;
    let before = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail");
    let mut restarted = host(store.clone(), 9);
    let outcome = restarted
        .turn(&workflow, execution_spec(execution_id, input))
        .await;
    let after = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail");
    ScenarioEvidence::new(
        id,
        "persist dispatch exposure, discard host, and replay unresolved work",
        [
            (
                "restart enters quarantine with the same logical activity",
                matches!(
                    &outcome,
                    HostOutcome::Quarantined { activity, .. } if activity == &logical
                ),
            ),
            (
                "quarantine retains the persisted winning attempt",
                matches!(
                    outcome,
                    HostOutcome::Quarantined { attempt_id, .. }
                        if attempt_id == permit.attempt_id()
                ),
            ),
            ("quarantine performs no persistence", before == after),
        ],
    )
}

async fn lost_reply_then_observation(id: ScenarioId) -> ScenarioEvidence {
    let (store, _, workflow, execution_id, input, logical, permit) = prepared_one(4).await;
    let expected_attempt = permit.attempt_id();
    let mut effect = SyntheticEffect::default();
    let lost_effect_reply = effect.invoke(permit);
    drop(lost_effect_reply);
    let mut restarted = host(store, 40);
    let quarantined = restarted
        .turn(&workflow, execution_spec(execution_id, input.clone()))
        .await;
    let observed = restarted
        .observe(
            &execution_spec(execution_id, input.clone()),
            ActivityObservation::new(logical.clone(), bytes(b"effect-result")),
        )
        .await;
    let completed = restarted
        .turn(&workflow, execution_spec(execution_id, input))
        .await;
    ScenarioEvidence::new(
        id,
        "invoke one permitted external effect, discard its reply, restart into quarantine, then inject an authoritative observation",
        [
            (
                "exactly one permitted external effect was invoked",
                effect.calls
                    == [SyntheticEffectCall {
                        activity: logical.clone(),
                        attempt_id: expected_attempt,
                    }],
            ),
            (
                "discarded effect reply leaves the same logical attempt quarantined",
                matches!(
                    quarantined,
                    HostOutcome::Quarantined {
                        activity,
                        attempt_id,
                        ..
                    } if activity == logical && attempt_id == expected_attempt
                ),
            ),
            (
                "authoritative observation is accepted",
                matches!(observed, HostOutcome::ObservationAccepted { .. }),
            ),
            (
                "observed result replays to workflow completion",
                matches!(
                    completed,
                    HostOutcome::WorkflowCompleted {
                        outcome: TerminalOutcome::Succeeded(result),
                        ..
                    } if result == bytes(b"effect-result")
                ),
            ),
        ],
    )
}

async fn schedule_conflict(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::one("effect", 1, b"A");
    let execution_id = execution(5);
    let input = bytes(b"workflow");
    let outcomes =
        contending_turns(store.clone(), workflow.clone(), execution_id, input.clone()).await;
    let accepted = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, HostOutcome::ScheduleAccepted { .. }))
        .count();
    let conflicts = outcomes
        .iter()
        .filter(|outcome| {
            matches!(
                outcome,
                HostOutcome::ReloadRequired {
                    boundary: PersistenceBoundary::Schedule,
                    reason: ReloadReason::Conflict
                }
            )
        })
        .count();
    let permits = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, HostOutcome::DispatchPermitted { .. }))
        .count();
    let mut after_race = host(store, 5);
    let exposed = after_race
        .turn(&workflow, execution_spec(execution_id, input))
        .await;
    ScenarioEvidence::new(
        id,
        "race two hosts loaded at the missing-checkpoint schedule revision",
        [
            (
                "atomic CAS accepts exactly one schedule contender",
                accepted == 1 && conflicts == 1,
            ),
            (
                "schedule contenders receive no dispatch permit",
                permits == 0,
            ),
            (
                "dispatch is possible only on the later exposure turn",
                matches!(exposed, HostOutcome::DispatchPermitted { .. }),
            ),
        ],
    )
}

async fn exposure_conflict(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::one("effect", 1, b"A");
    let execution_id = execution(6);
    let input = bytes(b"workflow");
    let mut scheduling_host = host(store.clone(), 6);
    let logical = schedule(&mut scheduling_host, &workflow, execution_id, &input)
        .await
        .unwrap();
    let outcomes =
        contending_turns(store.clone(), workflow.clone(), execution_id, input.clone()).await;
    let accepted_permits: Vec<_> = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            HostOutcome::DispatchPermitted { permit, .. } => Some(permit),
            _ => None,
        })
        .collect();
    let conflicts = outcomes
        .iter()
        .filter(|outcome| {
            matches!(
                outcome,
                HostOutcome::ReloadRequired {
                    boundary: PersistenceBoundary::Exposure,
                    reason: ReloadReason::Conflict
                }
            )
        })
        .count();
    let mut after_race = host(store, 6);
    let duplicate = after_race
        .turn(&workflow, execution_spec(execution_id, input))
        .await;
    ScenarioEvidence::new(
        id,
        "race two hosts loaded at the same accepted schedule revision",
        [
            (
                "atomic CAS accepts exactly one exposure contender",
                accepted_permits.len() == 1 && conflicts == 1,
            ),
            (
                "the sole permit retains the scheduled logical identity",
                accepted_permits
                    .first()
                    .is_some_and(|permit| permit.activity() == &logical),
            ),
            (
                "a stale duplicate poll quarantines instead of redispatching",
                matches!(duplicate, HostOutcome::Quarantined { .. }),
            ),
        ],
    )
}

async fn changed_order(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let original = LinearWorkflow::two();
    let execution_id = execution(7);
    let input = bytes(b"workflow");
    let mut durable_host = host(store, 7);
    let logical = schedule(&mut durable_host, &original, execution_id, &input)
        .await
        .unwrap();
    expose(&mut durable_host, &original, execution_id, &input)
        .await
        .unwrap();
    durable_host
        .observe(
            &execution_spec(execution_id, input.clone()),
            ActivityObservation::new(logical, bytes(b"done")),
        )
        .await;
    let reordered = LinearWorkflow {
        activities: vec![
            activity_spec("second", 1, b"B", 1024),
            activity_spec("first", 1, b"A", 1024),
        ],
    };
    let outcome = durable_host
        .turn(&reordered, execution_spec(execution_id, input))
        .await;
    ScenarioEvidence::new(
        id,
        "complete the first recorded activity and replay a reordered definition",
        [(
            "changed order is nondeterminism before any new dispatch",
            matches!(
                outcome,
                HostOutcome::Nondeterminism(Nondeterminism::ActivityMismatch { .. })
            ),
        )],
    )
}

async fn changed_name(id: ScenarioId) -> ScenarioEvidence {
    changed_definition(
        id,
        "replay a completed activity with a changed versioned name",
        LinearWorkflow::one("effect", 2, b"A"),
    )
    .await
}

async fn changed_input(id: ScenarioId) -> ScenarioEvidence {
    changed_definition(
        id,
        "replay a completed activity with changed exact bytes",
        LinearWorkflow::one("effect", 1, b"a"),
    )
    .await
}

async fn changed_definition(
    id: ScenarioId,
    setup: &'static str,
    changed: LinearWorkflow,
) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let original = LinearWorkflow::one("effect", 1, b"A");
    let execution_id = execution(if id == ScenarioId::ChangedActivityName {
        8
    } else {
        9
    });
    let input = bytes(b"workflow");
    let mut durable_host = host(store, 8);
    let logical = schedule(&mut durable_host, &original, execution_id, &input)
        .await
        .unwrap();
    expose(&mut durable_host, &original, execution_id, &input)
        .await
        .unwrap();
    durable_host
        .observe(
            &execution_spec(execution_id, input.clone()),
            ActivityObservation::new(logical, bytes(b"done")),
        )
        .await;
    let outcome = durable_host
        .turn(&changed, execution_spec(execution_id, input))
        .await;
    ScenarioEvidence::new(
        id,
        setup,
        [(
            "definition mismatch is nondeterminism before dispatch",
            matches!(
                outcome,
                HostOutcome::Nondeterminism(Nondeterminism::ActivityMismatch { .. })
            ),
        )],
    )
}

async fn refreshed_attempt(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::one("effect", 1, b"A");
    let execution_id = execution(10);
    let input = bytes(b"workflow");
    let mut durable_host = host(store.clone(), 10);
    let logical = schedule(&mut durable_host, &workflow, execution_id, &input)
        .await
        .unwrap();
    store.fail_next_compare_and_swap(InMemoryFault::FailBeforeRequest(
        StoreErrorKind::Unavailable,
    ));
    let rejected = durable_host
        .turn(&workflow, execution_spec(execution_id, input.clone()))
        .await;
    let accepted = durable_host
        .turn(&workflow, execution_spec(execution_id, input))
        .await;
    ScenarioEvidence::new(
        id,
        "discard a pre-exposure attempt after rejection and prepare a fresh attempt",
        [
            (
                "discarded attempt confers no permit",
                matches!(
                    rejected,
                    HostOutcome::StoreFailed {
                        operation: kuberic_durable_execution::StoreOperation::CompareAndSwap(
                            PersistenceBoundary::Exposure
                        ),
                        ..
                    }
                ),
            ),
            (
                "accepted attempt was refreshed",
                matches!(
                    &accepted,
                    HostOutcome::DispatchPermitted { permit, .. }
                        if permit.attempt_id().counter() == 2
                ),
            ),
            (
                "logical identity is unchanged across attempts",
                matches!(
                    accepted,
                    HostOutcome::DispatchPermitted { permit, .. }
                        if permit.activity() == &logical
                ),
            ),
        ],
    )
}

async fn quarantine(id: ScenarioId) -> ScenarioEvidence {
    let (store, mut durable_host, workflow, execution_id, input, logical, _) =
        prepared_one(11).await;
    let changed = LinearWorkflow::one("changed-effect", 1, b"A");
    let before = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail");
    let first = durable_host
        .turn(&workflow, execution_spec(execution_id, input.clone()))
        .await;
    let changed_while_unresolved = durable_host
        .turn(&changed, execution_spec(execution_id, input.clone()))
        .await;
    let second = durable_host
        .turn(&workflow, execution_spec(execution_id, input.clone()))
        .await;
    let after = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail");
    let observed = durable_host
        .observe(
            &execution_spec(execution_id, input.clone()),
            ActivityObservation::new(logical.clone(), bytes(b"resolved")),
        )
        .await;
    let changed_after_resolution = durable_host
        .turn(&changed, execution_spec(execution_id, input))
        .await;
    ScenarioEvidence::new(
        id,
        "poll exposed unresolved work repeatedly and with a changed workflow definition",
        [
            (
                "first replay is quarantined",
                matches!(&first, HostOutcome::Quarantined { activity, .. } if activity == &logical),
            ),
            (
                "quarantine takes precedence over changed-definition matching",
                matches!(
                    &changed_while_unresolved,
                    HostOutcome::Quarantined { activity, .. } if activity == &logical
                ),
            ),
            (
                "duplicate replay remains quarantined without a permit",
                matches!(second, HostOutcome::Quarantined { .. }),
            ),
            ("quarantine does not mutate the checkpoint", before == after),
            (
                "after observation, the changed definition reports nondeterminism",
                matches!(observed, HostOutcome::ObservationAccepted { .. })
                    && matches!(
                        changed_after_resolution,
                        HostOutcome::Nondeterminism(Nondeterminism::ActivityMismatch { .. })
                    ),
            ),
        ],
    )
}

async fn completed_replay(id: ScenarioId) -> ScenarioEvidence {
    let (_, mut durable_host, workflow, execution_id, input, logical, _) = prepared_one(12).await;
    durable_host
        .observe(
            &execution_spec(execution_id, input.clone()),
            ActivityObservation::new(logical, bytes(b"recorded")),
        )
        .await;
    let first = durable_host
        .turn(&workflow, execution_spec(execution_id, input.clone()))
        .await;
    let second = durable_host
        .turn(&workflow, execution_spec(execution_id, input))
        .await;
    ScenarioEvidence::new(
        id,
        "replay the same completed checkpoint twice",
        [
            (
                "first replay returns the recorded result",
                matches!(
                    &first,
                    HostOutcome::WorkflowCompleted {
                        outcome: TerminalOutcome::Succeeded(result),
                        ..
                    } if result == &bytes(b"recorded")
                ),
            ),
            (
                "accepted completion and authoritative reload are semantically identical",
                matches!(
                    (&first, &second),
                    (
                        HostOutcome::WorkflowCompleted {
                            outcome: first_outcome,
                            completed_activity_count: first_count,
                            revision: first_revision,
                            boundary: first_boundary,
                            checkpoint_status: TerminalCheckpointStatus::Accepted,
                            ..
                        },
                        HostOutcome::WorkflowCompleted {
                            outcome: second_outcome,
                            completed_activity_count: second_count,
                            revision: second_revision,
                            boundary: second_boundary,
                            checkpoint_status: TerminalCheckpointStatus::Reloaded,
                            ..
                        },
                    ) if first_outcome == second_outcome
                        && first_count == second_count
                        && first_revision == second_revision
                        && first_boundary == second_boundary
                ),
            ),
            (
                "neither completed replay grants dispatch",
                !matches!(first, HostOutcome::DispatchPermitted { .. })
                    && !matches!(second, HostOutcome::DispatchPermitted { .. }),
            ),
        ],
    )
}

async fn unsupported_format(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let execution_id = execution(13);
    let input = bytes(b"workflow");
    let valid = CheckpointEnvelope::encode(&CheckpointPayload::active(
        ExecutionContract::new(
            execution_spec(execution_id, input.clone()),
            generous_limits().max_active_encoded_bytes() as u64,
        ),
        Vec::new(),
    ))
    .unwrap();
    let unsupported = CheckpointEnvelope::new(2, valid.payload().clone());
    let stored = store
        .compare_and_swap(execution_id, None, unsupported)
        .await;
    let mut durable_host = host(store, 13);
    let polls = Cell::new(0);
    let outcome = durable_host
        .turn(
            &PollSentinelWorkflow { polls: &polls },
            execution_spec(execution_id, input),
        )
        .await;
    ScenarioEvidence::new(
        id,
        "load a checkpoint with an unsupported envelope version",
        [
            (
                "test checkpoint was accepted by opaque storage",
                matches!(stored, Ok(CasOutcome::Accepted(_))),
            ),
            (
                "host rejects format before workflow or dispatch",
                matches!(
                    outcome,
                    HostOutcome::CheckpointRejected(CheckpointError::UnsupportedFormat {
                        actual: 2,
                        supported: CHECKPOINT_FORMAT_VERSION
                    })
                ) && polls.get() == 0,
            ),
        ],
    )
}

async fn schedule_outcome_unknown_after_apply(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::one("effect", 1, b"A");
    let execution_id = execution(14);
    let input = bytes(b"workflow");
    let mut durable_host = host(store.clone(), 14);
    store.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownAfterApply);
    let lost = durable_host
        .turn(&workflow, execution_spec(execution_id, input.clone()))
        .await;
    let loaded = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail");
    let next = durable_host
        .turn(&workflow, execution_spec(execution_id, input))
        .await;
    ScenarioEvidence::new(
        id,
        "lose the response after applying the schedule CAS",
        [
            (
                "uncertain response requires schedule reload with no permit",
                matches!(
                    lost,
                    HostOutcome::ReloadRequired {
                        boundary: PersistenceBoundary::Schedule,
                        reason: ReloadReason::OutcomeUnknown
                    }
                ),
            ),
            ("schedule was nevertheless persisted", loaded.is_some()),
            (
                "later exposure turn may receive permission",
                matches!(next, HostOutcome::DispatchPermitted { .. }),
            ),
        ],
    )
}

async fn exposure_outcome_unknown_after_apply(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::one("effect", 1, b"A");
    let execution_id = execution(15);
    let input = bytes(b"workflow");
    let mut durable_host = host(store, 15);
    schedule(&mut durable_host, &workflow, execution_id, &input)
        .await
        .unwrap();
    durable_host
        .store()
        .fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownAfterApply);
    let lost = durable_host
        .turn(&workflow, execution_spec(execution_id, input.clone()))
        .await;
    let next = durable_host
        .turn(&workflow, execution_spec(execution_id, input))
        .await;
    ScenarioEvidence::new(
        id,
        "lose the response after applying dispatch exposure",
        [
            (
                "uncertain exposure returns no permit",
                matches!(
                    lost,
                    HostOutcome::ReloadRequired {
                        boundary: PersistenceBoundary::Exposure,
                        reason: ReloadReason::OutcomeUnknown
                    }
                ),
            ),
            (
                "reload immediately quarantines unresolved work",
                matches!(next, HostOutcome::Quarantined { .. }),
            ),
        ],
    )
}

async fn observation_outcome_unknown_after_apply(id: ScenarioId) -> ScenarioEvidence {
    let (store, mut durable_host, workflow, execution_id, input, logical, _) =
        prepared_one(16).await;
    store.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownAfterApply);
    let lost = durable_host
        .observe(
            &execution_spec(execution_id, input.clone()),
            ActivityObservation::new(logical, bytes(b"observed")),
        )
        .await;
    let next = durable_host
        .turn(&workflow, execution_spec(execution_id, input))
        .await;
    ScenarioEvidence::new(
        id,
        "lose the response after applying an authoritative observation",
        [
            (
                "uncertain observation requires reload",
                matches!(
                    lost,
                    HostOutcome::ReloadRequired {
                        boundary: PersistenceBoundary::Observation,
                        reason: ReloadReason::OutcomeUnknown
                    }
                ),
            ),
            (
                "reload replays the applied observed result",
                matches!(
                    next,
                    HostOutcome::WorkflowCompleted {
                        outcome: TerminalOutcome::Succeeded(result),
                        ..
                    } if result == bytes(b"observed")
                ),
            ),
        ],
    )
}

async fn mismatched_observation(id: ScenarioId) -> ScenarioEvidence {
    let (_, mut durable_host, workflow, execution_id, input, logical, _) = prepared_one(17).await;
    let mismatched = LogicalActivityId::new(
        execution_id,
        ActivitySequence::new(0),
        activity_spec("effect", 1, b"different", 1024),
    );
    let rejected = durable_host
        .observe(
            &execution_spec(execution_id, input.clone()),
            ActivityObservation::new(mismatched, bytes(b"result")),
        )
        .await;
    let next = durable_host
        .turn(&workflow, execution_spec(execution_id, input))
        .await;
    ScenarioEvidence::new(
        id,
        "inject a result for a different exact logical activity",
        [
            (
                "mismatched observation is rejected",
                matches!(
                    rejected,
                    HostOutcome::ObservationRejected(
                        ObservationRejection::LogicalActivityMismatch { expected, .. }
                    ) if expected == logical
                ),
            ),
            (
                "rejected observation leaves the activity quarantined",
                matches!(next, HostOutcome::Quarantined { .. }),
            ),
        ],
    )
}

async fn competing_observations(id: ScenarioId) -> ScenarioEvidence {
    let (store, _, workflow, execution_id, input, logical, _) = prepared_one(18).await;
    let (first_store, second_store) = ContendedStore::pair(store.clone());
    let logical_for_first = logical.clone();
    let input_for_first = input.clone();
    let replay_input = input.clone();
    let first_host = DurableHost::new(first_store, epoch(203), generous_limits());
    let second_host = DurableHost::new(second_store, epoch(204), generous_limits());
    let first_result = bytes(b"first");
    let second_result = bytes(b"second");
    let first_execution = execution_spec(execution_id, input_for_first);
    let second_execution = execution_spec(execution_id, input.clone());
    let (first_outcome, second_outcome) = join!(
        first_host.observe(
            &first_execution,
            ActivityObservation::new(logical_for_first, first_result.clone()),
        ),
        second_host.observe(
            &second_execution,
            ActivityObservation::new(logical, second_result.clone()),
        )
    );
    let observations = [
        (first_result, first_outcome),
        (second_result, second_outcome),
    ];
    let accepted_results: Vec<_> = observations
        .iter()
        .filter_map(|(result, outcome)| {
            matches!(outcome, HostOutcome::ObservationAccepted { .. }).then_some(result)
        })
        .collect();
    let conflicts = observations
        .iter()
        .filter(|(_, outcome)| {
            matches!(
                outcome,
                HostOutcome::ReloadRequired {
                    boundary: PersistenceBoundary::Observation,
                    reason: ReloadReason::Conflict
                }
            )
        })
        .count();
    let mut durable_host = host(store, 18);
    let completed = durable_host
        .turn(&workflow, execution_spec(execution_id, replay_input))
        .await;
    ScenarioEvidence::new(
        id,
        "race two observations loaded from the same exposed revision",
        [
            (
                "atomic CAS accepts exactly one competing observation",
                accepted_results.len() == 1 && conflicts == 1,
            ),
            (
                "the accepted observation remains authoritative",
                matches!(
                    completed,
                    HostOutcome::WorkflowCompleted {
                        outcome: TerminalOutcome::Succeeded(result),
                        ..
                    } if accepted_results.first().is_some_and(|accepted| &result == *accepted)
                ),
            ),
        ],
    )
}

async fn quarantine_resolution(id: ScenarioId) -> ScenarioEvidence {
    let (store, mut durable_host, workflow, execution_id, input, logical, permit) =
        prepared_one(19).await;
    let quarantined = durable_host
        .turn(&workflow, execution_spec(execution_id, input.clone()))
        .await;
    let accepted = durable_host
        .observe(
            &execution_spec(execution_id, input.clone()),
            ActivityObservation::new(logical.clone(), bytes(b"resolved")),
        )
        .await;
    let before_stale = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail");
    let stale = durable_host
        .observe(
            &execution_spec(execution_id, input.clone()),
            ActivityObservation::new(logical.clone(), bytes(b"stale")),
        )
        .await;
    let stale_left_checkpoint_unchanged = before_stale
        == store
            .load(execution_id)
            .await
            .expect("scenario load must not fail");
    let completed = durable_host
        .turn(&workflow, execution_spec(execution_id, input))
        .await;
    ScenarioEvidence::new(
        id,
        "resolve quarantined work only through the public observation API",
        [
            (
                "quarantine retains logical and attempt identity",
                matches!(
                    quarantined,
                    HostOutcome::Quarantined {
                        activity,
                        attempt_id,
                        ..
                    } if activity == logical && attempt_id == permit.attempt_id()
                ),
            ),
            (
                "public observation resolves the same logical activity",
                matches!(
                    accepted,
                    HostOutcome::ObservationAccepted { activity, .. } if activity == logical
                ),
            ),
            (
                "a stale observation is rejected without changing the completed record",
                matches!(
                    stale,
                    HostOutcome::ObservationRejected(ObservationRejection::ActivityNotExposed)
                ) && stale_left_checkpoint_unchanged,
            ),
            (
                "resolution replays without redispatch",
                matches!(completed, HostOutcome::WorkflowCompleted { .. }),
            ),
        ],
    )
}

async fn quarantine_blocks_progress(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::two();
    let execution_id = execution(20);
    let input = bytes(b"workflow");
    let mut durable_host = host(store.clone(), 20);
    let first_logical = schedule(&mut durable_host, &workflow, execution_id, &input)
        .await
        .unwrap();
    expose(&mut durable_host, &workflow, execution_id, &input)
        .await
        .unwrap();
    let before = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail");
    let blocked_one = durable_host
        .turn(&workflow, execution_spec(execution_id, input.clone()))
        .await;
    let blocked_two = durable_host
        .turn(&workflow, execution_spec(execution_id, input.clone()))
        .await;
    let still_same = before
        == store
            .load(execution_id)
            .await
            .expect("scenario load must not fail");
    durable_host
        .observe(
            &execution_spec(execution_id, input.clone()),
            ActivityObservation::new(first_logical, bytes(b"first-result")),
        )
        .await;
    let after_observation = durable_host
        .turn(&workflow, execution_spec(execution_id, input))
        .await;
    ScenarioEvidence::new(
        id,
        "replay a two-activity workflow while the first exposure is unresolved",
        [
            (
                "quarantine returns neither redispatch nor another schedule",
                matches!(blocked_one, HostOutcome::Quarantined { .. })
                    && matches!(blocked_two, HostOutcome::Quarantined { .. }),
            ),
            (
                "no compensation or other mutation occurs in quarantine",
                still_same,
            ),
            (
                "the subsequent activity schedules only after observation",
                matches!(
                    after_observation,
                    HostOutcome::ScheduleAccepted { activity, .. }
                        if activity.sequence() == ActivitySequence::new(1)
                ),
            ),
        ],
    )
}

async fn load_absence_and_provider_failures(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::one("effect", 1, b"A");
    let execution_id = execution(21);
    let input = bytes(b"workflow");
    let absence_is_distinct = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail")
        .is_none();

    let mut kinds_preserved = true;
    let mut descriptions_preserved = true;
    let mut failures_granted_no_permit = true;
    for kind in [
        StoreErrorKind::Authorization,
        StoreErrorKind::Unavailable,
        StoreErrorKind::Timeout,
        StoreErrorKind::MalformedResponse,
        StoreErrorKind::Other,
    ] {
        store.fail_next_load(StoreError::new(kind, format!("{kind} provider detail")));
        let mut durable_host = host(store.clone(), 21);
        let outcome = durable_host
            .turn(&workflow, execution_spec(execution_id, input.clone()))
            .await;
        kinds_preserved &= matches!(
            &outcome,
            HostOutcome::StoreFailed {
                operation: kuberic_durable_execution::StoreOperation::Load,
                error
            } if error.kind() == kind
        );
        descriptions_preserved &= matches!(
            &outcome,
            HostOutcome::StoreFailed { error, .. }
                if error.description().contains("provider detail")
        );
        failures_granted_no_permit &= !matches!(outcome, HostOutcome::DispatchPermitted { .. });
    }

    let (observation_store, durable_host, _, observed_execution, observed_input, logical, _) =
        prepared_one(121).await;
    observation_store.fail_next_load(StoreError::new(
        StoreErrorKind::Timeout,
        "observation load timed out",
    ));
    let observation_failure = durable_host
        .observe(
            &execution_spec(observed_execution, observed_input),
            ActivityObservation::new(logical, bytes(b"result")),
        )
        .await;

    ScenarioEvidence::new(
        id,
        "distinguish missing checkpoints from portable provider failures on async host paths",
        [
            ("missing checkpoint loads as absence", absence_is_distinct),
            (
                "all portable load failure classes reach the host unchanged",
                kinds_preserved,
            ),
            (
                "provider source descriptions remain available as text",
                descriptions_preserved,
            ),
            (
                "load failures grant no dispatch permit",
                failures_granted_no_permit,
            ),
            (
                "observation load failure is propagated without mutation",
                matches!(
                    observation_failure,
                    HostOutcome::StoreFailed {
                        operation: kuberic_durable_execution::StoreOperation::Load,
                        error
                    } if error.kind() == StoreErrorKind::Timeout
                ),
            ),
        ],
    )
}

async fn opaque_storage_revisions(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::one("effect", 1, b"A");
    let execution_id = execution(22);
    let input = bytes(b"workflow");
    let mut durable_host = host(store, 22);
    let revision = match durable_host
        .turn(&workflow, execution_spec(execution_id, input))
        .await
    {
        HostOutcome::ScheduleAccepted { revision, .. } => Some(revision),
        _ => None,
    };
    let round_trip = revision.as_ref().and_then(|revision| {
        StorageRevision::new(revision.as_str())
            .ok()
            .map(|decoded| decoded == *revision)
    });
    let empty = StorageRevision::new("").unwrap_err();

    ScenarioEvidence::new(
        id,
        "round-trip a nonnumeric opaque provider revision and reject an empty token",
        [
            (
                "in-memory revision is an opaque nonnumeric string",
                revision
                    .as_ref()
                    .is_some_and(|revision| revision.as_str().parse::<u64>().is_err()),
            ),
            (
                "revision equality survives provider string round-trip",
                round_trip == Some(true),
            ),
            (
                "empty provider revision is a malformed response",
                empty.kind() == StoreErrorKind::MalformedResponse,
            ),
        ],
    )
}

async fn outcome_unknown_apply_state_hidden(id: ScenarioId) -> ScenarioEvidence {
    let workflow = LinearWorkflow::one("effect", 1, b"A");
    let execution_id = execution(23);
    let input = bytes(b"workflow");

    let schedule_unapplied_store = InMemoryCheckpointStore::new();
    schedule_unapplied_store.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownWithoutApply);
    let mut schedule_unapplied_host = host(schedule_unapplied_store.clone(), 23);
    let schedule_unapplied = schedule_unapplied_host
        .turn(&workflow, execution_spec(execution_id, input.clone()))
        .await;
    let schedule_remained_absent = schedule_unapplied_store
        .load(execution_id)
        .await
        .expect("scenario load must not fail")
        .is_none();

    let schedule_applied_store = InMemoryCheckpointStore::new();
    schedule_applied_store.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownAfterApply);
    let mut schedule_applied_host = host(schedule_applied_store.clone(), 24);
    let schedule_applied = schedule_applied_host
        .turn(&workflow, execution_spec(execution_id, input.clone()))
        .await;
    let schedule_became_present = schedule_applied_store
        .load(execution_id)
        .await
        .expect("scenario load must not fail")
        .is_some();

    let exposure_unapplied_store = InMemoryCheckpointStore::new();
    let mut exposure_unapplied_host = host(exposure_unapplied_store.clone(), 25);
    schedule(
        &mut exposure_unapplied_host,
        &workflow,
        execution(123),
        &input,
    )
    .await
    .unwrap();
    let exposure_unapplied_before = exposure_unapplied_store
        .load(execution(123))
        .await
        .expect("scenario load must not fail");
    exposure_unapplied_store.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownWithoutApply);
    let exposure_unapplied = exposure_unapplied_host
        .turn(&workflow, execution_spec(execution(123), input.clone()))
        .await;
    let exposure_unapplied_after = exposure_unapplied_store
        .load(execution(123))
        .await
        .expect("scenario load must not fail");

    let exposure_applied_store = InMemoryCheckpointStore::new();
    let mut exposure_applied_host = host(exposure_applied_store.clone(), 26);
    schedule(
        &mut exposure_applied_host,
        &workflow,
        execution(124),
        &input,
    )
    .await
    .unwrap();
    let exposure_applied_before = exposure_applied_store
        .load(execution(124))
        .await
        .expect("scenario load must not fail");
    exposure_applied_store.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownAfterApply);
    let exposure_applied = exposure_applied_host
        .turn(&workflow, execution_spec(execution(124), input.clone()))
        .await;
    let exposure_applied_after = exposure_applied_store
        .load(execution(124))
        .await
        .expect("scenario load must not fail");

    let (
        observation_unapplied_store,
        observation_unapplied_host,
        _,
        observation_unapplied_execution,
        observation_unapplied_input,
        observation_unapplied_logical,
        _,
    ) = prepared_one(125).await;
    let observation_unapplied_before = observation_unapplied_store
        .load(observation_unapplied_execution)
        .await
        .expect("scenario load must not fail");
    observation_unapplied_store
        .fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownWithoutApply);
    let observation_unapplied = observation_unapplied_host
        .observe(
            &execution_spec(observation_unapplied_execution, observation_unapplied_input),
            ActivityObservation::new(observation_unapplied_logical, bytes(b"result")),
        )
        .await;
    let observation_unapplied_after = observation_unapplied_store
        .load(observation_unapplied_execution)
        .await
        .expect("scenario load must not fail");

    let (
        observation_applied_store,
        observation_applied_host,
        _,
        observation_applied_execution,
        observation_applied_input,
        observation_applied_logical,
        _,
    ) = prepared_one(126).await;
    let observation_applied_before = observation_applied_store
        .load(observation_applied_execution)
        .await
        .expect("scenario load must not fail");
    observation_applied_store.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownAfterApply);
    let observation_applied = observation_applied_host
        .observe(
            &execution_spec(observation_applied_execution, observation_applied_input),
            ActivityObservation::new(observation_applied_logical, bytes(b"result")),
        )
        .await;
    let observation_applied_after = observation_applied_store
        .load(observation_applied_execution)
        .await
        .expect("scenario load must not fail");

    ScenarioEvidence::new(
        id,
        "inject the same outcome-unknown result with and without applying each CAS boundary",
        [
            (
                "schedule uncertainty hides whether the mutation applied",
                schedule_unapplied == schedule_applied
                    && matches!(
                        schedule_unapplied,
                        HostOutcome::ReloadRequired {
                            boundary: PersistenceBoundary::Schedule,
                            reason: ReloadReason::OutcomeUnknown
                        }
                    )
                    && schedule_remained_absent
                    && schedule_became_present,
            ),
            (
                "exposure uncertainty hides whether the mutation applied",
                exposure_unapplied == exposure_applied
                    && matches!(
                        exposure_unapplied,
                        HostOutcome::ReloadRequired {
                            boundary: PersistenceBoundary::Exposure,
                            reason: ReloadReason::OutcomeUnknown
                        }
                    )
                    && exposure_unapplied_before == exposure_unapplied_after
                    && exposure_applied_before != exposure_applied_after,
            ),
            (
                "observation uncertainty hides whether the mutation applied",
                observation_unapplied == observation_applied
                    && matches!(
                        observation_unapplied,
                        HostOutcome::ReloadRequired {
                            boundary: PersistenceBoundary::Observation,
                            reason: ReloadReason::OutcomeUnknown
                        }
                    )
                    && observation_unapplied_before == observation_unapplied_after
                    && observation_applied_before != observation_applied_after,
            ),
            (
                "no uncertain CAS outcome grants a dispatch permit",
                !matches!(schedule_applied, HostOutcome::DispatchPermitted { .. })
                    && !matches!(exposure_applied, HostOutcome::DispatchPermitted { .. })
                    && !matches!(observation_applied, HostOutcome::DispatchPermitted { .. }),
            ),
        ],
    )
}

async fn changed_result_bound(id: ScenarioId) -> ScenarioEvidence {
    let (_, mut durable_host, _workflow, execution_id, input, logical, _) = prepared_one(127).await;
    durable_host
        .observe(
            &execution_spec(execution_id, input.clone()),
            ActivityObservation::new(logical.clone(), bytes(b"done")),
        )
        .await;
    let changed = LinearWorkflow::one_with_bound("effect", 1, b"activity", 1025);
    let changed_logical = LogicalActivityId::new(
        execution_id,
        ActivitySequence::new(0),
        activity_spec("effect", 1, b"activity", 1025),
    );
    let outcome = durable_host
        .turn(&changed, execution_spec(execution_id, input))
        .await;

    ScenarioEvidence::new(
        id,
        "replay a completed activity with only its declared result bound changed",
        [
            (
                "changed result bound is nondeterminism before dispatch",
                matches!(
                    outcome,
                    HostOutcome::Nondeterminism(Nondeterminism::ActivityMismatch { .. })
                ),
            ),
            (
                "declared result bound participates in logical identity",
                logical != changed_logical
                    && logical.to_external_id() != changed_logical.to_external_id(),
            ),
        ],
    )
}

async fn activity_count_and_growing_history(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let execution_id = execution(128);
    let input = bytes(b"workflow");
    let limits = CheckpointLimits::new(3, 1_000_000, 1_000_000).unwrap();
    let workflow = LinearWorkflow {
        activities: vec![
            activity_spec("first", 1, b"A", 16),
            activity_spec("second", 1, b"B", 16),
            activity_spec("third", 1, b"C", 16),
        ],
    };
    let mut durable_host = DurableHost::new(store.clone(), epoch(128), limits);
    let mut all_observations_accepted = true;
    for result in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        let logical = match durable_host
            .turn(&workflow, execution_spec(execution_id, input.clone()))
            .await
        {
            HostOutcome::ScheduleAccepted { activity, .. } => activity,
            other => {
                return ScenarioEvidence::new(
                    id,
                    "grow completed history to the exact configured activity count",
                    [(
                        "every activity through the exact count boundary schedules",
                        matches!(other, HostOutcome::ScheduleAccepted { .. }),
                    )],
                );
            }
        };
        let permitted = matches!(
            durable_host
                .turn(&workflow, execution_spec(execution_id, input.clone()))
                .await,
            HostOutcome::DispatchPermitted { .. }
        );
        let observed = durable_host
            .observe(
                &execution_spec(execution_id, input.clone()),
                ActivityObservation::new(logical, bytes(result)),
            )
            .await;
        all_observations_accepted &=
            permitted && matches!(observed, HostOutcome::ObservationAccepted { .. });
    }
    let persisted_active_count = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail")
        .and_then(|stored| {
            stored
                .checkpoint()
                .decode_and_validate(&execution_spec(execution_id, input.clone()), limits)
                .ok()
                .and_then(|payload| {
                    payload
                        .active_activities()
                        .map(|activities| activities.len())
                })
        });

    let mut four = workflow.clone();
    four.activities.push(activity_spec("fourth", 1, b"D", 16));
    let first_excess = durable_host
        .turn(&four, execution_spec(execution_id, input.clone()))
        .await;
    let completed = durable_host
        .turn(&workflow, execution_spec(execution_id, input))
        .await;

    ScenarioEvidence::new(
        id,
        "grow completed history to the exact activity limit and request one more record",
        [
            (
                "completed history grows successfully through the exact count boundary",
                all_observations_accepted
                    && persisted_active_count == Some(3)
                    && matches!(completed, HostOutcome::WorkflowCompleted { .. }),
            ),
            (
                "the first record beyond the configured maximum is rejected",
                matches!(
                    first_excess,
                    HostOutcome::CheckpointRejected(CheckpointError::ActivityRecordLimitExceeded {
                        actual: 4,
                        maximum: 3
                    })
                ),
            ),
            (
                "count-capacity rejection grants no dispatch permit",
                !matches!(first_excess, HostOutcome::DispatchPermitted { .. }),
            ),
        ],
    )
}

async fn encoded_byte_reservation(id: ScenarioId) -> ScenarioEvidence {
    const MAX_RESULT: usize = 256;
    let execution_id = execution(129);
    let input = bytes(b"workflow");
    let activity = activity_spec("bounded", 1, b"input", MAX_RESULT as u64);
    let exact_execution = ExecutionSpec::new(execution_id, input.clone(), MAX_RESULT as u64);
    let terminal_capacity = generous_limits().max_terminal_encoded_bytes() as u64;
    let mut exact_completed = 1_000_000_u64;
    for _ in 0..8 {
        let probe = CheckpointPayload::active(
            ExecutionContract::with_encoded_limits(
                exact_execution.clone(),
                exact_completed,
                terminal_capacity,
            ),
            vec![kuberic_durable_execution::ActivityRecord::scheduled(
                ActivitySequence::new(0),
                activity.clone(),
            )],
        );
        let required = probe.maximum_activity_completed_encoded_len().unwrap();
        let required = u64::try_from(required).unwrap();
        if required == exact_completed {
            break;
        }
        exact_completed = required;
    }
    let exact_completed = usize::try_from(exact_completed).unwrap();

    let exact_store = InMemoryCheckpointStore::new();
    let exact_limits =
        CheckpointLimits::new(1, exact_completed, terminal_capacity as usize).unwrap();
    let workflow = LinearWorkflow {
        activities: vec![activity.clone()],
    };
    let mut exact_host = DurableHost::new(exact_store.clone(), epoch(129), exact_limits);
    let scheduled = exact_host.turn(&workflow, exact_execution.clone()).await;
    let logical = match &scheduled {
        HostOutcome::ScheduleAccepted { activity, .. } => Some(activity.clone()),
        _ => None,
    };
    let exposure = exact_host.turn(&workflow, exact_execution.clone()).await;
    let exact_result = ExactBytes::new(vec![0; MAX_RESULT]);
    let observation = match logical {
        Some(logical) => {
            exact_host
                .observe(
                    &exact_execution,
                    ActivityObservation::new(logical, exact_result),
                )
                .await
        }
        None => HostOutcome::ObservationRejected(ObservationRejection::CheckpointMissing),
    };
    let final_size = exact_store
        .load(execution_id)
        .await
        .expect("scenario load must not fail")
        .and_then(|stored| stored.checkpoint().encoded_len().ok());

    let tight_store = InMemoryCheckpointStore::new();
    let tight_limits =
        CheckpointLimits::new(1, exact_completed - 1, terminal_capacity as usize).unwrap();
    let mut tight_host = DurableHost::new(tight_store, epoch(130), tight_limits);
    let tight_execution = ExecutionSpec::new(execution_id, input.clone(), MAX_RESULT as u64);
    let tight_schedule = tight_host.turn(&workflow, tight_execution.clone()).await;
    let tight_exposure = tight_host.turn(&workflow, tight_execution).await;

    let huge_store = InMemoryCheckpointStore::new();
    let huge_workflow = LinearWorkflow::one_with_bound("bounded", 1, b"input", u64::MAX);
    let mut huge_host = DurableHost::new(
        huge_store,
        epoch(132),
        CheckpointLimits::new(1, 100_000, 100_000).unwrap(),
    );
    let huge_execution = ExecutionSpec::new(
        execution(131),
        bytes(b"workflow"),
        MAX_TERMINAL_PAYLOAD_BYTES,
    );
    let huge_schedule = huge_host.turn(&huge_workflow, huge_execution.clone()).await;
    let huge_exposure = huge_host.turn(&huge_workflow, huge_execution).await;

    ScenarioEvidence::new(
        id,
        "reserve the exact maximum completed checkpoint before exposure",
        [
            (
                "the exact encoded-byte boundary accepts schedule and exposure",
                matches!(scheduled, HostOutcome::ScheduleAccepted { .. })
                    && matches!(exposure, HostOutcome::DispatchPermitted { .. }),
            ),
            (
                "a permitted result at its declared maximum persists at the exact boundary",
                matches!(observation, HostOutcome::ObservationAccepted { .. })
                    && final_size == Some(exact_completed),
            ),
            (
                "one byte below reserved capacity rejects before a permit",
                matches!(tight_schedule, HostOutcome::ScheduleAccepted { .. })
                    && matches!(
                        tight_exposure,
                        HostOutcome::CheckpointRejected(
                            CheckpointError::EncodedCheckpointLimitExceeded { .. }
                        )
                    )
                    && !matches!(tight_exposure, HostOutcome::DispatchPermitted { .. }),
            ),
            (
                "an unrepresentable declaration is rejected without allocating or permitting",
                matches!(huge_schedule, HostOutcome::ScheduleAccepted { .. })
                    && matches!(
                        huge_exposure,
                        HostOutcome::CheckpointRejected(
                            CheckpointError::EncodedLengthOverflow
                                | CheckpointError::ResultLengthUnrepresentable
                        )
                    )
                    && !matches!(huge_exposure, HostOutcome::DispatchPermitted { .. }),
            ),
        ],
    )
}

async fn oversized_observation(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let execution_id = execution(130);
    let input = bytes(b"workflow");
    let workflow = LinearWorkflow::one_with_bound("bounded", 1, b"input", 3);
    let mut durable_host = DurableHost::new(store.clone(), epoch(131), generous_limits());
    let logical = schedule(&mut durable_host, &workflow, execution_id, &input)
        .await
        .unwrap();
    let permit = expose(&mut durable_host, &workflow, execution_id, &input)
        .await
        .is_some();
    let before = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail");
    let oversized = durable_host
        .observe(
            &execution_spec(execution_id, input.clone()),
            ActivityObservation::new(logical.clone(), bytes(b"four")),
        )
        .await;
    let unchanged = before
        == store
            .load(execution_id)
            .await
            .expect("scenario load must not fail");
    let exact = durable_host
        .observe(
            &execution_spec(execution_id, input),
            ActivityObservation::new(logical, bytes(b"tri")),
        )
        .await;

    ScenarioEvidence::new(
        id,
        "reject an observation above its reservation and accept the exact result bound",
        [
            ("capacity was reserved before a dispatch permit", permit),
            (
                "one byte over the declared result bound is rejected without mutation",
                matches!(
                    oversized,
                    HostOutcome::ObservationRejected(
                        ObservationRejection::ResultExceedsDeclaredBound {
                            actual: 4,
                            maximum: 3
                        }
                    )
                ) && unchanged,
            ),
            (
                "a result exactly at the declared bound is accepted",
                matches!(exact, HostOutcome::ObservationAccepted { .. }),
            ),
        ],
    )
}

async fn active_to_terminal_compaction(id: ScenarioId) -> ScenarioEvidence {
    let (store, mut durable_host, workflow, execution_id, input, logical, _) =
        prepared_one(132).await;
    let execution = execution_spec(execution_id, input.clone());
    let observed = durable_host
        .observe(
            &execution,
            ActivityObservation::new(logical, bytes(b"compacted")),
        )
        .await;
    let active_before = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail")
        .and_then(|stored| {
            stored
                .checkpoint()
                .decode_and_validate(&execution, generous_limits())
                .ok()
        });
    let completed = durable_host.turn(&workflow, execution.clone()).await;
    let stored_terminal = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail");
    let decoded_terminal = stored_terminal.as_ref().and_then(|stored| {
        stored
            .checkpoint()
            .decode_and_validate(&execution, generous_limits())
            .ok()
    });
    let terminal_json = stored_terminal
        .as_ref()
        .and_then(|stored| std::str::from_utf8(stored.checkpoint().payload().as_slice()).ok())
        .unwrap_or_default();
    let accepted_revision_matches = match (&completed, &stored_terminal) {
        (HostOutcome::WorkflowCompleted { revision, .. }, Some(stored)) => {
            revision == stored.revision()
        }
        _ => false,
    };

    ScenarioEvidence::new(
        id,
        "complete one observed activity and CAS-replace active history with terminal state",
        [
            (
                "authoritative activity result is accepted before completion",
                matches!(observed, HostOutcome::ObservationAccepted { .. }),
            ),
            (
                "active checkpoint retains the complete activity before terminalization",
                active_before
                    .as_ref()
                    .and_then(CheckpointPayload::active_activities)
                    .is_some_and(|activities| activities.len() == 1),
            ),
            (
                "accepted completion returns exact outcome and accepted revision",
                matches!(
                    &completed,
                    HostOutcome::WorkflowCompleted {
                        outcome: TerminalOutcome::Succeeded(result),
                        ..
                    } if result == &bytes(b"compacted")
                ) && accepted_revision_matches,
            ),
            (
                "terminal checkpoint contains outcome and completed count but no activity history",
                matches!(
                    decoded_terminal.as_ref().map(CheckpointPayload::state),
                    Some(CheckpointState::Terminal {
                        outcome: TerminalOutcome::Succeeded(result),
                        completed_activity_count: 1,
                        ..
                    }) if result == &bytes(b"compacted")
                ) && !terminal_json.contains("activities")
                    && !terminal_json.contains("history")
                    && !terminal_json.contains("digest"),
            ),
        ],
    )
}

async fn terminal_reload_without_poll(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let execution_id = execution(133);
    let execution = execution_spec(execution_id, bytes(b"workflow"));
    let outcome = TerminalOutcome::failed(bytes(b"terminal-error"));
    let mut writer = host(store.clone(), 133);
    let accepted = writer
        .turn(
            &TerminalWorkflow {
                outcome: outcome.clone(),
            },
            execution.clone(),
        )
        .await;
    let polls = Cell::new(0);
    let mut reader = host(store, 134);
    let reloaded = reader
        .turn(&PollSentinelWorkflow { polls: &polls }, execution)
        .await;

    ScenarioEvidence::new(
        id,
        "reload an accepted failed terminal outcome through a workflow-poll sentinel",
        [
            (
                "zero-activity failure terminalization was accepted",
                matches!(
                    &accepted,
                    HostOutcome::WorkflowCompleted {
                        outcome: TerminalOutcome::Failed(payload),
                        ..
                    } if payload == &bytes(b"terminal-error")
                ),
            ),
            (
                "terminal reload returns the same outcome and revision with authoritative status",
                matches!(
                    (&accepted, &reloaded),
                    (
                        HostOutcome::WorkflowCompleted {
                            outcome: accepted_outcome,
                            completed_activity_count: accepted_count,
                            revision: accepted_revision,
                            boundary: accepted_boundary,
                            checkpoint_status: TerminalCheckpointStatus::Accepted,
                            ..
                        },
                        HostOutcome::WorkflowCompleted {
                            outcome: reloaded_outcome,
                            completed_activity_count: reloaded_count,
                            revision: reloaded_revision,
                            boundary: reloaded_boundary,
                            checkpoint_status: TerminalCheckpointStatus::Reloaded,
                            ..
                        },
                    ) if accepted_outcome == reloaded_outcome
                        && accepted_count == reloaded_count
                        && accepted_revision == reloaded_revision
                        && accepted_boundary == reloaded_boundary
                ),
            ),
            (
                "terminal reload does not poll workflow code",
                polls.get() == 0,
            ),
        ],
    )
}

async fn zero_activity_terminalization(id: ScenarioId) -> ScenarioEvidence {
    let success_store = InMemoryCheckpointStore::new();
    let success_execution = execution_spec(execution(134), bytes(b"zero-success"));
    let mut success_host = host(success_store.clone(), 135);
    let success = success_host
        .turn(
            &TerminalWorkflow {
                outcome: TerminalOutcome::succeeded(bytes(b"done")),
            },
            success_execution.clone(),
        )
        .await;
    let success_count = success_store
        .load(success_execution.execution_id())
        .await
        .expect("scenario load must not fail")
        .and_then(|stored| {
            stored
                .checkpoint()
                .decode_and_validate(&success_execution, generous_limits())
                .ok()
        })
        .and_then(|payload| {
            payload
                .terminal_outcome()
                .map(|(_, completed_activity_count)| completed_activity_count)
        });

    let failure_store = InMemoryCheckpointStore::new();
    let failure_execution = execution_spec(execution(135), bytes(b"zero-failure"));
    let mut failure_host = host(failure_store.clone(), 136);
    let failure = failure_host
        .turn(
            &TerminalWorkflow {
                outcome: TerminalOutcome::failed(bytes(b"failed")),
            },
            failure_execution.clone(),
        )
        .await;
    let failure_count = failure_store
        .load(failure_execution.execution_id())
        .await
        .expect("scenario load must not fail")
        .and_then(|stored| {
            stored
                .checkpoint()
                .decode_and_validate(&failure_execution, generous_limits())
                .ok()
        })
        .and_then(|payload| {
            payload
                .terminal_outcome()
                .map(|(_, completed_activity_count)| completed_activity_count)
        });

    ScenarioEvidence::new(
        id,
        "terminalize zero-activity success and failure workflows directly from absent state",
        [
            (
                "zero-activity success is durably completed",
                matches!(
                    success,
                    HostOutcome::WorkflowCompleted {
                        outcome: TerminalOutcome::Succeeded(_),
                        ..
                    }
                ) && success_count == Some(0),
            ),
            (
                "zero-activity failure is durably completed",
                matches!(
                    failure,
                    HostOutcome::WorkflowCompleted {
                        outcome: TerminalOutcome::Failed(_),
                        ..
                    }
                ) && failure_count == Some(0),
            ),
        ],
    )
}

async fn terminal_outcome_bounds(id: ScenarioId) -> ScenarioEvidence {
    async fn run_case(
        execution_id: ExecutionId,
        outcome: TerminalOutcome,
        maximum: u64,
    ) -> (HostOutcome, bool) {
        let store = InMemoryCheckpointStore::new();
        let execution = ExecutionSpec::new(execution_id, bytes(b"bounds"), maximum);
        let mut durable_host = host(store.clone(), execution_id.as_bytes()[0]);
        let result = durable_host
            .turn(&TerminalWorkflow { outcome }, execution)
            .await;
        let absent = store
            .load(execution_id)
            .await
            .expect("scenario load must not fail")
            .is_none();
        (result, absent)
    }

    let (empty, _) = run_case(
        execution(136),
        TerminalOutcome::succeeded(ExactBytes::default()),
        0,
    )
    .await;
    let (success_exact, _) = run_case(
        execution(137),
        TerminalOutcome::succeeded(bytes(b"four")),
        4,
    )
    .await;
    let (failure_exact, _) =
        run_case(execution(138), TerminalOutcome::failed(bytes(b"four")), 4).await;
    let (success_oversized, success_absent) = run_case(
        execution(139),
        TerminalOutcome::succeeded(bytes(b"five!")),
        4,
    )
    .await;
    let (failure_oversized, failure_absent) =
        run_case(execution(140), TerminalOutcome::failed(bytes(b"five!")), 4).await;

    ScenarioEvidence::new(
        id,
        "enforce zero, exact, and maximum-plus-one terminal payload contracts for both outcomes",
        [
            (
                "zero terminal bound accepts an empty payload",
                matches!(empty, HostOutcome::WorkflowCompleted { .. }),
            ),
            (
                "success and failure payloads at the exact bound are accepted",
                matches!(success_exact, HostOutcome::WorkflowCompleted { .. })
                    && matches!(failure_exact, HostOutcome::WorkflowCompleted { .. }),
            ),
            (
                "success payload above the declaration is rejected without persistence",
                matches!(
                    success_oversized,
                    HostOutcome::CheckpointRejected(
                        CheckpointError::TerminalPayloadExceedsDeclared {
                            actual: 5,
                            maximum: 4
                        }
                    )
                ) && success_absent,
            ),
            (
                "failure payload above the declaration is rejected without persistence",
                matches!(
                    failure_oversized,
                    HostOutcome::CheckpointRejected(
                        CheckpointError::TerminalPayloadExceedsDeclared {
                            actual: 5,
                            maximum: 4
                        }
                    )
                ) && failure_absent,
            ),
        ],
    )
}

fn exact_terminal_capacity(execution: &ExecutionSpec) -> usize {
    let mut admitted = 1_000_000_u64;
    for _ in 0..16 {
        let payload = CheckpointPayload::active(
            ExecutionContract::with_encoded_limits(execution.clone(), 1_000_000, admitted),
            Vec::new(),
        );
        let required = u64::try_from(payload.maximum_terminal_encoded_len().unwrap()).unwrap();
        if required == admitted {
            return usize::try_from(required).unwrap();
        }
        admitted = required;
    }
    panic!("terminal capacity projection did not converge")
}

async fn terminal_capacity_admission(id: ScenarioId) -> ScenarioEvidence {
    let exact_execution = ExecutionSpec::new(execution(141), bytes(b"capacity"), 8);
    let exact_capacity = exact_terminal_capacity(&exact_execution);
    let exact_store = InMemoryCheckpointStore::new();
    let mut exact_host = DurableHost::new(
        exact_store,
        epoch(141),
        CheckpointLimits::new(1, 1_000_000, exact_capacity).unwrap(),
    );
    let exact = exact_host
        .turn(
            &TerminalWorkflow {
                outcome: TerminalOutcome::succeeded(ExactBytes::new([7; 8])),
            },
            exact_execution,
        )
        .await;

    let tight_store = InMemoryCheckpointStore::new();
    let tight_execution = ExecutionSpec::new(execution(142), bytes(b"capacity"), 8);
    let tight_polls = Cell::new(0);
    let mut tight_host = DurableHost::new(
        tight_store.clone(),
        epoch(142),
        CheckpointLimits::new(1, 1_000_000, exact_capacity - 1).unwrap(),
    );
    let tight = tight_host
        .turn(
            &PollSentinelWorkflow {
                polls: &tight_polls,
            },
            tight_execution.clone(),
        )
        .await;
    let tight_absent = tight_store
        .load(tight_execution.execution_id())
        .await
        .expect("scenario load must not fail")
        .is_none();

    let huge_store = InMemoryCheckpointStore::new();
    let huge_execution = ExecutionSpec::new(execution(143), bytes(b"capacity"), u64::MAX);
    let huge_polls = Cell::new(0);
    let mut huge_host = host(huge_store.clone(), 143);
    let huge = huge_host
        .turn(&PollSentinelWorkflow { polls: &huge_polls }, huge_execution)
        .await;

    ScenarioEvidence::new(
        id,
        "admit canonical maximum terminal capacity before workflow polling or effects",
        [
            (
                "the exact projected terminal capacity is accepted",
                matches!(exact, HostOutcome::WorkflowCompleted { .. }),
            ),
            (
                "one byte below projected capacity rejects before polling, persistence, or permit",
                matches!(
                    tight,
                    HostOutcome::CheckpointRejected(
                        CheckpointError::AdmittedTerminalCapacityInsufficient { .. }
                    )
                ) && tight_polls.get() == 0
                    && tight_absent
                    && !matches!(tight, HostOutcome::DispatchPermitted { .. }),
            ),
            (
                "largest declaration rejects without proportional allocation or polling",
                matches!(
                    huge,
                    HostOutcome::CheckpointRejected(
                        CheckpointError::EncodedLengthOverflow
                            | CheckpointError::TerminalPayloadLengthUnrepresentable
                    )
                ) && huge_polls.get() == 0,
            ),
        ],
    )
}

async fn completion_conflict(id: ScenarioId) -> ScenarioEvidence {
    let (store, durable_host, workflow, execution_id, input, logical, _) = prepared_one(144).await;
    let execution = execution_spec(execution_id, input);
    let observed = durable_host
        .observe(
            &execution,
            ActivityObservation::new(logical, bytes(b"complete")),
        )
        .await;
    let (first_store, second_store) = ContendedStore::pair(store.clone());
    let mut first = DurableHost::new(first_store, epoch(145), generous_limits());
    let mut second = DurableHost::new(second_store, epoch(146), generous_limits());
    let (first_outcome, second_outcome) = join!(
        first.turn(&workflow, execution.clone()),
        second.turn(&workflow, execution.clone())
    );
    let outcomes = [first_outcome, second_outcome];
    let completed = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, HostOutcome::WorkflowCompleted { .. }))
        .count();
    let conflicts = outcomes
        .iter()
        .filter(|outcome| {
            matches!(
                outcome,
                HostOutcome::ReloadRequired {
                    boundary: PersistenceBoundary::Completion,
                    reason: ReloadReason::Conflict
                }
            )
        })
        .count();
    let terminal = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail")
        .and_then(|stored| {
            stored
                .checkpoint()
                .decode_and_validate(&execution, generous_limits())
                .ok()
        })
        .is_some_and(|payload| matches!(payload.state(), CheckpointState::Terminal { .. }));

    ScenarioEvidence::new(
        id,
        "race two completion CAS operations from the same active revision",
        [
            (
                "activity observation prepared completed active history",
                matches!(observed, HostOutcome::ObservationAccepted { .. }),
            ),
            (
                "exactly one completion CAS is accepted and one conflicts",
                completed == 1 && conflicts == 1,
            ),
            (
                "completion conflict grants no permit or false second completion",
                outcomes
                    .iter()
                    .all(|outcome| !matches!(outcome, HostOutcome::DispatchPermitted { .. })),
            ),
            (
                "accepted contender leaves one terminal checkpoint",
                terminal,
            ),
        ],
    )
}

async fn completion_outcome_unknown_after_apply(id: ScenarioId) -> ScenarioEvidence {
    let (store, mut durable_host, workflow, execution_id, input, logical, _) =
        prepared_one(147).await;
    let execution = execution_spec(execution_id, input);
    durable_host
        .observe(
            &execution,
            ActivityObservation::new(logical, bytes(b"applied")),
        )
        .await;
    store.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownAfterApply);
    let uncertain = durable_host.turn(&workflow, execution.clone()).await;
    let stored_terminal = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail")
        .and_then(|stored| {
            stored
                .checkpoint()
                .decode_and_validate(&execution, generous_limits())
                .ok()
        })
        .is_some_and(|payload| matches!(payload.state(), CheckpointState::Terminal { .. }));
    let polls = Cell::new(0);
    let recovered = durable_host
        .turn(&PollSentinelWorkflow { polls: &polls }, execution)
        .await;

    ScenarioEvidence::new(
        id,
        "lose the response after the completion CAS is applied",
        [
            (
                "applied completion uncertainty requires reload with no permit or completion",
                matches!(
                    uncertain,
                    HostOutcome::ReloadRequired {
                        boundary: PersistenceBoundary::Completion,
                        reason: ReloadReason::OutcomeUnknown
                    }
                ),
            ),
            (
                "unknown write actually left terminal state",
                stored_terminal,
            ),
            (
                "reload returns accepted terminal outcome without workflow polling",
                matches!(recovered, HostOutcome::WorkflowCompleted { .. }) && polls.get() == 0,
            ),
        ],
    )
}

async fn completion_outcome_unknown_without_apply(id: ScenarioId) -> ScenarioEvidence {
    let (store, mut durable_host, workflow, execution_id, input, logical, _) =
        prepared_one(148).await;
    let execution = execution_spec(execution_id, input);
    durable_host
        .observe(
            &execution,
            ActivityObservation::new(logical, bytes(b"unapplied")),
        )
        .await;
    let before = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail");
    store.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownWithoutApply);
    let uncertain = durable_host.turn(&workflow, execution.clone()).await;
    let after_unknown = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail");
    let retried = durable_host.turn(&workflow, execution.clone()).await;
    let reloaded = durable_host.turn(&workflow, execution).await;

    ScenarioEvidence::new(
        id,
        "lose the response without applying completion, then retry from active completed history",
        [
            (
                "unapplied completion uncertainty requires reload",
                matches!(
                    uncertain,
                    HostOutcome::ReloadRequired {
                        boundary: PersistenceBoundary::Completion,
                        reason: ReloadReason::OutcomeUnknown
                    }
                ),
            ),
            (
                "unapplied uncertainty leaves active completed state unchanged",
                before == after_unknown,
            ),
            (
                "retry compacts without dispatch and later reload remains terminal",
                matches!(retried, HostOutcome::WorkflowCompleted { .. })
                    && matches!(reloaded, HostOutcome::WorkflowCompleted { .. })
                    && !matches!(retried, HostOutcome::DispatchPermitted { .. }),
            ),
        ],
    )
}

async fn completion_store_failures(id: ScenarioId) -> ScenarioEvidence {
    let (store, mut durable_host, workflow, execution_id, input, logical, _) =
        prepared_one(149).await;
    let execution = execution_spec(execution_id, input);
    durable_host
        .observe(
            &execution,
            ActivityObservation::new(logical, bytes(b"stored")),
        )
        .await;
    let before = store
        .load(execution_id)
        .await
        .expect("scenario load must not fail");
    store.fail_next_compare_and_swap(InMemoryFault::FailBeforeRequest(
        StoreErrorKind::Unavailable,
    ));
    let failed_write = durable_host.turn(&workflow, execution.clone()).await;
    let unchanged = before
        == store
            .load(execution_id)
            .await
            .expect("scenario load must not fail");
    let retried = durable_host.turn(&workflow, execution.clone()).await;
    store.fail_next_load(StoreError::new(
        StoreErrorKind::Timeout,
        "terminal reload timed out",
    ));
    let failed_load = durable_host.turn(&workflow, execution.clone()).await;
    let recovered = durable_host.turn(&workflow, execution).await;

    ScenarioEvidence::new(
        id,
        "fail a completion write before request and a later terminal load",
        [
            (
                "completion store error reports the completion boundary without completion",
                matches!(
                    failed_write,
                    HostOutcome::StoreFailed {
                        operation: kuberic_durable_execution::StoreOperation::CompareAndSwap(
                            PersistenceBoundary::Completion
                        ),
                        ..
                    }
                ) && unchanged,
            ),
            (
                "fresh retry terminalizes completed active history without a permit",
                matches!(retried, HostOutcome::WorkflowCompleted { .. })
                    && !matches!(retried, HostOutcome::DispatchPermitted { .. }),
            ),
            (
                "terminal load failure reports no false completion and a later fresh load recovers",
                matches!(
                    failed_load,
                    HostOutcome::StoreFailed {
                        operation: kuberic_durable_execution::StoreOperation::Load,
                        ..
                    }
                ) && matches!(recovered, HostOutcome::WorkflowCompleted { .. }),
            ),
        ],
    )
}

async fn turn_injected_checkpoint(
    checkpoint: CheckpointEnvelope,
    storage_key: ExecutionId,
    caller: ExecutionSpec,
    epoch_value: u8,
    polls: &Cell,
) -> HostOutcome {
    let store = InMemoryCheckpointStore::new();
    store
        .compare_and_swap(storage_key, None, checkpoint)
        .await
        .unwrap();
    let mut durable_host = host(store, epoch_value);
    durable_host
        .turn(&PollSentinelWorkflow { polls }, caller)
        .await
}

async fn execution_contract_validation(id: ScenarioId) -> ScenarioEvidence {
    let active_store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::one("effect", 1, b"A");
    let active_execution = execution_spec(execution(150), bytes(b"active"));
    let mut active_writer = host(active_store.clone(), 150);
    let active_created = active_writer
        .turn(&workflow, active_execution.clone())
        .await;
    let active_polls = Cell::new(0);
    let active_mismatch = active_writer
        .turn(
            &PollSentinelWorkflow {
                polls: &active_polls,
            },
            ExecutionSpec::new(
                active_execution.execution_id(),
                active_execution.workflow_input().clone(),
                MAX_TERMINAL_PAYLOAD_BYTES + 1,
            ),
        )
        .await;

    let terminal_store = InMemoryCheckpointStore::new();
    let terminal_execution = execution_spec(execution(151), bytes(b"terminal"));
    let mut terminal_writer = host(terminal_store.clone(), 151);
    terminal_writer
        .turn(
            &TerminalWorkflow {
                outcome: TerminalOutcome::succeeded(bytes(b"done")),
            },
            terminal_execution.clone(),
        )
        .await;
    let terminal_polls = Cell::new(0);
    let terminal_mismatch = terminal_writer
        .turn(
            &PollSentinelWorkflow {
                polls: &terminal_polls,
            },
            ExecutionSpec::new(
                terminal_execution.execution_id(),
                terminal_execution.workflow_input().clone(),
                MAX_TERMINAL_PAYLOAD_BYTES + 1,
            ),
        )
        .await;

    let missing_store = InMemoryCheckpointStore::new();
    let missing_execution = execution_spec(execution(152), bytes(b"missing"));
    let valid_payload = CheckpointPayload::active(
        ExecutionContract::new(
            missing_execution.clone(),
            generous_limits().max_active_encoded_bytes() as u64,
        ),
        Vec::new(),
    );
    let mut value = serde_json::to_value(&valid_payload).unwrap();
    value["execution"]["spec"]
        .as_object_mut()
        .unwrap()
        .remove("max_terminal_payload_bytes");
    let missing_envelope = CheckpointEnvelope::new(
        CHECKPOINT_FORMAT_VERSION,
        ExactBytes::new(serde_json::to_vec(&value).unwrap()),
    );
    missing_store
        .compare_and_swap(missing_execution.execution_id(), None, missing_envelope)
        .await
        .unwrap();
    let missing_polls = Cell::new(0);
    let mut missing_host = host(missing_store, 152);
    let missing = missing_host
        .turn(
            &PollSentinelWorkflow {
                polls: &missing_polls,
            },
            missing_execution,
        )
        .await;

    let inconsistent_active_store = InMemoryCheckpointStore::new();
    let inconsistent_active_execution = execution_spec(execution(153), bytes(b"inconsistent"));
    let inconsistent_active = CheckpointEnvelope::encode(&CheckpointPayload::active(
        ExecutionContract::new(inconsistent_active_execution.clone(), 1),
        Vec::new(),
    ))
    .unwrap();
    inconsistent_active_store
        .compare_and_swap(
            inconsistent_active_execution.execution_id(),
            None,
            inconsistent_active,
        )
        .await
        .unwrap();
    let inconsistent_active_polls = Cell::new(0);
    let mut inconsistent_active_host = host(inconsistent_active_store, 153);
    let inconsistent_active_result = inconsistent_active_host
        .turn(
            &PollSentinelWorkflow {
                polls: &inconsistent_active_polls,
            },
            inconsistent_active_execution,
        )
        .await;

    let inconsistent_terminal_store = InMemoryCheckpointStore::new();
    let inconsistent_terminal_execution =
        execution_spec(execution(154), bytes(b"inconsistent-terminal"));
    let inconsistent_terminal = CheckpointEnvelope::encode(&CheckpointPayload::terminal(
        ExecutionContract::new(inconsistent_terminal_execution.clone(), 1),
        TerminalOutcome::succeeded(bytes(b"done")),
        0,
    ))
    .unwrap();
    inconsistent_terminal_store
        .compare_and_swap(
            inconsistent_terminal_execution.execution_id(),
            None,
            inconsistent_terminal,
        )
        .await
        .unwrap();
    let inconsistent_terminal_polls = Cell::new(0);
    let mut inconsistent_terminal_host = host(inconsistent_terminal_store, 154);
    let inconsistent_terminal_result = inconsistent_terminal_host
        .turn(
            &PollSentinelWorkflow {
                polls: &inconsistent_terminal_polls,
            },
            inconsistent_terminal_execution,
        )
        .await;

    let active_downgrade_store = InMemoryCheckpointStore::new();
    let active_downgrade_execution = execution_spec(execution(155), bytes(b"downgrade-active"));
    let mut active_downgrade_writer = host(active_downgrade_store.clone(), 155);
    active_downgrade_writer
        .turn(&workflow, active_downgrade_execution.clone())
        .await;
    let mut active_downgrade_host = DurableHost::new(
        active_downgrade_store,
        epoch(156),
        CheckpointLimits::new(
            128,
            generous_limits().max_active_encoded_bytes() - 1,
            generous_limits().max_terminal_encoded_bytes(),
        )
        .unwrap(),
    );
    let active_downgrade_polls = Cell::new(0);
    let active_downgrade = active_downgrade_host
        .turn(
            &PollSentinelWorkflow {
                polls: &active_downgrade_polls,
            },
            active_downgrade_execution,
        )
        .await;

    let terminal_downgrade_store = InMemoryCheckpointStore::new();
    let terminal_downgrade_execution = execution_spec(execution(156), bytes(b"downgrade-terminal"));
    let mut terminal_downgrade_writer = host(terminal_downgrade_store.clone(), 157);
    terminal_downgrade_writer
        .turn(
            &TerminalWorkflow {
                outcome: TerminalOutcome::succeeded(bytes(b"done")),
            },
            terminal_downgrade_execution.clone(),
        )
        .await;
    let mut terminal_downgrade_host = DurableHost::new(
        terminal_downgrade_store,
        epoch(158),
        CheckpointLimits::new(
            128,
            generous_limits().max_active_encoded_bytes(),
            generous_limits().max_terminal_encoded_bytes() - 1,
        )
        .unwrap(),
    );
    let terminal_downgrade_polls = Cell::new(0);
    let terminal_downgrade = terminal_downgrade_host
        .turn(
            &PollSentinelWorkflow {
                polls: &terminal_downgrade_polls,
            },
            terminal_downgrade_execution,
        )
        .await;

    let active_identity_polls = Cell::new(0);
    let active_identity_caller = execution_spec(execution(157), bytes(b"active-identity"));
    let active_identity = turn_injected_checkpoint(
        CheckpointEnvelope::encode(&CheckpointPayload::active(
            ExecutionContract::new(
                ExecutionSpec::new(
                    execution(158),
                    active_identity_caller.workflow_input().clone(),
                    MAX_TERMINAL_PAYLOAD_BYTES,
                ),
                generous_limits().max_active_encoded_bytes() as u64,
            ),
            Vec::new(),
        ))
        .unwrap(),
        active_identity_caller.execution_id(),
        active_identity_caller,
        157,
        &active_identity_polls,
    )
    .await;

    let active_input_polls = Cell::new(0);
    let active_input_caller = execution_spec(execution(159), bytes(b"active-input"));
    let active_input = turn_injected_checkpoint(
        CheckpointEnvelope::encode(&CheckpointPayload::active(
            ExecutionContract::new(
                ExecutionSpec::new(
                    active_input_caller.execution_id(),
                    bytes(b"different-active-input"),
                    MAX_TERMINAL_PAYLOAD_BYTES,
                ),
                generous_limits().max_active_encoded_bytes() as u64,
            ),
            Vec::new(),
        ))
        .unwrap(),
        active_input_caller.execution_id(),
        active_input_caller,
        159,
        &active_input_polls,
    )
    .await;

    let terminal_identity_polls = Cell::new(0);
    let terminal_identity_caller = execution_spec(execution(160), bytes(b"terminal-identity"));
    let terminal_identity = turn_injected_checkpoint(
        CheckpointEnvelope::encode(&CheckpointPayload::terminal(
            ExecutionContract::new(
                ExecutionSpec::new(
                    execution(161),
                    terminal_identity_caller.workflow_input().clone(),
                    MAX_TERMINAL_PAYLOAD_BYTES,
                ),
                generous_limits().max_active_encoded_bytes() as u64,
            ),
            TerminalOutcome::succeeded(bytes(b"done")),
            0,
        ))
        .unwrap(),
        terminal_identity_caller.execution_id(),
        terminal_identity_caller,
        160,
        &terminal_identity_polls,
    )
    .await;

    let terminal_input_polls = Cell::new(0);
    let terminal_input_caller = execution_spec(execution(162), bytes(b"terminal-input"));
    let terminal_input = turn_injected_checkpoint(
        CheckpointEnvelope::encode(&CheckpointPayload::terminal(
            ExecutionContract::new(
                ExecutionSpec::new(
                    terminal_input_caller.execution_id(),
                    bytes(b"different-terminal-input"),
                    MAX_TERMINAL_PAYLOAD_BYTES,
                ),
                generous_limits().max_active_encoded_bytes() as u64,
            ),
            TerminalOutcome::succeeded(bytes(b"done")),
            0,
        ))
        .unwrap(),
        terminal_input_caller.execution_id(),
        terminal_input_caller,
        162,
        &terminal_input_polls,
    )
    .await;

    let terminal_missing_polls = Cell::new(0);
    let terminal_missing_caller = execution_spec(execution(163), bytes(b"terminal-missing"));
    let mut terminal_missing_value = serde_json::to_value(CheckpointPayload::terminal(
        ExecutionContract::new(
            terminal_missing_caller.clone(),
            generous_limits().max_active_encoded_bytes() as u64,
        ),
        TerminalOutcome::succeeded(bytes(b"done")),
        0,
    ))
    .unwrap();
    terminal_missing_value["execution"]["spec"]
        .as_object_mut()
        .unwrap()
        .remove("max_terminal_payload_bytes");
    let terminal_missing = turn_injected_checkpoint(
        CheckpointEnvelope::new(
            CHECKPOINT_FORMAT_VERSION,
            ExactBytes::new(serde_json::to_vec(&terminal_missing_value).unwrap()),
        ),
        terminal_missing_caller.execution_id(),
        terminal_missing_caller,
        163,
        &terminal_missing_polls,
    )
    .await;

    let mixed_terminal_polls = Cell::new(0);
    let mixed_terminal_caller = execution_spec(execution(164), bytes(b"mixed-terminal"));
    let mut mixed_terminal_value = serde_json::to_value(CheckpointPayload::terminal(
        ExecutionContract::new(
            mixed_terminal_caller.clone(),
            generous_limits().max_active_encoded_bytes() as u64,
        ),
        TerminalOutcome::succeeded(bytes(b"done")),
        0,
    ))
    .unwrap();
    mixed_terminal_value["state"]["activities"] = serde_json::json!([]);
    let mixed_terminal = turn_injected_checkpoint(
        CheckpointEnvelope::new(
            CHECKPOINT_FORMAT_VERSION,
            ExactBytes::new(serde_json::to_vec(&mixed_terminal_value).unwrap()),
        ),
        mixed_terminal_caller.execution_id(),
        mixed_terminal_caller,
        164,
        &mixed_terminal_polls,
    )
    .await;

    let active_unknown_polls = Cell::new(0);
    let active_unknown_caller = execution_spec(execution(165), bytes(b"active-unknown"));
    let mut active_unknown_value = serde_json::to_value(CheckpointPayload::active(
        ExecutionContract::new(
            active_unknown_caller.clone(),
            generous_limits().max_active_encoded_bytes() as u64,
        ),
        Vec::new(),
    ))
    .unwrap();
    active_unknown_value["execution"]["spec"]["unexpected"] = serde_json::json!(true);
    let active_unknown = turn_injected_checkpoint(
        CheckpointEnvelope::new(
            CHECKPOINT_FORMAT_VERSION,
            ExactBytes::new(serde_json::to_vec(&active_unknown_value).unwrap()),
        ),
        active_unknown_caller.execution_id(),
        active_unknown_caller,
        165,
        &active_unknown_polls,
    )
    .await;

    let terminal_unknown_polls = Cell::new(0);
    let terminal_unknown_caller = execution_spec(execution(166), bytes(b"terminal-unknown"));
    let mut terminal_unknown_value = serde_json::to_value(CheckpointPayload::terminal(
        ExecutionContract::new(
            terminal_unknown_caller.clone(),
            generous_limits().max_active_encoded_bytes() as u64,
        ),
        TerminalOutcome::succeeded(bytes(b"done")),
        0,
    ))
    .unwrap();
    terminal_unknown_value["execution"]["spec"]["unexpected"] = serde_json::json!(true);
    let terminal_unknown = turn_injected_checkpoint(
        CheckpointEnvelope::new(
            CHECKPOINT_FORMAT_VERSION,
            ExactBytes::new(serde_json::to_vec(&terminal_unknown_value).unwrap()),
        ),
        terminal_unknown_caller.execution_id(),
        terminal_unknown_caller,
        166,
        &terminal_unknown_polls,
    )
    .await;

    let nested_history_polls = Cell::new(0);
    let nested_history_caller = execution_spec(execution(167), bytes(b"nested-history"));
    let mut nested_history_value = serde_json::to_value(CheckpointPayload::terminal(
        ExecutionContract::new(
            nested_history_caller.clone(),
            generous_limits().max_active_encoded_bytes() as u64,
        ),
        TerminalOutcome::succeeded(bytes(b"done")),
        0,
    ))
    .unwrap();
    nested_history_value["state"]["outcome"]["activities"] = serde_json::json!([]);
    let nested_history = turn_injected_checkpoint(
        CheckpointEnvelope::new(
            CHECKPOINT_FORMAT_VERSION,
            ExactBytes::new(serde_json::to_vec(&nested_history_value).unwrap()),
        ),
        nested_history_caller.execution_id(),
        nested_history_caller,
        167,
        &nested_history_polls,
    )
    .await;

    ScenarioEvidence::new(
        id,
        "reject changed, missing, inconsistent, or downgraded execution contracts before polling",
        [
            (
                "active and terminal caller-bound mismatches reject before polling",
                matches!(active_created, HostOutcome::ScheduleAccepted { .. })
                    && matches!(
                        active_mismatch,
                        HostOutcome::CheckpointRejected(
                            CheckpointError::TerminalPayloadBoundMismatch { .. }
                        )
                    )
                    && matches!(
                        terminal_mismatch,
                        HostOutcome::CheckpointRejected(
                            CheckpointError::TerminalPayloadBoundMismatch { .. }
                        )
                    )
                    && active_polls.get() == 0
                    && terminal_polls.get() == 0,
            ),
            (
                "missing persisted declaration is malformed and rejected before polling",
                matches!(
                    &missing,
                    HostOutcome::CheckpointRejected(CheckpointError::InvalidJson(_))
                ) && missing_polls.get() == 0,
            ),
            (
                "self-inconsistent admitted capacity rejects active and terminal state",
                matches!(
                    inconsistent_active_result,
                    HostOutcome::CheckpointRejected(
                        CheckpointError::AdmittedTerminalCapacityInsufficient { .. }
                    )
                ) && matches!(
                    inconsistent_terminal_result,
                    HostOutcome::CheckpointRejected(
                        CheckpointError::AdmittedTerminalCapacityInsufficient { .. }
                    )
                ) && inconsistent_active_polls.get() == 0
                    && inconsistent_terminal_polls.get() == 0,
            ),
            (
                "configured capacity downgrade rejects active and terminal state before polling",
                matches!(
                    active_downgrade,
                    HostOutcome::CheckpointRejected(
                        CheckpointError::ConfiguredCapacityBelowAdmission { .. }
                    )
                ) && matches!(
                    terminal_downgrade,
                    HostOutcome::CheckpointRejected(
                        CheckpointError::TerminalEncodedCheckpointCapacityMismatch { .. }
                    )
                ) && active_downgrade_polls.get() == 0
                    && terminal_downgrade_polls.get() == 0,
            ),
            (
                "caller identity and input mismatches reject active and terminal state before polling",
                matches!(
                    active_identity,
                    HostOutcome::CheckpointRejected(CheckpointError::ExecutionMismatch { .. })
                ) && matches!(
                    active_input,
                    HostOutcome::CheckpointRejected(CheckpointError::WorkflowInputMismatch { .. })
                ) && matches!(
                    terminal_identity,
                    HostOutcome::CheckpointRejected(CheckpointError::ExecutionMismatch { .. })
                ) && matches!(
                    terminal_input,
                    HostOutcome::CheckpointRejected(CheckpointError::WorkflowInputMismatch { .. })
                ) && active_identity_polls.get() == 0
                    && active_input_polls.get() == 0
                    && terminal_identity_polls.get() == 0
                    && terminal_input_polls.get() == 0,
            ),
            (
                "malformed declarations reject active and terminal state before polling",
                matches!(
                    &missing,
                    HostOutcome::CheckpointRejected(CheckpointError::InvalidJson(_))
                ) && matches!(
                    terminal_missing,
                    HostOutcome::CheckpointRejected(CheckpointError::InvalidJson(_))
                ) && matches!(
                    active_unknown,
                    HostOutcome::CheckpointRejected(CheckpointError::InvalidJson(_))
                ) && matches!(
                    terminal_unknown,
                    HostOutcome::CheckpointRejected(CheckpointError::InvalidJson(_))
                ) && missing_polls.get() == 0
                    && terminal_missing_polls.get() == 0
                    && active_unknown_polls.get() == 0
                    && terminal_unknown_polls.get() == 0,
            ),
            (
                "terminal state rejects retained active-history fields before polling",
                matches!(
                    mixed_terminal,
                    HostOutcome::CheckpointRejected(CheckpointError::InvalidJson(_))
                ) && matches!(
                    nested_history,
                    HostOutcome::CheckpointRejected(CheckpointError::InvalidJson(_))
                ) && mixed_terminal_polls.get() == 0
                    && nested_history_polls.get() == 0,
            ),
        ],
    )
}

async fn fused_schedule_exposure(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::one("fused", 1, b"command");
    let execution_id = execution(180);
    let execution = execution_spec(execution_id, bytes(b"workflow"));
    let mut durable_host = host(store.clone(), 180);

    let outcome = durable_host
        .turn_and_expose_with(&workflow, execution.clone(), &PreparedPrefixResolver)
        .await;
    let permit_identity = match &outcome {
        HostOutcome::DispatchPermitted { permit, .. } => {
            Some((permit.activity().clone(), permit.attempt_id()))
        }
        _ => None,
    };
    let stored = store.load(execution_id).await.unwrap().unwrap();
    let payload = stored
        .checkpoint()
        .decode_and_validate(&execution, generous_limits())
        .unwrap();
    let exposed = payload
        .active_activities()
        .and_then(|activities| activities.last())
        .is_some_and(|record| {
            permit_identity.as_ref().map(|identity| &identity.0)
                == Some(&record.logical_id(execution_id))
                && matches!(
                    record.state(),
                    ActivityState::DispatchExposed { attempt_id }
                        if permit_identity.as_ref().map(|identity| identity.1) == Some(*attempt_id)
                )
                && record.input().as_slice() == b"prepared:command"
        });

    ScenarioEvidence::new(
        id,
        "fuse initial schedule and dispatch exposure into one accepted checkpoint",
        [
            (
                "one accepted fused boundary returns one exact permit",
                permit_identity.is_some(),
            ),
            (
                "the exact command is dispatch-exposed in authoritative storage before use",
                exposed,
            ),
        ],
    )
}

async fn fused_schedule_exposure_faults(id: ScenarioId) -> ScenarioEvidence {
    let faults = [
        InMemoryFault::FailBeforeRequest(StoreErrorKind::Unavailable),
        InMemoryFault::ConflictWithoutApply,
        InMemoryFault::OutcomeUnknownWithoutApply,
        InMemoryFault::OutcomeUnknownAfterApply,
    ];
    let mut no_false_permits = true;
    let mut boundaries_are_fused = true;
    let mut applied_unknown_quarantines = false;
    let mut unapplied_unknown_keeps_predecessor = false;

    for (index, fault) in faults.into_iter().enumerate() {
        let store = InMemoryCheckpointStore::new();
        store.fail_next_compare_and_swap(fault);
        let workflow = LinearWorkflow::one("fused-fault", 1, b"command");
        let execution_id = execution(181 + index as u8);
        let execution = execution_spec(execution_id, bytes(b"workflow"));
        let mut durable_host = host(store.clone(), 181 + index as u8);
        let outcome = durable_host
            .turn_and_expose_with(&workflow, execution.clone(), &PreparedPrefixResolver)
            .await;
        no_false_permits &= !matches!(&outcome, HostOutcome::DispatchPermitted { .. });
        boundaries_are_fused &= matches!(
            &outcome,
            HostOutcome::ReloadRequired {
                boundary: PersistenceBoundary::ScheduleExposure,
                ..
            } | HostOutcome::StoreFailed {
                operation: StoreOperation::CompareAndSwap(PersistenceBoundary::ScheduleExposure),
                ..
            }
        );
        if fault == InMemoryFault::OutcomeUnknownAfterApply {
            applied_unknown_quarantines = matches!(
                durable_host
                    .turn_and_expose_with(&workflow, execution, &PreparedPrefixResolver)
                    .await,
                HostOutcome::Quarantined { .. }
            );
        } else if fault == InMemoryFault::OutcomeUnknownWithoutApply {
            unapplied_unknown_keeps_predecessor = store.load(execution_id).await.unwrap().is_none();
        }
    }

    ScenarioEvidence::new(
        id,
        "inject every CAS failure class at fused schedule/exposure",
        [
            (
                "no failed or uncertain fused CAS grants a permit",
                no_false_permits,
            ),
            (
                "all failures identify the fused schedule/exposure boundary",
                boundaries_are_fused,
            ),
            (
                "unknown-after-apply reloads as conservative quarantine",
                applied_unknown_quarantines,
            ),
            (
                "unknown-without-apply retains the authoritative predecessor",
                unapplied_unknown_keeps_predecessor,
            ),
        ],
    )
}

async fn fused_observation_next_exposure(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::two();
    let execution_id = execution(186);
    let execution = execution_spec(execution_id, bytes(b"workflow"));
    let mut durable_host = host(store.clone(), 186);
    let first = match durable_host
        .turn_and_expose_with(&workflow, execution.clone(), &PreparedPrefixResolver)
        .await
    {
        HostOutcome::DispatchPermitted { permit, .. } => permit,
        _ => {
            return ScenarioEvidence::new(
                id,
                "prepare first fused effect",
                [("first fused effect was exposed", false)],
            );
        }
    };
    let outcome = durable_host
        .observe_and_turn_with(
            &workflow,
            &execution,
            ActivityObservation::new(first.activity().clone(), bytes(b"one")),
            &PreparedPrefixResolver,
        )
        .await;
    let second = match &outcome {
        HostOutcome::DispatchPermitted { permit, .. } => Some(permit.activity().clone()),
        _ => None,
    };
    let payload = store
        .load(execution_id)
        .await
        .unwrap()
        .unwrap()
        .checkpoint()
        .decode_and_validate(&execution, generous_limits())
        .unwrap();
    let history = payload.active_activities().unwrap();

    ScenarioEvidence::new(
        id,
        "fuse authoritative observation, replay, and next command exposure",
        [
            (
                "one observation CAS returns only the next exact permit",
                second
                    .as_ref()
                    .is_some_and(|activity| activity.sequence() == ActivitySequence::new(1)),
            ),
            (
                "authoritative history contains completed prior result and one exposed tail",
                history.len() == 2
                    && matches!(
                        history[0].state(),
                        ActivityState::Completed { result } if result == &bytes(b"one")
                    )
                    && matches!(history[1].state(), ActivityState::DispatchExposed { .. }),
            ),
        ],
    )
}

async fn fused_observation_terminal(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::one("fused-terminal", 1, b"command");
    let execution_id = execution(187);
    let execution = execution_spec(execution_id, bytes(b"workflow"));
    let mut durable_host = host(store.clone(), 187);
    let permit = match durable_host
        .turn_and_expose_with(&workflow, execution.clone(), &PreparedPrefixResolver)
        .await
    {
        HostOutcome::DispatchPermitted { permit, .. } => permit,
        _ => {
            return ScenarioEvidence::new(
                id,
                "prepare terminal fused effect",
                [("first fused effect was exposed", false)],
            );
        }
    };
    let outcome = durable_host
        .observe_and_turn_with(
            &workflow,
            &execution,
            ActivityObservation::new(permit.activity().clone(), bytes(b"done")),
            &PreparedPrefixResolver,
        )
        .await;
    let stored = store.load(execution_id).await.unwrap().unwrap();
    let payload = stored
        .checkpoint()
        .decode_and_validate(&execution, generous_limits())
        .unwrap();

    ScenarioEvidence::new(
        id,
        "fuse final observation, deterministic replay, and terminal compaction",
        [
            (
                "accepted final observation returns the durable workflow outcome",
                matches!(
                    outcome,
                    HostOutcome::WorkflowCompleted {
                        outcome: TerminalOutcome::Succeeded(ref result),
                        ..
                    } if result == &bytes(b"done")
                ),
            ),
            (
                "authoritative checkpoint is terminal with one completed activity",
                matches!(
                    payload.state(),
                    CheckpointState::Terminal {
                        completed_activity_count: 1,
                        ..
                    }
                ),
            ),
        ],
    )
}

async fn fused_observation_faults(id: ScenarioId) -> ScenarioEvidence {
    let faults = [
        InMemoryFault::FailBeforeRequest(StoreErrorKind::Unavailable),
        InMemoryFault::ConflictWithoutApply,
        InMemoryFault::OutcomeUnknownWithoutApply,
        InMemoryFault::OutcomeUnknownAfterApply,
    ];
    let mut no_false_permits = true;
    let mut boundaries_are_fused = true;
    let mut reloads_quarantine = true;

    for (index, fault) in faults.into_iter().enumerate() {
        let store = InMemoryCheckpointStore::new();
        let workflow = LinearWorkflow::two();
        let execution_id = execution(188 + index as u8);
        let execution = execution_spec(execution_id, bytes(b"workflow"));
        let mut durable_host = host(store.clone(), 188 + index as u8);
        let permit = match durable_host
            .turn_and_expose_with(&workflow, execution.clone(), &PreparedPrefixResolver)
            .await
        {
            HostOutcome::DispatchPermitted { permit, .. } => permit,
            _ => {
                reloads_quarantine = false;
                continue;
            }
        };
        store.fail_next_compare_and_swap(fault);
        let outcome = durable_host
            .observe_and_turn_with(
                &workflow,
                &execution,
                ActivityObservation::new(permit.activity().clone(), bytes(b"one")),
                &PreparedPrefixResolver,
            )
            .await;
        no_false_permits &= !matches!(&outcome, HostOutcome::DispatchPermitted { .. });
        boundaries_are_fused &= matches!(
            &outcome,
            HostOutcome::ReloadRequired {
                boundary: PersistenceBoundary::ObservationProgression,
                ..
            } | HostOutcome::StoreFailed {
                operation: StoreOperation::CompareAndSwap(
                    PersistenceBoundary::ObservationProgression
                ),
                ..
            }
        );
        let reloaded = durable_host
            .turn_and_expose_with(&workflow, execution, &PreparedPrefixResolver)
            .await;
        reloads_quarantine &= match fault {
            InMemoryFault::OutcomeUnknownAfterApply => matches!(
                reloaded,
                HostOutcome::Quarantined { ref activity, .. }
                    if activity.sequence() == ActivitySequence::new(1)
            ),
            _ => matches!(
                reloaded,
                HostOutcome::Quarantined { ref activity, .. }
                    if activity.sequence() == ActivitySequence::new(0)
            ),
        };
    }

    ScenarioEvidence::new(
        id,
        "inject every CAS failure class at fused observation progression",
        [
            (
                "no failed or uncertain observation progression grants the next permit",
                no_false_permits,
            ),
            (
                "all failures identify the fused observation progression boundary",
                boundaries_are_fused,
            ),
            (
                "reload conservatively quarantines original or applied next exposure",
                reloads_quarantine,
            ),
        ],
    )
}

async fn fused_terminal_faults(id: ScenarioId) -> ScenarioEvidence {
    let faults = [
        InMemoryFault::FailBeforeRequest(StoreErrorKind::Unavailable),
        InMemoryFault::ConflictWithoutApply,
        InMemoryFault::OutcomeUnknownWithoutApply,
        InMemoryFault::OutcomeUnknownAfterApply,
    ];
    let mut no_false_permits_or_completion = true;
    let mut boundaries_are_fused = true;
    let mut reload_classification_is_exact = true;

    for (index, fault) in faults.into_iter().enumerate() {
        let store = InMemoryCheckpointStore::new();
        let workflow = LinearWorkflow::one("fused-terminal-fault", 1, b"command");
        let execution_id = execution(193 + index as u8);
        let execution = execution_spec(execution_id, bytes(b"workflow"));
        let mut durable_host = host(store.clone(), 193 + index as u8);
        let permit = match durable_host
            .turn_and_expose_with(&workflow, execution.clone(), &PreparedPrefixResolver)
            .await
        {
            HostOutcome::DispatchPermitted { permit, .. } => permit,
            _ => {
                reload_classification_is_exact = false;
                continue;
            }
        };
        store.fail_next_compare_and_swap(fault);
        let outcome = durable_host
            .observe_and_turn_with(
                &workflow,
                &execution,
                ActivityObservation::new(permit.activity().clone(), bytes(b"done")),
                &PreparedPrefixResolver,
            )
            .await;
        no_false_permits_or_completion &= !matches!(
            &outcome,
            HostOutcome::DispatchPermitted { .. } | HostOutcome::WorkflowCompleted { .. }
        );
        boundaries_are_fused &= matches!(
            &outcome,
            HostOutcome::ReloadRequired {
                boundary: PersistenceBoundary::ObservationProgression,
                ..
            } | HostOutcome::StoreFailed {
                operation: StoreOperation::CompareAndSwap(
                    PersistenceBoundary::ObservationProgression
                ),
                ..
            }
        );
        let reloaded = durable_host
            .turn_and_expose_with(&workflow, execution, &PreparedPrefixResolver)
            .await;
        reload_classification_is_exact &= match fault {
            InMemoryFault::OutcomeUnknownAfterApply => matches!(
                reloaded,
                HostOutcome::WorkflowCompleted {
                    outcome: TerminalOutcome::Succeeded(ref result),
                    ..
                } if result == &bytes(b"done")
            ),
            _ => matches!(
                reloaded,
                HostOutcome::Quarantined { ref activity, .. }
                    if activity.sequence() == ActivitySequence::new(0)
            ),
        };
    }

    ScenarioEvidence::new(
        id,
        "inject every CAS failure class at fused observation/terminal compaction",
        [
            (
                "failed or uncertain terminal fusion reports neither permit nor completion",
                no_false_permits_or_completion,
            ),
            (
                "terminal fusion failures identify observation progression",
                boundaries_are_fused,
            ),
            (
                "only unknown-after-apply reloads terminal; all unapplied cases quarantine original exposure",
                reload_classification_is_exact,
            ),
        ],
    )
}

async fn fused_capacity_reservation(id: ScenarioId) -> ScenarioEvidence {
    let store = InMemoryCheckpointStore::new();
    let workflow = LinearWorkflow::one_with_bound("fused-huge", 1, b"command", u64::MAX);
    let execution_id = execution(192);
    let mut durable_host = host(store.clone(), 192);
    let outcome = durable_host
        .turn_and_expose_with(
            &workflow,
            execution_spec(execution_id, bytes(b"workflow")),
            &PreparedPrefixResolver,
        )
        .await;

    ScenarioEvidence::new(
        id,
        "reserve maximum result capacity before fused exposure",
        [
            (
                "unrepresentable result capacity rejects fused exposure",
                matches!(&outcome, HostOutcome::CheckpointRejected(_)),
            ),
            (
                "capacity rejection grants no permit and accepts no checkpoint",
                !matches!(&outcome, HostOutcome::DispatchPermitted { .. })
                    && store.load(execution_id).await.unwrap().is_none(),
            ),
        ],
    )
}

async fn base64_exact_bytes(id: ScenarioId) -> ScenarioEvidence {
    let representative: Vec<u8> = (0..=255).collect();
    let exact = ExactBytes::new(representative.clone());
    let encoded = serde_json::to_vec(&exact).unwrap();
    let decoded = serde_json::from_slice::<ExactBytes>(&encoded).ok();
    let integer_array = serde_json::to_vec(&representative).unwrap();
    let invalid_rejected = serde_json::from_str::<ExactBytes>(r#""not base64!""#).is_err();

    ScenarioEvidence::new(
        id,
        "round-trip representative exact bytes through compact validated base64 JSON",
        [
            (
                "base64 JSON round-trips exact binary equality",
                decoded == Some(exact),
            ),
            ("invalid base64 is rejected", invalid_rejected),
            (
                "representative base64 JSON is less than half the integer-array JSON",
                encoded.len() * 2 < integer_array.len(),
            ),
        ],
    )
    .mark_conformance_only("representative base64 JSON is less than half the integer-array JSON")
}

use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

struct Cell(AtomicUsize);

impl Cell {
    fn new(value: usize) -> Self {
        Self(AtomicUsize::new(value))
    }

    fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }

    fn set(&self, value: usize) {
        self.0.store(value, Ordering::Relaxed);
    }
}
