use crate::{
    ActivityFailure, ActivityRecord, ActivityState, AttemptId, CasOutcome, CheckpointEnvelope,
    CheckpointError, CheckpointLimits, CheckpointPayload, CheckpointStore, CompletionMetadata,
    EffectObservationDisposition, Evaluation, ExactBytes, ExecutionId, ExecutionSpec, HostEpoch,
    LogicalActivityId, Nondeterminism, PreparedActivityResolver, PreparedCommand,
    PreparedEffectResolver, StorageRevision, StoreError, TerminalOutcome, Workflow, evaluate,
    evaluate_effects, evaluate_prepared,
};

/// The persistence boundary that must be reloaded after an uncertain CAS result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersistenceBoundary {
    Schedule,
    Exposure,
    ScheduleExposure,
    Observation,
    ObservationProgression,
    Retry,
    Wait,
    Completion,
}

/// Why the caller must reload instead of acting on a proposal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReloadReason {
    Conflict,
    OutcomeUnknown,
}

/// Provider operation that failed before its result could be classified.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreOperation {
    Load,
    CompareAndSwap(PersistenceBoundary),
}

/// Whether a terminal checkpoint was just accepted or authoritatively reloaded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalCheckpointStatus {
    Accepted,
    Reloaded,
}

/// Rejection of an authoritative result observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservationRejection {
    CheckpointMissing,
    ActivityNotExposed,
    LogicalActivityMismatch {
        expected: LogicalActivityId,
        observed: Box<LogicalActivityId>,
    },
    ResultExceedsDeclaredBound {
        actual: u64,
        maximum: u64,
    },
}

/// An authoritative result for one exact logical activity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivityObservation {
    activity: LogicalActivityId,
    result: ExactBytes,
}

impl ActivityObservation {
    pub fn new(activity: LogicalActivityId, result: ExactBytes) -> Self {
        Self { activity, result }
    }

    pub const fn activity(&self) -> &LogicalActivityId {
        &self.activity
    }

    pub const fn result(&self) -> &ExactBytes {
        &self.result
    }
}

/// An authoritative typed-effect observation for one exact persisted attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectObservation {
    activity: LogicalActivityId,
    attempt_id: AttemptId,
    result: ExactBytes,
    failure: Option<ActivityFailure>,
    disposition: EffectObservationDisposition,
}

impl EffectObservation {
    /// Construct an authoritative completed observation from a result encoded
    /// for the activity contract. The host still validates activity identity,
    /// attempt identity, and result bounds against the persisted record.
    pub fn completed(
        activity: LogicalActivityId,
        attempt_id: AttemptId,
        result: ExactBytes,
    ) -> Self {
        Self {
            activity,
            attempt_id,
            result,
            failure: None,
            disposition: EffectObservationDisposition::Completed,
        }
    }

    pub fn completed_failure(
        activity: LogicalActivityId,
        attempt_id: AttemptId,
        failure: ActivityFailure,
    ) -> Self {
        Self {
            activity,
            attempt_id,
            result: ExactBytes::default(),
            failure: Some(failure),
            disposition: EffectObservationDisposition::Completed,
        }
    }

    pub fn proven_no_admission(
        activity: LogicalActivityId,
        attempt_id: AttemptId,
        failure: ActivityFailure,
    ) -> Self {
        Self {
            activity,
            attempt_id,
            result: ExactBytes::default(),
            failure: Some(failure),
            disposition: EffectObservationDisposition::ProvenNoAdmission,
        }
    }

    pub fn from_outcome<E: crate::DurableEffect>(
        activity: LogicalActivityId,
        attempt_id: AttemptId,
        outcome: &crate::EffectOutcome<E::Output>,
    ) -> Result<Self, crate::ActivityCallError> {
        let disposition = match outcome {
            crate::EffectOutcome::ProvenNoAdmission => {
                EffectObservationDisposition::ProvenNoAdmission
            }
            crate::EffectOutcome::Applied(_)
            | crate::EffectOutcome::DomainFailure(_)
            | crate::EffectOutcome::DeadlineExceeded(_)
            | crate::EffectOutcome::UnavailableAtDeadline(_)
            | crate::EffectOutcome::ConflictingEvidence(_) => {
                EffectObservationDisposition::Completed
            }
        };
        Ok(Self {
            activity,
            attempt_id,
            result: crate::encode_activity_result::<crate::EffectActivity<E>>(outcome)?,
            failure: None,
            disposition,
        })
    }

    /// Whether this observation proves that the exposed attempt was not
    /// admitted and is therefore eligible for the single bounded redelivery.
    pub const fn is_proven_no_admission(&self) -> bool {
        matches!(
            self.disposition,
            EffectObservationDisposition::ProvenNoAdmission
        )
    }
}

/// Unforgeable evidence that the dispatch-exposed checkpoint was accepted.
///
/// External callers can inspect a permit but cannot construct one:
///
/// ```compile_fail
/// use kuberic_dex::{
///     ActivityName, ActivitySequence, ActivitySpec, AttemptId, DispatchPermit, ExactBytes,
///     ExecutionId, HostEpoch, LogicalActivityId,
/// };
///
/// let activity = LogicalActivityId::new(
///     ExecutionId::from_bytes([1; 16]),
///     ActivitySequence::new(0),
///     ActivitySpec::new(
///         ActivityName::new("effect", 1).unwrap(),
///         ExactBytes::new(b"input"),
///         1024,
///     ),
/// );
/// let attempt_id = AttemptId::new(HostEpoch::from_bytes([2; 16]), 1).unwrap();
/// let forged = DispatchPermit {
///     activity,
///     attempt_id,
///     prepared_command: None,
/// };
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct DispatchPermit {
    activity: LogicalActivityId,
    attempt_id: AttemptId,
    attempt_ordinal: u32,
    retry_not_before_unix_millis: Option<i64>,
    prepared_command: Option<PreparedCommand>,
}

impl DispatchPermit {
    fn new(
        activity: LogicalActivityId,
        attempt_id: AttemptId,
        attempt_ordinal: u32,
        retry_not_before_unix_millis: Option<i64>,
        prepared_command: Option<PreparedCommand>,
    ) -> Self {
        Self {
            activity,
            attempt_id,
            attempt_ordinal,
            retry_not_before_unix_millis,
            prepared_command,
        }
    }

    pub const fn activity(&self) -> &LogicalActivityId {
        &self.activity
    }

    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    pub const fn attempt_ordinal(&self) -> u32 {
        self.attempt_ordinal
    }

    pub const fn retry_not_before_unix_millis(&self) -> Option<i64> {
        self.retry_not_before_unix_millis
    }

    pub const fn prepared_command(&self) -> Option<&PreparedCommand> {
        self.prepared_command.as_ref()
    }
}

macro_rules! define_host_outcomes {
    ($($variant:ident $body:tt),+ $(,)?) => {
        /// The complete public outcome set for a durable host turn or observation.
        #[derive(Debug, Eq, PartialEq)]
        pub enum HostOutcome {
            $($variant $body),+
        }

        /// Variant names generated from the same declaration as [`HostOutcome`].
        pub const HOST_OUTCOME_VARIANTS: &[&str] = &[$(stringify!($variant)),+];
    };
}

define_host_outcomes! {
    ScheduleAccepted {
        activity: LogicalActivityId,
        revision: StorageRevision,
    },
    DispatchPermitted {
        permit: DispatchPermit,
        revision: StorageRevision,
        boundary: PersistenceBoundary,
    },
    ObservationAccepted {
        activity: LogicalActivityId,
        revision: StorageRevision,
    },
    RetryScheduled {
        activity: LogicalActivityId,
        retry_not_before_unix_millis: i64,
        revision: StorageRevision,
    },
    Waiting {
        activity: LogicalActivityId,
        wake_at_unix_millis: i64,
    },
    WorkflowCompleted {
        outcome: TerminalOutcome,
        completed_activity_count: u64,
        completion_metadata: Option<CompletionMetadata>,
        revision: StorageRevision,
        boundary: PersistenceBoundary,
        checkpoint_status: TerminalCheckpointStatus,
    },
    Quarantined {
        activity: LogicalActivityId,
        attempt_id: AttemptId,
        attempt_ordinal: u32,
        retry_not_before_unix_millis: Option<i64>,
        prepared_command: Option<PreparedCommand>,
        completion_class: Option<crate::CompletionClass>,
    },
    Nondeterminism(Nondeterminism),
    CheckpointRejected(CheckpointError),
    ObservationRejected(ObservationRejection),
    ReloadRequired {
        boundary: PersistenceBoundary,
        reason: ReloadReason,
    },
    StoreFailed {
        operation: StoreOperation,
        error: StoreError,
    },
}

/// Public in-process host for one-turn replay, persistence, and observation.
pub struct DurableHost<S> {
    store: S,
    host_epoch: HostEpoch,
    next_attempt_counter: u64,
    limits: CheckpointLimits,
}

impl<S: CheckpointStore> DurableHost<S> {
    pub fn new(store: S, host_epoch: HostEpoch, limits: CheckpointLimits) -> Self {
        Self {
            store,
            host_epoch,
            next_attempt_counter: 1,
            limits,
        }
    }

    pub const fn store(&self) -> &S {
        &self.store
    }

    pub const fn checkpoint_limits(&self) -> CheckpointLimits {
        self.limits
    }

    /// Evaluate and, when needed, commit exactly one schedule or exposure turn.
    pub async fn turn<W: Workflow>(
        &mut self,
        workflow: &W,
        execution: ExecutionSpec,
    ) -> HostOutcome {
        let execution_id = execution.execution_id();
        let loaded = match self.store.load(execution_id).await {
            Ok(loaded) => loaded,
            Err(error) => {
                return HostOutcome::StoreFailed {
                    operation: StoreOperation::Load,
                    error,
                };
            }
        };
        let expected_revision = loaded.as_ref().map(|stored| stored.revision().clone());
        if let Some(stored) = loaded.as_ref() {
            let payload = match stored
                .checkpoint()
                .decode_and_validate(&execution, self.limits)
            {
                Ok(payload) => payload,
                Err(error) => return HostOutcome::CheckpointRejected(error),
            };
            if let Some((outcome, completed_activity_count)) = payload.terminal_outcome() {
                return terminal_host_outcome(
                    outcome.clone(),
                    completed_activity_count,
                    payload.terminal_completion_metadata(),
                    stored.revision().clone(),
                    PersistenceBoundary::Completion,
                    TerminalCheckpointStatus::Reloaded,
                );
            }
            if let Some(record) = payload
                .active_activities()
                .and_then(|activities| activities.last())
                && let ActivityState::DispatchExposed { attempt_id } = record.state()
            {
                return HostOutcome::Quarantined {
                    activity: record.logical_id(execution_id),
                    attempt_id: *attempt_id,
                    attempt_ordinal: record.attempt().ordinal(),
                    retry_not_before_unix_millis: record.attempt().retry_not_before_unix_millis(),
                    prepared_command: record.prepared_command().cloned(),
                    completion_class: record.completion_class(),
                };
            }
        }
        let evaluation = evaluate(
            workflow,
            &execution,
            loaded.as_ref().map(|stored| stored.checkpoint()),
            self.limits,
        );

        match evaluation {
            Evaluation::Scheduled {
                activity,
                checkpoint,
            } => {
                self.commit_schedule(execution_id, expected_revision, activity, checkpoint)
                    .await
            }
            Evaluation::Pending {
                activity,
                state: ActivityState::Scheduled,
            } => {
                let stored = loaded.expect("pending evaluation requires a loaded checkpoint");
                match self.prepare_exposure(
                    &execution,
                    Some(stored.revision().clone()),
                    stored.checkpoint(),
                    activity,
                ) {
                    Ok(proposal) => {
                        self.commit_exposure(proposal, PersistenceBoundary::Exposure)
                            .await
                    }
                    Err(error) => HostOutcome::CheckpointRejected(error),
                }
            }
            Evaluation::Pending {
                activity,
                state: ActivityState::DispatchExposed { attempt_id },
            } => {
                let (attempt_ordinal, retry_not_before_unix_millis) = loaded
                    .as_ref()
                    .and_then(|stored| {
                        stored
                            .checkpoint()
                            .decode_and_validate(&execution, self.limits)
                            .ok()
                    })
                    .and_then(|payload| {
                        payload
                            .active_activities()
                            .and_then(|activities| activities.last())
                            .map(|record| {
                                (
                                    record.attempt().ordinal(),
                                    record.attempt().retry_not_before_unix_millis(),
                                )
                            })
                    })
                    .expect("pending exposed evaluation requires persisted attempt metadata");
                HostOutcome::Quarantined {
                    activity,
                    attempt_id,
                    attempt_ordinal,
                    retry_not_before_unix_millis,
                    prepared_command: None,
                    completion_class: None,
                }
            }
            Evaluation::Pending {
                state: ActivityState::Completed { .. },
                ..
            } => unreachable!("completed activities are replayed rather than pending"),
            Evaluation::Complete {
                outcome,
                completed_activity_count,
                checkpoint,
            } => {
                self.commit_completion(
                    expected_revision,
                    &execution,
                    outcome,
                    completed_activity_count,
                    checkpoint,
                )
                .await
            }
            Evaluation::Terminal {
                outcome,
                completed_activity_count,
            } => {
                let stored = loaded.expect("terminal evaluation requires a loaded checkpoint");
                HostOutcome::WorkflowCompleted {
                    outcome,
                    completed_activity_count,
                    completion_metadata: None,
                    revision: stored.revision().clone(),
                    boundary: PersistenceBoundary::Completion,
                    checkpoint_status: TerminalCheckpointStatus::Reloaded,
                }
            }
            Evaluation::Nondeterminism(error) => HostOutcome::Nondeterminism(error),
            Evaluation::CheckpointRejected(error) => HostOutcome::CheckpointRejected(error),
            Evaluation::PreparationRejected(error) => {
                HostOutcome::CheckpointRejected(CheckpointError::PreparedActivityRejected(error))
            }
            Evaluation::WorkflowStalled => {
                HostOutcome::Nondeterminism(Nondeterminism::UnsupportedSuspension)
            }
        }
    }

    /// Evaluate and atomically persist the next activity as dispatch-exposed.
    ///
    /// Unlike [`Self::turn`], a newly scheduled activity does not require an
    /// intermediate accepted schedule checkpoint. The exact command and its
    /// result reservation are part of the single exposed checkpoint, and a
    /// permit is created only after that CAS is accepted.
    pub async fn turn_and_expose<W: Workflow>(
        &mut self,
        workflow: &W,
        execution: ExecutionSpec,
    ) -> HostOutcome {
        self.turn_and_expose_with(workflow, execution, &crate::IdentityActivityResolver)
            .await
    }

    /// Evaluate an ordinary activity while enforcing persisted retry
    /// not-before and action-deadline wakeups against a caller-supplied
    /// deterministic clock.
    pub async fn turn_and_expose_at<W: Workflow>(
        &mut self,
        workflow: &W,
        execution: ExecutionSpec,
        now_unix_millis: i64,
    ) -> HostOutcome {
        self.turn_and_expose_resolved(
            workflow,
            execution,
            ExposureResolver::Activity(&crate::IdentityActivityResolver),
            Some(now_unix_millis),
        )
        .await
    }

    /// Evaluate and atomically expose an activity resolved to an exact prepared
    /// specification.
    pub async fn turn_and_expose_with<W: Workflow>(
        &mut self,
        workflow: &W,
        execution: ExecutionSpec,
        resolver: &dyn PreparedActivityResolver,
    ) -> HostOutcome {
        self.turn_and_expose_resolved(
            workflow,
            execution,
            ExposureResolver::Activity(resolver),
            None,
        )
        .await
    }

    /// Evaluate and atomically expose a typed effect whose exact command is
    /// prepared separately from its logical request.
    pub async fn turn_and_expose_effects<W: Workflow>(
        &mut self,
        workflow: &W,
        execution: ExecutionSpec,
        resolver: &dyn PreparedEffectResolver,
    ) -> HostOutcome {
        self.turn_and_expose_resolved(
            workflow,
            execution,
            ExposureResolver::Effect(resolver),
            None,
        )
        .await
    }

    async fn turn_and_expose_resolved<W: Workflow>(
        &mut self,
        workflow: &W,
        execution: ExecutionSpec,
        resolver: ExposureResolver<'_>,
        now_unix_millis: Option<i64>,
    ) -> HostOutcome {
        let execution_id = execution.execution_id();
        let loaded = match self.store.load(execution_id).await {
            Ok(loaded) => loaded,
            Err(error) => {
                return HostOutcome::StoreFailed {
                    operation: StoreOperation::Load,
                    error,
                };
            }
        };
        let expected_revision = loaded.as_ref().map(|stored| stored.revision().clone());
        if let Some(stored) = loaded.as_ref() {
            let payload = match stored
                .checkpoint()
                .decode_and_validate(&execution, self.limits)
            {
                Ok(payload) => payload,
                Err(error) => return HostOutcome::CheckpointRejected(error),
            };
            if let Some((outcome, completed_activity_count)) = payload.terminal_outcome() {
                return terminal_host_outcome(
                    outcome.clone(),
                    completed_activity_count,
                    payload.terminal_completion_metadata(),
                    stored.revision().clone(),
                    PersistenceBoundary::Completion,
                    TerminalCheckpointStatus::Reloaded,
                );
            }
            if let Some(record) = payload
                .active_activities()
                .and_then(|activities| activities.last())
                && let ActivityState::DispatchExposed { attempt_id } = record.state()
            {
                return HostOutcome::Quarantined {
                    activity: record.logical_id(execution_id),
                    attempt_id: *attempt_id,
                    attempt_ordinal: record.attempt().ordinal(),
                    retry_not_before_unix_millis: record.attempt().retry_not_before_unix_millis(),
                    prepared_command: record.prepared_command().cloned(),
                    completion_class: record.completion_class(),
                };
            }
        }
        let checkpoint = loaded.as_ref().map(|stored| stored.checkpoint());
        if let (Some(now), Some(stored)) = (now_unix_millis, loaded.as_ref())
            && let Ok(payload) = stored
                .checkpoint()
                .decode_and_validate(&execution, self.limits)
            && let Some(record) = payload
                .active_activities()
                .and_then(|activities| activities.last())
            && matches!(record.state(), ActivityState::Scheduled)
        {
            let attempt = record.attempt();
            let gated_wakeup = [
                attempt.retry_not_before_unix_millis(),
                attempt.handler_wait_until_unix_millis(),
            ]
            .into_iter()
            .flatten()
            .filter(|wake| *wake > now)
            .max();
            if let Some(gated_wakeup) = gated_wakeup {
                let wake_at_unix_millis = record
                    .spec()
                    .options()
                    .action_deadline_unix_millis()
                    .map_or(gated_wakeup, |deadline| deadline.min(gated_wakeup));
                return HostOutcome::Waiting {
                    activity: record.logical_id(execution_id),
                    wake_at_unix_millis,
                };
            }
        }
        let evaluation = match resolver {
            ExposureResolver::Activity(resolver) => {
                evaluate_prepared(workflow, &execution, checkpoint, self.limits, resolver)
            }
            ExposureResolver::Effect(resolver) => {
                evaluate_effects(workflow, &execution, checkpoint, self.limits, resolver)
            }
        };
        match evaluation {
            Evaluation::Scheduled {
                activity,
                checkpoint,
            } => {
                match self.prepare_exposure(&execution, expected_revision, &checkpoint, activity) {
                    Ok(proposal) => {
                        self.commit_exposure(proposal, PersistenceBoundary::ScheduleExposure)
                            .await
                    }
                    Err(error) => HostOutcome::CheckpointRejected(error),
                }
            }
            Evaluation::Pending {
                activity,
                state: ActivityState::Scheduled,
            } => {
                let stored = loaded.expect("pending evaluation requires a loaded checkpoint");
                match self.prepare_exposure(
                    &execution,
                    Some(stored.revision().clone()),
                    stored.checkpoint(),
                    activity,
                ) {
                    Ok(proposal) => {
                        self.commit_exposure(proposal, PersistenceBoundary::ScheduleExposure)
                            .await
                    }
                    Err(error) => HostOutcome::CheckpointRejected(error),
                }
            }
            Evaluation::Pending {
                activity,
                state: ActivityState::DispatchExposed { attempt_id },
            } => {
                let (
                    attempt_ordinal,
                    retry_not_before_unix_millis,
                    prepared_command,
                    completion_class,
                ) = loaded
                    .as_ref()
                    .and_then(|stored| {
                        stored
                            .checkpoint()
                            .decode_and_validate(&execution, self.limits)
                            .ok()
                    })
                    .and_then(|payload| {
                        payload
                            .active_activities()
                            .and_then(|activities| activities.last())
                            .map(|record| {
                                (
                                    record.attempt().ordinal(),
                                    record.attempt().retry_not_before_unix_millis(),
                                    record.prepared_command().cloned(),
                                    record.completion_class(),
                                )
                            })
                    })
                    .expect("pending exposed evaluation requires persisted activity metadata");
                HostOutcome::Quarantined {
                    activity,
                    attempt_id,
                    attempt_ordinal,
                    retry_not_before_unix_millis,
                    prepared_command,
                    completion_class,
                }
            }
            Evaluation::Pending {
                state: ActivityState::Completed { .. },
                ..
            } => unreachable!("completed activities are replayed rather than pending"),
            Evaluation::Complete {
                outcome,
                completed_activity_count,
                checkpoint,
            } => {
                self.commit_completion(
                    expected_revision,
                    &execution,
                    outcome,
                    completed_activity_count,
                    checkpoint,
                )
                .await
            }
            Evaluation::Terminal {
                outcome,
                completed_activity_count,
            } => {
                let stored = loaded.expect("terminal evaluation requires a loaded checkpoint");
                HostOutcome::WorkflowCompleted {
                    outcome,
                    completed_activity_count,
                    completion_metadata: None,
                    revision: stored.revision().clone(),
                    boundary: PersistenceBoundary::Completion,
                    checkpoint_status: TerminalCheckpointStatus::Reloaded,
                }
            }
            Evaluation::Nondeterminism(error) => HostOutcome::Nondeterminism(error),
            Evaluation::CheckpointRejected(error) => HostOutcome::CheckpointRejected(error),
            Evaluation::PreparationRejected(error) => {
                HostOutcome::CheckpointRejected(CheckpointError::PreparedActivityRejected(error))
            }
            Evaluation::WorkflowStalled => {
                HostOutcome::Nondeterminism(Nondeterminism::UnsupportedSuspension)
            }
        }
    }

    /// Persist an authoritative result only for the currently exposed activity.
    pub async fn observe(
        &self,
        execution: &ExecutionSpec,
        observation: ActivityObservation,
    ) -> HostOutcome {
        let execution_id = execution.execution_id();
        let loaded = match self.store.load(execution_id).await {
            Ok(loaded) => loaded,
            Err(error) => {
                return HostOutcome::StoreFailed {
                    operation: StoreOperation::Load,
                    error,
                };
            }
        };
        let Some(stored) = loaded else {
            return HostOutcome::ObservationRejected(ObservationRejection::CheckpointMissing);
        };
        let mut payload = match stored
            .checkpoint()
            .decode_and_validate(execution, self.limits)
        {
            Ok(payload) => payload,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        let Some(record) = payload
            .active_activities()
            .and_then(|activities| activities.last())
            .cloned()
        else {
            return HostOutcome::ObservationRejected(ObservationRejection::ActivityNotExposed);
        };
        let expected_activity = record.logical_id(execution_id);
        if expected_activity != *observation.activity() {
            return HostOutcome::ObservationRejected(
                ObservationRejection::LogicalActivityMismatch {
                    expected: expected_activity,
                    observed: Box::new(observation.activity),
                },
            );
        }
        if !matches!(record.state(), ActivityState::DispatchExposed { .. }) {
            return HostOutcome::ObservationRejected(ObservationRejection::ActivityNotExposed);
        }
        let actual_result_bytes = match u64::try_from(observation.result().as_slice().len()) {
            Ok(actual) => actual,
            Err(_) => {
                return HostOutcome::ObservationRejected(
                    ObservationRejection::ResultExceedsDeclaredBound {
                        actual: u64::MAX,
                        maximum: record.max_result_bytes(),
                    },
                );
            }
        };
        if actual_result_bytes > record.max_result_bytes() {
            return HostOutcome::ObservationRejected(
                ObservationRejection::ResultExceedsDeclaredBound {
                    actual: actual_result_bytes,
                    maximum: record.max_result_bytes(),
                },
            );
        }

        let activity = expected_activity;
        replace_final_record(
            &mut payload,
            record.with_state(ActivityState::Completed {
                result: observation.result,
            }),
        );
        let checkpoint = match CheckpointEnvelope::encode_with_limits(&payload, self.limits) {
            Ok(checkpoint) => checkpoint,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        match self
            .store
            .compare_and_swap(execution_id, Some(stored.revision().clone()), checkpoint)
            .await
        {
            Ok(CasOutcome::Accepted(revision)) => {
                HostOutcome::ObservationAccepted { activity, revision }
            }
            Ok(other) => reload_outcome(PersistenceBoundary::Observation, other),
            Err(error) => store_failed(PersistenceBoundary::Observation, error),
        }
    }

    /// Persist another physical attempt for an exposed ordinary activity.
    ///
    /// The caller may use this for a retryable application failure or a
    /// crash/lost-result recovery decision. Codec, registration,
    /// nondeterminism, storage, and timeout failures must not call this API.
    pub async fn schedule_retry(
        &self,
        execution: &ExecutionSpec,
        activity: &LogicalActivityId,
        observed_at_unix_millis: i64,
    ) -> HostOutcome {
        let execution_id = execution.execution_id();
        let loaded = match self.store.load(execution_id).await {
            Ok(loaded) => loaded,
            Err(error) => {
                return HostOutcome::StoreFailed {
                    operation: StoreOperation::Load,
                    error,
                };
            }
        };
        let Some(stored) = loaded else {
            return HostOutcome::ObservationRejected(ObservationRejection::CheckpointMissing);
        };
        let mut payload = match stored
            .checkpoint()
            .decode_and_validate(execution, self.limits)
        {
            Ok(payload) => payload,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        let Some(record) = payload
            .active_activities()
            .and_then(|activities| activities.last())
            .cloned()
        else {
            return HostOutcome::ObservationRejected(ObservationRejection::ActivityNotExposed);
        };
        let expected = record.logical_id(execution_id);
        if &expected != activity {
            return HostOutcome::ObservationRejected(
                ObservationRejection::LogicalActivityMismatch {
                    expected,
                    observed: Box::new(activity.clone()),
                },
            );
        }
        if !matches!(record.state(), ActivityState::DispatchExposed { .. }) {
            return HostOutcome::ObservationRejected(ObservationRejection::ActivityNotExposed);
        }
        let Some(retry_not_before_unix_millis) = record
            .spec()
            .options()
            .retry_not_before_unix_millis(record.attempt().ordinal(), observed_at_unix_millis)
        else {
            return HostOutcome::CheckpointRejected(
                CheckpointError::ActivityAttemptLimitExceeded {
                    sequence: record.sequence(),
                    attempted: record.attempt().ordinal().saturating_add(1),
                    maximum: record.spec().options().max_attempts(),
                },
            );
        };
        let retried = match record.schedule_retry(retry_not_before_unix_millis) {
            Ok(record) => record,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        replace_final_record(&mut payload, retried);
        let checkpoint = match CheckpointEnvelope::encode_with_limits(&payload, self.limits) {
            Ok(checkpoint) => checkpoint,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        match self
            .store
            .compare_and_swap(execution_id, Some(stored.revision().clone()), checkpoint)
            .await
        {
            Ok(CasOutcome::Accepted(revision)) => HostOutcome::RetryScheduled {
                activity: activity.clone(),
                retry_not_before_unix_millis,
                revision,
            },
            Ok(other) => reload_outcome(PersistenceBoundary::Retry, other),
            Err(error) => store_failed(PersistenceBoundary::Retry, error),
        }
    }

    /// Persist a terminal ordinary activity failure for replay through the
    /// workflow's normal `Result` value.
    pub async fn observe_failure(
        &self,
        execution: &ExecutionSpec,
        activity: &LogicalActivityId,
        failure: ActivityFailure,
    ) -> HostOutcome {
        let execution_id = execution.execution_id();
        let loaded = match self.store.load(execution_id).await {
            Ok(loaded) => loaded,
            Err(error) => {
                return HostOutcome::StoreFailed {
                    operation: StoreOperation::Load,
                    error,
                };
            }
        };
        let Some(stored) = loaded else {
            return HostOutcome::ObservationRejected(ObservationRejection::CheckpointMissing);
        };
        let mut payload = match stored
            .checkpoint()
            .decode_and_validate(execution, self.limits)
        {
            Ok(payload) => payload,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        let Some(record) = payload
            .active_activities()
            .and_then(|activities| activities.last())
            .cloned()
        else {
            return HostOutcome::ObservationRejected(ObservationRejection::ActivityNotExposed);
        };
        let expected = record.logical_id(execution_id);
        if &expected != activity {
            return HostOutcome::ObservationRejected(
                ObservationRejection::LogicalActivityMismatch {
                    expected,
                    observed: Box::new(activity.clone()),
                },
            );
        }
        if !matches!(record.state(), ActivityState::DispatchExposed { .. }) {
            return HostOutcome::ObservationRejected(ObservationRejection::ActivityNotExposed);
        }
        let actual = failure.payload().map_or(0, |payload| {
            u64::try_from(payload.as_slice().len()).unwrap_or(u64::MAX)
        });
        if actual > record.max_result_bytes() {
            return HostOutcome::ObservationRejected(
                ObservationRejection::ResultExceedsDeclaredBound {
                    actual,
                    maximum: record.max_result_bytes(),
                },
            );
        }
        replace_final_record(&mut payload, record.with_failure(failure));
        let checkpoint = match CheckpointEnvelope::encode_with_limits(&payload, self.limits) {
            Ok(checkpoint) => checkpoint,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        match self
            .store
            .compare_and_swap(execution_id, Some(stored.revision().clone()), checkpoint)
            .await
        {
            Ok(CasOutcome::Accepted(revision)) => HostOutcome::ObservationAccepted {
                activity: activity.clone(),
                revision,
            },
            Ok(other) => reload_outcome(PersistenceBoundary::Observation, other),
            Err(error) => store_failed(PersistenceBoundary::Observation, error),
        }
    }

    /// Persist a handler-requested observation wait without consuming a retry.
    pub async fn defer_wait(
        &self,
        execution: &ExecutionSpec,
        activity: &LogicalActivityId,
        wait_until_unix_millis: i64,
    ) -> HostOutcome {
        let execution_id = execution.execution_id();
        let loaded = match self.store.load(execution_id).await {
            Ok(loaded) => loaded,
            Err(error) => {
                return HostOutcome::StoreFailed {
                    operation: StoreOperation::Load,
                    error,
                };
            }
        };
        let Some(stored) = loaded else {
            return HostOutcome::ObservationRejected(ObservationRejection::CheckpointMissing);
        };
        let mut payload = match stored
            .checkpoint()
            .decode_and_validate(execution, self.limits)
        {
            Ok(payload) => payload,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        let Some(record) = payload
            .active_activities()
            .and_then(|activities| activities.last())
            .cloned()
        else {
            return HostOutcome::ObservationRejected(ObservationRejection::ActivityNotExposed);
        };
        let expected = record.logical_id(execution_id);
        if &expected != activity {
            return HostOutcome::ObservationRejected(
                ObservationRejection::LogicalActivityMismatch {
                    expected,
                    observed: Box::new(activity.clone()),
                },
            );
        }
        let deferred = match record.defer_wait(wait_until_unix_millis) {
            Ok(record) => record,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        replace_final_record(&mut payload, deferred);
        let checkpoint = match CheckpointEnvelope::encode_with_limits(&payload, self.limits) {
            Ok(checkpoint) => checkpoint,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        match self
            .store
            .compare_and_swap(execution_id, Some(stored.revision().clone()), checkpoint)
            .await
        {
            Ok(CasOutcome::Accepted(_)) => HostOutcome::Waiting {
                activity: activity.clone(),
                wake_at_unix_millis: wait_until_unix_millis,
            },
            Ok(other) => reload_outcome(PersistenceBoundary::Wait, other),
            Err(error) => store_failed(PersistenceBoundary::Wait, error),
        }
    }

    /// Persist an authoritative effect observation for the exact exposed
    /// attempt. Proven non-admission reopens the same logical activity once;
    /// all other observations complete it.
    pub async fn observe_effect(
        &self,
        execution: &ExecutionSpec,
        observation: EffectObservation,
    ) -> HostOutcome {
        self.observe_effect_with_redelivery(execution, observation, true)
            .await
    }

    /// Resolve a quarantined attempt from observation only. Even proven
    /// non-admission completes the logical call because dispatch uncertainty
    /// permanently removes redelivery authority.
    pub async fn observe_quarantined_effect(
        &self,
        execution: &ExecutionSpec,
        observation: EffectObservation,
    ) -> HostOutcome {
        self.observe_effect_with_redelivery(execution, observation, false)
            .await
    }

    async fn observe_effect_with_redelivery(
        &self,
        execution: &ExecutionSpec,
        mut observation: EffectObservation,
        allow_redelivery: bool,
    ) -> HostOutcome {
        if !allow_redelivery {
            observation.disposition = EffectObservationDisposition::Completed;
        }
        let execution_id = execution.execution_id();
        let loaded = match self.store.load(execution_id).await {
            Ok(loaded) => loaded,
            Err(error) => {
                return HostOutcome::StoreFailed {
                    operation: StoreOperation::Load,
                    error,
                };
            }
        };
        let Some(stored) = loaded else {
            return HostOutcome::ObservationRejected(ObservationRejection::CheckpointMissing);
        };
        let mut payload = match stored
            .checkpoint()
            .decode_and_validate(execution, self.limits)
        {
            Ok(payload) => payload,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        let Some(record) = payload
            .active_activities()
            .and_then(|activities| activities.last())
            .cloned()
        else {
            return HostOutcome::ObservationRejected(ObservationRejection::ActivityNotExposed);
        };
        let expected_activity = record.logical_id(execution_id);
        if expected_activity != observation.activity {
            return HostOutcome::ObservationRejected(
                ObservationRejection::LogicalActivityMismatch {
                    expected: expected_activity,
                    observed: Box::new(observation.activity),
                },
            );
        }
        if !matches!(
            record.state(),
            ActivityState::DispatchExposed { attempt_id }
                if *attempt_id == observation.attempt_id
        ) {
            return HostOutcome::ObservationRejected(ObservationRejection::ActivityNotExposed);
        }
        let actual_result_bytes =
            u64::try_from(observation.result.as_slice().len()).unwrap_or(u64::MAX);
        let actual_failure_bytes = observation
            .failure
            .as_ref()
            .and_then(ActivityFailure::payload)
            .map_or(0, |payload| {
                u64::try_from(payload.as_slice().len()).unwrap_or(u64::MAX)
            });
        let actual_bytes = actual_result_bytes.max(actual_failure_bytes);
        if actual_bytes > record.max_result_bytes() {
            return HostOutcome::ObservationRejected(
                ObservationRejection::ResultExceedsDeclaredBound {
                    actual: actual_bytes,
                    maximum: record.max_result_bytes(),
                },
            );
        }
        let activity = expected_activity;
        let replacement = match record.observe_effect_attempt(
            observation.attempt_id,
            observation.disposition,
            observation.result,
            observation.failure,
        ) {
            Ok(record) => record,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        replace_final_record(&mut payload, replacement);
        let checkpoint = match CheckpointEnvelope::encode_with_limits(&payload, self.limits) {
            Ok(checkpoint) => checkpoint,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        match self
            .store
            .compare_and_swap(execution_id, Some(stored.revision().clone()), checkpoint)
            .await
        {
            Ok(CasOutcome::Accepted(revision)) => {
                HostOutcome::ObservationAccepted { activity, revision }
            }
            Ok(other) => reload_outcome(PersistenceBoundary::Observation, other),
            Err(error) => store_failed(PersistenceBoundary::Observation, error),
        }
    }

    /// Atomically persist an observation and replay to the next exposed
    /// activity or terminal checkpoint.
    ///
    /// No intermediate completed checkpoint is accepted. A next-effect permit
    /// is returned only when the CAS containing both the completed result and
    /// exact next exposed command is accepted.
    pub async fn observe_and_turn<W: Workflow>(
        &mut self,
        workflow: &W,
        execution: &ExecutionSpec,
        observation: ActivityObservation,
    ) -> HostOutcome {
        self.observe_and_turn_with(
            workflow,
            execution,
            observation,
            &crate::IdentityActivityResolver,
        )
        .await
    }

    /// Persist an observation and resolve the next activity to an exact
    /// prepared specification in the same accepted checkpoint.
    pub async fn observe_and_turn_with<W: Workflow>(
        &mut self,
        workflow: &W,
        execution: &ExecutionSpec,
        observation: ActivityObservation,
        resolver: &dyn PreparedActivityResolver,
    ) -> HostOutcome {
        let execution_id = execution.execution_id();
        let loaded = match self.store.load(execution_id).await {
            Ok(loaded) => loaded,
            Err(error) => {
                return HostOutcome::StoreFailed {
                    operation: StoreOperation::Load,
                    error,
                };
            }
        };
        let Some(stored) = loaded else {
            return HostOutcome::ObservationRejected(ObservationRejection::CheckpointMissing);
        };
        let mut payload = match stored
            .checkpoint()
            .decode_and_validate(execution, self.limits)
        {
            Ok(payload) => payload,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        let Some(record) = payload
            .active_activities()
            .and_then(|activities| activities.last())
            .cloned()
        else {
            return HostOutcome::ObservationRejected(ObservationRejection::ActivityNotExposed);
        };
        let expected_activity = record.logical_id(execution_id);
        if expected_activity != *observation.activity() {
            return HostOutcome::ObservationRejected(
                ObservationRejection::LogicalActivityMismatch {
                    expected: expected_activity,
                    observed: Box::new(observation.activity),
                },
            );
        }
        if !matches!(record.state(), ActivityState::DispatchExposed { .. }) {
            return HostOutcome::ObservationRejected(ObservationRejection::ActivityNotExposed);
        }
        let actual_result_bytes =
            u64::try_from(observation.result().as_slice().len()).unwrap_or(u64::MAX);
        if actual_result_bytes > record.max_result_bytes() {
            return HostOutcome::ObservationRejected(
                ObservationRejection::ResultExceedsDeclaredBound {
                    actual: actual_result_bytes,
                    maximum: record.max_result_bytes(),
                },
            );
        }

        let attempt_ordinal = record.attempt().ordinal();
        let retry_not_before_unix_millis = record.attempt().retry_not_before_unix_millis();
        replace_final_record(
            &mut payload,
            record.with_state(ActivityState::Completed {
                result: observation.result,
            }),
        );
        let completed_checkpoint =
            match CheckpointEnvelope::encode_with_limits(&payload, self.limits) {
                Ok(checkpoint) => checkpoint,
                Err(error) => return HostOutcome::CheckpointRejected(error),
            };
        let expected_revision = Some(stored.revision().clone());
        match evaluate_prepared(
            workflow,
            execution,
            Some(&completed_checkpoint),
            self.limits,
            resolver,
        ) {
            Evaluation::Scheduled {
                activity,
                checkpoint,
            } => match self.prepare_exposure(execution, expected_revision, &checkpoint, activity) {
                Ok(proposal) => {
                    self.commit_exposure(proposal, PersistenceBoundary::ObservationProgression)
                        .await
                }
                Err(error) => HostOutcome::CheckpointRejected(error),
            },
            Evaluation::Complete {
                outcome,
                completed_activity_count,
                checkpoint,
            } => {
                self.commit_completion_at_boundary(
                    expected_revision,
                    execution,
                    outcome,
                    completed_activity_count,
                    checkpoint,
                    PersistenceBoundary::ObservationProgression,
                )
                .await
            }
            Evaluation::Pending {
                activity,
                state: ActivityState::DispatchExposed { attempt_id },
            } => HostOutcome::Quarantined {
                activity,
                attempt_id,
                attempt_ordinal,
                retry_not_before_unix_millis,
                prepared_command: None,
                completion_class: None,
            },
            Evaluation::Pending { .. } => {
                HostOutcome::Nondeterminism(Nondeterminism::UnsupportedSuspension)
            }
            Evaluation::Terminal {
                outcome,
                completed_activity_count,
            } => HostOutcome::WorkflowCompleted {
                outcome,
                completed_activity_count,
                completion_metadata: None,
                revision: stored.revision().clone(),
                boundary: PersistenceBoundary::Completion,
                checkpoint_status: TerminalCheckpointStatus::Reloaded,
            },
            Evaluation::Nondeterminism(error) => HostOutcome::Nondeterminism(error),
            Evaluation::CheckpointRejected(error) => HostOutcome::CheckpointRejected(error),
            Evaluation::PreparationRejected(error) => {
                HostOutcome::CheckpointRejected(CheckpointError::PreparedActivityRejected(error))
            }
            Evaluation::WorkflowStalled => {
                HostOutcome::Nondeterminism(Nondeterminism::UnsupportedSuspension)
            }
        }
    }

    async fn commit_schedule(
        &self,
        execution_id: ExecutionId,
        expected_revision: Option<StorageRevision>,
        activity: LogicalActivityId,
        checkpoint: CheckpointEnvelope,
    ) -> HostOutcome {
        match self
            .store
            .compare_and_swap(execution_id, expected_revision, checkpoint)
            .await
        {
            Ok(CasOutcome::Accepted(revision)) => {
                HostOutcome::ScheduleAccepted { activity, revision }
            }
            Ok(other) => reload_outcome(PersistenceBoundary::Schedule, other),
            Err(error) => store_failed(PersistenceBoundary::Schedule, error),
        }
    }

    fn prepare_exposure(
        &mut self,
        execution: &ExecutionSpec,
        expected_revision: Option<StorageRevision>,
        checkpoint: &CheckpointEnvelope,
        activity: LogicalActivityId,
    ) -> Result<PreparedExposure, CheckpointError> {
        let mut payload = checkpoint.decode_and_validate(execution, self.limits)?;
        let record = payload
            .active_activities()
            .and_then(|activities| activities.last())
            .cloned()
            .expect("scheduled evaluation requires an activity record");
        let reserved_encoded_bytes = payload.maximum_activity_completed_encoded_len()?;
        if reserved_encoded_bytes > self.limits.max_active_encoded_bytes() {
            return Err(CheckpointError::EncodedCheckpointLimitExceeded {
                actual: reserved_encoded_bytes,
                maximum: self.limits.max_active_encoded_bytes(),
            });
        }
        let attempt_id = self.next_attempt();
        let attempt_ordinal = record.attempt().ordinal();
        let retry_not_before_unix_millis = record.attempt().retry_not_before_unix_millis();
        let prepared_command = record.prepared_command().cloned();
        replace_final_record(&mut payload, record.expose_attempt(attempt_id)?);
        Ok(PreparedExposure {
            execution_id: execution.execution_id(),
            expected_revision,
            checkpoint: CheckpointEnvelope::encode_with_limits(&payload, self.limits)?,
            activity,
            attempt_id,
            attempt_ordinal,
            retry_not_before_unix_millis,
            prepared_command,
        })
    }

    async fn commit_exposure(
        &self,
        proposal: PreparedExposure,
        boundary: PersistenceBoundary,
    ) -> HostOutcome {
        match self
            .store
            .compare_and_swap(
                proposal.execution_id,
                proposal.expected_revision,
                proposal.checkpoint,
            )
            .await
        {
            Ok(CasOutcome::Accepted(revision)) => HostOutcome::DispatchPermitted {
                permit: DispatchPermit::new(
                    proposal.activity,
                    proposal.attempt_id,
                    proposal.attempt_ordinal,
                    proposal.retry_not_before_unix_millis,
                    proposal.prepared_command,
                ),
                revision,
                boundary,
            },
            Ok(other) => reload_outcome(boundary, other),
            Err(error) => store_failed(boundary, error),
        }
    }

    async fn commit_completion(
        &self,
        expected_revision: Option<StorageRevision>,
        execution: &ExecutionSpec,
        outcome: TerminalOutcome,
        completed_activity_count: u64,
        active_checkpoint: CheckpointEnvelope,
    ) -> HostOutcome {
        self.commit_completion_at_boundary(
            expected_revision,
            execution,
            outcome,
            completed_activity_count,
            active_checkpoint,
            PersistenceBoundary::Completion,
        )
        .await
    }

    async fn commit_completion_at_boundary(
        &self,
        expected_revision: Option<StorageRevision>,
        execution: &ExecutionSpec,
        outcome: TerminalOutcome,
        completed_activity_count: u64,
        active_checkpoint: CheckpointEnvelope,
        boundary: PersistenceBoundary,
    ) -> HostOutcome {
        let execution_id = execution.execution_id();
        let active = match active_checkpoint.decode_and_validate(execution, self.limits) {
            Ok(active) => active,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        let terminal = match active.into_terminal(outcome.clone(), completed_activity_count) {
            Ok(terminal) => terminal,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        let completion_metadata = terminal.terminal_completion_metadata();
        let checkpoint = match CheckpointEnvelope::encode_with_limits(&terminal, self.limits) {
            Ok(checkpoint) => checkpoint,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        match self
            .store
            .compare_and_swap(execution_id, expected_revision, checkpoint)
            .await
        {
            Ok(CasOutcome::Accepted(revision)) => terminal_host_outcome(
                outcome,
                completed_activity_count,
                completion_metadata,
                revision,
                boundary,
                TerminalCheckpointStatus::Accepted,
            ),
            Ok(other) => reload_outcome(boundary, other),
            Err(error) => store_failed(boundary, error),
        }
    }

    fn next_attempt(&mut self) -> AttemptId {
        let counter = self.next_attempt_counter;
        self.next_attempt_counter = counter
            .checked_add(1)
            .expect("host attempt counter exhausted");
        AttemptId::new(self.host_epoch, counter)
            .expect("host attempt counters start above the reserved zero value")
    }
}

struct PreparedExposure {
    execution_id: ExecutionId,
    expected_revision: Option<StorageRevision>,
    checkpoint: CheckpointEnvelope,
    activity: LogicalActivityId,
    attempt_id: AttemptId,
    attempt_ordinal: u32,
    retry_not_before_unix_millis: Option<i64>,
    prepared_command: Option<PreparedCommand>,
}

enum ExposureResolver<'a> {
    Activity(&'a dyn PreparedActivityResolver),
    Effect(&'a dyn PreparedEffectResolver),
}

fn replace_final_record(payload: &mut CheckpointPayload, replacement: ActivityRecord) {
    *payload
        .active_activities_mut()
        .expect("replacement requires active state")
        .last_mut()
        .expect("replacement requires a final activity") = replacement;
}

fn terminal_host_outcome(
    outcome: TerminalOutcome,
    completed_activity_count: u64,
    completion_metadata: Option<CompletionMetadata>,
    revision: StorageRevision,
    boundary: PersistenceBoundary,
    checkpoint_status: TerminalCheckpointStatus,
) -> HostOutcome {
    HostOutcome::WorkflowCompleted {
        outcome,
        completed_activity_count,
        completion_metadata,
        revision,
        boundary,
        checkpoint_status,
    }
}

fn reload_outcome(boundary: PersistenceBoundary, result: CasOutcome) -> HostOutcome {
    let reason = match result {
        CasOutcome::Conflict => ReloadReason::Conflict,
        CasOutcome::OutcomeUnknown => ReloadReason::OutcomeUnknown,
        CasOutcome::Accepted(_) => unreachable!("accepted CAS handled by caller"),
    };
    HostOutcome::ReloadRequired { boundary, reason }
}

fn store_failed(boundary: PersistenceBoundary, error: StoreError) -> HostOutcome {
    HostOutcome::StoreFailed {
        operation: StoreOperation::CompareAndSwap(boundary),
        error,
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use futures::executor::block_on;

    use super::*;
    use crate::{
        ActivityName, ActivityOptions, ActivitySequence, ActivitySpec, CheckpointLimits,
        CompletionClass, DurableEffect, EffectActivity, EffectMetadata, EffectOutcome,
        InMemoryCheckpointStore, InMemoryFault, PreparedActivityError, PreparedActivityResolver,
        PreparedCommand, PreparedEffectResolver, StoreErrorKind, WorkflowContext,
    };
    use serde::{Deserialize, Serialize};

    struct OneActivity;

    #[derive(Deserialize, Serialize)]
    struct UnitRequest;

    #[derive(Deserialize, Serialize)]
    struct UnitCommand {
        exact: u8,
    }

    struct OneEffect;

    impl DurableEffect for OneEffect {
        type Request = UnitRequest;
        type Command = UnitCommand;
        type Output = String;

        const NAME: &'static str = "unit.effect";
        const VERSION: u32 = 1;
        const MAX_REQUEST_BYTES: u64 = 16;
        const MAX_COMMAND_BYTES: u64 = 32;
        const MAX_RESULT_BYTES: u64 = 256;
        const MAX_ERROR_MESSAGE_BYTES: u64 = 64;
        const COMPLETION_CLASS: CompletionClass = CompletionClass::ExternalEffect;
    }

    struct OneEffectWorkflow;

    #[async_trait]
    impl Workflow for OneEffectWorkflow {
        async fn run(
            &self,
            context: &mut WorkflowContext<'_>,
            _input: ExactBytes,
        ) -> TerminalOutcome {
            match context
                .schedule_activity_typed::<EffectActivity<OneEffect>>(
                    OneEffect::NAME,
                    &UnitRequest,
                    ActivityOptions::default(),
                )
                .await
            {
                Ok(outcome) => {
                    match outcome.into_workflow_result(OneEffect::MAX_ERROR_MESSAGE_BYTES) {
                        Ok(value) => TerminalOutcome::succeeded(value.into_bytes()),
                        Err(error) => TerminalOutcome::failed(error.to_string().into_bytes()),
                    }
                }
                Err(error) => TerminalOutcome::failed(error.to_string().into_bytes()),
            }
        }
    }

    struct UnitEffectResolver {
        mismatch_recorded: bool,
    }

    impl PreparedEffectResolver for UnitEffectResolver {
        fn resolve(
            &self,
            _execution_id: ExecutionId,
            _logical: &ActivitySpec,
            metadata: EffectMetadata,
            recorded: Option<&PreparedCommand>,
        ) -> Result<PreparedCommand, PreparedActivityError> {
            if let Some(recorded) = recorded
                && !self.mismatch_recorded
            {
                return Ok(recorded.clone());
            }
            let exact = if recorded.is_some() { 2 } else { 1 };
            crate::encode_effect_command::<OneEffect>(&UnitCommand { exact })
                .and_then(|command| {
                    if command.max_bytes() == metadata.max_command_bytes() {
                        Ok(command)
                    } else {
                        Err(crate::EffectContractError::CommandTooLarge {
                            actual_bytes: command.max_bytes(),
                            max_bytes: metadata.max_command_bytes(),
                        })
                    }
                })
                .map_err(|_| PreparedActivityError::Encoding)
        }
    }

    struct WrongCommandBoundResolver;

    impl PreparedEffectResolver for WrongCommandBoundResolver {
        fn resolve(
            &self,
            _execution_id: ExecutionId,
            _logical: &ActivitySpec,
            _metadata: EffectMetadata,
            _recorded: Option<&PreparedCommand>,
        ) -> Result<PreparedCommand, PreparedActivityError> {
            PreparedCommand::new(ExactBytes::new(b"{}"), 999)
                .map_err(|_| PreparedActivityError::Encoding)
        }
    }

    #[async_trait]
    impl Workflow for OneActivity {
        async fn run(
            &self,
            context: &mut WorkflowContext<'_>,
            input: ExactBytes,
        ) -> TerminalOutcome {
            TerminalOutcome::succeeded(
                context
                    .activity(ActivitySpec::new(
                        ActivityName::new("unit", 1).unwrap(),
                        input,
                        1024,
                    ))
                    .await,
            )
        }
    }

    fn assert_send<T: Send>(_: T) {}

    #[test]
    fn host_turn_and_observation_futures_are_send() {
        let store = InMemoryCheckpointStore::new();
        let execution_id = ExecutionId::from_bytes([2; 16]);
        let spec = ActivitySpec::new(
            ActivityName::new("unit", 1).unwrap(),
            ExactBytes::new(b"unit"),
            1024,
        );
        let execution = ExecutionSpec::new(execution_id, ExactBytes::new(b"unit"), 1024);
        let mut host = DurableHost::new(
            store,
            HostEpoch::from_bytes([1; 16]),
            CheckpointLimits::new(16, 100_000, 100_000).unwrap(),
        );
        assert_send(host.turn(&OneActivity, execution.clone()));
        assert_send(host.turn_and_expose(&OneActivity, execution.clone()));

        let activity = LogicalActivityId::new(execution_id, ActivitySequence::new(0), spec);
        assert_send(host.observe(
            &execution,
            ActivityObservation::new(activity.clone(), ExactBytes::new(b"result")),
        ));
        assert_send(host.observe_and_turn(
            &OneActivity,
            &execution,
            ActivityObservation::new(activity, ExactBytes::new(b"result")),
        ));
    }

    #[test]
    fn only_an_accepted_consumed_exposure_proposal_constructs_a_permit() {
        block_on(async {
            let store = InMemoryCheckpointStore::new();
            let execution_id = ExecutionId::from_bytes([2; 16]);
            let input = ExactBytes::new(b"unit");
            let execution = || ExecutionSpec::new(execution_id, input.clone(), 1024);
            let mut host = DurableHost::new(
                store.clone(),
                HostEpoch::from_bytes([1; 16]),
                CheckpointLimits::new(16, 100_000, 100_000).unwrap(),
            );
            assert!(matches!(
                host.turn(&OneActivity, execution()).await,
                HostOutcome::ScheduleAccepted { .. }
            ));

            for fault in [
                InMemoryFault::FailBeforeRequest(StoreErrorKind::Unavailable),
                InMemoryFault::OutcomeUnknownWithoutApply,
                InMemoryFault::OutcomeUnknownAfterApply,
            ] {
                store.fail_next_compare_and_swap(fault);
                let outcome = host.turn(&OneActivity, execution()).await;
                assert!(!matches!(outcome, HostOutcome::DispatchPermitted { .. }));
                if fault == InMemoryFault::OutcomeUnknownAfterApply {
                    assert!(matches!(
                        host.turn(&OneActivity, execution()).await,
                        HostOutcome::Quarantined { .. }
                    ));
                    return;
                }
            }
            panic!("applied outcome-unknown case did not execute");
        });
    }

    #[test]
    fn typed_effect_persists_command_redelivers_once_and_authenticates_completion() {
        block_on(async {
            let store = InMemoryCheckpointStore::new();
            let execution = ExecutionSpec::new(
                ExecutionId::from_bytes([22; 16]),
                ExactBytes::new(b"workflow"),
                1024,
            );
            let mut host = DurableHost::new(
                store,
                HostEpoch::from_bytes([23; 16]),
                CheckpointLimits::new(4, 100_000, 100_000).unwrap(),
            );
            let resolver = UnitEffectResolver {
                mismatch_recorded: false,
            };

            let HostOutcome::DispatchPermitted { permit: first, .. } = host
                .turn_and_expose_effects(&OneEffectWorkflow, execution.clone(), &resolver)
                .await
            else {
                panic!("first effect attempt was not exposed");
            };
            assert!(first.prepared_command().is_some());
            let logical = first.activity().clone();
            let first_attempt = first.attempt_id();
            assert!(matches!(
                host.observe_effect(
                    &execution,
                    EffectObservation::from_outcome::<OneEffect>(
                        logical.clone(),
                        first_attempt,
                        &EffectOutcome::<String>::ProvenNoAdmission,
                    )
                    .unwrap(),
                )
                .await,
                HostOutcome::ObservationAccepted { .. }
            ));

            let HostOutcome::DispatchPermitted { permit: second, .. } = host
                .turn_and_expose_effects(&OneEffectWorkflow, execution.clone(), &resolver)
                .await
            else {
                panic!("second effect attempt was not exposed");
            };
            assert_eq!(second.activity(), &logical);
            assert_ne!(second.attempt_id(), first_attempt);
            assert_eq!(second.prepared_command(), first.prepared_command());
            assert!(matches!(
                host.observe_effect(
                    &execution,
                    EffectObservation::from_outcome::<OneEffect>(
                        logical,
                        second.attempt_id(),
                        &EffectOutcome::<String>::ProvenNoAdmission,
                    )
                    .unwrap(),
                )
                .await,
                HostOutcome::ObservationAccepted { .. }
            ));

            let HostOutcome::WorkflowCompleted {
                completion_metadata: Some(metadata),
                checkpoint_status: TerminalCheckpointStatus::Accepted,
                ..
            } = host
                .turn_and_expose_effects(&OneEffectWorkflow, execution.clone(), &resolver)
                .await
            else {
                panic!("effect workflow did not complete with authenticated metadata");
            };
            assert_eq!(metadata.completed_activity_count(), 1);
            assert_eq!(metadata.external_effect_count(), 1);
            assert_eq!(metadata.passive_observation_count(), 0);

            assert!(matches!(
                host.turn_and_expose_effects(&OneEffectWorkflow, execution, &resolver)
                    .await,
                HostOutcome::WorkflowCompleted {
                    completion_metadata: Some(_),
                    checkpoint_status: TerminalCheckpointStatus::Reloaded,
                    ..
                }
            ));
        });
    }

    #[test]
    fn typed_effect_unknown_outcome_quarantines_and_recorded_command_mismatch_is_nondeterminism() {
        block_on(async {
            let store = InMemoryCheckpointStore::new();
            let execution = ExecutionSpec::new(
                ExecutionId::from_bytes([24; 16]),
                ExactBytes::new(b"workflow"),
                1024,
            );
            let mut host = DurableHost::new(
                store.clone(),
                HostEpoch::from_bytes([25; 16]),
                CheckpointLimits::new(4, 100_000, 100_000).unwrap(),
            );
            let resolver = UnitEffectResolver {
                mismatch_recorded: false,
            };
            assert!(matches!(
                host.turn_and_expose_effects(&OneEffectWorkflow, execution.clone(), &resolver)
                    .await,
                HostOutcome::DispatchPermitted { .. }
            ));
            let HostOutcome::Quarantined {
                activity: quarantined_activity,
                attempt_id: quarantined_attempt,
                prepared_command: Some(quarantined_command),
                ..
            } = host
                .turn_and_expose_effects(&OneEffectWorkflow, execution.clone(), &resolver)
                .await
            else {
                panic!("typed effect did not retain its prepared command in quarantine");
            };
            assert_eq!(
                quarantined_command,
                crate::encode_effect_command::<OneEffect>(&UnitCommand { exact: 1 }).unwrap()
            );
            let mismatching = UnitEffectResolver {
                mismatch_recorded: true,
            };
            let stored = store.load(execution.execution_id()).await.unwrap().unwrap();
            assert!(matches!(
                evaluate_effects(
                    &OneEffectWorkflow,
                    &execution,
                    Some(stored.checkpoint()),
                    host.checkpoint_limits(),
                    &mismatching,
                ),
                Evaluation::Nondeterminism(Nondeterminism::PreparedCommandMismatch { .. })
            ));

            assert!(matches!(
                host.observe_quarantined_effect(
                    &execution,
                    EffectObservation::from_outcome::<OneEffect>(
                        quarantined_activity,
                        quarantined_attempt,
                        &EffectOutcome::<String>::ProvenNoAdmission,
                    )
                    .unwrap(),
                )
                .await,
                HostOutcome::ObservationAccepted { .. }
            ));
            assert!(matches!(
                host.turn_and_expose_effects(&OneEffectWorkflow, execution.clone(), &resolver)
                    .await,
                HostOutcome::WorkflowCompleted { .. }
            ));
        });
    }

    #[test]
    fn typed_effect_rejects_resolver_command_bound_mismatch_before_persistence() {
        block_on(async {
            let store = InMemoryCheckpointStore::new();
            let execution = ExecutionSpec::new(
                ExecutionId::from_bytes([26; 16]),
                ExactBytes::new(b"workflow"),
                1024,
            );
            let mut host = DurableHost::new(
                store.clone(),
                HostEpoch::from_bytes([27; 16]),
                CheckpointLimits::new(4, 100_000, 100_000).unwrap(),
            );
            assert!(matches!(
                host.turn_and_expose_effects(
                    &OneEffectWorkflow,
                    execution.clone(),
                    &WrongCommandBoundResolver,
                )
                .await,
                HostOutcome::CheckpointRejected(CheckpointError::PreparedActivityRejected(
                    PreparedActivityError::CommandBoundMismatch {
                        actual_bytes: 999,
                        max_bytes: 32,
                    }
                ))
            ));
            assert!(
                store
                    .load(execution.execution_id())
                    .await
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[derive(Clone)]
    struct PreparedResolver(Result<ActivitySpec, PreparedActivityError>);

    impl PreparedActivityResolver for PreparedResolver {
        fn resolve(
            &self,
            _logical: &ActivitySpec,
            _recorded: Option<&ActivitySpec>,
        ) -> Result<ActivitySpec, PreparedActivityError> {
            self.0.clone()
        }
    }

    #[test]
    fn fused_prepared_failures_never_reach_the_store_or_construct_a_permit() {
        block_on(async {
            for error in [
                PreparedActivityError::Derivation,
                PreparedActivityError::Validation,
                PreparedActivityError::Encoding,
                PreparedActivityError::InputTooLarge {
                    actual_bytes: 2,
                    max_bytes: 1,
                },
                PreparedActivityError::ResultBoundTooLarge {
                    actual_bytes: 2,
                    max_bytes: 1,
                },
                PreparedActivityError::CommandTooLarge {
                    actual_bytes: 2,
                    max_bytes: 1,
                },
                PreparedActivityError::CommandBoundMismatch {
                    actual_bytes: 2,
                    max_bytes: 1,
                },
            ] {
                let store = InMemoryCheckpointStore::new();
                let mut host = DurableHost::new(
                    store.clone(),
                    HostEpoch::from_bytes([3; 16]),
                    CheckpointLimits::new(16, 100_000, 100_000).unwrap(),
                );
                let outcome = host
                    .turn_and_expose_with(
                        &OneActivity,
                        ExecutionSpec::new(
                            ExecutionId::from_bytes([4; 16]),
                            ExactBytes::new(b"logical"),
                            1024,
                        ),
                        &PreparedResolver(Err(error.clone())),
                    )
                    .await;
                assert_eq!(
                    outcome,
                    HostOutcome::CheckpointRejected(CheckpointError::PreparedActivityRejected(
                        error
                    ))
                );
                assert!(
                    store
                        .load(ExecutionId::from_bytes([4; 16]))
                        .await
                        .unwrap()
                        .is_none()
                );
            }
        });
    }

    #[test]
    fn fused_unknown_after_apply_reloads_complete_prepared_exposure_as_quarantined() {
        block_on(async {
            let store = InMemoryCheckpointStore::new();
            store.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownAfterApply);
            let execution_id = ExecutionId::from_bytes([5; 16]);
            let execution = ExecutionSpec::new(execution_id, ExactBytes::new(b"logical"), 1024);
            let prepared = ActivitySpec::new(
                ActivityName::new("prepared", 9).unwrap(),
                ExactBytes::new(b"exact-command-and-fences"),
                77,
            );
            let mut host = DurableHost::new(
                store.clone(),
                HostEpoch::from_bytes([6; 16]),
                CheckpointLimits::new(16, 100_000, 100_000).unwrap(),
            );

            assert!(matches!(
                host.turn_and_expose_with(
                    &OneActivity,
                    execution.clone(),
                    &PreparedResolver(Ok(prepared.clone())),
                )
                .await,
                HostOutcome::ReloadRequired {
                    boundary: PersistenceBoundary::ScheduleExposure,
                    reason: ReloadReason::OutcomeUnknown,
                }
            ));
            let HostOutcome::Quarantined { activity, .. } = host
                .turn_and_expose_with(
                    &OneActivity,
                    execution,
                    &PreparedResolver(Ok(prepared.clone())),
                )
                .await
            else {
                panic!("applied prepared exposure did not reload as quarantined");
            };
            assert_eq!(activity.spec(), &prepared);
        });
    }

    #[test]
    fn fused_unknown_without_apply_reloads_the_authoritative_predecessor() {
        block_on(async {
            let store = InMemoryCheckpointStore::new();
            store.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownWithoutApply);
            let execution_id = ExecutionId::from_bytes([10; 16]);
            let execution = ExecutionSpec::new(execution_id, ExactBytes::new(b"logical"), 1024);
            let prepared = ActivitySpec::new(
                ActivityName::new("prepared", 9).unwrap(),
                ExactBytes::new(b"exact-command-and-fences"),
                77,
            );
            let mut host = DurableHost::new(
                store.clone(),
                HostEpoch::from_bytes([11; 16]),
                CheckpointLimits::new(16, 100_000, 100_000).unwrap(),
            );

            assert!(matches!(
                host.turn_and_expose_with(
                    &OneActivity,
                    execution.clone(),
                    &PreparedResolver(Ok(prepared.clone())),
                )
                .await,
                HostOutcome::ReloadRequired {
                    boundary: PersistenceBoundary::ScheduleExposure,
                    reason: ReloadReason::OutcomeUnknown,
                }
            ));
            assert!(store.load(execution_id).await.unwrap().is_none());
            let HostOutcome::DispatchPermitted { permit, .. } = host
                .turn_and_expose_with(
                    &OneActivity,
                    execution,
                    &PreparedResolver(Ok(prepared.clone())),
                )
                .await
            else {
                panic!("unapplied prepared exposure did not reload its empty predecessor");
            };
            assert_eq!(permit.activity().spec(), &prepared);
        });
    }

    #[test]
    fn fused_prepared_specification_controls_result_and_checkpoint_admission() {
        block_on(async {
            let execution_id = ExecutionId::from_bytes([7; 16]);
            let execution = ExecutionSpec::new(execution_id, ExactBytes::new(b"logical"), 1024);

            let result_store = InMemoryCheckpointStore::new();
            let mut result_host = DurableHost::new(
                result_store.clone(),
                HostEpoch::from_bytes([8; 16]),
                CheckpointLimits::new(16, 100_000, 100_000).unwrap(),
            );
            let unrepresentable_result = ActivitySpec::new(
                ActivityName::new("prepared", 1).unwrap(),
                ExactBytes::new(b"bounded-command"),
                u64::MAX,
            );
            assert!(matches!(
                result_host
                    .turn_and_expose_with(
                        &OneActivity,
                        execution.clone(),
                        &PreparedResolver(Ok(unrepresentable_result)),
                    )
                    .await,
                HostOutcome::CheckpointRejected(
                    CheckpointError::ResultLengthUnrepresentable
                        | CheckpointError::EncodedLengthOverflow
                )
            ));
            assert!(result_store.load(execution_id).await.unwrap().is_none());

            let input_store = InMemoryCheckpointStore::new();
            let mut input_host = DurableHost::new(
                input_store.clone(),
                HostEpoch::from_bytes([9; 16]),
                CheckpointLimits::new(16, 512, 100_000).unwrap(),
            );
            let oversized_prepared = ActivitySpec::new(
                ActivityName::new("prepared", 1).unwrap(),
                ExactBytes::new(vec![b'x'; 1024]),
                1,
            );
            assert!(matches!(
                input_host
                    .turn_and_expose_with(
                        &OneActivity,
                        ExecutionSpec::new(execution_id, ExactBytes::new(b"logical"), 1),
                        &PreparedResolver(Ok(oversized_prepared)),
                    )
                    .await,
                HostOutcome::CheckpointRejected(
                    CheckpointError::EncodedCheckpointLimitExceeded { .. }
                )
            ));
            assert!(input_store.load(execution_id).await.unwrap().is_none());
        });
    }
}
