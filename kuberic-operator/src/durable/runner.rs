//! Shared bounded lifecycle runner for operator-hosted durable workflows.

use async_trait::async_trait;
use kuberic_durable_execution::{
    ActivityObservation, AttemptId, CheckpointError, CheckpointLimits, CheckpointPayload,
    CheckpointStore, ExecutionSpec, HostOutcome, LogicalActivityId, Nondeterminism,
    ObservationRejection, PersistenceBoundary, PreparedActivityResolver, ReloadReason, StoreError,
    StoreOperation, TerminalCheckpointStatus, TerminalOutcome, Workflow,
};
use thiserror::Error;

use super::workflow_host::{DurableOperatorHost, DurablePermitGuard};

const MIN_REQUEUE_SECONDS: u64 = 1;
const MAX_REQUEUE_SECONDS: u64 = 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableWorkflowContract {
    version: u32,
    checkpoint_limits: CheckpointLimits,
}

impl DurableWorkflowContract {
    pub const fn new(version: u32, checkpoint_limits: CheckpointLimits) -> Self {
        Self {
            version,
            checkpoint_limits,
        }
    }

    pub const fn version(self) -> u32 {
        self.version
    }

    pub const fn checkpoint_limits(self) -> CheckpointLimits {
        self.checkpoint_limits
    }

    pub fn validate(
        self,
        version: u32,
        checkpoint_limits: CheckpointLimits,
    ) -> Result<CheckpointLimits, DurableContractError> {
        if version != self.version {
            return Err(DurableContractError::UnsupportedVersion {
                actual: version,
                supported: self.version,
            });
        }
        if checkpoint_limits != self.checkpoint_limits {
            return Err(DurableContractError::CheckpointLimitsChanged {
                expected: self.checkpoint_limits,
                actual: checkpoint_limits,
            });
        }
        Ok(self.checkpoint_limits)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DurableContractError {
    #[error(
        "durable workflow contract version {actual} is unsupported; supported version is {supported}"
    )]
    UnsupportedVersion { actual: u32, supported: u32 },
    #[error("durable workflow checkpoint limits changed without a contract-version change")]
    CheckpointLimitsChanged {
        expected: CheckpointLimits,
        actual: CheckpointLimits,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableCheckpointDisposition {
    Incompatible,
    Rejected,
    Isolated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurableAdapterBoundary {
    Observed(ActivityObservation),
    ObserveAndWait {
        observation: Box<ActivityObservation>,
        reason: String,
        detail: String,
        requeue_after_seconds: u64,
    },
    ObserveAndProgressThenWait {
        observation: Box<ActivityObservation>,
        reason: String,
        detail: String,
        requeue_after_seconds: u64,
    },
    Wait {
        reason: String,
        detail: String,
    },
    Incompatible(String),
    Rejected(String),
    Isolated(String),
}

#[async_trait]
pub trait DurableOperationAdapter: Send {
    type Resolver: PreparedActivityResolver + Sync;
    type Terminal: Send;
    type Publication: Send;

    /// Validate operation authority and resolve a logical request to its exact
    /// durable boundary command.
    fn resolver(&self) -> &Self::Resolver;

    /// Restore operation-specific replay state from the checkpoint loaded by
    /// the runner. The runner remains the sole owner of lifecycle loading and
    /// error classification.
    fn restore(
        &mut self,
        _checkpoint: Option<&CheckpointPayload>,
    ) -> Result<(), DurableAdapterBoundary> {
        Ok(())
    }

    /// Collect operation-specific evidence only after the runner proves the
    /// authoritative checkpoint is not terminal.
    async fn prepare(&mut self) -> Result<(), DurableAdapterBoundary> {
        Ok(())
    }

    /// Collect passive evidence or dispatch an exact effect using the one-use
    /// permit supplied by the host.
    async fn observe_or_dispatch(
        &mut self,
        activity: &LogicalActivityId,
        attempt_id: AttemptId,
        permit: &mut DurablePermitGuard,
    ) -> DurableAdapterBoundary;

    /// Resolve an exposed command from authoritative observation without a new
    /// dispatch authority.
    async fn resolve_quarantine(
        &mut self,
        activity: LogicalActivityId,
        attempt_id: AttemptId,
    ) -> DurableAdapterBoundary;

    fn interrupt_after_accepted_exposure(
        &mut self,
        _activity: &LogicalActivityId,
        _attempt_id: AttemptId,
    ) -> Option<DurableAdapterWait> {
        None
    }

    /// Operation deadline used only to bound the reconcile requeue.
    fn deadline_unix_seconds(&self) -> i64;

    /// Operation-specific condition and retry policy for a prepared activity
    /// that cannot yet be derived from authoritative evidence.
    fn preparation_wait(&self, error: &CheckpointError) -> DurableAdapterWait {
        DurableAdapterWait {
            reason: "AwaitingEffectPreparation".to_string(),
            detail: format!("durable activity preparation awaits authoritative evidence: {error}"),
            requeue_after_seconds: None,
        }
    }

    /// Classify a persisted contract or checkpoint rejection.
    fn checkpoint_disposition(&self, _error: &CheckpointError) -> DurableCheckpointDisposition {
        DurableCheckpointDisposition::Rejected
    }

    /// Validate operation-specific terminal authority and evidence.
    fn validate_terminal(
        &mut self,
        outcome: TerminalOutcome,
        completed_activity_count: u64,
    ) -> Result<Self::Terminal, DurableAdapterBoundary>;

    /// Build the operation-specific publication/condition handoff. The runner
    /// does not publish topology or status itself.
    fn publication_handoff(&mut self, terminal: Self::Terminal) -> Self::Publication;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableActiveReason {
    Adapter,
    FuelExhausted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableAdapterWait {
    pub reason: String,
    pub detail: String,
    pub requeue_after_seconds: Option<u64>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum DurableRunnerOutcome<P> {
    Terminal(P),
    Active {
        reason: DurableActiveReason,
        condition_reason: String,
        detail: String,
        requeue_after_seconds: u64,
    },
    Incompatible(String),
    Rejected(String),
    Isolated(String),
    ReloadRequired {
        boundary: PersistenceBoundary,
        reason: ReloadReason,
    },
    PersistenceFailed {
        operation: StoreOperation,
        error: StoreError,
    },
    Nondeterministic(Nondeterminism),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableRunner {
    max_host_outcomes: usize,
}

impl DurableRunner {
    pub fn new(max_host_outcomes: usize) -> Result<Self, DurableRunnerError> {
        if max_host_outcomes == 0 {
            return Err(DurableRunnerError::ZeroFuel);
        }
        Ok(Self { max_host_outcomes })
    }

    pub async fn run<W, A>(
        &self,
        host: &mut DurableOperatorHost,
        workflow: &W,
        execution: ExecutionSpec,
        adapter: &mut A,
        now_unix_seconds: i64,
    ) -> DurableRunnerOutcome<A::Publication>
    where
        W: Workflow,
        A: DurableOperationAdapter,
    {
        let loaded = match host.store().load(execution.execution_id()).await {
            Ok(loaded) => loaded,
            Err(error) => {
                return DurableRunnerOutcome::PersistenceFailed {
                    operation: StoreOperation::Load,
                    error,
                };
            }
        };
        let checkpoint = if let Some(stored) = loaded.as_ref() {
            let payload = match stored
                .checkpoint()
                .decode_and_validate(&execution, host.checkpoint_limits())
            {
                Ok(payload) => payload,
                Err(error) => {
                    let message = error.to_string();
                    return match adapter.checkpoint_disposition(&error) {
                        DurableCheckpointDisposition::Incompatible => {
                            DurableRunnerOutcome::Incompatible(message)
                        }
                        DurableCheckpointDisposition::Rejected => {
                            DurableRunnerOutcome::Rejected(message)
                        }
                        DurableCheckpointDisposition::Isolated => {
                            DurableRunnerOutcome::Isolated(message)
                        }
                    };
                }
            };
            if let Some((outcome, completed_activity_count)) = payload.terminal_outcome() {
                return match adapter.validate_terminal(outcome.clone(), completed_activity_count) {
                    Ok(terminal) => {
                        DurableRunnerOutcome::Terminal(adapter.publication_handoff(terminal))
                    }
                    Err(boundary) => self
                        .handle_adapter_boundary(boundary, adapter, now_unix_seconds)
                        .into_result(),
                };
            }
            Some(payload)
        } else {
            None
        };
        if let Err(boundary) = adapter.restore(checkpoint.as_ref()) {
            return self
                .handle_adapter_boundary(boundary, adapter, now_unix_seconds)
                .into_result();
        }
        if let Err(boundary) = adapter.prepare().await {
            return self
                .handle_adapter_boundary(boundary, adapter, now_unix_seconds)
                .into_result();
        }
        let mut outcome = host
            .turn_and_expose_with(workflow, execution.clone(), adapter.resolver())
            .await;
        let mut observation_wait = None;
        let mut progression_wait = None;
        for _ in 0..self.max_host_outcomes {
            host.store().correlate_host_outcome(&outcome);
            if let Some((condition_reason, detail, requeue_after_seconds)) = progression_wait.take()
                && matches!(
                    &outcome,
                    HostOutcome::DispatchPermitted { .. }
                        | HostOutcome::Quarantined { .. }
                        | HostOutcome::ScheduleAccepted { .. }
                        | HostOutcome::ObservationAccepted { .. }
                )
            {
                return DurableRunnerOutcome::Active {
                    reason: DurableActiveReason::Adapter,
                    condition_reason,
                    detail,
                    requeue_after_seconds,
                };
            }
            outcome = match outcome {
                HostOutcome::DispatchPermitted { permit, .. } => {
                    let activity = permit.activity().clone();
                    let attempt_id = permit.attempt_id();
                    if let Some(wait) =
                        adapter.interrupt_after_accepted_exposure(&activity, attempt_id)
                    {
                        return DurableRunnerOutcome::Active {
                            reason: DurableActiveReason::Adapter,
                            condition_reason: wait.reason,
                            detail: wait.detail,
                            requeue_after_seconds: wait.requeue_after_seconds.unwrap_or_else(
                                || {
                                    deadline_requeue_seconds(
                                        now_unix_seconds,
                                        adapter.deadline_unix_seconds(),
                                    )
                                },
                            ),
                        };
                    }
                    let mut guard = DurablePermitGuard::new(permit);
                    let boundary = adapter
                        .observe_or_dispatch(&activity, attempt_id, &mut guard)
                        .await;
                    if guard.activity().is_some() {
                        return DurableRunnerOutcome::Isolated(
                            "operation adapter did not consume the durable dispatch permit"
                                .to_string(),
                        );
                    }
                    match self.handle_adapter_boundary(boundary, adapter, now_unix_seconds) {
                        AdapterHandling::Observe(observation) => {
                            host.observe_and_turn_with(
                                workflow,
                                &execution,
                                observation,
                                adapter.resolver(),
                            )
                            .await
                        }
                        AdapterHandling::ObserveAndWait {
                            observation,
                            reason,
                            detail,
                            requeue_after_seconds,
                        } => {
                            observation_wait = Some((reason, detail, requeue_after_seconds));
                            host.observe(&execution, observation).await
                        }
                        AdapterHandling::ObserveAndProgressThenWait {
                            observation,
                            reason,
                            detail,
                            requeue_after_seconds,
                        } => {
                            progression_wait = Some((reason, detail, requeue_after_seconds));
                            host.observe_and_turn_with(
                                workflow,
                                &execution,
                                observation,
                                adapter.resolver(),
                            )
                            .await
                        }
                        AdapterHandling::Return(result) => return result,
                    }
                }
                HostOutcome::Quarantined {
                    activity,
                    attempt_id,
                } => {
                    let boundary = adapter.resolve_quarantine(activity, attempt_id).await;
                    match self.handle_adapter_boundary(boundary, adapter, now_unix_seconds) {
                        AdapterHandling::Observe(observation) => {
                            host.observe_and_turn_with(
                                workflow,
                                &execution,
                                observation,
                                adapter.resolver(),
                            )
                            .await
                        }
                        AdapterHandling::ObserveAndWait {
                            observation,
                            reason,
                            detail,
                            requeue_after_seconds,
                        } => {
                            observation_wait = Some((reason, detail, requeue_after_seconds));
                            host.observe(&execution, observation).await
                        }
                        AdapterHandling::ObserveAndProgressThenWait {
                            observation,
                            reason,
                            detail,
                            requeue_after_seconds,
                        } => {
                            progression_wait = Some((reason, detail, requeue_after_seconds));
                            host.observe_and_turn_with(
                                workflow,
                                &execution,
                                observation,
                                adapter.resolver(),
                            )
                            .await
                        }
                        AdapterHandling::Return(result) => return result,
                    }
                }
                HostOutcome::WorkflowCompleted {
                    checkpoint_status: TerminalCheckpointStatus::Accepted,
                    ..
                } => {
                    host.turn_and_expose_with(workflow, execution.clone(), adapter.resolver())
                        .await
                }
                HostOutcome::WorkflowCompleted {
                    outcome,
                    completed_activity_count,
                    revision: _,
                    boundary: _,
                    checkpoint_status: TerminalCheckpointStatus::Reloaded,
                } => {
                    return match adapter.validate_terminal(outcome, completed_activity_count) {
                        Ok(terminal) => {
                            DurableRunnerOutcome::Terminal(adapter.publication_handoff(terminal))
                        }
                        Err(boundary) => self
                            .handle_adapter_boundary(boundary, adapter, now_unix_seconds)
                            .into_result(),
                    };
                }
                HostOutcome::CheckpointRejected(CheckpointError::PreparedActivityRejected(
                    ref error,
                )) => {
                    let wait = adapter.preparation_wait(
                        &CheckpointError::PreparedActivityRejected(error.clone()),
                    );
                    return DurableRunnerOutcome::Active {
                        reason: DurableActiveReason::Adapter,
                        condition_reason: wait.reason,
                        detail: wait.detail,
                        requeue_after_seconds: wait.requeue_after_seconds.unwrap_or_else(|| {
                            deadline_requeue_seconds(
                                now_unix_seconds,
                                adapter.deadline_unix_seconds(),
                            )
                        }),
                    };
                }
                HostOutcome::CheckpointRejected(error) => {
                    let message = error.to_string();
                    return match adapter.checkpoint_disposition(&error) {
                        DurableCheckpointDisposition::Incompatible => {
                            DurableRunnerOutcome::Incompatible(message)
                        }
                        DurableCheckpointDisposition::Rejected => {
                            DurableRunnerOutcome::Rejected(message)
                        }
                        DurableCheckpointDisposition::Isolated => {
                            DurableRunnerOutcome::Isolated(message)
                        }
                    };
                }
                HostOutcome::ObservationRejected(error) => {
                    return DurableRunnerOutcome::Isolated(observation_rejection(error));
                }
                HostOutcome::ReloadRequired { boundary, reason } => {
                    return DurableRunnerOutcome::ReloadRequired { boundary, reason };
                }
                HostOutcome::StoreFailed { operation, error } => {
                    return DurableRunnerOutcome::PersistenceFailed { operation, error };
                }
                HostOutcome::Nondeterminism(error) => {
                    return DurableRunnerOutcome::Nondeterministic(error);
                }
                HostOutcome::ObservationAccepted { .. } => {
                    let Some((condition_reason, detail, requeue_after_seconds)) =
                        observation_wait.take()
                    else {
                        return DurableRunnerOutcome::Nondeterministic(
                            Nondeterminism::UnsupportedSuspension,
                        );
                    };
                    return DurableRunnerOutcome::Active {
                        reason: DurableActiveReason::Adapter,
                        condition_reason,
                        detail,
                        requeue_after_seconds,
                    };
                }
                HostOutcome::ScheduleAccepted { .. } => {
                    return DurableRunnerOutcome::Nondeterministic(
                        Nondeterminism::UnsupportedSuspension,
                    );
                }
            };
        }

        DurableRunnerOutcome::Active {
            reason: DurableActiveReason::FuelExhausted,
            condition_reason: "FuelExhausted".to_string(),
            detail: "durable runner exhausted its bounded host-outcome fuel".to_string(),
            requeue_after_seconds: deadline_requeue_seconds(
                now_unix_seconds,
                adapter.deadline_unix_seconds(),
            ),
        }
    }

    fn handle_adapter_boundary<A: DurableOperationAdapter>(
        &self,
        boundary: DurableAdapterBoundary,
        adapter: &A,
        now_unix_seconds: i64,
    ) -> AdapterHandling<A::Publication> {
        match boundary {
            DurableAdapterBoundary::Observed(observation) => AdapterHandling::Observe(observation),
            DurableAdapterBoundary::ObserveAndWait {
                observation,
                reason,
                detail,
                requeue_after_seconds,
            } => AdapterHandling::ObserveAndWait {
                observation: *observation,
                reason,
                detail,
                requeue_after_seconds,
            },
            DurableAdapterBoundary::ObserveAndProgressThenWait {
                observation,
                reason,
                detail,
                requeue_after_seconds,
            } => AdapterHandling::ObserveAndProgressThenWait {
                observation: *observation,
                reason,
                detail,
                requeue_after_seconds,
            },
            DurableAdapterBoundary::Wait { reason, detail } => {
                AdapterHandling::Return(DurableRunnerOutcome::Active {
                    reason: DurableActiveReason::Adapter,
                    condition_reason: reason,
                    detail,
                    requeue_after_seconds: deadline_requeue_seconds(
                        now_unix_seconds,
                        adapter.deadline_unix_seconds(),
                    ),
                })
            }
            DurableAdapterBoundary::Incompatible(message) => {
                AdapterHandling::Return(DurableRunnerOutcome::Incompatible(message))
            }
            DurableAdapterBoundary::Rejected(message) => {
                AdapterHandling::Return(DurableRunnerOutcome::Rejected(message))
            }
            DurableAdapterBoundary::Isolated(message) => {
                AdapterHandling::Return(DurableRunnerOutcome::Isolated(message))
            }
        }
    }
}

enum AdapterHandling<P> {
    Observe(ActivityObservation),
    ObserveAndWait {
        observation: ActivityObservation,
        reason: String,
        detail: String,
        requeue_after_seconds: u64,
    },
    ObserveAndProgressThenWait {
        observation: ActivityObservation,
        reason: String,
        detail: String,
        requeue_after_seconds: u64,
    },
    Return(DurableRunnerOutcome<P>),
}

impl<P> AdapterHandling<P> {
    fn into_result(self) -> DurableRunnerOutcome<P> {
        match self {
            Self::Return(result) => result,
            Self::Observe(_)
            | Self::ObserveAndWait { .. }
            | Self::ObserveAndProgressThenWait { .. } => {
                DurableRunnerOutcome::Nondeterministic(Nondeterminism::UnsupportedSuspension)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DurableRunnerError {
    #[error("durable runner fuel must be greater than zero")]
    ZeroFuel,
}

fn deadline_requeue_seconds(now_unix_seconds: i64, deadline_unix_seconds: i64) -> u64 {
    let remaining = deadline_unix_seconds.saturating_sub(now_unix_seconds);
    u64::try_from(remaining)
        .unwrap_or(MIN_REQUEUE_SECONDS)
        .clamp(MIN_REQUEUE_SECONDS, MAX_REQUEUE_SECONDS)
}

fn observation_rejection(error: ObservationRejection) -> String {
    match error {
        ObservationRejection::CheckpointMissing => {
            "durable observation checkpoint is missing".to_string()
        }
        ObservationRejection::ActivityNotExposed => {
            "durable observation activity is not exposed".to_string()
        }
        ObservationRejection::LogicalActivityMismatch { .. } => {
            "durable observation does not match the exposed activity".to_string()
        }
        ObservationRejection::ResultExceedsDeclaredBound { actual, maximum } => {
            format!("durable observation result uses {actual} bytes; declared maximum is {maximum}")
        }
    }
}

#[cfg(test)]
mod durable_runner_tests {
    use async_trait::async_trait;
    use kuberic_durable_execution::{
        ActivitySpec, ActivityState, CheckpointEnvelope, CheckpointStore, DurableActivity,
        ExactBytes, ExecutionId, HostEpoch, InMemoryCheckpointStore, InMemoryFault,
        PreparedActivityError, StoreErrorKind, WorkflowContext,
    };

    use super::*;
    use crate::durable::checkpoint_store::{
        CheckpointMeasurementDecoder, DurableActivityAccounting, DurableActivityClass,
        DurableCheckpointStore, MeasuredDurableCheckpointStore,
    };

    const ACTIVE_LIMIT: usize = 100_000;
    const TERMINAL_LIMIT: usize = 20_000;

    struct Effect;

    impl DurableActivity for Effect {
        type Input = String;
        type Output = String;

        const NAME: &'static str = "fixture.effect";
        const VERSION: u32 = 1;
        const MAX_INPUT_BYTES: u64 = 64;
        const MAX_RESULT_BYTES: u64 = 64;
    }

    struct OneEffect;

    #[async_trait]
    impl Workflow for OneEffect {
        async fn run(
            &self,
            context: &mut WorkflowContext<'_>,
            _input: ExactBytes,
        ) -> TerminalOutcome {
            match context.call::<Effect>("apply".to_string()).await {
                Ok(result) => TerminalOutcome::succeeded(result.into_bytes()),
                Err(error) => TerminalOutcome::failed(error.to_string().into_bytes()),
            }
        }
    }

    struct TwoEffects;

    #[async_trait]
    impl Workflow for TwoEffects {
        async fn run(
            &self,
            context: &mut WorkflowContext<'_>,
            _input: ExactBytes,
        ) -> TerminalOutcome {
            for value in ["one", "two"] {
                if let Err(error) = context.call::<Effect>(value.to_string()).await {
                    return TerminalOutcome::failed(error.to_string().into_bytes());
                }
            }
            TerminalOutcome::succeeded(ExactBytes::new(b"done"))
        }
    }

    struct EmptyWorkflow;

    #[async_trait]
    impl Workflow for EmptyWorkflow {
        async fn run(
            &self,
            _context: &mut WorkflowContext<'_>,
            _input: ExactBytes,
        ) -> TerminalOutcome {
            TerminalOutcome::succeeded(ExactBytes::new(b"done"))
        }
    }

    #[derive(Clone, Copy)]
    enum AdapterMode {
        Observe,
        ObserveAndWait,
        Wait,
        WrongObservation,
    }

    struct FakeResolver {
        rejection: Option<PreparedActivityError>,
    }

    impl PreparedActivityResolver for FakeResolver {
        fn resolve(
            &self,
            logical: &ActivitySpec,
            _recorded: Option<&ActivitySpec>,
        ) -> Result<ActivitySpec, PreparedActivityError> {
            match &self.rejection {
                Some(error) => Err(error.clone()),
                None => Ok(logical.clone()),
            }
        }
    }

    struct FakeAdapter {
        resolver: FakeResolver,
        mode: AdapterMode,
        deadline: i64,
        checkpoint_disposition: DurableCheckpointDisposition,
        dispatch_calls: usize,
        publication_calls: usize,
        second_consume_rejected: bool,
    }

    impl FakeAdapter {
        fn new(mode: AdapterMode) -> Self {
            Self {
                resolver: FakeResolver { rejection: None },
                mode,
                deadline: 100,
                checkpoint_disposition: DurableCheckpointDisposition::Rejected,
                dispatch_calls: 0,
                publication_calls: 0,
                second_consume_rejected: false,
            }
        }

        fn reject_preparation(&mut self, error: PreparedActivityError) {
            self.resolver.rejection = Some(error);
        }
    }

    #[async_trait]
    impl DurableOperationAdapter for FakeAdapter {
        type Resolver = FakeResolver;
        type Terminal = (TerminalOutcome, u64);
        type Publication = (TerminalOutcome, u64);

        fn resolver(&self) -> &Self::Resolver {
            &self.resolver
        }

        async fn observe_or_dispatch(
            &mut self,
            activity: &LogicalActivityId,
            attempt_id: AttemptId,
            permit: &mut DurablePermitGuard,
        ) -> DurableAdapterBoundary {
            self.dispatch_calls += 1;
            if matches!(self.mode, AdapterMode::Wait) {
                permit
                    .consume(activity.spec(), activity, attempt_id, "fixture")
                    .unwrap();
                return DurableAdapterBoundary::Wait {
                    reason: "AwaitingFixtureEvidence".to_string(),
                    detail: "fresh evidence required".to_string(),
                };
            }
            permit
                .consume(activity.spec(), activity, attempt_id, "fixture")
                .unwrap();
            self.second_consume_rejected = permit
                .consume(activity.spec(), activity, attempt_id, "fixture")
                .is_err();
            let observed = if matches!(self.mode, AdapterMode::WrongObservation) {
                LogicalActivityId::new(
                    ExecutionId::from_bytes([99; 16]),
                    activity.sequence(),
                    activity.spec().clone(),
                )
            } else {
                activity.clone()
            };
            let observation = ActivityObservation::new(observed, ExactBytes::new(br#""applied""#));
            if matches!(self.mode, AdapterMode::ObserveAndWait) {
                DurableAdapterBoundary::ObserveAndWait {
                    observation: Box::new(observation),
                    reason: "RefreshingFixtureEvidence".to_string(),
                    detail: "refresh authoritative fence evidence".to_string(),
                    requeue_after_seconds: 1,
                }
            } else {
                DurableAdapterBoundary::Observed(observation)
            }
        }

        async fn resolve_quarantine(
            &mut self,
            activity: LogicalActivityId,
            _attempt_id: AttemptId,
        ) -> DurableAdapterBoundary {
            DurableAdapterBoundary::Observed(ActivityObservation::new(
                activity,
                ExactBytes::new(br#""recovered""#),
            ))
        }

        fn deadline_unix_seconds(&self) -> i64 {
            self.deadline
        }

        fn checkpoint_disposition(&self, _error: &CheckpointError) -> DurableCheckpointDisposition {
            self.checkpoint_disposition
        }

        fn validate_terminal(
            &mut self,
            outcome: TerminalOutcome,
            completed_activity_count: u64,
        ) -> Result<Self::Terminal, DurableAdapterBoundary> {
            Ok((outcome, completed_activity_count))
        }

        fn publication_handoff(&mut self, terminal: Self::Terminal) -> Self::Publication {
            self.publication_calls += 1;
            terminal
        }
    }

    fn execution(seed: u8) -> ExecutionSpec {
        ExecutionSpec::new(
            ExecutionId::from_bytes([seed; 16]),
            ExactBytes::new(b"fixture"),
            1024,
        )
    }

    fn limits() -> CheckpointLimits {
        CheckpointLimits::new(16, ACTIVE_LIMIT, TERMINAL_LIMIT).unwrap()
    }

    fn decoder() -> CheckpointMeasurementDecoder {
        CheckpointMeasurementDecoder::new(
            "fixture",
            |_| Some(DurableActivityClass::ExternalEffect),
            |_, count| {
                Some(DurableActivityAccounting {
                    external_effect_count: count,
                    passive_observation_count: 0,
                })
            },
        )
    }

    fn fixture_host(store: InMemoryCheckpointStore, seed: u8) -> DurableOperatorHost {
        DurableOperatorHost::new(
            MeasuredDurableCheckpointStore::with_decoder(
                execution(seed).execution_id(),
                DurableCheckpointStore::InMemory(store),
                decoder(),
            ),
            HostEpoch::from_bytes([seed; 16]),
            limits(),
        )
    }

    #[tokio::test]
    async fn freshly_accepted_terminal_reloads_before_publication_and_permit_is_one_use() {
        let store = InMemoryCheckpointStore::new();
        let mut host = fixture_host(store.clone(), 1);
        let mut adapter = FakeAdapter::new(AdapterMode::Observe);
        let outcome = DurableRunner::new(8)
            .unwrap()
            .run(&mut host, &OneEffect, execution(1), &mut adapter, 0)
            .await;

        assert!(matches!(
            outcome,
            DurableRunnerOutcome::Terminal((TerminalOutcome::Succeeded { .. }, 1))
        ));
        assert!(adapter.second_consume_rejected);
        assert_eq!(adapter.publication_calls, 1);
        assert_eq!(host.store().measurements().load_attempts, 4);

        let mut restarted = fixture_host(store, 1);
        let mut restarted_adapter = FakeAdapter::new(AdapterMode::Observe);
        let reloaded = DurableRunner::new(8)
            .unwrap()
            .run(
                &mut restarted,
                &OneEffect,
                execution(1),
                &mut restarted_adapter,
                0,
            )
            .await;
        assert!(matches!(
            reloaded,
            DurableRunnerOutcome::Terminal((TerminalOutcome::Succeeded(_), 1))
        ));
        assert_eq!(restarted_adapter.dispatch_calls, 0);
        assert_eq!(restarted_adapter.publication_calls, 1);
        assert_eq!(restarted.store().measurements().load_attempts, 1);
    }

    #[tokio::test]
    async fn adapter_wait_and_deadline_requeue_are_clamped() {
        for (deadline, expected) in [(-1, 1), (5, 5), (100, 10)] {
            let seed = u8::try_from(deadline.max(0) + 2).unwrap();
            let mut host = fixture_host(InMemoryCheckpointStore::new(), seed);
            let mut adapter = FakeAdapter::new(AdapterMode::Wait);
            adapter.deadline = deadline;
            let outcome = DurableRunner::new(2)
                .unwrap()
                .run(&mut host, &OneEffect, execution(seed), &mut adapter, 0)
                .await;
            assert!(matches!(
                outcome,
                DurableRunnerOutcome::Active {
                    reason: DurableActiveReason::Adapter,
                    requeue_after_seconds,
                    ..
                } if requeue_after_seconds == expected
            ));
        }
    }

    #[tokio::test]
    async fn observation_mismatch_is_isolated() {
        let mut host = fixture_host(InMemoryCheckpointStore::new(), 8);
        let mut adapter = FakeAdapter::new(AdapterMode::WrongObservation);
        let outcome = DurableRunner::new(4)
            .unwrap()
            .run(&mut host, &OneEffect, execution(8), &mut adapter, 0)
            .await;
        assert!(matches!(outcome, DurableRunnerOutcome::Isolated(_)));
    }

    #[tokio::test]
    async fn observe_and_wait_persists_without_same_cycle_progression() {
        let store = InMemoryCheckpointStore::new();
        let mut host = fixture_host(store.clone(), 9);
        let mut adapter = FakeAdapter::new(AdapterMode::ObserveAndWait);
        let outcome = DurableRunner::new(4)
            .unwrap()
            .run(&mut host, &TwoEffects, execution(9), &mut adapter, 0)
            .await;

        assert!(matches!(
            outcome,
            DurableRunnerOutcome::Active {
                reason: DurableActiveReason::Adapter,
                ref detail,
                ..
            } if detail == "refresh authoritative fence evidence"
        ));
        assert_eq!(adapter.dispatch_calls, 1);
        assert_eq!(adapter.publication_calls, 0);

        let stored = store
            .load(execution(9).execution_id())
            .await
            .unwrap()
            .unwrap();
        let payload = stored
            .checkpoint()
            .decode_and_validate(&execution(9), limits())
            .unwrap();
        let activities = payload.active_activities().unwrap();
        assert_eq!(activities.len(), 1);
        assert!(matches!(
            activities[0].state(),
            ActivityState::Completed { .. }
        ));

        let mut restarted = fixture_host(store, 9);
        let mut restarted_adapter = FakeAdapter::new(AdapterMode::Observe);
        let completed = DurableRunner::new(8)
            .unwrap()
            .run(
                &mut restarted,
                &TwoEffects,
                execution(9),
                &mut restarted_adapter,
                0,
            )
            .await;
        assert!(matches!(
            completed,
            DurableRunnerOutcome::Terminal((TerminalOutcome::Succeeded { .. }, 2))
        ));
        assert_eq!(restarted_adapter.dispatch_calls, 1);
    }

    #[tokio::test]
    async fn prepared_activity_rejection_is_an_active_wait() {
        let store = InMemoryCheckpointStore::new();
        let mut host = fixture_host(store.clone(), 10);
        let mut adapter = FakeAdapter::new(AdapterMode::Observe);
        adapter.reject_preparation(PreparedActivityError::Derivation);

        let outcome = DurableRunner::new(2)
            .unwrap()
            .run(&mut host, &OneEffect, execution(10), &mut adapter, 0)
            .await;

        assert!(matches!(
            outcome,
            DurableRunnerOutcome::Active {
                reason: DurableActiveReason::Adapter,
                ref detail,
                requeue_after_seconds: 10,
                ..
            } if detail.contains("preparation awaits authoritative evidence")
        ));
        assert_eq!(adapter.dispatch_calls, 0);
        assert_eq!(adapter.publication_calls, 0);
        assert!(
            store
                .load(execution(10).execution_id())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn conflict_unknown_write_and_persistence_failure_stop_before_redelivery() {
        let cases = [
            (
                InMemoryFault::ConflictWithoutApply,
                "conflict",
                Some(ReloadReason::Conflict),
            ),
            (
                InMemoryFault::OutcomeUnknownWithoutApply,
                "unknown_without_apply",
                Some(ReloadReason::OutcomeUnknown),
            ),
            (
                InMemoryFault::OutcomeUnknownAfterApply,
                "unknown_after_apply",
                Some(ReloadReason::OutcomeUnknown),
            ),
            (
                InMemoryFault::FailBeforeRequest(StoreErrorKind::Unavailable),
                "failure",
                None,
            ),
        ];
        for (index, (fault, _name, expected_reload)) in cases.into_iter().enumerate() {
            let seed = u8::try_from(index + 10).unwrap();
            let store = InMemoryCheckpointStore::new();
            store.fail_next_compare_and_swap(fault);
            let mut host = fixture_host(store, seed);
            let mut adapter = FakeAdapter::new(AdapterMode::Observe);
            let outcome = DurableRunner::new(4)
                .unwrap()
                .run(&mut host, &OneEffect, execution(seed), &mut adapter, 0)
                .await;
            assert_eq!(adapter.dispatch_calls, 0);
            match expected_reload {
                Some(reason) => assert!(matches!(
                    outcome,
                    DurableRunnerOutcome::ReloadRequired {
                        reason: actual,
                        ..
                    } if actual == reason
                )),
                None => assert!(matches!(
                    outcome,
                    DurableRunnerOutcome::PersistenceFailed { .. }
                )),
            }
        }
    }

    #[tokio::test]
    async fn checkpoint_dispositions_cover_rejected_and_incompatible() {
        for (seed, disposition) in [
            (20, DurableCheckpointDisposition::Rejected),
            (21, DurableCheckpointDisposition::Incompatible),
        ] {
            let store = InMemoryCheckpointStore::new();
            store
                .compare_and_swap(
                    execution(seed).execution_id(),
                    None,
                    CheckpointEnvelope::new(1, ExactBytes::new(b"legacy")),
                )
                .await
                .unwrap();
            let mut host = fixture_host(store, seed);
            let mut adapter = FakeAdapter::new(AdapterMode::Observe);
            adapter.checkpoint_disposition = disposition;
            let outcome = DurableRunner::new(2)
                .unwrap()
                .run(&mut host, &OneEffect, execution(seed), &mut adapter, 0)
                .await;
            assert!(match disposition {
                DurableCheckpointDisposition::Rejected =>
                    matches!(outcome, DurableRunnerOutcome::Rejected(_)),
                DurableCheckpointDisposition::Incompatible =>
                    matches!(outcome, DurableRunnerOutcome::Incompatible(_)),
                DurableCheckpointDisposition::Isolated => false,
            });
        }
    }

    #[tokio::test]
    async fn nondeterminism_and_bounded_fuel_have_distinct_outcomes() {
        let store = InMemoryCheckpointStore::new();
        let mut writer = fixture_host(store.clone(), 30);
        let mut adapter = FakeAdapter::new(AdapterMode::Observe);
        let first = DurableRunner::new(1)
            .unwrap()
            .run(&mut writer, &TwoEffects, execution(30), &mut adapter, 0)
            .await;
        assert!(matches!(
            first,
            DurableRunnerOutcome::Active {
                reason: DurableActiveReason::FuelExhausted,
                ..
            }
        ));

        let mut reader = fixture_host(store, 30);
        let mut adapter = FakeAdapter::new(AdapterMode::Observe);
        let second = DurableRunner::new(2)
            .unwrap()
            .run(&mut reader, &EmptyWorkflow, execution(30), &mut adapter, 0)
            .await;
        assert!(matches!(
            second,
            DurableRunnerOutcome::Nondeterministic(Nondeterminism::UnusedHistory { .. })
        ));
    }

    #[test]
    fn workflow_contract_binds_version_and_independent_limits() {
        let native_remove_limits = CheckpointLimits::new(16, 262_144, 12_288).unwrap();
        let contract = DurableWorkflowContract::new(2, native_remove_limits);
        assert_eq!(
            contract.validate(2, native_remove_limits),
            Ok(native_remove_limits)
        );
        assert!(matches!(
            contract.validate(1, native_remove_limits),
            Err(DurableContractError::UnsupportedVersion { .. })
        ));
        assert!(matches!(
            contract.validate(2, CheckpointLimits::new(16, 262_144, 12_289).unwrap()),
            Err(DurableContractError::CheckpointLimitsChanged { .. })
        ));
    }
}
