use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kuberic_core::{
    error::KubericError,
    types::{ReplicaId, ReplicaInstanceId},
};
use kuberic_durable_execution::{
    ActivityContext, ActivityFailure, ActivityHandlerError, ActivitySpec, BoundedEffectError,
    CheckpointError, CheckpointLimits, CompletionMetadata, DurableEffect, EffectErrorKind,
    EffectHostStep, EffectMetadata, EffectObservation, EffectOutcome, ExactBytes, ExecutionId,
    ExecutionSpec, LogicalActivityId, PreparedActivityError, PreparedCommand,
    PreparedEffectResolver, ScopedActivityRegistry, ScopedTypedHandlerFuture, TerminalOutcome,
    decode_effect_command, decode_effect_request, encode_activity_result,
    strict::{
        DispatchEffect, EffectQuarantineContext, ObserveEffect, ObserveQuarantinedEffect,
        PrepareEffect, resolve_prepared_effect,
    },
};
#[cfg(test)]
use kuberic_durable_execution::{ActivityObservation, ExecutionContract};

use crate::{
    cluster_api::ClusterApi,
    crd::KubericSet,
    durable::{
        OperationObservations,
        checkpoint_store::{
            CheckpointMeasurementDecoder, DurableActivityAccounting, DurableActivityClass,
            MeasuredDurableCheckpointStore,
        },
        effects::{
            DispatchFailureDisposition, classify_dispatch_failure, execute_label_command_checked,
            execute_replica_command_state,
        },
        runner::{
            DurableAdapterWait, DurableCheckpointDisposition, DurableEffectQuarantine,
            ReconcilerActivityAdapter, TypedDurableAdapterBoundary,
        },
        workflow_host::DurablePermitGuard,
    },
};

#[cfg(test)]
use super::workflow::DirectSwitchoverWorkflow;
use super::{
    SwitchoverExposureFault, SwitchoverRunnerContext, SwitchoverWorkflowInput,
    activities::{
        AttestCompensatedTopologyActivity, AttestTargetTopologyActivity, CaptureFrozenLsnActivity,
        CompensateDistributeReplicaEpochActivity, CompensatePromoteOldPrimaryActivity,
        DIRECT_SWITCHOVER_CONTRACT_VERSION, DemoteOldPrimaryActivity,
        DistributeReplicaEpochActivity, InstallCompensationCatchUpConfigurationActivity,
        InstallCompensationCurrentConfigurationActivity, InstallTargetCatchUpConfigurationActivity,
        InstallTargetCurrentConfigurationActivity, OrdinarySwitchoverActivity,
        PromoteTargetActivity, PublishOldPrimarySecondaryLabelActivity,
        PublishTargetPrimaryLabelActivity, RestoreOldPrimaryLabelActivity,
        RestorePreviousCurrentConfigurationActivity, RestoreTargetSecondaryLabelActivity,
        RevokeWritesActivity, StrictSwitchoverActivityContract, SwitchoverActivityClass,
        SwitchoverActivityContract, SwitchoverActivityInput, WaitTargetCaughtUpActivity,
        WaitTargetWriteQuorumActivity, activity_class,
    },
    collect_switchover_runner_context, encode_execution_id,
    model::{DirectSwitchoverDefinition, same_topology},
    prepare::{
        LabelOperation, OrdinaryActivityStep, PassiveOperation, ReplicaOperation,
        SwitchoverDispatch, SwitchoverEffectFamily, evaluate_ordinary_label,
        evaluate_ordinary_passive, evaluate_ordinary_replica,
    },
    workflow::{DirectSwitchoverTerminalBranch, DirectSwitchoverTerminalRecord},
};

pub const DIRECT_SWITCHOVER_MAX_ACTIVITY_RECORDS: usize =
    crate::crd::KUBERIC_MAX_REPLICAS as usize + 10;
pub const DIRECT_SWITCHOVER_MAX_RUNNER_FUEL: usize = 32;
pub const DIRECT_SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES: usize = 4_096;
pub const DIRECT_SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES: usize = 512 * 1_024;
pub const DIRECT_SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES: usize = 16 * 1_024;
pub const DIRECT_SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES: u64 = 4_096;

pub struct DirectSwitchoverPreparedActivityResolver {
    definition: DirectSwitchoverDefinition,
    observations: OperationObservations,
    addressed_instances: BTreeMap<ReplicaId, ReplicaInstanceId>,
    now: i64,
    deadline: Arc<AtomicI64>,
}

impl DirectSwitchoverPreparedActivityResolver {
    pub(crate) fn new(
        definition: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
        addressed_instances: &BTreeMap<ReplicaId, ReplicaInstanceId>,
        now: i64,
        deadline: Arc<AtomicI64>,
    ) -> Self {
        Self {
            definition: definition.clone(),
            observations: observations.clone(),
            addressed_instances: addressed_instances.clone(),
            now,
            deadline,
        }
    }
}

impl PreparedEffectResolver for DirectSwitchoverPreparedActivityResolver {
    fn resolve(
        &self,
        _execution_id: ExecutionId,
        logical: &ActivitySpec,
        metadata: EffectMetadata,
        recorded: Option<&PreparedCommand>,
    ) -> Result<PreparedCommand, PreparedActivityError> {
        let deadline = logical
            .options()
            .action_deadline_unix_millis()
            .ok_or(PreparedActivityError::Validation)?
            / 1_000;
        self.deadline.store(deadline, Ordering::Relaxed);
        macro_rules! route {
            ($effect:ty) => {
                if logical.name().name() == <$effect as DurableEffect>::NAME {
                    return resolve_prepared_effect::<$effect, _>(
                        self, logical, metadata, recorded,
                    );
                }
            };
        }
        route!(RevokeWritesActivity);
        route!(DemoteOldPrimaryActivity);
        route!(PromoteTargetActivity);
        route!(CompensatePromoteOldPrimaryActivity);
        Err(PreparedActivityError::Validation)
    }
}

impl<E> PrepareEffect<E> for DirectSwitchoverPreparedActivityResolver
where
    E: DurableEffect
        + SwitchoverActivityContract<
            Request = <E as DurableEffect>::Request,
            Output = <E as DurableEffect>::Output,
        > + StrictSwitchoverActivityContract
        + 'static,
    E::Family: SwitchoverEffectFamily<E>,
{
    type Evidence = ();
    type Authority = ();
    type Error = PreparedActivityError;

    fn prepare(
        &self,
        request: &<E as DurableEffect>::Request,
        _authority: &Self::Authority,
        _evidence: &Self::Evidence,
    ) -> Result<<E as DurableEffect>::Command, Self::Error> {
        let deadline = self.deadline.load(Ordering::Relaxed);
        <E::Family as SwitchoverEffectFamily<E>>::prepare_command(
            request,
            &self.definition,
            &self.observations,
            &self.addressed_instances,
            self.now,
            deadline,
        )
    }

    fn validate_recorded(
        &self,
        request: &<E as DurableEffect>::Request,
        command: &<E as DurableEffect>::Command,
        _authority: &Self::Authority,
    ) -> Result<(), Self::Error> {
        let deadline = self.deadline.load(Ordering::Relaxed);
        <E::Family as SwitchoverEffectFamily<E>>::validate_recorded_command(
            request,
            command,
            &self.definition,
            deadline,
        )
    }
}

pub struct DirectSwitchoverRunnerAdapter<'a> {
    initial: &'a crate::crd::DurableOperationStatus,
    definition: DirectSwitchoverDefinition,
    set: &'a KubericSet,
    current_pods: &'a [(ReplicaId, ReplicaInstanceId, &'a Pod)],
    api: &'a dyn ClusterApi,
    namespace: String,
    resolver: DirectSwitchoverPreparedActivityResolver,
    context: Option<SwitchoverRunnerContext>,
    now: i64,
    deadline: Arc<AtomicI64>,
    exposure_fault: Option<SwitchoverExposureFault>,
    activity_completion_wait: Option<DurableAdapterWait>,
}

enum ActivityDispatch<O> {
    Accepted,
    Pending,
    Outcome(EffectOutcome<O>),
}

fn passive_handler<'a, A>(
    adapter: &'a mut DirectSwitchoverRunnerAdapter<'_>,
    context: ActivityContext,
    input: SwitchoverActivityInput<A>,
) -> ScopedTypedHandlerFuture<'a, A::Output>
where
    A: PassiveOperation + Send + Sync + 'static,
    A::Request: Send,
    A::Output: Send,
{
    Box::pin(adapter.run_passive_activity::<A>(context, input.into_request()))
}

fn replica_handler<'a, A>(
    adapter: &'a mut DirectSwitchoverRunnerAdapter<'_>,
    context: ActivityContext,
    input: SwitchoverActivityInput<A>,
) -> ScopedTypedHandlerFuture<'a, A::Output>
where
    A: ReplicaOperation + Send + Sync + 'static,
    A::Request: Send,
{
    Box::pin(adapter.run_replica_activity::<A>(context, input.into_request()))
}

fn label_handler<'a, A>(
    adapter: &'a mut DirectSwitchoverRunnerAdapter<'_>,
    context: ActivityContext,
    input: SwitchoverActivityInput<A>,
) -> ScopedTypedHandlerFuture<'a, A::Output>
where
    A: LabelOperation + Send + Sync + 'static,
    A::Request: Send,
{
    Box::pin(adapter.run_label_activity::<A>(context, input.into_request()))
}

fn ordinary_activity_registry<'a>() -> ScopedActivityRegistry<DirectSwitchoverRunnerAdapter<'a>> {
    ScopedActivityRegistry::builder()
        .register::<OrdinarySwitchoverActivity<CaptureFrozenLsnActivity>, _>(
            CaptureFrozenLsnActivity::NAME,
            passive_handler::<CaptureFrozenLsnActivity>,
        )
        .register::<OrdinarySwitchoverActivity<WaitTargetCaughtUpActivity>, _>(
            WaitTargetCaughtUpActivity::NAME,
            passive_handler::<WaitTargetCaughtUpActivity>,
        )
        .register::<OrdinarySwitchoverActivity<AttestTargetTopologyActivity>, _>(
            AttestTargetTopologyActivity::NAME,
            passive_handler::<AttestTargetTopologyActivity>,
        )
        .register::<OrdinarySwitchoverActivity<AttestCompensatedTopologyActivity>, _>(
            AttestCompensatedTopologyActivity::NAME,
            passive_handler::<AttestCompensatedTopologyActivity>,
        )
        .register::<OrdinarySwitchoverActivity<DistributeReplicaEpochActivity>, _>(
            DistributeReplicaEpochActivity::NAME,
            replica_handler::<DistributeReplicaEpochActivity>,
        )
        .register::<OrdinarySwitchoverActivity<InstallTargetCatchUpConfigurationActivity>, _>(
            InstallTargetCatchUpConfigurationActivity::NAME,
            replica_handler::<InstallTargetCatchUpConfigurationActivity>,
        )
        .register::<OrdinarySwitchoverActivity<WaitTargetWriteQuorumActivity>, _>(
            WaitTargetWriteQuorumActivity::NAME,
            replica_handler::<WaitTargetWriteQuorumActivity>,
        )
        .register::<OrdinarySwitchoverActivity<InstallTargetCurrentConfigurationActivity>, _>(
            InstallTargetCurrentConfigurationActivity::NAME,
            replica_handler::<InstallTargetCurrentConfigurationActivity>,
        )
        .register::<OrdinarySwitchoverActivity<RestorePreviousCurrentConfigurationActivity>, _>(
            RestorePreviousCurrentConfigurationActivity::NAME,
            replica_handler::<RestorePreviousCurrentConfigurationActivity>,
        )
        .register::<OrdinarySwitchoverActivity<CompensateDistributeReplicaEpochActivity>, _>(
            CompensateDistributeReplicaEpochActivity::NAME,
            replica_handler::<CompensateDistributeReplicaEpochActivity>,
        )
        .register::<OrdinarySwitchoverActivity<InstallCompensationCatchUpConfigurationActivity>, _>(
            InstallCompensationCatchUpConfigurationActivity::NAME,
            replica_handler::<InstallCompensationCatchUpConfigurationActivity>,
        )
        .register::<OrdinarySwitchoverActivity<InstallCompensationCurrentConfigurationActivity>, _>(
            InstallCompensationCurrentConfigurationActivity::NAME,
            replica_handler::<InstallCompensationCurrentConfigurationActivity>,
        )
        .register::<OrdinarySwitchoverActivity<PublishTargetPrimaryLabelActivity>, _>(
            PublishTargetPrimaryLabelActivity::NAME,
            label_handler::<PublishTargetPrimaryLabelActivity>,
        )
        .register::<OrdinarySwitchoverActivity<PublishOldPrimarySecondaryLabelActivity>, _>(
            PublishOldPrimarySecondaryLabelActivity::NAME,
            label_handler::<PublishOldPrimarySecondaryLabelActivity>,
        )
        .register::<OrdinarySwitchoverActivity<RestoreOldPrimaryLabelActivity>, _>(
            RestoreOldPrimaryLabelActivity::NAME,
            label_handler::<RestoreOldPrimaryLabelActivity>,
        )
        .register::<OrdinarySwitchoverActivity<RestoreTargetSecondaryLabelActivity>, _>(
            RestoreTargetSecondaryLabelActivity::NAME,
            label_handler::<RestoreTargetSecondaryLabelActivity>,
        )
        .build()
        .expect("ordinary switchover activity registry is statically valid")
}

fn strict_step_boundary(result: Result<EffectHostStep, String>) -> TypedDurableAdapterBoundary {
    match result {
        Ok(EffectHostStep::Observed(observation)) => {
            if observation.is_proven_no_admission() {
                TypedDurableAdapterBoundary::ObserveAndWait {
                    observation,
                    reason: "RefreshingReplicaObservation".to_string(),
                    detail: "proven non-admission was persisted before the one allowed redelivery"
                        .to_string(),
                    requeue_after_seconds: 1,
                }
            } else {
                TypedDurableAdapterBoundary::Observed(observation)
            }
        }
        Ok(EffectHostStep::Pending) => TypedDurableAdapterBoundary::Wait {
            reason: "AwaitingEffectObservation".to_string(),
            detail: "typed switchover effect awaits exact authoritative evidence".to_string(),
        },
        Err(error) => TypedDurableAdapterBoundary::Isolated(error),
    }
}

fn strict_quarantine_boundary(
    result: Result<Option<kuberic_durable_execution::EffectObservation>, String>,
) -> TypedDurableAdapterBoundary {
    match result {
        Ok(Some(observation)) => TypedDurableAdapterBoundary::Observed(Box::new(observation)),
        Ok(None) => TypedDurableAdapterBoundary::Wait {
            reason: "Quarantined".to_string(),
            detail: "typed switchover effect remains observation-only until exact evidence arrives"
                .to_string(),
        },
        Err(error) => TypedDurableAdapterBoundary::Isolated(error),
    }
}

fn strict_observation<E>(
    activity: &LogicalActivityId,
    attempt_id: kuberic_durable_execution::AttemptId,
    outcome: EffectOutcome<<E as DurableEffect>::Output>,
) -> Result<EffectObservation, String>
where
    E: DurableEffect
        + SwitchoverActivityContract<
            Request = <E as DurableEffect>::Request,
            Output = <E as DurableEffect>::Output,
        >,
{
    match outcome {
        EffectOutcome::Applied(output) => {
            encode_activity_result::<OrdinarySwitchoverActivity<E>>(&output)
                .map(|result| EffectObservation::completed(activity.clone(), attempt_id, result))
                .map_err(|error| error.to_string())
        }
        EffectOutcome::ProvenNoAdmission => {
            let error = BoundedEffectError::observed_at(
                EffectErrorKind::ProvenNoAdmission,
                "strict dispatch was proven not admitted",
                0,
                <E as SwitchoverActivityContract>::MAX_ERROR_MESSAGE_BYTES,
            )
            .map_err(|error| error.to_string())?;
            let failure = serde_json::to_vec(&error)
                .map(ExactBytes::new)
                .map(ActivityFailure::Application)
                .map_err(|error| error.to_string())?;
            Ok(EffectObservation::proven_no_admission(
                activity.clone(),
                attempt_id,
                failure,
            ))
        }
        EffectOutcome::DomainFailure(error)
        | EffectOutcome::DeadlineExceeded(error)
        | EffectOutcome::UnavailableAtDeadline(error)
        | EffectOutcome::ConflictingEvidence(error) => {
            let failure = serde_json::to_vec(&error)
                .map(ExactBytes::new)
                .map(ActivityFailure::Application)
                .map_err(|error| error.to_string())?;
            Ok(EffectObservation::completed_failure(
                activity.clone(),
                attempt_id,
                failure,
            ))
        }
    }
}

async fn invoke_strict_effect<E, H>(
    handler: &mut H,
    activity: &LogicalActivityId,
    attempt_id: kuberic_durable_execution::AttemptId,
    prepared: &PreparedCommand,
) -> Result<EffectHostStep, String>
where
    E: DurableEffect
        + SwitchoverActivityContract<
            Request = <E as DurableEffect>::Request,
            Output = <E as DurableEffect>::Output,
        >,
    H: Send + DispatchEffect<E> + ObserveEffect<E>,
    <H as DispatchEffect<E>>::Error: std::fmt::Display,
    <H as ObserveEffect<E>>::Error: std::fmt::Display,
{
    let request = decode_effect_request::<E>(activity.spec().input())
        .map_err(|error| format!("decode strict activity request: {error}"))?;
    let command = decode_effect_command::<E>(prepared)
        .map_err(|error| format!("decode strict activity command: {error}"))?;
    if let Some(outcome) = <H as ObserveEffect<E>>::observe(handler, &request, &command, attempt_id)
        .await
        .map_err(|error| error.to_string())?
    {
        return strict_observation::<E>(activity, attempt_id, outcome)
            .map(Box::new)
            .map(EffectHostStep::Observed);
    }
    match <H as DispatchEffect<E>>::dispatch(handler, &request, &command, attempt_id)
        .await
        .map_err(|error| error.to_string())?
    {
        Some(outcome) => strict_observation::<E>(activity, attempt_id, outcome)
            .map(Box::new)
            .map(EffectHostStep::Observed),
        None => Ok(EffectHostStep::Pending),
    }
}

async fn observe_quarantined_strict_effect<E, H>(
    handler: &mut H,
    activity: &LogicalActivityId,
    attempt_id: kuberic_durable_execution::AttemptId,
    prepared: &PreparedCommand,
) -> Result<Option<EffectObservation>, String>
where
    E: DurableEffect
        + SwitchoverActivityContract<
            Request = <E as DurableEffect>::Request,
            Output = <E as DurableEffect>::Output,
        >,
    H: Send + ObserveQuarantinedEffect<E>,
    <H as ObserveQuarantinedEffect<E>>::Error: std::fmt::Display,
{
    let request = decode_effect_request::<E>(activity.spec().input())
        .map_err(|error| format!("decode strict activity request: {error}"))?;
    let command = decode_effect_command::<E>(prepared)
        .map_err(|error| format!("decode strict activity command: {error}"))?;
    let context = EffectQuarantineContext::<E>::new(&request, &command, attempt_id);
    <H as ObserveQuarantinedEffect<E>>::observe_quarantined(handler, context)
        .await
        .map_err(|error| error.to_string())?
        .map(|outcome| strict_observation::<E>(activity, attempt_id, outcome))
        .transpose()
}

impl<'a> DirectSwitchoverRunnerAdapter<'a> {
    pub fn new(
        initial: &'a crate::crd::DurableOperationStatus,
        set: &'a KubericSet,
        current_pods: &'a [(ReplicaId, ReplicaInstanceId, &'a Pod)],
        api: &'a dyn ClusterApi,
        _store: MeasuredDurableCheckpointStore,
        now: i64,
    ) -> Result<Self, String> {
        let definition = DirectSwitchoverDefinition::from_initial(initial)?;
        let deadline = Arc::new(AtomicI64::new(definition.initial_deadline_unix_seconds));
        Ok(Self {
            initial,
            definition: definition.clone(),
            set,
            current_pods,
            api,
            namespace: set.metadata.namespace.clone().unwrap_or_default(),
            resolver: DirectSwitchoverPreparedActivityResolver::new(
                &definition,
                &OperationObservations::new(),
                &BTreeMap::new(),
                now,
                deadline.clone(),
            ),
            context: None,
            now,
            deadline,
            exposure_fault: None,
            activity_completion_wait: None,
        })
    }

    pub(crate) fn with_exposure_fault(
        mut self,
        exposure_fault: Option<SwitchoverExposureFault>,
    ) -> Self {
        self.exposure_fault = exposure_fault;
        self
    }

    fn context(&self) -> Result<&SwitchoverRunnerContext, String> {
        self.context
            .as_ref()
            .ok_or_else(|| "direct switchover adapter was not prepared".to_string())
    }

    async fn dispatch_activity<E>(
        &mut self,
        command: &<E as DurableEffect>::Command,
    ) -> Result<ActivityDispatch<<E as DurableEffect>::Output>, String>
    where
        E: DurableEffect
            + StrictSwitchoverActivityContract
            + SwitchoverActivityContract<
                Request = <E as DurableEffect>::Request,
                Output = <E as DurableEffect>::Output,
            >,
        E::Family: SwitchoverEffectFamily<E>,
    {
        match <E::Family as SwitchoverEffectFamily<E>>::dispatch(command) {
            SwitchoverDispatch::ObservationOnly => Ok(ActivityDispatch::Pending),
            SwitchoverDispatch::Replica(command) => {
                let context = self.context()?;
                let Some(handle) = context.handles.get(&command.target_id) else {
                    return Ok(ActivityDispatch::Pending);
                };
                if handle.instance_id().as_str() != command.target_instance_id {
                    return Ok(ActivityDispatch::Pending);
                }
                match execute_replica_command_state(handle.as_ref(), command).await {
                    Ok(kuberic_core::types::DurableActionState::Completed) => {
                        Ok(ActivityDispatch::Accepted)
                    }
                    Ok(
                        kuberic_core::types::DurableActionState::Scheduled
                        | kuberic_core::types::DurableActionState::InProgress,
                    ) => Ok(ActivityDispatch::Pending),
                    Ok(kuberic_core::types::DurableActionState::Failed) => {
                        unreachable!("failed acknowledgements are returned as typed errors")
                    }
                    Err(error) => {
                        let kind = match classify_dispatch_failure(&error) {
                            DispatchFailureDisposition::ProvenNoAdmission => {
                                return Ok(ActivityDispatch::Outcome(
                                    EffectOutcome::ProvenNoAdmission,
                                ));
                            }
                            DispatchFailureDisposition::DefiniteFailure
                                if matches!(error, KubericError::RemoteAgentConflict(_)) =>
                            {
                                EffectErrorKind::ConflictingEvidence
                            }
                            DispatchFailureDisposition::DefiniteFailure => {
                                EffectErrorKind::DomainFailure
                            }
                            DispatchFailureDisposition::Unknown => {
                                return Ok(ActivityDispatch::Pending);
                            }
                        };
                        let bounded = BoundedEffectError::observed_at(
                            kind,
                            super::bounded_utf8(
                                &error.to_string(),
                                super::SWITCHOVER_MAX_ERROR_BYTES,
                            ),
                            self.now,
                            <E as SwitchoverActivityContract>::MAX_ERROR_MESSAGE_BYTES,
                        )
                        .map_err(|error| error.to_string())?;
                        Ok(ActivityDispatch::Outcome(match kind {
                            EffectErrorKind::DomainFailure => EffectOutcome::DomainFailure(bounded),
                            EffectErrorKind::ConflictingEvidence => {
                                EffectOutcome::ConflictingEvidence(bounded)
                            }
                            _ => unreachable!("dispatch failures use a terminal failure kind"),
                        }))
                    }
                }
            }
        }
    }

    fn wait_for_evidence(&self, context: &ActivityContext) -> ActivityHandlerError {
        let next_observation = self.now.saturating_add(1).saturating_mul(1_000);
        let wake_at = context
            .action_deadline_unix_millis()
            .map_or(next_observation, |deadline| deadline.min(next_observation));
        ActivityHandlerError::WaitUntil(wake_at)
    }

    fn handler_outcome<O>(&self, outcome: EffectOutcome<O>) -> Result<O, ActivityHandlerError> {
        match outcome {
            EffectOutcome::Applied(output) => Ok(output),
            EffectOutcome::ProvenNoAdmission => {
                let error = BoundedEffectError::observed_at(
                    EffectErrorKind::ProvenNoAdmission,
                    "ordinary invocation was proven not admitted",
                    self.now,
                    super::SWITCHOVER_MAX_ERROR_BYTES as u64,
                )
                .expect("fixed ordinary retry error fits the configured bound");
                Err(ActivityHandlerError::Retryable(ExactBytes::new(
                    serde_json::to_vec(&error)
                        .expect("bounded ordinary retry error is serializable"),
                )))
            }
            EffectOutcome::DomainFailure(error)
            | EffectOutcome::DeadlineExceeded(error)
            | EffectOutcome::UnavailableAtDeadline(error)
            | EffectOutcome::ConflictingEvidence(error) => {
                let encoded = serde_json::to_vec(&error).map_err(|_| {
                    ActivityHandlerError::Codec(
                        kuberic_durable_execution::ActivityCallError::ResultEncoding,
                    )
                })?;
                Err(ActivityHandlerError::Terminal(ExactBytes::new(encoded)))
            }
        }
    }

    async fn run_passive_activity<E>(
        &mut self,
        context: ActivityContext,
        input: E::Request,
    ) -> Result<E::Output, ActivityHandlerError>
    where
        E: PassiveOperation,
    {
        let deadline = context
            .action_deadline_unix_millis()
            .map(|value| value / 1_000)
            .unwrap_or_else(|| self.deadline.load(Ordering::Relaxed));
        let observations = &self.context().map_err(terminal_handler_error)?.observations;
        match evaluate_ordinary_passive::<E>(
            &input,
            &self.definition,
            observations,
            self.now,
            deadline,
        )
        .map_err(terminal_handler_error)?
        {
            OrdinaryActivityStep::Complete(outcome) => self.handler_outcome(outcome),
            OrdinaryActivityStep::AwaitEvidence { .. } => Err(self.wait_for_evidence(&context)),
            OrdinaryActivityStep::DispatchReplica(_) | OrdinaryActivityStep::DispatchLabel(_) => {
                Err(terminal_handler_error(
                    "passive activity attempted external dispatch".to_string(),
                ))
            }
        }
    }

    async fn run_replica_activity<E>(
        &mut self,
        context: ActivityContext,
        input: E::Request,
    ) -> Result<E::Output, ActivityHandlerError>
    where
        E: ReplicaOperation,
    {
        let deadline = context
            .action_deadline_unix_millis()
            .map(|value| value / 1_000)
            .unwrap_or_else(|| self.deadline.load(Ordering::Relaxed));
        let runner_context = self.context().map_err(terminal_handler_error)?;
        let step = evaluate_ordinary_replica::<E>(
            &input,
            context.activity_instance_id(),
            &self.definition,
            &runner_context.observations,
            &runner_context.addressed_instances,
            self.now,
            deadline,
        )
        .map_err(terminal_handler_error)?;
        match step {
            OrdinaryActivityStep::Complete(outcome) => self.handler_outcome(outcome),
            OrdinaryActivityStep::AwaitEvidence {
                authoritative_pending,
            } => {
                if context.dispatch_authorized() || authoritative_pending {
                    Err(self.wait_for_evidence(&context))
                } else {
                    Err(retryable_handler_error(
                        "ordinary replica activity result was lost".to_string(),
                    ))
                }
            }
            OrdinaryActivityStep::DispatchReplica(command) => {
                if !context.dispatch_authorized() {
                    return Err(retryable_handler_error(
                        "ordinary replica activity requires a persisted retry attempt".to_string(),
                    ));
                }
                let runner_context = self.context().map_err(terminal_handler_error)?;
                let Some(handle) = runner_context.handles.get(&command.target_id) else {
                    return Err(self.wait_for_evidence(&context));
                };
                if handle.instance_id().as_str() != command.target_instance_id {
                    return Err(terminal_handler_error(
                        "ordinary replica target incarnation changed".to_string(),
                    ));
                }
                match execute_replica_command_state(handle.as_ref(), &command).await {
                    Ok(kuberic_core::types::DurableActionState::Completed) => {
                        self.activity_completion_wait = Some(DurableAdapterWait {
                            reason: "ActivityDispatched".to_string(),
                            detail: format!(
                                "ordinary activity {} was accepted; reconciliation will refresh evidence",
                                E::NAME
                            ),
                            requeue_after_seconds: Some(1),
                        });
                        Ok(super::activities::EffectApplied {
                            observed_at_unix_seconds: self.now,
                        })
                    }
                    Ok(
                        kuberic_core::types::DurableActionState::Scheduled
                        | kuberic_core::types::DurableActionState::InProgress,
                    ) => Err(self.wait_for_evidence(&context)),
                    Ok(kuberic_core::types::DurableActionState::Failed) => {
                        unreachable!("failed acknowledgements are returned as typed errors")
                    }
                    Err(error) => {
                        let bounded = BoundedEffectError::observed_at(
                            match classify_dispatch_failure(&error) {
                                DispatchFailureDisposition::ProvenNoAdmission => {
                                    EffectErrorKind::ProvenNoAdmission
                                }
                                DispatchFailureDisposition::DefiniteFailure
                                    if matches!(error, KubericError::RemoteAgentConflict(_)) =>
                                {
                                    EffectErrorKind::ConflictingEvidence
                                }
                                DispatchFailureDisposition::DefiniteFailure => {
                                    EffectErrorKind::DomainFailure
                                }
                                DispatchFailureDisposition::Unknown => {
                                    return Err(retryable_handler_error(error.to_string()));
                                }
                            },
                            super::bounded_utf8(
                                &error.to_string(),
                                super::SWITCHOVER_MAX_ERROR_BYTES,
                            ),
                            self.now,
                            E::MAX_ERROR_MESSAGE_BYTES,
                        )
                        .map_err(|error| terminal_handler_error(error.to_string()))?;
                        self.handler_outcome(match bounded.kind() {
                            EffectErrorKind::ProvenNoAdmission => EffectOutcome::ProvenNoAdmission,
                            EffectErrorKind::DomainFailure => EffectOutcome::DomainFailure(bounded),
                            EffectErrorKind::ConflictingEvidence => {
                                EffectOutcome::ConflictingEvidence(bounded)
                            }
                            _ => unreachable!("dispatch failure classification is exhaustive"),
                        })
                    }
                }
            }
            OrdinaryActivityStep::DispatchLabel(_) => Err(terminal_handler_error(
                "replica activity attempted label dispatch".to_string(),
            )),
        }
    }

    async fn run_label_activity<E>(
        &mut self,
        context: ActivityContext,
        input: E::Request,
    ) -> Result<E::Output, ActivityHandlerError>
    where
        E: LabelOperation,
    {
        let deadline = context
            .action_deadline_unix_millis()
            .map(|value| value / 1_000)
            .unwrap_or_else(|| self.deadline.load(Ordering::Relaxed));
        let observations = &self.context().map_err(terminal_handler_error)?.observations;
        match evaluate_ordinary_label::<E>(
            &input,
            &self.definition,
            observations,
            self.now,
            deadline,
        )
        .map_err(terminal_handler_error)?
        {
            OrdinaryActivityStep::Complete(outcome) => self.handler_outcome(outcome),
            OrdinaryActivityStep::AwaitEvidence { .. } => {
                if context.dispatch_authorized() {
                    Err(self.wait_for_evidence(&context))
                } else {
                    Err(retryable_handler_error(
                        "ordinary label activity result was lost".to_string(),
                    ))
                }
            }
            OrdinaryActivityStep::DispatchLabel(command) => {
                if !context.dispatch_authorized() {
                    return Err(retryable_handler_error(
                        "ordinary label activity requires a persisted retry attempt".to_string(),
                    ));
                }
                execute_label_command_checked(self.api, &self.namespace, &command)
                    .await
                    .map_err(retryable_handler_error)?;
                Err(self.wait_for_evidence(&context))
            }
            OrdinaryActivityStep::DispatchReplica(_) => Err(terminal_handler_error(
                "label activity attempted replica dispatch".to_string(),
            )),
        }
    }
}

fn terminal_handler_error(message: String) -> ActivityHandlerError {
    ActivityHandlerError::Terminal(ExactBytes::new(message.into_bytes()))
}

fn retryable_handler_error(message: String) -> ActivityHandlerError {
    ActivityHandlerError::Retryable(ExactBytes::new(message.into_bytes()))
}

#[async_trait]
impl<E> ObserveEffect<E> for DirectSwitchoverRunnerAdapter<'_>
where
    E: DurableEffect
        + SwitchoverActivityContract<
            Request = <E as DurableEffect>::Request,
            Output = <E as DurableEffect>::Output,
        > + StrictSwitchoverActivityContract,
    E::Family: SwitchoverEffectFamily<E>,
{
    type Error = String;

    async fn observe(
        &mut self,
        request: &<E as DurableEffect>::Request,
        command: &<E as DurableEffect>::Command,
        _attempt_id: kuberic_durable_execution::AttemptId,
    ) -> Result<Option<EffectOutcome<<E as DurableEffect>::Output>>, Self::Error> {
        let observations = &self.context()?.observations;
        <E::Family as SwitchoverEffectFamily<E>>::observe(
            request,
            command,
            &self.definition,
            observations,
            self.now,
            self.deadline.load(Ordering::Relaxed),
        )
    }
}

#[async_trait]
impl<E> ObserveQuarantinedEffect<E> for DirectSwitchoverRunnerAdapter<'_>
where
    E: DurableEffect
        + SwitchoverActivityContract<
            Request = <E as DurableEffect>::Request,
            Output = <E as DurableEffect>::Output,
        > + StrictSwitchoverActivityContract
        + 'static,
    E::Family: SwitchoverEffectFamily<E>,
{
    type Error = String;

    async fn observe_quarantined(
        &mut self,
        context: EffectQuarantineContext<'_, E>,
    ) -> Result<Option<EffectOutcome<<E as DurableEffect>::Output>>, Self::Error> {
        let observations = &self.context()?.observations;
        <E::Family as SwitchoverEffectFamily<E>>::observe_quarantined(
            context.request(),
            context.command(),
            &self.definition,
            observations,
            self.now,
            self.deadline.load(Ordering::Relaxed),
        )
    }
}

#[async_trait]
impl<E> DispatchEffect<E> for DirectSwitchoverRunnerAdapter<'_>
where
    E: DurableEffect
        + SwitchoverActivityContract<
            Request = <E as DurableEffect>::Request,
            Output = <E as DurableEffect>::Output,
        > + StrictSwitchoverActivityContract,
    E::Family: SwitchoverEffectFamily<E>,
{
    type Error = String;

    async fn dispatch(
        &mut self,
        _request: &<E as DurableEffect>::Request,
        command: &<E as DurableEffect>::Command,
        _attempt_id: kuberic_durable_execution::AttemptId,
    ) -> Result<Option<EffectOutcome<<E as DurableEffect>::Output>>, Self::Error> {
        match self.dispatch_activity::<E>(command).await? {
            ActivityDispatch::Outcome(outcome) => Ok(Some(outcome)),
            ActivityDispatch::Accepted | ActivityDispatch::Pending => Ok(None),
        }
    }
}

#[async_trait]
impl ReconcilerActivityAdapter for DirectSwitchoverRunnerAdapter<'_> {
    type Resolver = DirectSwitchoverPreparedActivityResolver;
    type Terminal = DirectSwitchoverTerminalRecord;
    type Publication = DirectSwitchoverTerminalRecord;

    fn resolver(&self) -> &Self::Resolver {
        &self.resolver
    }

    async fn prepare(&mut self) -> Result<(), TypedDurableAdapterBoundary> {
        let context =
            collect_switchover_runner_context(self.initial, self.set, self.api, self.current_pods)
                .await
                .map_err(TypedDurableAdapterBoundary::Isolated)?;
        self.resolver = DirectSwitchoverPreparedActivityResolver::new(
            &self.definition,
            &context.observations,
            &context.addressed_instances,
            self.now,
            self.deadline.clone(),
        );
        self.context = Some(context);
        Ok(())
    }

    async fn invoke_activity(
        &mut self,
        context: ActivityContext,
        input: ExactBytes,
    ) -> Result<ExactBytes, ActivityHandlerError> {
        self.activity_completion_wait = None;
        ordinary_activity_registry()
            .invoke_classified(self, context, input)
            .await
    }

    fn take_activity_completion_wait(&mut self) -> Option<DurableAdapterWait> {
        self.activity_completion_wait.take()
    }

    async fn observe_or_dispatch(
        &mut self,
        activity_id: &LogicalActivityId,
        attempt_id: kuberic_durable_execution::AttemptId,
        permit: &mut DurablePermitGuard,
    ) -> TypedDurableAdapterBoundary {
        let command = match permit.consume_prepared_command(activity_id, attempt_id, "switchover") {
            Ok(command) => command,
            Err(error) => return TypedDurableAdapterBoundary::Isolated(error),
        };
        macro_rules! route {
            ($effect:ty) => {
                if activity_id.spec().name().name() == <$effect as DurableEffect>::NAME {
                    return strict_step_boundary(
                        invoke_strict_effect::<$effect, _>(self, activity_id, attempt_id, &command)
                            .await,
                    );
                }
            };
        }
        route!(RevokeWritesActivity);
        route!(DemoteOldPrimaryActivity);
        route!(PromoteTargetActivity);
        route!(CompensatePromoteOldPrimaryActivity);
        TypedDurableAdapterBoundary::Isolated(format!(
            "strict switchover activity {} is not registered",
            activity_id.spec().name().name()
        ))
    }

    async fn resolve_quarantine(
        &mut self,
        quarantine: DurableEffectQuarantine,
    ) -> TypedDurableAdapterBoundary {
        macro_rules! route {
            ($effect:ty) => {
                if quarantine.activity().spec().name().name() == <$effect as DurableEffect>::NAME {
                    return strict_quarantine_boundary(
                        observe_quarantined_strict_effect::<$effect, _>(
                            self,
                            quarantine.activity(),
                            quarantine.attempt_id(),
                            quarantine.prepared_command(),
                        )
                        .await,
                    );
                }
            };
        }
        route!(RevokeWritesActivity);
        route!(DemoteOldPrimaryActivity);
        route!(PromoteTargetActivity);
        route!(CompensatePromoteOldPrimaryActivity);
        TypedDurableAdapterBoundary::Isolated(format!(
            "strict switchover activity {} is not registered",
            quarantine.activity().spec().name().name()
        ))
    }

    fn interrupt_after_accepted_exposure(
        &mut self,
        activity: &LogicalActivityId,
        _attempt_id: kuberic_durable_execution::AttemptId,
    ) -> Option<DurableAdapterWait> {
        self.exposure_fault
            .as_ref()
            .filter(|fault| fault.interrupt(activity.spec().name().name()))
            .map(|_| DurableAdapterWait {
                reason: "ExposureInterrupted".to_string(),
                detail: format!(
                    "direct switchover exposure for {} was accepted before adapter evaluation",
                    activity.spec().name().name()
                ),
                requeue_after_seconds: Some(1),
            })
    }

    fn deadline_unix_seconds(&self) -> i64 {
        self.deadline.load(Ordering::Relaxed)
    }

    fn preparation_wait(&self, _error: &CheckpointError) -> DurableAdapterWait {
        DurableAdapterWait {
            reason: "AwaitingEffectPreparation".to_string(),
            detail: "direct switchover effect preparation awaits one exact observation snapshot"
                .to_string(),
            requeue_after_seconds: Some(1),
        }
    }

    fn checkpoint_disposition(&self, error: &CheckpointError) -> DurableCheckpointDisposition {
        if matches!(error, CheckpointError::UnsupportedFormat { .. }) {
            DurableCheckpointDisposition::Incompatible
        } else {
            DurableCheckpointDisposition::Rejected
        }
    }

    fn validate_terminal(
        &mut self,
        outcome: TerminalOutcome,
        completed_activity_count: u64,
        completion_metadata: Option<CompletionMetadata>,
    ) -> Result<Self::Terminal, TypedDurableAdapterBoundary> {
        let terminal =
            validate_direct_terminal(&self.definition, &outcome, completed_activity_count)
                .map_err(TypedDurableAdapterBoundary::Rejected)?;
        let metadata = completion_metadata.ok_or_else(|| {
            TypedDurableAdapterBoundary::Rejected(
                "direct switchover terminal lacks authenticated activity classification"
                    .to_string(),
            )
        })?;
        if metadata.completed_activity_count() != completed_activity_count {
            return Err(TypedDurableAdapterBoundary::Rejected(
                "direct switchover terminal completion metadata count changed".to_string(),
            ));
        }
        if let Some(expected) =
            decode_direct_terminal_accounting(&outcome, completed_activity_count)
            && (metadata.external_effect_count() != expected.external_effect_count
                || metadata.passive_observation_count() != expected.passive_observation_count)
        {
            return Err(TypedDurableAdapterBoundary::Rejected(
                "direct switchover terminal activity classification changed".to_string(),
            ));
        }

        Ok(terminal)
    }

    fn publication_handoff(&mut self, terminal: Self::Terminal) -> Self::Publication {
        terminal
    }
}

pub fn direct_execution_spec(
    execution_id: ExecutionId,
    initial_operation: crate::crd::DurableOperationStatus,
) -> Result<ExecutionSpec, String> {
    let definition = DirectSwitchoverDefinition::from_initial(&initial_operation)?;
    if definition.execution_id != initial_operation.execution_id {
        return Err("direct switchover admission execution identity changed".to_string());
    }
    let input = SwitchoverWorkflowInput {
        version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
        execution_id: encode_execution_id(execution_id),
        initial_operation,
    };
    let encoded = serde_json::to_vec(&input)
        .map_err(|error| format!("encode direct switchover workflow input: {error}"))?;
    validate_direct_workflow_input_bytes(encoded.len())?;
    Ok(ExecutionSpec::new(
        execution_id,
        ExactBytes::new(encoded),
        DIRECT_SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES,
    ))
}

pub fn direct_checkpoint_limits() -> CheckpointLimits {
    CheckpointLimits::new(
        DIRECT_SWITCHOVER_MAX_ACTIVITY_RECORDS,
        DIRECT_SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES,
        DIRECT_SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES,
    )
    .expect("direct switchover checkpoint limits are nonzero")
}

pub fn direct_checkpoint_measurement_decoder() -> CheckpointMeasurementDecoder {
    CheckpointMeasurementDecoder::new(
        "switchover",
        classify_direct_checkpoint_activity,
        decode_direct_terminal_accounting,
    )
}

fn classify_direct_checkpoint_activity(spec: &ActivitySpec) -> Option<DurableActivityClass> {
    if spec.name().version() != super::activities::DIRECT_ACTIVITY_VERSION {
        return None;
    }
    activity_class(spec.name().name()).map(|class| match class {
        SwitchoverActivityClass::PassiveReadOnly => DurableActivityClass::PassiveObservation,
        SwitchoverActivityClass::NaturallyIdempotent
        | SwitchoverActivityClass::IdentityFencedIdempotent
        | SwitchoverActivityClass::StrictEffectRequired => DurableActivityClass::ExternalEffect,
    })
}

fn decode_direct_terminal_accounting(
    outcome: &TerminalOutcome,
    completed_activity_count: u64,
) -> Option<DurableActivityAccounting> {
    let terminal =
        serde_json::from_slice::<DirectSwitchoverTerminalRecord>(outcome.payload().as_slice())
            .ok()?;
    let passive_observation_count = match terminal {
        DirectSwitchoverTerminalRecord::Complete {
            branch: DirectSwitchoverTerminalBranch::RevokeSafeFailure,
            ..
        } if completed_activity_count >= 1 => 1,
        DirectSwitchoverTerminalRecord::Complete { .. } if completed_activity_count >= 3 => 3,
        _ => return None,
    };
    Some(DurableActivityAccounting {
        external_effect_count: completed_activity_count - passive_observation_count,
        passive_observation_count,
    })
}

fn validate_direct_terminal(
    definition: &DirectSwitchoverDefinition,
    outcome: &TerminalOutcome,
    completed_activity_count: u64,
) -> Result<DirectSwitchoverTerminalRecord, String> {
    if completed_activity_count == 0
        || completed_activity_count > DIRECT_SWITCHOVER_MAX_ACTIVITY_RECORDS as u64
    {
        return Err("direct switchover terminal activity count is outside bounds".to_string());
    }
    let record =
        serde_json::from_slice::<DirectSwitchoverTerminalRecord>(outcome.payload().as_slice())
            .map_err(|error| format!("decode direct switchover terminal: {error}"))?;
    match (&outcome, &record) {
        (
            TerminalOutcome::Succeeded(_),
            DirectSwitchoverTerminalRecord::Complete {
                snapshot,
                compensated: false,
                branch: DirectSwitchoverTerminalBranch::TargetSuccess,
                reason: None,
            },
        ) if same_topology(snapshot, &definition.target_snapshot) => {}
        (
            TerminalOutcome::Succeeded(_),
            DirectSwitchoverTerminalRecord::Complete {
                snapshot,
                compensated: true,
                branch:
                    branch @ (DirectSwitchoverTerminalBranch::RevokeSafeFailure
                    | DirectSwitchoverTerminalBranch::PreviousConfigurationRestored),
                reason: Some(_),
            },
        ) if same_topology(snapshot, &definition.previous_snapshot) => {}
        (
            TerminalOutcome::Succeeded(_),
            DirectSwitchoverTerminalRecord::Complete {
                snapshot,
                compensated: true,
                branch: DirectSwitchoverTerminalBranch::PostPromotionCompensated,
                reason: Some(_),
            },
        ) if same_topology(snapshot, &definition.compensation_snapshot()) => {}
        (TerminalOutcome::Failed(_), DirectSwitchoverTerminalRecord::Stopped { .. }) => {}
        _ => {
            return Err(
                "direct switchover terminal kind or topology conflicts with admission".to_string(),
            );
        }
    }
    Ok(record)
}

pub fn validate_direct_workflow_input_bytes(actual: usize) -> Result<(), String> {
    validate_maximum(
        "workflow input",
        actual,
        DIRECT_SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES,
    )
}

#[cfg(test)]
pub fn validate_direct_active_checkpoint_bytes(actual: usize) -> Result<(), String> {
    validate_maximum(
        "active checkpoint",
        actual,
        DIRECT_SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES,
    )
}

#[cfg(test)]
pub fn validate_direct_terminal_checkpoint_bytes(actual: usize) -> Result<(), String> {
    validate_maximum(
        "terminal checkpoint",
        actual,
        DIRECT_SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES,
    )
}

#[cfg(test)]
pub fn validate_direct_terminal_payload_bytes(actual: usize) -> Result<(), String> {
    validate_maximum(
        "terminal payload",
        actual,
        DIRECT_SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES as usize,
    )
}

#[cfg(test)]
pub fn validate_direct_transition_fuel(actual: usize) -> Result<(), String> {
    validate_maximum(
        "transition fuel",
        actual,
        super::SWITCHOVER_MAX_TRANSITION_FUEL,
    )
}

pub fn validate_direct_runner_fuel(actual: usize) -> Result<(), String> {
    if actual == 0 {
        return Err("direct switchover runner fuel must be positive".to_string());
    }
    validate_maximum("runner fuel", actual, DIRECT_SWITCHOVER_MAX_RUNNER_FUEL)
}

#[cfg(test)]
pub fn validate_direct_error_bytes(actual: usize) -> Result<(), String> {
    validate_maximum("error", actual, super::SWITCHOVER_MAX_ERROR_BYTES)
}

fn validate_maximum(label: &str, actual: usize, maximum: usize) -> Result<(), String> {
    if actual > maximum {
        Err(format!(
            "direct switchover {label} is {actual}; maximum is {maximum}"
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
pub fn direct_execution_contract(execution: ExecutionSpec) -> ExecutionContract {
    ExecutionContract::with_encoded_limits(
        execution,
        DIRECT_SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES as u64,
        DIRECT_SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES as u64,
    )
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{Arc, Mutex as StdMutex},
    };

    use async_trait::async_trait;
    use k8s_openapi::{
        api::core::v1::{PersistentVolumeClaim, Pod, Service},
        apimachinery::pkg::apis::meta::v1::ObjectMeta,
    };
    use kuberic_core::{
        driver::ReplicaHandle,
        error::KubericError,
        types::{
            AccessStatus, AgentControlVersion, AgentGeneration, CorrelatedActionObservation,
            CorrelatedControlActionAcknowledgement, CorrelatedControlActionRequest,
            DurableActionErrorClass, DurableActionObservation, DurableActionState,
            DurableReplicaAction, Epoch, ReplicaAgentStatus, ReplicaConfigurationMemberStatus,
            ReplicaConfigurationMode, ReplicaConfigurationStatus, ReplicaId, ReplicaInstanceId,
            ReplicaStatusInfo, Role,
        },
    };
    use kuberic_durable_execution::{
        ActivityName, ActivityRecord, ActivitySequence, CheckpointEnvelope, CheckpointError,
        CheckpointPayload, CheckpointStore, DurableActivity, DurableHost, HostEpoch, HostOutcome,
        InMemoryCheckpointStore, InMemoryFault, StoreErrorKind,
    };

    use crate::{
        cluster_api::ClusterApi,
        crd::{
            EpochStatus, KubericSet, KubericSetSpec, KubericSetStatus, PvcRetentionPolicy,
            StablePartitionSnapshotStatus, StableReplicaRoleStatus, StableReplicaSnapshotStatus,
        },
        durable::{
            checkpoint_store::{
                DurableCheckpointMeasurementsSnapshot, DurableCheckpointStore,
                MeasuredDurableCheckpointStore,
            },
            runner::{DurableRunner, DurableRunnerOutcome},
            switchover_execution::direct_initial_operation,
        },
    };

    use super::super::activities::{
        AttestCompensatedTopologyActivity, AttestTargetTopologyActivity, CaptureFrozenLsnActivity,
        CaptureFrozenLsnOutput, CompensateDistributeReplicaEpochActivity,
        CompensatePromoteOldPrimaryActivity, DemoteOldPrimaryActivity, DemoteOldPrimaryOutput,
        DistributeReplicaEpochActivity, InstallCompensationCatchUpConfigurationActivity,
        InstallCompensationCurrentConfigurationActivity, InstallTargetCatchUpConfigurationActivity,
        InstallTargetCurrentConfigurationActivity, PromoteTargetActivity,
        PublishOldPrimarySecondaryLabelActivity, PublishTargetPrimaryLabelActivity,
        RestoreOldPrimaryLabelActivity, RestorePreviousCurrentConfigurationActivity,
        RestoreTargetSecondaryLabelActivity, RevokeWritesActivity, RevokeWritesInput,
        RevokeWritesOutput, WaitTargetCaughtUpActivity, WaitTargetCaughtUpOutput,
        WaitTargetWriteQuorumActivity,
    };
    use super::*;

    #[test]
    fn direct_switchover_ordinary_registry_contains_exactly_non_strict_contracts() {
        let registry = ordinary_activity_registry();
        let ordinary = [
            CaptureFrozenLsnActivity::NAME,
            WaitTargetCaughtUpActivity::NAME,
            DistributeReplicaEpochActivity::NAME,
            InstallTargetCatchUpConfigurationActivity::NAME,
            WaitTargetWriteQuorumActivity::NAME,
            InstallTargetCurrentConfigurationActivity::NAME,
            PublishTargetPrimaryLabelActivity::NAME,
            PublishOldPrimarySecondaryLabelActivity::NAME,
            AttestTargetTopologyActivity::NAME,
            RestorePreviousCurrentConfigurationActivity::NAME,
            CompensateDistributeReplicaEpochActivity::NAME,
            InstallCompensationCatchUpConfigurationActivity::NAME,
            InstallCompensationCurrentConfigurationActivity::NAME,
            RestoreOldPrimaryLabelActivity::NAME,
            RestoreTargetSecondaryLabelActivity::NAME,
            AttestCompensatedTopologyActivity::NAME,
        ];
        assert_eq!(ordinary.len(), 16);
        for name in ordinary {
            assert!(
                registry.contains(&ActivityName::new(name, 1).expect("valid activity identity")),
                "ordinary contract {name} must be registered"
            );
        }

        for name in [
            <RevokeWritesActivity as SwitchoverActivityContract>::NAME,
            <DemoteOldPrimaryActivity as SwitchoverActivityContract>::NAME,
            <PromoteTargetActivity as SwitchoverActivityContract>::NAME,
            <CompensatePromoteOldPrimaryActivity as SwitchoverActivityContract>::NAME,
        ] {
            assert!(
                !registry.contains(&ActivityName::new(name, 1).expect("valid activity identity")),
                "strict contract {name} must not be registered as ordinary"
            );
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Scenario {
        Success,
        PrePromotionCompensation,
        PostPromotionCompensation,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum TestReplicaOperation {
        DemoteOldPrimary,
        PromoteTarget,
    }

    fn expected_scenario_activity_identities(
        member_count: usize,
        scenario: Scenario,
        redeliver_replica_effects: bool,
    ) -> Vec<(String, u32)> {
        fn push_replica<A: SwitchoverActivityContract>(
            identities: &mut Vec<(String, u32)>,
            redeliver: bool,
        ) {
            identities.push((
                <A as SwitchoverActivityContract>::NAME.to_string(),
                <A as SwitchoverActivityContract>::VERSION,
            ));
            if redeliver {
                identities.push((
                    <A as SwitchoverActivityContract>::NAME.to_string(),
                    <A as SwitchoverActivityContract>::VERSION,
                ));
            }
        }

        fn push_once<A: SwitchoverActivityContract>(identities: &mut Vec<(String, u32)>) {
            identities.push((
                <A as SwitchoverActivityContract>::NAME.to_string(),
                <A as SwitchoverActivityContract>::VERSION,
            ));
        }

        let mut identities = Vec::new();
        push_replica::<RevokeWritesActivity>(&mut identities, redeliver_replica_effects);
        push_once::<CaptureFrozenLsnActivity>(&mut identities);
        push_once::<WaitTargetCaughtUpActivity>(&mut identities);
        match scenario {
            Scenario::PrePromotionCompensation => {
                push_replica::<RestorePreviousCurrentConfigurationActivity>(
                    &mut identities,
                    redeliver_replica_effects,
                );
                push_once::<AttestCompensatedTopologyActivity>(&mut identities);
            }
            Scenario::Success => {
                push_replica::<DemoteOldPrimaryActivity>(
                    &mut identities,
                    redeliver_replica_effects,
                );
                push_replica::<PromoteTargetActivity>(&mut identities, redeliver_replica_effects);
                for _ in 0..member_count.saturating_sub(2) {
                    push_replica::<DistributeReplicaEpochActivity>(
                        &mut identities,
                        redeliver_replica_effects,
                    );
                }
                push_replica::<InstallTargetCatchUpConfigurationActivity>(
                    &mut identities,
                    redeliver_replica_effects,
                );
                push_replica::<WaitTargetWriteQuorumActivity>(
                    &mut identities,
                    redeliver_replica_effects,
                );
                push_replica::<InstallTargetCurrentConfigurationActivity>(
                    &mut identities,
                    redeliver_replica_effects,
                );
                push_once::<PublishTargetPrimaryLabelActivity>(&mut identities);
                push_once::<PublishOldPrimarySecondaryLabelActivity>(&mut identities);
                push_once::<AttestTargetTopologyActivity>(&mut identities);
            }
            Scenario::PostPromotionCompensation => {
                push_replica::<DemoteOldPrimaryActivity>(
                    &mut identities,
                    redeliver_replica_effects,
                );
                push_replica::<PromoteTargetActivity>(&mut identities, redeliver_replica_effects);
                push_replica::<CompensatePromoteOldPrimaryActivity>(
                    &mut identities,
                    redeliver_replica_effects,
                );
                for _ in 0..member_count.saturating_sub(1) {
                    push_replica::<CompensateDistributeReplicaEpochActivity>(
                        &mut identities,
                        redeliver_replica_effects,
                    );
                }
                push_replica::<InstallCompensationCatchUpConfigurationActivity>(
                    &mut identities,
                    redeliver_replica_effects,
                );
                push_replica::<InstallCompensationCurrentConfigurationActivity>(
                    &mut identities,
                    redeliver_replica_effects,
                );
                push_once::<RestoreOldPrimaryLabelActivity>(&mut identities);
                push_once::<RestoreTargetSecondaryLabelActivity>(&mut identities);
                push_once::<AttestCompensatedTopologyActivity>(&mut identities);
            }
        }
        identities
    }

    fn expected_persisted_scenario_activity_prefix(
        member_count: usize,
        scenario: Scenario,
        redeliver_replica_effects: bool,
    ) -> Vec<(String, u32)> {
        let mut identities = expected_scenario_activity_identities(
            member_count,
            scenario,
            redeliver_replica_effects,
        );
        let terminal_fused_suffix = match scenario {
            Scenario::Success | Scenario::PrePromotionCompensation => 1,
            Scenario::PostPromotionCompensation => 3,
        };
        identities.truncate(identities.len() - terminal_fused_suffix);
        identities
    }

    #[derive(Default)]
    struct TestWorld {
        statuses: BTreeMap<ReplicaId, ReplicaStatusInfo>,
        labels: BTreeMap<ReplicaId, String>,
        attempts: BTreeMap<String, usize>,
        requests: Vec<CorrelatedControlActionRequest>,
        label_patches: Vec<(ReplicaId, String)>,
        fail_sequences: BTreeSet<u32>,
        busy_first_for_every_action: bool,
        busy_always_sequences: BTreeSet<u32>,
        unknown_after_apply_sequences: BTreeSet<u32>,
        unknown_without_apply_sequences: BTreeSet<u32>,
        in_progress_without_apply_sequences: BTreeSet<u32>,
        unapplied_label_patch_attempts: BTreeSet<usize>,
        lost_label_patch_attempts: BTreeSet<usize>,
    }

    impl TestWorld {
        fn pods(&self) -> Vec<Pod> {
            self.statuses
                .iter()
                .map(|(id, status)| Pod {
                    metadata: ObjectMeta {
                        name: Some(format!("set-{id}")),
                        namespace: Some("default".to_string()),
                        uid: Some(status.instance_id.to_string()),
                        labels: Some(BTreeMap::from([
                            ("kuberic.io/replica-id".to_string(), id.to_string()),
                            (
                                "kuberic.io/role".to_string(),
                                self.labels
                                    .get(id)
                                    .cloned()
                                    .unwrap_or_else(|| "secondary".to_string()),
                            ),
                        ])),
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .collect()
        }

        fn change_generation(&mut self, target_id: ReplicaId) {
            let status = self.statuses.get_mut(&target_id).unwrap();
            status.agent.generation =
                AgentGeneration::parse("ffffffffffffffffffffffffffffffff").unwrap();
            status.agent.control_version = AgentControlVersion::new(1);
            status.agent.current_action = None;
            status.agent.retained_terminal_actions.clear();
        }
    }

    struct TestHandle {
        id: ReplicaId,
        world: Arc<StdMutex<TestWorld>>,
    }

    #[async_trait]
    impl ReplicaHandle for TestHandle {
        fn id(&self) -> ReplicaId {
            self.id
        }

        fn instance_id(&self) -> ReplicaInstanceId {
            self.world
                .lock()
                .unwrap()
                .statuses
                .get(&self.id)
                .unwrap()
                .instance_id
                .clone()
        }

        fn current_progress(&self) -> i64 {
            self.world
                .lock()
                .unwrap()
                .statuses
                .get(&self.id)
                .unwrap()
                .current_progress
        }

        fn catch_up_capability(&self) -> i64 {
            self.world
                .lock()
                .unwrap()
                .statuses
                .get(&self.id)
                .unwrap()
                .catch_up_capability
                .unwrap_or_default()
        }

        fn control_address(&self) -> String {
            format!("http://set-{}:9090", self.id)
        }

        fn replicator_address(&self) -> String {
            format!("http://set-{}:9091", self.id)
        }

        async fn get_status(&self) -> kuberic_core::Result<ReplicaStatusInfo> {
            Ok(self
                .world
                .lock()
                .unwrap()
                .statuses
                .get(&self.id)
                .unwrap()
                .clone())
        }

        async fn execute_correlated_control_action(
            &self,
            request: CorrelatedControlActionRequest,
        ) -> kuberic_core::Result<CorrelatedControlActionAcknowledgement> {
            let mut world = self.world.lock().unwrap();
            let sequence = action_sequence(&request.action_id);
            let attempt = {
                let attempt = world.attempts.entry(request.action_id.clone()).or_default();
                *attempt += 1;
                *attempt
            };
            world.requests.push(request.clone());
            if world.busy_always_sequences.contains(&sequence)
                || (world.busy_first_for_every_action && attempt == 1)
            {
                return Err(KubericError::AgentBusy);
            }
            if world.fail_sequences.contains(&sequence) {
                return Err(KubericError::RemoteAgentTerminalFailure {
                    class: DurableActionErrorClass::Internal,
                    message: format!("injected failure for sequence {sequence}"),
                });
            }
            if world.unknown_without_apply_sequences.remove(&sequence) {
                return Err(KubericError::Internal(Box::new(std::io::Error::other(
                    "ambiguous dispatch before application",
                ))));
            }
            if world.in_progress_without_apply_sequences.remove(&sequence) {
                let observation = CorrelatedActionObservation {
                    generation: request.expected_agent_generation.clone(),
                    control_version: request.expected_control_version,
                    action: DurableActionObservation {
                        action_id: request.action_id.clone(),
                        signature: request.input_signature.clone(),
                        state: DurableActionState::InProgress,
                        error_class: None,
                        error: None,
                        result: None,
                        add_replica_progress: None,
                        remove_replica_progress: None,
                    },
                };
                world
                    .statuses
                    .get_mut(&request.target_replica_id)
                    .unwrap()
                    .agent
                    .current_action = Some(observation);
                return Err(KubericError::Internal(Box::new(std::io::Error::other(
                    "ambiguous dispatch with an in-progress ledger record",
                ))));
            }
            let acknowledgement = apply_action(&mut world, &request)?;
            if world.unknown_after_apply_sequences.remove(&sequence) {
                return Err(KubericError::Internal(Box::new(std::io::Error::other(
                    "lost reply after application",
                ))));
            }
            Ok(acknowledgement)
        }
    }

    struct TestApi {
        world: Arc<StdMutex<TestWorld>>,
        apply_labels: bool,
    }

    #[async_trait]
    impl ClusterApi for TestApi {
        async fn list_pods(&self, _: &str, _: &str) -> Result<Vec<Pod>, String> {
            Ok(self.world.lock().unwrap().pods())
        }

        async fn create_pod(&self, _: &str, _: &Pod) -> Result<(), String> {
            unreachable!()
        }

        async fn delete_pod(&self, _: &str, _: &str, _: &str) -> Result<(), String> {
            unreachable!()
        }

        async fn patch_pod_labels(
            &self,
            _: &str,
            _: &str,
            _: BTreeMap<String, String>,
        ) -> Result<(), String> {
            unreachable!()
        }

        async fn patch_pod_labels_if_uid(
            &self,
            _: &str,
            pod_name: &str,
            expected_uid: &str,
            labels: BTreeMap<String, String>,
        ) -> Result<(), String> {
            let mut world = self.world.lock().unwrap();
            let Some((&id, _)) = world.statuses.iter().find(|(id, status)| {
                pod_name == format!("set-{id}") && status.instance_id.as_str() == expected_uid
            }) else {
                return Err("UID-fenced test label target was not found".to_string());
            };
            let role = labels
                .get("kuberic.io/role")
                .cloned()
                .ok_or_else(|| "test label patch has no role".to_string())?;
            world.label_patches.push((id, role.clone()));
            let attempt = world.label_patches.len();
            let leave_unapplied = world.unapplied_label_patch_attempts.remove(&attempt);
            if self.apply_labels && !leave_unapplied {
                world.labels.insert(id, role);
            }
            if world.lost_label_patch_attempts.remove(&attempt) {
                return Err("injected lost label reply after apply".to_string());
            }
            Ok(())
        }

        async fn patch_set_status(
            &self,
            _: &str,
            _: &str,
            _: &KubericSetStatus,
            _: Option<&str>,
        ) -> Result<(), String> {
            unreachable!()
        }

        async fn create_replica_handle(
            &self,
            replica_id: ReplicaId,
            _: &Pod,
            _: &KubericSetSpec,
        ) -> Result<Box<dyn ReplicaHandle>, String> {
            Ok(Box::new(TestHandle {
                id: replica_id,
                world: self.world.clone(),
            }))
        }

        async fn get_pvc(&self, _: &str, _: &str) -> Result<PersistentVolumeClaim, String> {
            unreachable!()
        }

        async fn create_pvc(&self, _: &str, _: &PersistentVolumeClaim) -> Result<(), String> {
            unreachable!()
        }

        async fn list_pvcs(&self, _: &str, _: &str) -> Result<Vec<PersistentVolumeClaim>, String> {
            unreachable!()
        }

        async fn delete_pvc(&self, _: &str, _: &str) -> Result<(), String> {
            unreachable!()
        }

        async fn get_service(&self, _: &str, _: &str) -> Result<Service, String> {
            unreachable!()
        }

        async fn create_service(&self, _: &str, _: &Service) -> Result<(), String> {
            unreachable!()
        }

        async fn delete_service(&self, _: &str, _: &str) -> Result<(), String> {
            unreachable!()
        }
    }

    struct ScenarioResult {
        terminal: DirectSwitchoverTerminalRecord,
        measurements: DurableCheckpointMeasurementsSnapshot,
        requests: usize,
        request_sequences: Vec<u32>,
        request_action_ids: Vec<String>,
        label_patches: usize,
        activity_identities: Vec<(String, u32)>,
    }

    async fn record_scenario_activity_identities(
        backend: &InMemoryCheckpointStore,
        execution: &ExecutionSpec,
        longest: &mut Vec<(String, u32)>,
    ) {
        let Some(stored) = backend.load(execution.execution_id()).await.unwrap() else {
            return;
        };
        let payload = stored
            .checkpoint()
            .decode_and_validate(execution, direct_checkpoint_limits())
            .unwrap();
        let Some(activities) = payload.active_activities() else {
            return;
        };
        let identities = activities
            .iter()
            .map(|activity| {
                (
                    activity.name().name().to_string(),
                    activity.name().version(),
                )
            })
            .collect::<Vec<_>>();
        let shared = longest.len().min(identities.len());
        assert_eq!(identities[..shared], longest[..shared]);
        if identities.len() > longest.len() {
            *longest = identities;
        }
    }

    fn resolver_for_world(
        definition: &DirectSwitchoverDefinition,
        world: &TestWorld,
        now: i64,
    ) -> DirectSwitchoverPreparedActivityResolver {
        let observations = observations(world);
        let addressed = observations
            .iter()
            .map(|(id, observed)| (*id, observed.status.instance_id.clone()))
            .collect();
        DirectSwitchoverPreparedActivityResolver::new(
            definition,
            &observations,
            &addressed,
            now,
            Arc::new(AtomicI64::new(definition.initial_deadline_unix_seconds)),
        )
    }

    async fn persist_direct_result<A: SwitchoverActivityContract>(
        host: &mut DurableHost<MeasuredDurableCheckpointStore>,
        execution: &ExecutionSpec,
        resolver: &DirectSwitchoverPreparedActivityResolver,
        output: &A::Output,
        expect_prepared: bool,
    ) where
        A::Output: Clone,
    {
        let HostOutcome::DispatchPermitted { permit, .. } = host
            .turn_and_expose_effects(&DirectSwitchoverWorkflow, execution.clone(), resolver)
            .await
        else {
            panic!(
                "{} was not exposed",
                <A as SwitchoverActivityContract>::NAME
            );
        };
        assert_eq!(
            permit.activity().spec().name().name(),
            <A as SwitchoverActivityContract>::NAME
        );
        assert_eq!(
            permit
                .prepared_command()
                .is_some_and(|command| command.bytes().as_slice() != b"null"),
            expect_prepared,
            "{} preparation classification",
            <A as SwitchoverActivityContract>::NAME
        );
        let result = encode_activity_result::<OrdinarySwitchoverActivity<A>>(output).unwrap();
        let observed = if matches!(
            activity_class(<A as SwitchoverActivityContract>::NAME),
            Some(SwitchoverActivityClass::StrictEffectRequired)
        ) {
            let observation = kuberic_durable_execution::EffectObservation::completed(
                permit.activity().clone(),
                permit.attempt_id(),
                result,
            );
            host.observe_effect(execution, observation).await
        } else {
            host.observe(
                execution,
                ActivityObservation::new(permit.activity().clone(), result),
            )
            .await
        };
        assert!(matches!(observed, HostOutcome::ObservationAccepted { .. }));
    }

    async fn run_scenario(
        member_count: usize,
        scenario: Scenario,
        busy_first_for_every_action: bool,
    ) -> ScenarioResult {
        run_configured_scenario(member_count, scenario, busy_first_for_every_action, |_| {}).await
    }

    async fn run_configured_scenario(
        member_count: usize,
        scenario: Scenario,
        busy_first_for_every_action: bool,
        configure: impl FnOnce(&mut TestWorld),
    ) -> ScenarioResult {
        let initial = direct_initial_operation(
            &format!("set-{member_count}-{scenario:?}"),
            snapshot(member_count),
            2,
            100,
        )
        .unwrap();
        let execution_id = ExecutionId::from_bytes([u8::try_from(member_count).unwrap(); 16]);
        let execution = direct_execution_spec(execution_id, initial.clone()).unwrap();
        let mut world_value = world_for(&initial, scenario);
        world_value.busy_first_for_every_action = busy_first_for_every_action;
        configure(&mut world_value);
        let world = Arc::new(StdMutex::new(world_value));
        let api = TestApi {
            world: world.clone(),
            apply_labels: true,
        };
        let set = test_set(member_count);
        let backend = InMemoryCheckpointStore::new();
        let store = MeasuredDurableCheckpointStore::with_decoder(
            execution_id,
            DurableCheckpointStore::InMemory(backend.clone()),
            direct_checkpoint_measurement_decoder(),
        );
        let mut host = DurableHost::new(
            store,
            HostEpoch::from_bytes([91; 16]),
            direct_checkpoint_limits(),
        );
        let runner = DurableRunner::new(DIRECT_SWITCHOVER_MAX_RUNNER_FUEL).unwrap();
        let mut now = 100;
        let mut activity_identities = Vec::new();

        for _ in 0..512 {
            let pods = world.lock().unwrap().pods();
            let current_pods = pod_references(&pods);
            let mut adapter = DirectSwitchoverRunnerAdapter::new(
                &initial,
                &set,
                &current_pods,
                &api,
                host.store().clone(),
                now,
            )
            .unwrap();
            let outcome = runner
                .run_activities(
                    &mut host,
                    &DirectSwitchoverWorkflow,
                    execution.clone(),
                    &mut adapter,
                    now,
                )
                .await;
            record_scenario_activity_identities(&backend, &execution, &mut activity_identities)
                .await;
            match outcome {
                DurableRunnerOutcome::Terminal(terminal) => {
                    let world = world.lock().unwrap();
                    return ScenarioResult {
                        terminal,
                        measurements: host.store().measurements(),
                        requests: world.requests.len(),
                        request_sequences: world
                            .requests
                            .iter()
                            .map(|request| action_sequence(&request.action_id))
                            .collect(),
                        request_action_ids: world
                            .requests
                            .iter()
                            .map(|request| request.action_id.clone())
                            .collect(),
                        label_patches: world.label_patches.len(),
                        activity_identities,
                    };
                }
                DurableRunnerOutcome::Active { .. }
                | DurableRunnerOutcome::ReloadRequired { .. } => {
                    now += 1;
                }
                other => panic!("unexpected direct switchover runner outcome: {other:?}"),
            }
        }
        panic!("direct switchover scenario did not terminate");
    }

    async fn run_scenario_restarting_every_turn(
        member_count: usize,
        scenario: Scenario,
    ) -> DirectSwitchoverTerminalRecord {
        let initial = direct_initial_operation(
            &format!("restart-{member_count}-{scenario:?}"),
            snapshot(member_count),
            2,
            100,
        )
        .unwrap();
        let execution_id = ExecutionId::from_bytes([88; 16]);
        let execution = direct_execution_spec(execution_id, initial.clone()).unwrap();
        let world = Arc::new(StdMutex::new(world_for(&initial, scenario)));
        let api = TestApi {
            world: world.clone(),
            apply_labels: true,
        };
        let set = test_set(member_count);
        let backend = InMemoryCheckpointStore::new();
        let runner = DurableRunner::new(DIRECT_SWITCHOVER_MAX_RUNNER_FUEL).unwrap();
        let mut now = 100;
        for turn in 0..512 {
            let store = MeasuredDurableCheckpointStore::with_decoder(
                execution_id,
                DurableCheckpointStore::InMemory(backend.clone()),
                direct_checkpoint_measurement_decoder(),
            );
            let mut host = DurableHost::new(
                store,
                HostEpoch::from_bytes([u8::try_from(turn % 251 + 1).unwrap(); 16]),
                direct_checkpoint_limits(),
            );
            let pods = world.lock().unwrap().pods();
            let current_pods = pod_references(&pods);
            let mut adapter = DirectSwitchoverRunnerAdapter::new(
                &initial,
                &set,
                &current_pods,
                &api,
                host.store().clone(),
                now,
            )
            .unwrap();
            match runner
                .run_activities(
                    &mut host,
                    &DirectSwitchoverWorkflow,
                    execution.clone(),
                    &mut adapter,
                    now,
                )
                .await
            {
                DurableRunnerOutcome::Terminal(terminal) => return terminal,
                DurableRunnerOutcome::Active { .. }
                | DurableRunnerOutcome::ReloadRequired { .. } => now += 1,
                other => panic!("unexpected restart-every-turn outcome: {other:?}"),
            }
        }
        panic!("restart-every-turn scenario did not terminate");
    }

    #[tokio::test]
    async fn direct_switchover_adapter_runs_success_and_both_compensation_families_at_scale() {
        for member_count in [2, 4, 9] {
            for scenario in [
                Scenario::Success,
                Scenario::PrePromotionCompensation,
                Scenario::PostPromotionCompensation,
            ] {
                let result = run_scenario(member_count, scenario, false).await;
                match (&result.terminal, scenario) {
                    (
                        DirectSwitchoverTerminalRecord::Complete {
                            compensated: false, ..
                        },
                        Scenario::Success,
                    ) => {}
                    (
                        DirectSwitchoverTerminalRecord::Complete {
                            compensated: true, ..
                        },
                        Scenario::PrePromotionCompensation | Scenario::PostPromotionCompensation,
                    ) => {}
                    other => panic!("unexpected direct terminal: {other:?}"),
                }
                assert_eq!(
                    result.measurements.completed_passive_observation_count,
                    Some(3)
                );
                assert!(
                    result
                        .measurements
                        .completed_activity_count
                        .is_some_and(|count| count <= DIRECT_SWITCHOVER_MAX_ACTIVITY_RECORDS as u64)
                );
                let (expected_activities, expected_external) = match scenario {
                    Scenario::Success => (member_count + 9, member_count + 6),
                    Scenario::PrePromotionCompensation => (5, 2),
                    Scenario::PostPromotionCompensation => (member_count + 10, member_count + 7),
                };
                assert_eq!(
                    result.measurements.completed_activity_count,
                    Some(expected_activities as u64)
                );
                assert_eq!(
                    result.measurements.completed_external_effect_count,
                    Some(expected_external as u64)
                );
                assert_eq!(
                    result.activity_identities,
                    expected_persisted_scenario_activity_prefix(member_count, scenario, false)
                );
                assert_eq!(
                    result.measurements.accepted_writes,
                    expected_activities as u64 * 2 + 1,
                    "member_count={member_count}, scenario={scenario:?}"
                );
                assert!(result.requests > 0);
                if scenario == Scenario::Success {
                    assert_eq!(result.label_patches, 2);
                } else {
                    assert_eq!(result.label_patches, 0);
                }
            }
        }
    }

    #[tokio::test]
    async fn direct_switchover_restart_every_turn_covers_all_terminal_branches() {
        for (scenario, expected_branch) in [
            (
                Scenario::Success,
                DirectSwitchoverTerminalBranch::TargetSuccess,
            ),
            (
                Scenario::PrePromotionCompensation,
                DirectSwitchoverTerminalBranch::PreviousConfigurationRestored,
            ),
            (
                Scenario::PostPromotionCompensation,
                DirectSwitchoverTerminalBranch::PostPromotionCompensated,
            ),
        ] {
            assert!(
                matches!(
                    run_scenario_restarting_every_turn(3, scenario).await,
                    DirectSwitchoverTerminalRecord::Complete { branch, .. }
                        if branch == expected_branch
                ),
                "scenario={scenario:?}"
            );
        }
    }

    #[tokio::test]
    async fn direct_switchover_deadline_effects_reach_measured_terminal_reload() {
        for (seed, deadline_operation, expected_branch, expected_accounting) in [
            (
                121,
                TestReplicaOperation::DemoteOldPrimary,
                DirectSwitchoverTerminalBranch::PreviousConfigurationRestored,
                (3_u64, 3_u64),
            ),
            (
                124,
                TestReplicaOperation::PromoteTarget,
                DirectSwitchoverTerminalBranch::PostPromotionCompensated,
                (10_u64, 3_u64),
            ),
        ] {
            let initial =
                direct_initial_operation(&format!("deadline-terminal-{seed}"), snapshot(3), 2, 100)
                    .unwrap();
            let definition = DirectSwitchoverDefinition::from_initial(&initial).unwrap();
            let execution_id = ExecutionId::from_bytes([seed; 16]);
            let execution = direct_execution_spec(execution_id, initial.clone()).unwrap();
            let backend = InMemoryCheckpointStore::new();
            let store = MeasuredDurableCheckpointStore::with_decoder(
                execution_id,
                DurableCheckpointStore::InMemory(backend.clone()),
                direct_checkpoint_measurement_decoder(),
            );
            let mut host = DurableHost::new(
                store,
                HostEpoch::from_bytes([seed; 16]),
                direct_checkpoint_limits(),
            );
            let mut world = world_for(&initial, Scenario::Success);

            let resolver = resolver_for_world(&definition, &world, 100);
            persist_direct_result::<RevokeWritesActivity>(
                &mut host,
                &execution,
                &resolver,
                &RevokeWritesOutput {
                    observed_at_unix_seconds: 100,
                },
                true,
            )
            .await;
            world
                .statuses
                .get_mut(&definition.old_primary_id)
                .unwrap()
                .write_status = AccessStatus::ReconfigurationPending;

            let resolver = resolver_for_world(&definition, &world, 100);
            persist_direct_result::<CaptureFrozenLsnActivity>(
                &mut host,
                &execution,
                &resolver,
                &CaptureFrozenLsnOutput {
                    frozen_lsn: 100,
                    observed_at_unix_seconds: 100,
                },
                false,
            )
            .await;

            let resolver = resolver_for_world(&definition, &world, 100);
            persist_direct_result::<WaitTargetCaughtUpActivity>(
                &mut host,
                &execution,
                &resolver,
                &WaitTargetCaughtUpOutput {
                    observed_at_unix_seconds: 100,
                },
                false,
            )
            .await;

            if deadline_operation == TestReplicaOperation::PromoteTarget {
                let resolver = resolver_for_world(&definition, &world, 100);
                persist_direct_result::<DemoteOldPrimaryActivity>(
                    &mut host,
                    &execution,
                    &resolver,
                    &DemoteOldPrimaryOutput {
                        observed_at_unix_seconds: 100,
                    },
                    true,
                )
                .await;
                let old_primary = world.statuses.get_mut(&definition.old_primary_id).unwrap();
                old_primary.role = Role::ActiveSecondary;
                old_primary.epoch = Epoch::new(
                    definition.target_snapshot.epoch.data_loss_number,
                    definition.target_snapshot.epoch.configuration_number,
                );
                old_primary.write_status = AccessStatus::NotPrimary;
            }

            let deadline_resolver = resolver_for_world(&definition, &world, 111);
            let HostOutcome::DispatchPermitted { permit, .. } = host
                .turn_and_expose_effects(
                    &DirectSwitchoverWorkflow,
                    execution.clone(),
                    &deadline_resolver,
                )
                .await
            else {
                panic!("deadline effect was not exposed");
            };
            assert_eq!(
                permit
                    .prepared_command()
                    .map(|command| command.bytes().as_slice()),
                Some(b"null".as_slice()),
                "deadline outcome must remain evidence-only"
            );
            assert_eq!(
                permit.activity().spec().name().name(),
                if deadline_operation == TestReplicaOperation::DemoteOldPrimary {
                    <DemoteOldPrimaryActivity as SwitchoverActivityContract>::NAME
                } else {
                    <PromoteTargetActivity as SwitchoverActivityContract>::NAME
                }
            );
            drop(permit);

            let world = Arc::new(StdMutex::new(world));
            let api = TestApi {
                world: world.clone(),
                apply_labels: true,
            };
            let set = test_set(3);
            let runner = DurableRunner::new(DIRECT_SWITCHOVER_MAX_RUNNER_FUEL).unwrap();
            let mut restarted = DurableHost::new(
                MeasuredDurableCheckpointStore::with_decoder(
                    execution_id,
                    DurableCheckpointStore::InMemory(backend.clone()),
                    direct_checkpoint_measurement_decoder(),
                ),
                HostEpoch::from_bytes([seed.saturating_add(1); 16]),
                direct_checkpoint_limits(),
            );
            let terminal = {
                let mut now = 111;
                loop {
                    let pods = world.lock().unwrap().pods();
                    let current_pods = pod_references(&pods);
                    let mut adapter = DirectSwitchoverRunnerAdapter::new(
                        &initial,
                        &set,
                        &current_pods,
                        &api,
                        restarted.store().clone(),
                        now,
                    )
                    .unwrap();
                    match runner
                        .run_activities(
                            &mut restarted,
                            &DirectSwitchoverWorkflow,
                            execution.clone(),
                            &mut adapter,
                            now,
                        )
                        .await
                    {
                        DurableRunnerOutcome::Terminal(terminal) => break terminal,
                        DurableRunnerOutcome::Active { .. }
                        | DurableRunnerOutcome::ReloadRequired { .. } => now += 1,
                        other => panic!("unexpected deadline terminal outcome: {other:?}"),
                    }
                }
            };
            assert!(matches!(
                terminal,
                DirectSwitchoverTerminalRecord::Complete { branch, .. }
                    if branch == expected_branch
            ));
            let measurements = restarted.store().measurements();
            assert_eq!(
                measurements.completed_activity_count,
                Some(expected_accounting.0 + expected_accounting.1)
            );
            assert_eq!(
                measurements.completed_external_effect_count,
                Some(expected_accounting.0)
            );
            assert_eq!(
                measurements.completed_passive_observation_count,
                Some(expected_accounting.1)
            );

            let reloaded = MeasuredDurableCheckpointStore::with_decoder(
                execution_id,
                DurableCheckpointStore::InMemory(backend),
                direct_checkpoint_measurement_decoder(),
            );
            assert!(reloaded.load(execution_id).await.unwrap().is_some());
            let reloaded_measurements = reloaded.measurements();
            assert_eq!(
                reloaded_measurements.completed_activity_count,
                Some(expected_accounting.0 + expected_accounting.1)
            );
            assert_eq!(
                reloaded_measurements.completed_external_effect_count,
                Some(expected_accounting.0)
            );
            assert_eq!(
                reloaded_measurements.completed_passive_observation_count,
                Some(expected_accounting.1)
            );
        }
    }

    #[tokio::test]
    async fn direct_switchover_compensation_lost_replies_are_observed_without_duplicates() {
        let restored =
            run_configured_scenario(3, Scenario::PrePromotionCompensation, false, |world| {
                world.unknown_after_apply_sequences.insert(1500);
            })
            .await;
        assert!(matches!(
            restored.terminal,
            DirectSwitchoverTerminalRecord::Complete {
                branch: DirectSwitchoverTerminalBranch::PreviousConfigurationRestored,
                ..
            }
        ));
        assert_eq!(
            restored
                .request_sequences
                .iter()
                .filter(|sequence| **sequence == 1500)
                .count(),
            1
        );

        let compensated =
            run_configured_scenario(3, Scenario::PostPromotionCompensation, false, |world| {
                world
                    .unknown_after_apply_sequences
                    .extend([2000, 2100, 2101, 2001, 2002]);
                world.labels.insert(1, "secondary".to_string());
                world.labels.insert(2, "primary".to_string());
                world.lost_label_patch_attempts.extend([1, 2]);
            })
            .await;
        assert!(matches!(
            compensated.terminal,
            DirectSwitchoverTerminalRecord::Complete {
                branch: DirectSwitchoverTerminalBranch::PostPromotionCompensated,
                ..
            }
        ));
        for sequence in [2000, 2100, 2101, 2001, 2002] {
            assert_eq!(
                compensated
                    .request_sequences
                    .iter()
                    .filter(|actual| **actual == sequence)
                    .count(),
                1,
                "lost reply for compensation sequence {sequence} duplicated dispatch"
            );
        }
        assert_eq!(compensated.label_patches, 2);
    }

    #[tokio::test]
    async fn direct_switchover_revoke_safe_failure_has_its_own_terminal_branch() {
        let result = run_configured_scenario(3, Scenario::Success, false, |world| {
            world.fail_sequences.insert(1);
        })
        .await;
        assert!(matches!(
            result.terminal,
            DirectSwitchoverTerminalRecord::Complete {
                compensated: true,
                branch: DirectSwitchoverTerminalBranch::RevokeSafeFailure,
                ..
            }
        ));
        assert_eq!(result.measurements.completed_activity_count, Some(2));
        assert_eq!(result.measurements.completed_external_effect_count, Some(1));
        assert_eq!(
            result.measurements.completed_passive_observation_count,
            Some(1)
        );
    }

    #[tokio::test]
    async fn direct_switchover_nine_member_success_all_replica_redelivery_is_measured() {
        let result = run_scenario(9, Scenario::Success, true).await;
        assert_eq!(result.measurements.completed_activity_count, Some(18));
        assert_eq!(
            result.measurements.completed_external_effect_count,
            Some(15)
        );
        assert_eq!(
            result.measurements.completed_passive_observation_count,
            Some(3)
        );
        assert_eq!(result.requests, 26);
        assert_eq!(result.label_patches, 2);
        assert_eq!(result.measurements.accepted_writes, 63);
        let ordinary_prefix = format!("{}:", encode_execution_id(ExecutionId::from_bytes([9; 16])));
        for sequence in result.request_sequences.iter().copied() {
            if [1, 2, 3, 2000].contains(&sequence) {
                continue;
            }
            let ids = result
                .request_action_ids
                .iter()
                .filter(|action_id| action_sequence(action_id) == sequence)
                .collect::<Vec<_>>();
            assert!(!ids.is_empty());
            assert!(
                ids.iter()
                    .all(|action_id| action_id.starts_with(&ordinary_prefix))
            );
            assert!(ids.iter().all(|action_id| *action_id == ids[0]));
        }
        assert_eq!(
            result.activity_identities,
            expected_persisted_scenario_activity_prefix(9, Scenario::Success, false)
        );
    }

    #[tokio::test]
    async fn direct_switchover_nine_member_max_fault_measurement_fits_exact_limits() {
        let result = run_scenario(9, Scenario::PostPromotionCompensation, true).await;
        assert_eq!(result.measurements.completed_activity_count, Some(19));
        assert_eq!(
            result.measurements.completed_external_effect_count,
            Some(16)
        );
        assert_eq!(
            result.measurements.completed_passive_observation_count,
            Some(3)
        );
        assert!(
            result.measurements.maximum_active_checkpoint_bytes
                <= DIRECT_SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES
        );
        assert!(
            result.measurements.maximum_terminal_checkpoint_bytes
                <= DIRECT_SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES
        );
        assert_eq!(result.requests, 28);
        assert_eq!(result.label_patches, 0);
        assert_eq!(result.measurements.accepted_writes, 67);
        assert_eq!(
            result.activity_identities,
            expected_persisted_scenario_activity_prefix(
                9,
                Scenario::PostPromotionCompensation,
                false,
            )
        );
        assert_eq!(result.activity_identities.len(), 16);
        eprintln!(
            "direct switchover max-fault measurement: records={}, active={} (headroom={}), terminal={} (headroom={})",
            result.measurements.completed_activity_count.unwrap(),
            result.measurements.maximum_active_checkpoint_bytes,
            DIRECT_SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES
                - result.measurements.maximum_active_checkpoint_bytes,
            result.measurements.maximum_terminal_checkpoint_bytes,
            DIRECT_SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES
                - result.measurements.maximum_terminal_checkpoint_bytes,
        );
    }

    #[tokio::test]
    async fn direct_switchover_adapter_is_deterministic_for_one_hundred_runs() {
        let baseline = run_scenario(2, Scenario::Success, false).await;
        for _ in 0..100 {
            let replay = run_scenario(2, Scenario::Success, false).await;
            assert_eq!(replay.terminal, baseline.terminal);
            assert_eq!(replay.measurements, baseline.measurements);
            assert_eq!(replay.requests, baseline.requests);
            assert_eq!(replay.label_patches, baseline.label_patches);
            assert_eq!(replay.activity_identities, baseline.activity_identities);
        }
    }

    #[tokio::test]
    async fn direct_switchover_lost_reply_resolves_without_duplicate_effect() {
        let initial = direct_initial_operation("lost-reply", snapshot(2), 2, 100).unwrap();
        let execution_id = ExecutionId::from_bytes([61; 16]);
        let execution = direct_execution_spec(execution_id, initial.clone()).unwrap();
        let mut world_value = world_for(&initial, Scenario::Success);
        world_value.unknown_after_apply_sequences.insert(1);
        let world = Arc::new(StdMutex::new(world_value));
        let api = TestApi {
            world: world.clone(),
            apply_labels: true,
        };
        let set = test_set(2);
        let mut host = DurableHost::new(
            MeasuredDurableCheckpointStore::with_decoder(
                execution_id,
                DurableCheckpointStore::InMemory(InMemoryCheckpointStore::new()),
                direct_checkpoint_measurement_decoder(),
            ),
            HostEpoch::from_bytes([62; 16]),
            direct_checkpoint_limits(),
        );
        let runner = DurableRunner::new(DIRECT_SWITCHOVER_MAX_RUNNER_FUEL).unwrap();
        let mut now = 100;
        for _ in 0..64 {
            let pods = world.lock().unwrap().pods();
            let current_pods = pod_references(&pods);
            let mut adapter = DirectSwitchoverRunnerAdapter::new(
                &initial,
                &set,
                &current_pods,
                &api,
                host.store().clone(),
                now,
            )
            .unwrap();
            match runner
                .run_activities(
                    &mut host,
                    &DirectSwitchoverWorkflow,
                    execution.clone(),
                    &mut adapter,
                    now,
                )
                .await
            {
                DurableRunnerOutcome::Terminal(_) => break,
                DurableRunnerOutcome::Active { .. }
                | DurableRunnerOutcome::ReloadRequired { .. } => now += 1,
                other => panic!("unexpected lost-reply outcome: {other:?}"),
            }
        }
        let world = world.lock().unwrap();
        assert_eq!(
            world
                .requests
                .iter()
                .filter(|request| action_sequence(&request.action_id) == 1)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn direct_switchover_unknown_completion_write_reloads_without_duplicate_effect() {
        let initial = direct_initial_operation("unknown-completion", snapshot(2), 2, 100).unwrap();
        let execution_id = ExecutionId::from_bytes([71; 16]);
        let execution = direct_execution_spec(execution_id, initial.clone()).unwrap();
        let world = Arc::new(StdMutex::new(world_for(&initial, Scenario::Success)));
        let api = TestApi {
            world: world.clone(),
            apply_labels: true,
        };
        let set = test_set(2);
        let backend = InMemoryCheckpointStore::new();
        let mut host = DurableHost::new(
            MeasuredDurableCheckpointStore::with_decoder(
                execution_id,
                DurableCheckpointStore::InMemory(backend.clone()),
                direct_checkpoint_measurement_decoder(),
            ),
            HostEpoch::from_bytes([72; 16]),
            direct_checkpoint_limits(),
        );
        let runner = DurableRunner::new(DIRECT_SWITCHOVER_MAX_RUNNER_FUEL).unwrap();

        let pods = world.lock().unwrap().pods();
        let current_pods = pod_references(&pods);
        let mut adapter = DirectSwitchoverRunnerAdapter::new(
            &initial,
            &set,
            &current_pods,
            &api,
            host.store().clone(),
            100,
        )
        .unwrap();
        assert!(matches!(
            runner
                .run_activities(
                    &mut host,
                    &DirectSwitchoverWorkflow,
                    execution.clone(),
                    &mut adapter,
                    100,
                )
                .await,
            DurableRunnerOutcome::Active { .. }
        ));

        backend.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownAfterApply);
        let pods = world.lock().unwrap().pods();
        let current_pods = pod_references(&pods);
        let mut adapter = DirectSwitchoverRunnerAdapter::new(
            &initial,
            &set,
            &current_pods,
            &api,
            host.store().clone(),
            101,
        )
        .unwrap();
        assert!(matches!(
            runner
                .run_activities(
                    &mut host,
                    &DirectSwitchoverWorkflow,
                    execution.clone(),
                    &mut adapter,
                    101,
                )
                .await,
            DurableRunnerOutcome::ReloadRequired { .. }
        ));

        let mut now = 102;
        let mut completed = false;
        for _ in 0..64 {
            let pods = world.lock().unwrap().pods();
            let current_pods = pod_references(&pods);
            let mut adapter = DirectSwitchoverRunnerAdapter::new(
                &initial,
                &set,
                &current_pods,
                &api,
                host.store().clone(),
                now,
            )
            .unwrap();
            match runner
                .run_activities(
                    &mut host,
                    &DirectSwitchoverWorkflow,
                    execution.clone(),
                    &mut adapter,
                    now,
                )
                .await
            {
                DurableRunnerOutcome::Terminal(_) => {
                    completed = true;
                    break;
                }
                DurableRunnerOutcome::Active { .. }
                | DurableRunnerOutcome::ReloadRequired { .. } => now += 1,
                other => panic!("unexpected unknown-completion outcome: {other:?}"),
            }
        }
        assert!(completed);
        let world = world.lock().unwrap();
        assert_eq!(
            world
                .requests
                .iter()
                .filter(|request| action_sequence(&request.action_id) == 1)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn direct_switchover_generation_change_allows_only_one_same_action_redelivery() {
        let initial = direct_initial_operation("generation-change", snapshot(2), 2, 100).unwrap();
        let execution_id = ExecutionId::from_bytes([63; 16]);
        let execution = direct_execution_spec(execution_id, initial.clone()).unwrap();
        let mut world_value = world_for(&initial, Scenario::Success);
        world_value.unknown_without_apply_sequences.insert(1);
        let world = Arc::new(StdMutex::new(world_value));
        let api = TestApi {
            world: world.clone(),
            apply_labels: true,
        };
        let set = test_set(2);
        let mut host = DurableHost::new(
            MeasuredDurableCheckpointStore::with_decoder(
                execution_id,
                DurableCheckpointStore::InMemory(InMemoryCheckpointStore::new()),
                direct_checkpoint_measurement_decoder(),
            ),
            HostEpoch::from_bytes([64; 16]),
            direct_checkpoint_limits(),
        );
        let runner = DurableRunner::new(DIRECT_SWITCHOVER_MAX_RUNNER_FUEL).unwrap();

        let pods = world.lock().unwrap().pods();
        let current_pods = pod_references(&pods);
        let mut adapter = DirectSwitchoverRunnerAdapter::new(
            &initial,
            &set,
            &current_pods,
            &api,
            host.store().clone(),
            100,
        )
        .unwrap();
        assert!(matches!(
            runner
                .run_activities(
                    &mut host,
                    &DirectSwitchoverWorkflow,
                    execution.clone(),
                    &mut adapter,
                    100,
                )
                .await,
            DurableRunnerOutcome::Active { .. }
        ));
        world.lock().unwrap().change_generation(1);

        let mut now = 101;
        for _ in 0..64 {
            let pods = world.lock().unwrap().pods();
            let current_pods = pod_references(&pods);
            let mut adapter = DirectSwitchoverRunnerAdapter::new(
                &initial,
                &set,
                &current_pods,
                &api,
                host.store().clone(),
                now,
            )
            .unwrap();
            match runner
                .run_activities(
                    &mut host,
                    &DirectSwitchoverWorkflow,
                    execution.clone(),
                    &mut adapter,
                    now,
                )
                .await
            {
                DurableRunnerOutcome::Terminal(_) => break,
                DurableRunnerOutcome::Active { .. }
                | DurableRunnerOutcome::ReloadRequired { .. } => now += 1,
                other => panic!("unexpected generation-change outcome: {other:?}"),
            }
        }
        let world = world.lock().unwrap();
        assert_eq!(
            world
                .requests
                .iter()
                .filter(|request| action_sequence(&request.action_id) == 1)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn direct_switchover_second_proven_non_admission_stops_without_third_dispatch() {
        let initial = direct_initial_operation("bounded-redelivery", snapshot(2), 2, 100).unwrap();
        let execution_id = ExecutionId::from_bytes([74; 16]);
        let execution = direct_execution_spec(execution_id, initial.clone()).unwrap();
        let mut world_value = world_for(&initial, Scenario::Success);
        world_value.busy_always_sequences.insert(1);
        let world = Arc::new(StdMutex::new(world_value));
        let api = TestApi {
            world: world.clone(),
            apply_labels: true,
        };
        let set = test_set(2);
        let mut host = DurableHost::new(
            MeasuredDurableCheckpointStore::with_decoder(
                execution_id,
                DurableCheckpointStore::InMemory(InMemoryCheckpointStore::new()),
                direct_checkpoint_measurement_decoder(),
            ),
            HostEpoch::from_bytes([75; 16]),
            direct_checkpoint_limits(),
        );
        let runner = DurableRunner::new(DIRECT_SWITCHOVER_MAX_RUNNER_FUEL).unwrap();
        let mut now = 100;
        let terminal = loop {
            let pods = world.lock().unwrap().pods();
            let current_pods = pod_references(&pods);
            let mut adapter = DirectSwitchoverRunnerAdapter::new(
                &initial,
                &set,
                &current_pods,
                &api,
                host.store().clone(),
                now,
            )
            .unwrap();
            match runner
                .run_activities(
                    &mut host,
                    &DirectSwitchoverWorkflow,
                    execution.clone(),
                    &mut adapter,
                    now,
                )
                .await
            {
                DurableRunnerOutcome::Terminal(terminal) => break terminal,
                DurableRunnerOutcome::Active { .. }
                | DurableRunnerOutcome::ReloadRequired { .. } => now += 1,
                other => panic!("unexpected bounded-redelivery outcome: {other:?}"),
            }
        };
        assert!(matches!(
            terminal,
            DirectSwitchoverTerminalRecord::Stopped { .. }
        ));
        assert_eq!(
            world
                .lock()
                .unwrap()
                .requests
                .iter()
                .filter(|request| action_sequence(&request.action_id) == 1)
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn direct_switchover_naturally_idempotent_label_reinvokes_until_observed() {
        let initial = direct_initial_operation("label-quarantine", snapshot(2), 2, 100).unwrap();
        let execution_id = ExecutionId::from_bytes([65; 16]);
        let execution = direct_execution_spec(execution_id, initial.clone()).unwrap();
        let world = Arc::new(StdMutex::new(world_for(&initial, Scenario::Success)));
        let api = TestApi {
            world: world.clone(),
            apply_labels: false,
        };
        let set = test_set(2);
        let mut host = DurableHost::new(
            MeasuredDurableCheckpointStore::with_decoder(
                execution_id,
                DurableCheckpointStore::InMemory(InMemoryCheckpointStore::new()),
                direct_checkpoint_measurement_decoder(),
            ),
            HostEpoch::from_bytes([66; 16]),
            direct_checkpoint_limits(),
        );
        let runner = DurableRunner::new(DIRECT_SWITCHOVER_MAX_RUNNER_FUEL).unwrap();
        let mut now = 100;
        for _ in 0..64 {
            let pods = world.lock().unwrap().pods();
            let current_pods = pod_references(&pods);
            let mut adapter = DirectSwitchoverRunnerAdapter::new(
                &initial,
                &set,
                &current_pods,
                &api,
                host.store().clone(),
                now,
            )
            .unwrap();
            let outcome = runner
                .run_activities(
                    &mut host,
                    &DirectSwitchoverWorkflow,
                    execution.clone(),
                    &mut adapter,
                    now,
                )
                .await;
            match outcome {
                DurableRunnerOutcome::Active { .. }
                | DurableRunnerOutcome::ReloadRequired { .. } => now += 1,
                DurableRunnerOutcome::Terminal(_) => {}
                other => panic!("unexpected label activity outcome: {other:?}"),
            }
            if world.lock().unwrap().label_patches.len() >= 2 {
                return;
            }
        }
        panic!("naturally idempotent label activity was not reinvoked");
    }

    #[derive(Clone, Copy)]
    enum UnresolvedReplicaEvidence {
        NoLedger,
        InProgress,
    }

    async fn assert_replica_remains_quarantined_past_deadline(
        sequence: u32,
        evidence: UnresolvedReplicaEvidence,
    ) {
        let initial =
            direct_initial_operation(&format!("quarantine-{sequence}"), snapshot(3), 2, 100)
                .unwrap();
        let execution_id = ExecutionId::from_bytes([u8::try_from(sequence % 251 + 1).unwrap(); 16]);
        let execution = direct_execution_spec(execution_id, initial.clone()).unwrap();
        let mut world_value = world_for(&initial, Scenario::Success);
        match evidence {
            UnresolvedReplicaEvidence::NoLedger => {
                world_value.unknown_without_apply_sequences.insert(sequence);
            }
            UnresolvedReplicaEvidence::InProgress => {
                world_value
                    .in_progress_without_apply_sequences
                    .insert(sequence);
            }
        }
        let world = Arc::new(StdMutex::new(world_value));
        let api = TestApi {
            world: world.clone(),
            apply_labels: true,
        };
        let set = test_set(3);
        let backend = InMemoryCheckpointStore::new();
        let mut host = DurableHost::new(
            MeasuredDurableCheckpointStore::with_decoder(
                execution_id,
                DurableCheckpointStore::InMemory(backend.clone()),
                direct_checkpoint_measurement_decoder(),
            ),
            HostEpoch::from_bytes([86; 16]),
            direct_checkpoint_limits(),
        );
        let runner = DurableRunner::new(DIRECT_SWITCHOVER_MAX_RUNNER_FUEL).unwrap();
        let mut exposed_at = None;
        for now in 100..228 {
            let pods = world.lock().unwrap().pods();
            let current_pods = pod_references(&pods);
            let mut adapter = DirectSwitchoverRunnerAdapter::new(
                &initial,
                &set,
                &current_pods,
                &api,
                host.store().clone(),
                now,
            )
            .unwrap();
            match runner
                .run_activities(
                    &mut host,
                    &DirectSwitchoverWorkflow,
                    execution.clone(),
                    &mut adapter,
                    now,
                )
                .await
            {
                DurableRunnerOutcome::Active { .. }
                | DurableRunnerOutcome::ReloadRequired { .. } => {}
                DurableRunnerOutcome::Terminal(terminal) => {
                    panic!(
                        "unresolved exposed sequence {sequence} crossed its deadline into {terminal:?}"
                    )
                }
                other => panic!("unexpected unresolved quarantine outcome: {other:?}"),
            }
            if exposed_at.is_none()
                && world
                    .lock()
                    .unwrap()
                    .requests
                    .iter()
                    .any(|request| action_sequence(&request.action_id) == sequence)
            {
                exposed_at = Some(now);
            }
            if exposed_at
                .is_some_and(|exposed| now > exposed + crate::durable::ACTION_DEADLINE_SECONDS + 2)
            {
                break;
            }
        }
        assert!(
            exposed_at.is_some(),
            "sequence {sequence} was never exposed"
        );
        {
            let world = world.lock().unwrap();
            assert_eq!(
                world
                    .requests
                    .iter()
                    .filter(|request| action_sequence(&request.action_id) == sequence)
                    .count(),
                1,
                "quarantined sequence {sequence} must never redeliver without proof"
            );
            assert_eq!(
                world
                    .requests
                    .last()
                    .map(|request| action_sequence(&request.action_id)),
                Some(sequence),
                "no later normal or compensation effect may dispatch"
            );
            assert!(world.label_patches.is_empty());
        }

        let stored = backend.load(execution_id).await.unwrap().unwrap();
        let payload = stored
            .checkpoint()
            .decode_and_validate(&execution, direct_checkpoint_limits())
            .unwrap();
        assert!(matches!(
            payload.active_activities().unwrap().last().unwrap().state(),
            kuberic_durable_execution::ActivityState::DispatchExposed { .. }
        ));
    }

    #[tokio::test]
    async fn direct_switchover_unresolved_strict_effects_remain_quarantined_past_deadline() {
        for sequence in [1, 2, 3] {
            assert_replica_remains_quarantined_past_deadline(
                sequence,
                UnresolvedReplicaEvidence::NoLedger,
            )
            .await;
            assert_replica_remains_quarantined_past_deadline(
                sequence,
                UnresolvedReplicaEvidence::InProgress,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn direct_switchover_unobserved_labels_are_safely_reinvoked() {
        for skipped_attempt in [1, 2] {
            let initial = direct_initial_operation(
                &format!("label-quarantine-{skipped_attempt}"),
                snapshot(3),
                2,
                100,
            )
            .unwrap();
            let execution_id =
                ExecutionId::from_bytes([u8::try_from(90 + skipped_attempt).unwrap(); 16]);
            let execution = direct_execution_spec(execution_id, initial.clone()).unwrap();
            let mut world_value = world_for(&initial, Scenario::Success);
            world_value
                .unapplied_label_patch_attempts
                .insert(skipped_attempt);
            let world = Arc::new(StdMutex::new(world_value));
            let api = TestApi {
                world: world.clone(),
                apply_labels: true,
            };
            let set = test_set(3);
            let mut host = DurableHost::new(
                MeasuredDurableCheckpointStore::with_decoder(
                    execution_id,
                    DurableCheckpointStore::InMemory(InMemoryCheckpointStore::new()),
                    direct_checkpoint_measurement_decoder(),
                ),
                HostEpoch::from_bytes([87; 16]),
                direct_checkpoint_limits(),
            );
            let runner = DurableRunner::new(DIRECT_SWITCHOVER_MAX_RUNNER_FUEL).unwrap();
            let mut exposed_at = None;
            let mut terminal = None;
            for now in 100..228 {
                let pods = world.lock().unwrap().pods();
                let current_pods = pod_references(&pods);
                let mut adapter = DirectSwitchoverRunnerAdapter::new(
                    &initial,
                    &set,
                    &current_pods,
                    &api,
                    host.store().clone(),
                    now,
                )
                .unwrap();
                match runner
                    .run_activities(
                        &mut host,
                        &DirectSwitchoverWorkflow,
                        execution.clone(),
                        &mut adapter,
                        now,
                    )
                    .await
                {
                    DurableRunnerOutcome::Active { .. }
                    | DurableRunnerOutcome::ReloadRequired { .. } => {}
                    DurableRunnerOutcome::Terminal(result) => {
                        terminal = Some(result);
                        break;
                    }
                    other => panic!("unexpected unresolved label outcome: {other:?}"),
                }
                if exposed_at.is_none()
                    && world.lock().unwrap().label_patches.len() == skipped_attempt
                {
                    exposed_at = Some(now);
                }
            }
            assert!(exposed_at.is_some());
            assert!(
                world.lock().unwrap().label_patches.len() > skipped_attempt,
                "exact-UID idempotent labels must be reinvoked when the prior result is unobserved"
            );
            assert!(matches!(
                terminal,
                Some(DirectSwitchoverTerminalRecord::Complete { .. })
            ));
        }
    }

    #[tokio::test]
    async fn direct_switchover_exposure_faults_grant_zero_false_permits() {
        for fault in [
            InMemoryFault::ConflictWithoutApply,
            InMemoryFault::OutcomeUnknownWithoutApply,
            InMemoryFault::OutcomeUnknownAfterApply,
            InMemoryFault::FailBeforeRequest(StoreErrorKind::Unavailable),
        ] {
            let initial = direct_initial_operation("exposure-fault", snapshot(2), 2, 100).unwrap();
            let execution_id = ExecutionId::from_bytes([67; 16]);
            let execution = direct_execution_spec(execution_id, initial.clone()).unwrap();
            let world = Arc::new(StdMutex::new(world_for(&initial, Scenario::Success)));
            let definition = DirectSwitchoverDefinition::from_initial(&initial).unwrap();
            let observations = observations(&world.lock().unwrap());
            let addressed = observations
                .iter()
                .map(|(id, observed)| (*id, observed.status.instance_id.clone()))
                .collect();
            let resolver = DirectSwitchoverPreparedActivityResolver::new(
                &definition,
                &observations,
                &addressed,
                100,
                Arc::new(AtomicI64::new(110)),
            );
            let backend = InMemoryCheckpointStore::new();
            backend.fail_next_compare_and_swap(fault);
            let mut host = DurableHost::new(
                MeasuredDurableCheckpointStore::with_decoder(
                    execution_id,
                    DurableCheckpointStore::InMemory(backend),
                    direct_checkpoint_measurement_decoder(),
                ),
                HostEpoch::from_bytes([68; 16]),
                direct_checkpoint_limits(),
            );
            assert!(!matches!(
                host.turn_and_expose_effects(&DirectSwitchoverWorkflow, execution, &resolver,)
                    .await,
                HostOutcome::DispatchPermitted { .. }
            ));
            assert!(world.lock().unwrap().requests.is_empty());
        }
    }

    #[tokio::test]
    async fn direct_switchover_prepared_command_is_persisted_before_permit() {
        let initial =
            direct_initial_operation("prepared-before-permit", snapshot(2), 2, 100).unwrap();
        let execution_id = ExecutionId::from_bytes([69; 16]);
        let execution = direct_execution_spec(execution_id, initial.clone()).unwrap();
        let world = Arc::new(StdMutex::new(world_for(&initial, Scenario::Success)));
        let definition = DirectSwitchoverDefinition::from_initial(&initial).unwrap();
        let observations = observations(&world.lock().unwrap());
        let addressed = observations
            .iter()
            .map(|(id, observed)| (*id, observed.status.instance_id.clone()))
            .collect();
        let resolver = DirectSwitchoverPreparedActivityResolver::new(
            &definition,
            &observations,
            &addressed,
            100,
            Arc::new(AtomicI64::new(110)),
        );
        let backend = InMemoryCheckpointStore::new();
        let mut host = DurableHost::new(
            MeasuredDurableCheckpointStore::with_decoder(
                execution_id,
                DurableCheckpointStore::InMemory(backend.clone()),
                direct_checkpoint_measurement_decoder(),
            ),
            HostEpoch::from_bytes([70; 16]),
            direct_checkpoint_limits(),
        );
        let HostOutcome::DispatchPermitted { permit, .. } = host
            .turn_and_expose_effects(&DirectSwitchoverWorkflow, execution, &resolver)
            .await
        else {
            panic!("direct prepared activity did not receive a permit");
        };
        assert!(world.lock().unwrap().requests.is_empty());
        let stored = backend.load(execution_id).await.unwrap().unwrap();
        let payload = stored
            .checkpoint()
            .decode_and_validate(
                &direct_execution_spec(execution_id, initial).unwrap(),
                direct_checkpoint_limits(),
            )
            .unwrap();
        let recorded = payload.active_activities().unwrap().last().unwrap();
        assert_eq!(recorded.spec(), permit.activity().spec());
        assert_eq!(recorded.prepared_command(), permit.prepared_command());
        assert!(
            recorded
                .prepared_command()
                .is_some_and(|command| command.bytes().as_slice() != b"null")
        );
    }

    #[test]
    fn direct_switchover_global_exact_bounds_and_one_over_are_enforced() {
        for (exact, validate) in [
            (
                DIRECT_SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES,
                validate_direct_workflow_input_bytes as fn(usize) -> Result<(), String>,
            ),
            (
                DIRECT_SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES,
                validate_direct_active_checkpoint_bytes,
            ),
            (
                DIRECT_SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES,
                validate_direct_terminal_checkpoint_bytes,
            ),
            (
                DIRECT_SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES as usize,
                validate_direct_terminal_payload_bytes,
            ),
            (
                super::super::SWITCHOVER_MAX_TRANSITION_FUEL,
                validate_direct_transition_fuel,
            ),
            (
                DIRECT_SWITCHOVER_MAX_RUNNER_FUEL,
                validate_direct_runner_fuel,
            ),
            (
                super::super::SWITCHOVER_MAX_ERROR_BYTES,
                validate_direct_error_bytes,
            ),
        ] {
            assert!(validate(exact).is_ok());
            assert!(validate(exact + 1).is_err());
        }
        assert!(validate_direct_runner_fuel(0).is_err());
        assert_eq!(direct_checkpoint_limits().max_activity_records(), 19);

        let execution_id = ExecutionId::from_bytes([73; 16]);
        let execution = ExecutionSpec::new(
            execution_id,
            ExactBytes::new(b"direct-bound".to_vec()),
            DIRECT_SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES,
        );
        let contract = direct_execution_contract(execution);
        let activity = ActivitySpec::new(
            ActivityName::new("kuberic.switchover.bound-proof", 1).unwrap(),
            ExactBytes::new(b"{}".to_vec()),
            2,
        );
        let exact_records = (0..DIRECT_SWITCHOVER_MAX_ACTIVITY_RECORDS)
            .map(|sequence| {
                ActivityRecord::completed(
                    ActivitySequence::new(sequence as u64),
                    activity.clone(),
                    ExactBytes::new(b"{}".to_vec()),
                )
            })
            .collect::<Vec<_>>();
        let exact_payload = CheckpointPayload::active(contract.clone(), exact_records.clone());
        let exact_checkpoint =
            CheckpointEnvelope::encode_with_limits(&exact_payload, direct_checkpoint_limits())
                .unwrap();
        assert!(
            exact_checkpoint.encoded_len().unwrap() <= DIRECT_SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES
        );

        let mut one_over_records = exact_records;
        one_over_records.push(ActivityRecord::completed(
            ActivitySequence::new(DIRECT_SWITCHOVER_MAX_ACTIVITY_RECORDS as u64),
            activity,
            ExactBytes::new(b"{}".to_vec()),
        ));
        assert!(matches!(
            CheckpointEnvelope::encode_with_limits(
                &CheckpointPayload::active(contract, one_over_records),
                direct_checkpoint_limits(),
            ),
            Err(CheckpointError::ActivityRecordLimitExceeded { .. })
        ));
    }

    #[test]
    fn direct_switchover_declared_max_fault_payloads_fit_global_byte_bounds() {
        fn push_records<A: SwitchoverActivityContract>(
            records: &mut Vec<ActivityRecord>,
            count: usize,
        ) {
            for _ in 0..count {
                let sequence = ActivitySequence::new(records.len() as u64);
                records.push(ActivityRecord::completed(
                    sequence,
                    ActivitySpec::new(
                        ActivityName::new(
                            <A as SwitchoverActivityContract>::NAME,
                            <A as SwitchoverActivityContract>::VERSION,
                        )
                        .unwrap(),
                        ExactBytes::new(vec![
                            b'x';
                            usize::try_from(
                                <A as SwitchoverActivityContract>::MAX_REQUEST_BYTES
                            )
                            .unwrap()
                        ]),
                        <OrdinarySwitchoverActivity<A> as DurableActivity>::MAX_RESULT_BYTES,
                    ),
                    ExactBytes::new(vec![
                        b'x';
                        usize::try_from(
                            <OrdinarySwitchoverActivity<A> as DurableActivity>::MAX_RESULT_BYTES
                        )
                        .unwrap()
                    ]),
                ));
            }
        }

        use super::super::activities::{
            AttestCompensatedTopologyActivity, CaptureFrozenLsnActivity,
            CompensateDistributeReplicaEpochActivity, CompensatePromoteOldPrimaryActivity,
            DemoteOldPrimaryActivity, InstallCompensationCatchUpConfigurationActivity,
            InstallCompensationCurrentConfigurationActivity, PromoteTargetActivity,
            RestoreOldPrimaryLabelActivity, RestoreTargetSecondaryLabelActivity,
            RevokeWritesActivity, WaitTargetCaughtUpActivity,
        };

        let mut records = Vec::new();
        push_records::<RevokeWritesActivity>(&mut records, 1);
        push_records::<CaptureFrozenLsnActivity>(&mut records, 1);
        push_records::<WaitTargetCaughtUpActivity>(&mut records, 1);
        push_records::<DemoteOldPrimaryActivity>(&mut records, 1);
        push_records::<PromoteTargetActivity>(&mut records, 1);
        push_records::<CompensatePromoteOldPrimaryActivity>(&mut records, 1);
        push_records::<CompensateDistributeReplicaEpochActivity>(&mut records, 8);
        push_records::<InstallCompensationCatchUpConfigurationActivity>(&mut records, 1);
        push_records::<InstallCompensationCurrentConfigurationActivity>(&mut records, 1);
        push_records::<RestoreOldPrimaryLabelActivity>(&mut records, 1);
        push_records::<RestoreTargetSecondaryLabelActivity>(&mut records, 1);
        push_records::<AttestCompensatedTopologyActivity>(&mut records, 1);
        assert_eq!(records.len(), DIRECT_SWITCHOVER_MAX_ACTIVITY_RECORDS);

        let execution = ExecutionSpec::new(
            ExecutionId::from_bytes([76; 16]),
            ExactBytes::new(vec![b'x'; DIRECT_SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES]),
            DIRECT_SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES,
        );
        let contract = direct_execution_contract(execution);
        let active = CheckpointEnvelope::encode_with_limits(
            &CheckpointPayload::active(contract.clone(), records),
            direct_checkpoint_limits(),
        )
        .unwrap();
        let active_bytes = active.encoded_len().unwrap();
        assert!(active_bytes <= DIRECT_SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES);

        let terminal = CheckpointEnvelope::encode_with_limits(
            &CheckpointPayload::terminal(
                contract,
                TerminalOutcome::succeeded(ExactBytes::new(vec![
                    b'x';
                    DIRECT_SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES
                        as usize
                ])),
                DIRECT_SWITCHOVER_MAX_ACTIVITY_RECORDS as u64,
            ),
            direct_checkpoint_limits(),
        )
        .unwrap();
        let terminal_bytes = terminal.encoded_len().unwrap();
        assert!(terminal_bytes <= DIRECT_SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES);
        eprintln!(
            "direct switchover declared-bound projection: active={active_bytes} (headroom={}), terminal={terminal_bytes} (headroom={})",
            DIRECT_SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES - active_bytes,
            DIRECT_SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES - terminal_bytes,
        );
    }

    #[test]
    fn direct_switchover_phase_three_routes_production_to_the_direct_engine() {
        let reconciler = include_str!("../../reconciler.rs");
        assert!(reconciler.contains("let execution = native_execution_spec(reference)?;"));
        assert!(reconciler.contains("DirectSwitchoverRunnerAdapter::new"));
        assert!(reconciler.contains("&DirectSwitchoverWorkflow"));
        assert!(!reconciler.contains("NativeSwitchoverWorkflow"));
    }

    #[test]
    fn direct_switchover_resolver_tracks_the_current_effect_deadline() {
        let initial = direct_initial_operation("deadline-routing", snapshot(3), 2, 100).unwrap();
        let definition = DirectSwitchoverDefinition::from_initial(&initial).unwrap();
        let deadline = Arc::new(AtomicI64::new(definition.initial_deadline_unix_seconds));
        let resolver = DirectSwitchoverPreparedActivityResolver::new(
            &definition,
            &OperationObservations::new(),
            &BTreeMap::new(),
            150,
            deadline.clone(),
        );
        let old_primary = definition.member(definition.old_primary_id).unwrap();
        let request = RevokeWritesInput {
            old_primary_id: definition.old_primary_id,
            old_primary_instance_id: old_primary.instance_id.clone(),
        };
        let input = super::super::activities::SwitchoverActivityInput::<RevokeWritesActivity>::new(
            request, 240,
        );
        let spec = ActivitySpec::with_bounds_and_options(
            kuberic_durable_execution::ActivityName::new(
                <RevokeWritesActivity as SwitchoverActivityContract>::NAME,
                1,
            )
            .unwrap(),
            kuberic_durable_execution::encode_activity_input::<
                OrdinarySwitchoverActivity<RevokeWritesActivity>,
            >(&input)
            .unwrap(),
            <RevokeWritesActivity as SwitchoverActivityContract>::MAX_REQUEST_BYTES,
            <RevokeWritesActivity as SwitchoverActivityContract>::MAX_RESULT_BYTES,
            kuberic_durable_execution::ActivityOptions::new(3, 1_000, Some(240_000), None).unwrap(),
        );
        let _ = resolver.resolve(
            ExecutionId::from_bytes([1; 16]),
            &spec,
            EffectMetadata::of::<RevokeWritesActivity>(),
            None,
        );

        assert_eq!(deadline.load(Ordering::Relaxed), 240);
    }

    fn snapshot(member_count: usize) -> StablePartitionSnapshotStatus {
        StablePartitionSnapshotStatus {
            epoch: EpochStatus {
                data_loss_number: 4,
                configuration_number: 8,
            },
            primary_id: 1,
            members: (1..=member_count)
                .map(|id| StableReplicaSnapshotStatus {
                    id: i64::try_from(id).unwrap(),
                    instance_id: format!("pod-{id}-uid"),
                    role: if id == 1 {
                        StableReplicaRoleStatus::Primary
                    } else {
                        StableReplicaRoleStatus::ActiveSecondary
                    },
                    election_metadata: None,
                })
                .collect(),
            write_quorum: u32::try_from(member_count / 2 + 1).unwrap(),
        }
    }

    fn world_for(initial: &crate::crd::DurableOperationStatus, scenario: Scenario) -> TestWorld {
        let previous = initial.previous_snapshot.as_ref().unwrap();
        let previous_configuration =
            configuration_status(ReplicaConfigurationMode::Current, previous);
        let statuses = previous
            .members
            .iter()
            .map(|member| {
                let role = match member.role {
                    StableReplicaRoleStatus::Primary => Role::Primary,
                    StableReplicaRoleStatus::ActiveSecondary => Role::ActiveSecondary,
                };
                let progress = if scenario == Scenario::PrePromotionCompensation && member.id == 2 {
                    0
                } else {
                    100
                };
                (
                    member.id,
                    ReplicaStatusInfo {
                        instance_id: ReplicaInstanceId::new(member.instance_id.clone()),
                        role,
                        epoch: Epoch::new(
                            previous.epoch.data_loss_number,
                            previous.epoch.configuration_number,
                        ),
                        current_progress: progress,
                        catch_up_capability: Some(100),
                        committed_lsn: 100,
                        healthy: true,
                        write_status: if role == Role::Primary {
                            AccessStatus::Granted
                        } else {
                            AccessStatus::NotPrimary
                        },
                        configuration: (role == Role::Primary)
                            .then(|| previous_configuration.clone()),
                        election_configuration: None,
                        deactivation_info: None,
                        active_replica_connections: Vec::new(),
                        build_observation: None,
                        agent: ReplicaAgentStatus {
                            protocol_version:
                                kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
                            lifecycle_peer_protocol_version: 1,
                            generation: AgentGeneration::parse(format!("{:032x}", member.id))
                                .unwrap(),
                            control_version: AgentControlVersion::new(1),
                            current_action: None,
                            retained_terminal_actions: Vec::new(),
                            local_faults: Vec::new(),
                        },
                    },
                )
            })
            .collect();
        let labels = previous
            .members
            .iter()
            .map(|member| {
                (
                    member.id,
                    if member.id == previous.primary_id {
                        "primary".to_string()
                    } else {
                        "secondary".to_string()
                    },
                )
            })
            .collect();
        let mut world = TestWorld {
            statuses,
            labels,
            ..Default::default()
        };
        if scenario == Scenario::PostPromotionCompensation {
            world.fail_sequences.insert(3);
        }
        world
    }

    fn test_set(member_count: usize) -> KubericSet {
        KubericSet {
            metadata: ObjectMeta {
                name: Some("set".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("set-uid".to_string()),
                ..Default::default()
            },
            spec: KubericSetSpec {
                replicas: i32::try_from(member_count).unwrap(),
                min_replicas: 1,
                image: "test:latest".to_string(),
                failover_delay: 0,
                switchover_delay: 30,
                port: 8080,
                control_port: 9090,
                data_port: 9091,
                storage: "256Mi".to_string(),
                pvc_retention_policy: PvcRetentionPolicy::Delete,
            },
            status: None,
        }
    }

    fn pod_references(pods: &[Pod]) -> Vec<(ReplicaId, ReplicaInstanceId, &Pod)> {
        pods.iter()
            .map(|pod| {
                let id = pod
                    .metadata
                    .labels
                    .as_ref()
                    .unwrap()
                    .get("kuberic.io/replica-id")
                    .unwrap()
                    .parse()
                    .unwrap();
                (
                    id,
                    ReplicaInstanceId::new(pod.metadata.uid.clone().unwrap()),
                    pod,
                )
            })
            .collect()
    }

    fn observations(world: &TestWorld) -> OperationObservations {
        world
            .statuses
            .iter()
            .map(|(id, status)| {
                (
                    *id,
                    crate::durable::ReplicaObservation {
                        status: status.clone(),
                        control_address: format!("http://set-{id}:9090"),
                        replicator_address: format!("http://set-{id}:9091"),
                        pod_name: format!("set-{id}"),
                        pod_role_label: world.labels.get(id).cloned(),
                    },
                )
            })
            .collect()
    }

    fn action_sequence(action_id: &str) -> u32 {
        action_id.rsplit(':').next().unwrap().parse().unwrap()
    }

    fn apply_action(
        world: &mut TestWorld,
        request: &CorrelatedControlActionRequest,
    ) -> kuberic_core::Result<CorrelatedControlActionAcknowledgement> {
        let status = world.statuses.get_mut(&request.target_replica_id).unwrap();
        match &request.action {
            DurableReplicaAction::RevokeWriteStatus => {
                status.write_status = AccessStatus::ReconfigurationPending;
            }
            DurableReplicaAction::ChangeRole { epoch, role } => {
                status.epoch = *epoch;
                status.role = *role;
                status.write_status = if *role == Role::Primary {
                    AccessStatus::ReconfigurationPending
                } else {
                    AccessStatus::NotPrimary
                };
            }
            DurableReplicaAction::UpdateEpoch { epoch } => {
                status.epoch = *epoch;
            }
            DurableReplicaAction::UpdateCatchUpConfiguration { current, .. } => {
                status.configuration = Some(ReplicaConfigurationStatus::from_config(
                    ReplicaConfigurationMode::CatchUp,
                    current,
                ));
            }
            DurableReplicaAction::WaitForCatchUpQuorum { .. } => {}
            DurableReplicaAction::UpdateCurrentConfiguration { current } => {
                status.configuration = Some(ReplicaConfigurationStatus::from_config(
                    ReplicaConfigurationMode::Current,
                    current,
                ));
                if status.role == Role::Primary {
                    status.write_status = AccessStatus::Granted;
                }
            }
            other => {
                return Err(KubericError::Internal(
                    format!("unexpected direct test action {other:?}").into(),
                ));
            }
        }
        status.agent.control_version =
            AgentControlVersion::new(status.agent.control_version.value().saturating_add(1));
        let action = DurableActionObservation {
            action_id: request.action_id.clone(),
            signature: request.input_signature.clone(),
            state: DurableActionState::Completed,
            error_class: None,
            error: None,
            result: None,
            add_replica_progress: None,
            remove_replica_progress: None,
        };
        let observation = CorrelatedActionObservation {
            generation: status.agent.generation.clone(),
            control_version: status.agent.control_version,
            action,
        };
        status
            .agent
            .retained_terminal_actions
            .push(observation.clone());
        Ok(CorrelatedControlActionAcknowledgement { observation })
    }

    fn configuration_status(
        mode: ReplicaConfigurationMode,
        snapshot: &StablePartitionSnapshotStatus,
    ) -> ReplicaConfigurationStatus {
        let mut members = snapshot
            .members
            .iter()
            .filter(|member| member.id != snapshot.primary_id)
            .map(|member| ReplicaConfigurationMemberStatus {
                id: member.id,
                instance_id: ReplicaInstanceId::new(member.instance_id.clone()),
                role: Role::ActiveSecondary,
            })
            .collect::<Vec<_>>();
        members.sort_by_key(|member| member.id);
        ReplicaConfigurationStatus {
            mode,
            members,
            write_quorum: snapshot.write_quorum,
        }
    }
}
