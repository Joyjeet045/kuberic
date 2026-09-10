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
#[cfg(test)]
use kuberic_durable_execution::ExecutionContract;
use kuberic_durable_execution::{
    ActivitySpec, BoundedEffectError, CheckpointError, CheckpointLimits, CompletionMetadata,
    DispatchEffect, DurableEffectSet, EffectErrorKind, EffectHostStep, EffectMetadata,
    EffectOutcome, EffectQuarantineContext, ExactBytes, ExecutionId, ExecutionSpec,
    HostedEffectSet, LogicalActivityId, ObserveEffect, ObserveQuarantinedEffect, PrepareEffect,
    PreparedActivityError, PreparedCommand, PreparedEffectResolver, PreparedEffectSet,
    TerminalOutcome,
};

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
            DispatchFailureDisposition, classify_dispatch_failure, execute_label_command,
            execute_replica_command,
        },
        runner::{
            DurableAdapterWait, DurableCheckpointDisposition, DurableEffectQuarantine,
            TypedDurableAdapterBoundary, TypedDurableOperationAdapter,
        },
        workflow_host::DurablePermitGuard,
    },
};

#[cfg(test)]
use super::workflow::DirectSwitchoverWorkflow;
use super::{
    SwitchoverExposureFault, SwitchoverRunnerContext, SwitchoverWorkflowInput,
    activities::{DIRECT_SWITCHOVER_CONTRACT_VERSION, SwitchoverEffect, SwitchoverEffects},
    collect_switchover_runner_context, encode_execution_id,
    model::{DirectSwitchoverDefinition, same_topology},
    prepare::{SwitchoverDispatch, SwitchoverEffectFamily},
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
        _metadata: EffectMetadata,
        recorded: Option<&PreparedCommand>,
    ) -> Result<PreparedCommand, PreparedActivityError> {
        SwitchoverEffects::resolve_prepared(self, _execution_id, logical, _metadata, recorded)
    }
}

impl<E> PrepareEffect<E> for DirectSwitchoverPreparedActivityResolver
where
    E: SwitchoverEffect + 'static,
    E::Family: SwitchoverEffectFamily<E>,
{
    type Evidence = ();
    type Authority = ();
    type Error = PreparedActivityError;

    fn prepare(
        &self,
        request: &E::Request,
        _authority: &Self::Authority,
        _evidence: &Self::Evidence,
    ) -> Result<E::Command, Self::Error> {
        let deadline = <E::Family as SwitchoverEffectFamily<E>>::deadline_unix_seconds(
            request,
            &self.definition,
        )?;
        self.deadline.store(deadline, Ordering::Relaxed);
        <E::Family as SwitchoverEffectFamily<E>>::prepare_command(
            request,
            &self.definition,
            &self.observations,
            &self.addressed_instances,
            self.now,
        )
    }

    fn validate_recorded(
        &self,
        request: &E::Request,
        command: &E::Command,
        _authority: &Self::Authority,
    ) -> Result<(), Self::Error> {
        let deadline = <E::Family as SwitchoverEffectFamily<E>>::deadline_unix_seconds(
            request,
            &self.definition,
        )?;
        self.deadline.store(deadline, Ordering::Relaxed);
        <E::Family as SwitchoverEffectFamily<E>>::validate_recorded_command(
            request,
            command,
            &self.definition,
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
}

#[async_trait]
impl<E> ObserveEffect<E> for DirectSwitchoverRunnerAdapter<'_>
where
    E: SwitchoverEffect,
    E::Family: SwitchoverEffectFamily<E>,
{
    type Error = String;

    async fn observe(
        &mut self,
        request: &E::Request,
        command: &E::Command,
        _attempt_id: kuberic_durable_execution::AttemptId,
    ) -> Result<Option<EffectOutcome<E::Output>>, Self::Error> {
        let observations = &self.context()?.observations;
        <E::Family as SwitchoverEffectFamily<E>>::observe(
            request,
            command,
            &self.definition,
            observations,
            self.now,
        )
    }
}

#[async_trait]
impl<E> ObserveQuarantinedEffect<E> for DirectSwitchoverRunnerAdapter<'_>
where
    E: SwitchoverEffect + 'static,
    E::Family: SwitchoverEffectFamily<E>,
{
    type Error = String;

    async fn observe_quarantined(
        &mut self,
        context: EffectQuarantineContext<'_, E>,
    ) -> Result<Option<EffectOutcome<E::Output>>, Self::Error> {
        let observations = &self.context()?.observations;
        <E::Family as SwitchoverEffectFamily<E>>::observe_quarantined(
            context.request(),
            context.command(),
            &self.definition,
            observations,
            self.now,
        )
    }
}

#[async_trait]
impl<E> DispatchEffect<E> for DirectSwitchoverRunnerAdapter<'_>
where
    E: SwitchoverEffect,
    E::Family: SwitchoverEffectFamily<E>,
{
    type Error = String;

    async fn dispatch(
        &mut self,
        _request: &E::Request,
        command: &E::Command,
        _attempt_id: kuberic_durable_execution::AttemptId,
    ) -> Result<Option<EffectOutcome<E::Output>>, Self::Error> {
        match <E::Family as SwitchoverEffectFamily<E>>::dispatch(command) {
            SwitchoverDispatch::ObservationOnly => Ok(None),
            SwitchoverDispatch::Replica(command) => {
                let context = self.context()?;
                let Some(handle) = context.handles.get(&command.target_id) else {
                    return Ok(None);
                };
                if handle.instance_id().as_str() != command.target_instance_id {
                    return Ok(None);
                }
                match execute_replica_command(handle.as_ref(), command).await {
                    Ok(()) => Ok(None),
                    Err(error) => {
                        let kind = match classify_dispatch_failure(&error) {
                            DispatchFailureDisposition::ProvenNoAdmission => {
                                return Ok(Some(EffectOutcome::ProvenNoAdmission));
                            }
                            DispatchFailureDisposition::DefiniteFailure
                                if matches!(error, KubericError::RemoteAgentConflict(_)) =>
                            {
                                EffectErrorKind::ConflictingEvidence
                            }
                            DispatchFailureDisposition::DefiniteFailure => {
                                EffectErrorKind::DomainFailure
                            }
                            DispatchFailureDisposition::Unknown => return Ok(None),
                        };
                        let bounded = BoundedEffectError::observed_at(
                            kind,
                            super::bounded_utf8(
                                &error.to_string(),
                                super::SWITCHOVER_MAX_ERROR_BYTES,
                            ),
                            self.now,
                            E::MAX_ERROR_MESSAGE_BYTES,
                        )
                        .map_err(|error| error.to_string())?;
                        Ok(Some(match kind {
                            EffectErrorKind::DomainFailure => EffectOutcome::DomainFailure(bounded),
                            EffectErrorKind::ConflictingEvidence => {
                                EffectOutcome::ConflictingEvidence(bounded)
                            }
                            _ => unreachable!("dispatch failures use a terminal failure kind"),
                        }))
                    }
                }
            }
            SwitchoverDispatch::Label(command) => {
                execute_label_command(self.api, &self.namespace, command).await;
                Ok(None)
            }
        }
    }
}

#[async_trait]
impl TypedDurableOperationAdapter for DirectSwitchoverRunnerAdapter<'_> {
    type Resolver = DirectSwitchoverPreparedActivityResolver;
    type Effects = SwitchoverEffects;
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

    async fn observe_or_dispatch(
        &mut self,
        activity_id: &LogicalActivityId,
        attempt_id: kuberic_durable_execution::AttemptId,
        permit: &mut DurablePermitGuard,
    ) -> TypedDurableAdapterBoundary {
        let command = match permit.consume_effect_command::<SwitchoverEffects>(
            activity_id,
            attempt_id,
            "switchover",
        ) {
            Ok(command) => command,
            Err(error) => return TypedDurableAdapterBoundary::Isolated(error),
        };
        match SwitchoverEffects::observe_or_dispatch(self, activity_id, attempt_id, &command).await
        {
            Ok(EffectHostStep::Observed(observation)) => {
                if observation.is_proven_no_admission() {
                    TypedDurableAdapterBoundary::ObserveAndWait {
                        observation: Box::new(observation),
                        reason: "RefreshingReplicaObservation".to_string(),
                        detail:
                            "proven non-admission was persisted before the one allowed redelivery"
                                .to_string(),
                        requeue_after_seconds: 1,
                    }
                } else {
                    TypedDurableAdapterBoundary::Observed(Box::new(observation))
                }
            }
            Ok(EffectHostStep::Pending) => TypedDurableAdapterBoundary::Wait {
                reason: "AwaitingEffectObservation".to_string(),
                detail: "typed switchover effect awaits exact authoritative evidence".to_string(),
            },
            Err(error) => TypedDurableAdapterBoundary::Isolated(error),
        }
    }

    async fn resolve_quarantine(
        &mut self,
        quarantine: DurableEffectQuarantine,
    ) -> TypedDurableAdapterBoundary {
        match SwitchoverEffects::observe_quarantined(
            self,
            quarantine.activity(),
            quarantine.attempt_id(),
            quarantine.prepared_command(),
        )
        .await
        {
            Ok(Some(observation)) => TypedDurableAdapterBoundary::Observed(Box::new(observation)),
            Ok(None) => TypedDurableAdapterBoundary::Wait {
                reason: "Quarantined".to_string(),
                detail:
                    "typed switchover effect remains observation-only until exact evidence arrives"
                        .to_string(),
            },
            Err(error) => TypedDurableAdapterBoundary::Isolated(error),
        }
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
        completion_metadata: CompletionMetadata,
    ) -> Result<Self::Terminal, TypedDurableAdapterBoundary> {
        validate_direct_terminal(&self.definition, &outcome, completion_metadata)
            .map_err(TypedDurableAdapterBoundary::Rejected)
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
    let registration = SwitchoverEffects::registrations()
        .iter()
        .find(|registration| registration.matches(spec))?;
    Some(match registration.completion_class() {
        kuberic_durable_execution::CompletionClass::ExternalEffect => {
            DurableActivityClass::ExternalEffect
        }
        kuberic_durable_execution::CompletionClass::PassiveObservation => {
            DurableActivityClass::PassiveObservation
        }
    })
}

fn decode_direct_terminal_accounting(
    _outcome: &TerminalOutcome,
    _completed_activity_count: u64,
) -> Option<DurableActivityAccounting> {
    None
}

fn validate_direct_terminal(
    definition: &DirectSwitchoverDefinition,
    outcome: &TerminalOutcome,
    completion_metadata: CompletionMetadata,
) -> Result<DirectSwitchoverTerminalRecord, String> {
    let completed_activity_count = completion_metadata.completed_activity_count();
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
        CheckpointPayload, CheckpointStore, DurableActivity, DurableEffect, DurableHost,
        EffectActivity, EffectOutcome, HostEpoch, HostOutcome, InMemoryCheckpointStore,
        InMemoryFault, StoreErrorKind,
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
        fn push_replica<A: DurableEffect>(identities: &mut Vec<(String, u32)>, redeliver: bool) {
            identities.push((A::NAME.to_string(), A::VERSION));
            if redeliver {
                identities.push((A::NAME.to_string(), A::VERSION));
            }
        }

        fn push_once<A: DurableEffect>(identities: &mut Vec<(String, u32)>) {
            identities.push((A::NAME.to_string(), A::VERSION));
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

    async fn persist_direct_result<A: DurableEffect>(
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
            panic!("{} was not exposed", A::NAME);
        };
        assert_eq!(permit.activity().spec().name().name(), A::NAME);
        assert_eq!(
            permit
                .prepared_command()
                .is_some_and(|command| command.bytes().as_slice() != b"null"),
            expect_prepared,
            "{} preparation classification",
            A::NAME
        );
        let observation = kuberic_durable_execution::EffectObservation::from_outcome::<A>(
            permit.activity().clone(),
            permit.attempt_id(),
            &EffectOutcome::Applied(output.clone()),
        )
        .unwrap();
        assert!(matches!(
            host.observe_effect(execution, observation).await,
            HostOutcome::ObservationAccepted { .. }
        ));
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
                .run_effects(
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
                .run_effects(
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
                    expected_activities as u64 * 2 + 1
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
            assert!(matches!(
                run_scenario_restarting_every_turn(3, scenario).await,
                DirectSwitchoverTerminalRecord::Complete { branch, .. }
                    if branch == expected_branch
            ));
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
                    DemoteOldPrimaryActivity::NAME
                } else {
                    PromoteTargetActivity::NAME
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
                        .run_effects(
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
                .run_effects(
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
                .run_effects(
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
                .run_effects(
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
                .run_effects(
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
                .run_effects(
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
                .run_effects(
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
                .run_effects(
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
    async fn direct_switchover_label_quarantine_never_redelivers() {
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
            match runner
                .run_effects(
                    &mut host,
                    &DirectSwitchoverWorkflow,
                    execution.clone(),
                    &mut adapter,
                    now,
                )
                .await
            {
                DurableRunnerOutcome::Active { .. }
                | DurableRunnerOutcome::ReloadRequired { .. } => now += 1,
                DurableRunnerOutcome::Terminal(_) => {
                    panic!("non-applied label must remain quarantined")
                }
                other => panic!("unexpected label quarantine outcome: {other:?}"),
            }
            if world.lock().unwrap().label_patches.len() == 1 {
                for _ in 0..8 {
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
                    assert!(matches!(
                        runner
                            .run_effects(
                                &mut host,
                                &DirectSwitchoverWorkflow,
                                execution.clone(),
                                &mut adapter,
                                now,
                            )
                            .await,
                        DurableRunnerOutcome::Active { .. }
                    ));
                    now += 1;
                }
                assert_eq!(world.lock().unwrap().label_patches.len(), 1);
                return;
            }
        }
        panic!("direct switchover did not reach a label effect");
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
                .run_effects(
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
    async fn direct_switchover_unresolved_replica_effects_remain_quarantined_past_deadline() {
        for sequence in [1, 2, 3, 1002] {
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
    async fn direct_switchover_unresolved_labels_remain_quarantined_past_deadline() {
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
                    .run_effects(
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
                            "unresolved label attempt {skipped_attempt} crossed its deadline into {terminal:?}"
                        )
                    }
                    other => panic!("unexpected unresolved label outcome: {other:?}"),
                }
                if exposed_at.is_none()
                    && world.lock().unwrap().label_patches.len() == skipped_attempt
                {
                    exposed_at = Some(now);
                }
                if exposed_at.is_some_and(|exposed| {
                    now > exposed + crate::durable::ACTION_DEADLINE_SECONDS + 2
                }) {
                    break;
                }
            }
            assert!(exposed_at.is_some());
            assert_eq!(
                world.lock().unwrap().label_patches.len(),
                skipped_attempt,
                "UID-fenced labels must never redeliver while exposed outcome is unknown"
            );
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
        fn push_records<A: DurableEffect>(records: &mut Vec<ActivityRecord>, count: usize) {
            for _ in 0..count {
                let sequence = ActivitySequence::new(records.len() as u64);
                records.push(ActivityRecord::completed(
                    sequence,
                    ActivitySpec::new(
                        ActivityName::new(A::NAME, A::VERSION).unwrap(),
                        ExactBytes::new(vec![b'x'; usize::try_from(A::MAX_REQUEST_BYTES).unwrap()]),
                        <EffectActivity<A> as DurableActivity>::MAX_RESULT_BYTES,
                    ),
                    ExactBytes::new(vec![
                        b'x';
                        usize::try_from(
                            <EffectActivity<A> as DurableActivity>::MAX_RESULT_BYTES
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
            contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
            execution_id: definition.execution_id.clone(),
            old_primary_id: definition.old_primary_id,
            old_primary_instance_id: old_primary.instance_id.clone(),
            deadline_unix_seconds: 240,
        };

        let _ = <DirectSwitchoverPreparedActivityResolver as PrepareEffect<
            RevokeWritesActivity,
        >>::prepare(&resolver, &request, &(), &());

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
