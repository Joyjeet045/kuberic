//! In-process operator effect preparation, dispatch, and recovery policy.
//!
//! This module does not own a worker or scheduler. It freezes commands from
//! authoritative observations and delegates the only correlated mutation to
//! `ReplicaHandle`; `ReplicaAgent` remains the admission, fencing, and replay
//! owner.

use std::collections::BTreeMap;

use kuberic_core::driver::ReplicaHandle;
use kuberic_core::error::KubericError;
use kuberic_core::types::{
    AgentControlVersion, AgentGeneration, CorrelatedControlActionAcknowledgement,
    CorrelatedControlActionRequest, DurableActionState, DurableReplicaAction, Epoch, ReplicaId,
    ReplicaInstanceId, ReplicaStatusInfo,
};
use serde::{Deserialize, Serialize};

use crate::cluster_api::ClusterApi;
use crate::crd::{DurableOperationStatus, EpochStatus, PendingActionStatus};

use super::{
    OperationObservations, correlated_action_observation, fail_closed, record_activity_error,
};

const MAX_EFFECT_DIAGNOSTIC_BYTES: usize = 512;

/// Exact compact command persisted before one correlated replica dispatch.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReplicaEffectCommand {
    pub action_id: String,
    pub action_signature: String,
    pub target_id: ReplicaId,
    pub target_instance_id: String,
    pub expected_epoch: EpochStatus,
    pub desired_postcondition: crate::crd::DurablePostconditionStatus,
    pub expected_agent_generation: String,
    pub expected_control_version: u64,
    pub observed_runtime_epoch: EpochStatus,
    pub action_payload: String,
}

impl ReplicaEffectCommand {
    pub fn from_pending(pending: &PendingActionStatus) -> Result<Self, String> {
        Self::from_pending_with_decoder(
            pending,
            kuberic_core::grpc::convert::decode_direct_correlated_action_payload,
        )
    }

    pub fn from_lifecycle_pending(pending: &PendingActionStatus) -> Result<Self, String> {
        Self::from_pending_with_decoder(
            pending,
            kuberic_core::grpc::convert::decode_correlated_action_payload,
        )
    }

    fn from_pending_with_decoder(
        pending: &PendingActionStatus,
        decode: fn(&str) -> Result<DurableReplicaAction, String>,
    ) -> Result<Self, String> {
        let action = decode(&pending.dispatch_action_payload)
            .map_err(|error| format!("decode frozen correlated action: {error}"))?;
        Ok(Self {
            action_id: pending.action_id.clone(),
            action_signature: action.signature(),
            target_id: pending.target_id,
            target_instance_id: pending.target_instance_id.clone(),
            expected_epoch: pending.expected_epoch.clone(),
            desired_postcondition: pending.desired_postcondition.clone(),
            expected_agent_generation: pending
                .dispatch_agent_generation
                .clone()
                .ok_or_else(|| "correlated command is missing agent generation".to_string())?,
            expected_control_version: pending
                .dispatch_agent_control_version
                .ok_or_else(|| "correlated command is missing control version".to_string())?,
            observed_runtime_epoch: pending
                .dispatch_observed_runtime_epoch
                .clone()
                .ok_or_else(|| "correlated command is missing runtime epoch".to_string())?,
            action_payload: pending.dispatch_action_payload.clone(),
        })
    }
}

/// Exact UID-fenced pod-label command persisted before patch dispatch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LabelEffectCommand {
    pub target_id: ReplicaId,
    pub pod_name: String,
    pub expected_uid: String,
    pub role: String,
    pub identity_signature: String,
}

impl LabelEffectCommand {
    pub fn new(target_id: ReplicaId, pod_name: String, expected_uid: String, role: String) -> Self {
        let identity_signature =
            label_identity_signature(target_id, &pod_name, &expected_uid, &role);
        Self {
            target_id,
            pod_name,
            expected_uid,
            role,
            identity_signature,
        }
    }

    pub fn has_valid_identity_signature(&self) -> bool {
        self.identity_signature
            == label_identity_signature(
                self.target_id,
                &self.pod_name,
                &self.expected_uid,
                &self.role,
            )
    }
}

fn label_identity_signature(
    target_id: ReplicaId,
    pod_name: &str,
    expected_uid: &str,
    role: &str,
) -> String {
    format!("{target_id}@{expected_uid}:{pod_name}:{role}")
}

/// Portable outcome vocabulary exposed by operator effect adapters.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "status",
    content = "detail",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum DurableEffectOutcome<T> {
    Applied(T),
    DefiniteFailure { class: String, message: String },
    ProvenNoAdmission { reason: String },
    Pending,
    Unknown { reason: String },
}

impl<T> DurableEffectOutcome<T> {
    pub fn definite_failure(class: &str, message: &str) -> Self {
        Self::DefiniteFailure {
            class: bounded(class),
            message: bounded(message),
        }
    }

    pub fn proven_no_admission(reason: &str) -> Self {
        Self::ProvenNoAdmission {
            reason: bounded(reason),
        }
    }

    pub fn unknown(reason: &str) -> Self {
        Self::Unknown {
            reason: bounded(reason),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchFailureDisposition {
    ProvenNoAdmission,
    DefiniteFailure,
    Unknown,
}

pub(crate) enum DispatchEvidencePlan {
    Ready,
    Persist(Box<PendingActionStatus>),
    WaitForExactIncarnation,
    WaitForSupportedProtocol,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableEffectPreparationError {
    WaitForExactIncarnation,
    WaitForSupportedProtocol,
    InvalidCommand,
}

pub fn prepare_replica_effect_command(
    pending: &PendingActionStatus,
    observed: &ReplicaStatusInfo,
    addressed_instance: &ReplicaInstanceId,
    action: &DurableReplicaAction,
) -> Result<(PendingActionStatus, ReplicaEffectCommand), DurableEffectPreparationError> {
    prepare_replica_effect_command_with_lifecycle_support(
        pending,
        observed,
        addressed_instance,
        action,
        false,
    )
}

pub fn prepare_lifecycle_replica_effect_command(
    pending: &PendingActionStatus,
    observed: &ReplicaStatusInfo,
    addressed_instance: &ReplicaInstanceId,
    action: &DurableReplicaAction,
) -> Result<(PendingActionStatus, ReplicaEffectCommand), DurableEffectPreparationError> {
    prepare_replica_effect_command_with_lifecycle_support(
        pending,
        observed,
        addressed_instance,
        action,
        true,
    )
}

fn prepare_replica_effect_command_with_lifecycle_support(
    pending: &PendingActionStatus,
    observed: &ReplicaStatusInfo,
    addressed_instance: &ReplicaInstanceId,
    action: &DurableReplicaAction,
    allow_lifecycle_intent: bool,
) -> Result<(PendingActionStatus, ReplicaEffectCommand), DurableEffectPreparationError> {
    let exact_incarnation = addressed_instance.as_str() == pending.target_instance_id
        && observed.instance_id.as_str() == pending.target_instance_id;
    if !exact_incarnation {
        return Err(DurableEffectPreparationError::WaitForExactIncarnation);
    }
    if observed.agent.protocol_version
        != kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION
    {
        return Err(DurableEffectPreparationError::WaitForSupportedProtocol);
    }
    let has_frozen_evidence = pending.dispatch_agent_generation.is_some()
        || pending.dispatch_agent_control_version.is_some()
        || pending.dispatch_observed_runtime_epoch.is_some()
        || !pending.dispatch_action_payload.is_empty();
    if has_frozen_evidence {
        let command = if allow_lifecycle_intent {
            ReplicaEffectCommand::from_lifecycle_pending(pending)
        } else {
            ReplicaEffectCommand::from_pending(pending)
        }
        .map_err(|_| DurableEffectPreparationError::InvalidCommand)?;
        let observed_epoch = EpochStatus {
            data_loss_number: observed.epoch.data_loss_number,
            configuration_number: observed.epoch.configuration_number,
        };
        if command.expected_agent_generation != observed.agent.generation.to_string()
            || command.expected_control_version != observed.agent.control_version.value()
            || command.observed_runtime_epoch != observed_epoch
        {
            return Err(DurableEffectPreparationError::WaitForExactIncarnation);
        }
        let decoded = if allow_lifecycle_intent {
            kuberic_core::grpc::convert::decode_correlated_action_payload(&command.action_payload)
        } else {
            kuberic_core::grpc::convert::decode_direct_correlated_action_payload(
                &command.action_payload,
            )
        }
        .map_err(|_| DurableEffectPreparationError::InvalidCommand)?;
        if decoded.signature() != action.signature() {
            return Err(DurableEffectPreparationError::InvalidCommand);
        }
        return Ok((pending.clone(), command));
    }
    let planned = match freeze_dispatch_evidence(
        pending,
        observed,
        addressed_instance,
        action,
        true,
        allow_lifecycle_intent,
    ) {
        Ok(planned) => planned,
        Err(DispatchEvidencePlan::WaitForExactIncarnation) => {
            return Err(DurableEffectPreparationError::WaitForExactIncarnation);
        }
        Err(DispatchEvidencePlan::WaitForSupportedProtocol) => {
            return Err(DurableEffectPreparationError::WaitForSupportedProtocol);
        }
        Err(DispatchEvidencePlan::Ready | DispatchEvidencePlan::Persist(_)) => {
            unreachable!("dispatch evidence freezing returns only wait errors")
        }
    };
    let command = if allow_lifecycle_intent {
        ReplicaEffectCommand::from_lifecycle_pending(&planned)
    } else {
        ReplicaEffectCommand::from_pending(&planned)
    }
    .map_err(|_| DurableEffectPreparationError::InvalidCommand)?;
    if command.action_id != pending.action_id
        || command.target_id != pending.target_id
        || command.target_instance_id != pending.target_instance_id
        || command.action_payload.is_empty()
    {
        return Err(DurableEffectPreparationError::InvalidCommand);
    }
    let decoded = if allow_lifecycle_intent {
        kuberic_core::grpc::convert::decode_correlated_action_payload(&command.action_payload)
    } else {
        kuberic_core::grpc::convert::decode_direct_correlated_action_payload(
            &command.action_payload,
        )
    }
    .map_err(|_| DurableEffectPreparationError::InvalidCommand)?;
    if decoded.signature() != action.signature() {
        return Err(DurableEffectPreparationError::InvalidCommand);
    }
    Ok((planned, command))
}

pub fn replica_action_matches_kind(
    kind: crate::crd::DurableActionKind,
    action: &DurableReplicaAction,
) -> bool {
    use crate::crd::DurableActionKind as Kind;
    matches!(
        (kind, action),
        (
            Kind::AddReplicaIntent,
            DurableReplicaAction::AddReplicaIntent { .. }
        ) | (
            Kind::RemoveReplicaIntent,
            DurableReplicaAction::RemoveReplicaIntent { .. }
        ) | (Kind::RevokeWrite, DurableReplicaAction::RevokeWriteStatus)
            | (
                Kind::DemoteOldPrimary | Kind::PromoteTarget | Kind::CompensatePromoteOldPrimary,
                DurableReplicaAction::ChangeRole { .. }
            )
            | (
                Kind::UpdateSecondaryEpoch | Kind::CompensateUpdateSecondaryEpoch,
                DurableReplicaAction::UpdateEpoch { .. }
            )
            | (
                Kind::UpdateCatchUpConfiguration | Kind::CompensateCatchUpConfiguration,
                DurableReplicaAction::UpdateCatchUpConfiguration { .. }
            )
            | (
                Kind::WaitForCatchUpQuorum,
                DurableReplicaAction::WaitForCatchUpQuorum { .. }
            )
            | (
                Kind::UpdateCurrentConfiguration
                    | Kind::RestorePreviousConfiguration
                    | Kind::CompensateCurrentConfiguration,
                DurableReplicaAction::UpdateCurrentConfiguration { .. }
            )
            | (
                Kind::CreateOpenPrimary | Kind::CreateOpenSecondary,
                DurableReplicaAction::Open { .. }
            )
            | (
                Kind::CreateCompensateCloseCandidate,
                DurableReplicaAction::Close
            )
            | (
                Kind::CreateBuildSecondary,
                DurableReplicaAction::BuildReplica { .. }
            )
            | (
                Kind::CreateCompensateRemoveCandidate,
                DurableReplicaAction::RemoveReplica { .. }
            )
            | (
                Kind::FailoverOnDataLoss,
                DurableReplicaAction::OnDataLoss { .. }
            )
            | (
                Kind::FailoverRecordElectionConfiguration,
                DurableReplicaAction::RecordElectionConfiguration { .. }
            )
    )
}

pub(crate) fn plan_dispatch_evidence(
    pending: &PendingActionStatus,
    observed: &ReplicaStatusInfo,
    addressed_instance: &ReplicaInstanceId,
    action: &DurableReplicaAction,
    persist_action_payload: bool,
) -> DispatchEvidencePlan {
    let planned = match freeze_dispatch_evidence(
        pending,
        observed,
        addressed_instance,
        action,
        persist_action_payload,
        false,
    ) {
        Ok(planned) => planned,
        Err(wait) => return wait,
    };

    if planned == *pending {
        DispatchEvidencePlan::Ready
    } else {
        DispatchEvidencePlan::Persist(Box::new(planned))
    }
}

fn freeze_dispatch_evidence(
    pending: &PendingActionStatus,
    observed: &ReplicaStatusInfo,
    addressed_instance: &ReplicaInstanceId,
    action: &DurableReplicaAction,
    persist_action_payload: bool,
    allow_lifecycle_intent: bool,
) -> Result<PendingActionStatus, DispatchEvidencePlan> {
    let mut planned = pending.clone();
    let exact_incarnation = addressed_instance.as_str() == pending.target_instance_id
        && observed.instance_id.as_str() == pending.target_instance_id;
    if !exact_incarnation {
        return Err(DispatchEvidencePlan::WaitForExactIncarnation);
    }
    let agent = &observed.agent;
    if agent.protocol_version != kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION {
        return Err(DispatchEvidencePlan::WaitForSupportedProtocol);
    }
    let generation = agent.generation.to_string();
    let control_version = agent.control_version.value();
    let runtime_epoch = EpochStatus {
        data_loss_number: observed.epoch.data_loss_number,
        configuration_number: observed.epoch.configuration_number,
    };
    let evidence_matches = pending.dispatch_agent_generation.as_deref() == Some(&generation)
        && pending.dispatch_agent_control_version == Some(control_version)
        && pending.dispatch_observed_runtime_epoch.as_ref() == Some(&runtime_epoch);
    planned.dispatch_agent_generation = Some(generation);
    planned.dispatch_agent_control_version = Some(control_version);
    planned.dispatch_observed_runtime_epoch = Some(runtime_epoch);
    let local_record_exists = correlated_action_observation(observed, &pending.action_id).is_some();
    if persist_action_payload {
        if planned.dispatch_action_payload.is_empty() || (!evidence_matches && !local_record_exists)
        {
            let payload = if allow_lifecycle_intent {
                kuberic_core::grpc::convert::encode_correlated_action_payload(action)
            } else {
                kuberic_core::grpc::convert::encode_direct_correlated_action_payload(action)
            };
            let Ok(payload) = payload else {
                return Err(DispatchEvidencePlan::WaitForSupportedProtocol);
            };
            planned.dispatch_action_payload = payload;
        }
    } else {
        planned.dispatch_action_payload.clear();
    }

    Ok(planned)
}

pub(crate) async fn execute_planned_control_action(
    handle: &dyn ReplicaHandle,
    pending: &PendingActionStatus,
    authoritative_action: Option<DurableReplicaAction>,
) -> kuberic_core::Result<()> {
    if let Some(action) = authoritative_action {
        let generation = pending
            .dispatch_agent_generation
            .as_deref()
            .ok_or_else(|| {
                KubericError::Internal("correlated dispatch is missing agent generation".into())
            })
            .and_then(|generation| {
                AgentGeneration::parse(generation)
                    .map_err(|error| KubericError::Internal(error.into()))
            })?;
        let control_version = pending.dispatch_agent_control_version.ok_or_else(|| {
            KubericError::Internal("correlated dispatch is missing agent control version".into())
        })?;
        let observed_epoch = pending
            .dispatch_observed_runtime_epoch
            .as_ref()
            .ok_or_else(|| {
                KubericError::Internal(
                    "correlated dispatch is missing observed runtime epoch".into(),
                )
            })?;
        return execute_correlated_action(
            handle,
            &pending.action_id,
            pending.target_id,
            &pending.target_instance_id,
            generation,
            AgentControlVersion::new(control_version),
            Epoch::new(
                observed_epoch.data_loss_number,
                observed_epoch.configuration_number,
            ),
            action,
        )
        .await;
    }

    let command = ReplicaEffectCommand::from_pending(pending)
        .map_err(|error| KubericError::Internal(error.into()))?;
    execute_replica_command(handle, &command).await
}

pub async fn execute_replica_command(
    handle: &dyn ReplicaHandle,
    command: &ReplicaEffectCommand,
) -> kuberic_core::Result<()> {
    let generation = AgentGeneration::parse(&command.expected_agent_generation)
        .map_err(|error| KubericError::Internal(error.into()))?;
    let action =
        kuberic_core::grpc::convert::decode_correlated_action_payload(&command.action_payload)
            .map_err(|error| KubericError::Internal(error.into()))?;
    if action.signature() != command.action_signature {
        return Err(KubericError::Internal(
            "persisted correlated action signature does not match its payload".into(),
        ));
    }
    execute_correlated_action(
        handle,
        &command.action_id,
        command.target_id,
        &command.target_instance_id,
        generation,
        AgentControlVersion::new(command.expected_control_version),
        Epoch::new(
            command.observed_runtime_epoch.data_loss_number,
            command.observed_runtime_epoch.configuration_number,
        ),
        action,
    )
    .await
}

pub async fn execute_label_command(
    api: &dyn ClusterApi,
    namespace: &str,
    command: &LabelEffectCommand,
) {
    let mut labels = BTreeMap::new();
    labels.insert("kuberic.io/role".to_string(), command.role.clone());
    let _ = api
        .patch_pod_labels_if_uid(namespace, &command.pod_name, &command.expected_uid, labels)
        .await;
}

#[allow(clippy::too_many_arguments)]
async fn execute_correlated_action(
    handle: &dyn ReplicaHandle,
    action_id: &str,
    target_id: ReplicaId,
    target_instance_id: &str,
    generation: AgentGeneration,
    control_version: AgentControlVersion,
    observed_runtime_epoch: Epoch,
    action: DurableReplicaAction,
) -> kuberic_core::Result<()> {
    let input_signature = action.signature();
    handle
        .execute_correlated_control_action(CorrelatedControlActionRequest {
            protocol_version: kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
            action_id: action_id.to_string(),
            input_signature: input_signature.clone(),
            target_replica_id: target_id,
            target_instance_id: ReplicaInstanceId::new(target_instance_id),
            expected_agent_generation: generation.clone(),
            expected_control_version: control_version,
            observed_runtime_epoch,
            action,
        })
        .await
        .and_then(|acknowledgement| {
            correlated_acknowledgement_result(
                acknowledgement,
                action_id,
                &input_signature,
                &generation,
                control_version,
            )
        })
}

pub(crate) fn correlated_acknowledgement_result(
    acknowledgement: CorrelatedControlActionAcknowledgement,
    expected_action_id: &str,
    expected_signature: &str,
    expected_generation: &AgentGeneration,
    expected_control_version: AgentControlVersion,
) -> kuberic_core::Result<()> {
    let observation = &acknowledgement.observation;
    if observation.generation != *expected_generation
        || observation.control_version.value() == 0
        || observation.control_version.value() > expected_control_version.value().saturating_add(1)
        || observation.action.action_id != expected_action_id
        || observation.action.signature != expected_signature
    {
        return Err(KubericError::RemoteAgentRequestRejected(
            "correlated acknowledgement does not match the dispatched action".to_string(),
        ));
    }
    if acknowledgement.observation.action.state != DurableActionState::Failed {
        return Ok(());
    }
    let action = acknowledgement.observation.action;
    let class = action.error_class.ok_or_else(|| {
        KubericError::RemoteAgentRequestRejected(
            "failed correlated acknowledgement has no error class".to_string(),
        )
    })?;
    Err(KubericError::RemoteAgentTerminalFailure {
        class,
        message: action
            .error
            .unwrap_or_else(|| "correlated control action failed".to_string()),
    })
}

pub fn classify_dispatch_failure(error: &KubericError) -> DispatchFailureDisposition {
    if dispatch_rejection_requires_refresh(error)
        || dispatch_rejection_is_retryable_without_execution(error)
    {
        DispatchFailureDisposition::ProvenNoAdmission
    } else if matches!(
        error,
        KubericError::RemoteAgentConflict(_) | KubericError::RemoteAgentTerminalFailure { .. }
    ) {
        DispatchFailureDisposition::DefiniteFailure
    } else {
        DispatchFailureDisposition::Unknown
    }
}

pub(crate) fn dispatch_rejection_requires_refresh(error: &KubericError) -> bool {
    matches!(
        error,
        KubericError::RemoteAgentPreconditionRejected(_)
            | KubericError::RemoteAgentContinuityUnavailable(_)
    )
}

pub(crate) fn dispatch_rejection_is_retryable_without_execution(error: &KubericError) -> bool {
    matches!(error, KubericError::AgentBusy)
}

pub(crate) fn clear_dispatch_evidence(pending: &mut PendingActionStatus) {
    pending.dispatch_agent_generation = None;
    pending.dispatch_agent_control_version = None;
    pending.dispatch_observed_runtime_epoch = None;
    pending.dispatch_action_payload.clear();
}

pub(crate) fn operation_after_dispatch_error(
    operation: &DurableOperationStatus,
    error: &KubericError,
) -> DurableOperationStatus {
    if matches!(error, KubericError::RemoteAgentConflict(_)) {
        fail_closed(operation, &error.to_string())
    } else if dispatch_rejection_requires_refresh(error) {
        let mut next = operation.clone();
        if let Some(pending) = next.pending_action.as_mut() {
            clear_dispatch_evidence(pending);
            pending.last_error = Some(bounded(&error.to_string()));
        }
        next
    } else if dispatch_rejection_is_retryable_without_execution(error) {
        let mut next = operation.clone();
        if let Some(pending) = next.pending_action.as_mut() {
            pending.last_error = Some(bounded(&error.to_string()));
        }
        next
    } else {
        record_activity_error(operation, &error.to_string())
    }
}

/// Workflow-neutral outcome of resolving or dispatching a prepared effect.
pub enum DurableEffectBridgeOutcome<T> {
    Observe(T),
    ObserveAfterFenceRefresh(T),
    Exposed,
    AwaitEvidence,
}

/// Exact replica decision reconstructed by a workflow during quarantine.
pub struct QuarantinedReplicaDecision {
    pub target_id: ReplicaId,
    pub action_id: String,
    pub action: DurableReplicaAction,
}

pub fn resolve_quarantined_observation<T>(
    result: T,
    authoritative: bool,
) -> DurableEffectBridgeOutcome<T> {
    if authoritative {
        DurableEffectBridgeOutcome::Observe(result)
    } else {
        DurableEffectBridgeOutcome::AwaitEvidence
    }
}

pub fn resolve_quarantined_replica_effect<T>(
    operation: &DurableOperationStatus,
    command: &ReplicaEffectCommand,
    decision: QuarantinedReplicaDecision,
    observations: &OperationObservations,
    proven_no_admission: impl FnOnce(DurableOperationStatus, String) -> T,
) -> Result<DurableEffectBridgeOutcome<T>, String> {
    let pending = operation.pending_action.as_ref().ok_or_else(|| {
        "quarantined prepared replica effect has no pending correlated action".to_string()
    })?;
    let recorded = ReplicaEffectCommand::from_lifecycle_pending(pending)
        .map_err(|error| format!("invalid quarantined prepared replica effect: {error}"))?;
    let action_identity_matches = decision.target_id == command.target_id
        && decision.target_id == pending.target_id
        && decision.action_id == command.action_id
        && decision.action_id == pending.action_id
        && decision.action.signature() == command.action_signature
        && recorded == *command;
    if !action_identity_matches {
        return Ok(DurableEffectBridgeOutcome::AwaitEvidence);
    }
    if generation_change_proves_no_admission(operation, command.target_id, observations) {
        let mut next = operation.clone();
        let next_pending = next
            .pending_action
            .as_mut()
            .expect("validated pending action remains present");
        clear_dispatch_evidence(next_pending);
        return Ok(DurableEffectBridgeOutcome::Observe(proven_no_admission(
            next,
            command.action_id.clone(),
        )));
    }
    Ok(DurableEffectBridgeOutcome::AwaitEvidence)
}

pub(crate) fn generation_change_proves_no_admission(
    operation: &DurableOperationStatus,
    target_id: ReplicaId,
    observations: &OperationObservations,
) -> bool {
    let Some(pending) = operation.pending_action.as_ref() else {
        return false;
    };
    let Some(dispatched_generation) = pending.dispatch_agent_generation.as_deref() else {
        return false;
    };
    let Some(observed) = observations.get(&target_id) else {
        return false;
    };
    command_generation_change_proves_no_admission(
        dispatched_generation,
        &pending.action_id,
        &observed.status,
    )
}

pub(crate) fn command_generation_change_proves_no_admission(
    dispatched_generation: &str,
    action_id: &str,
    observed: &ReplicaStatusInfo,
) -> bool {
    observed.agent.generation.as_str() != dispatched_generation
        && correlated_action_observation(observed, action_id).is_none()
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeleteEffectCommand {
    pub target_id: ReplicaId,
    pub pod_name: String,
    pub expected_uid: String,
    pub identity_signature: String,
}

impl DeleteEffectCommand {
    pub fn new(target_id: ReplicaId, pod_name: String, expected_uid: String) -> Self {
        let identity_signature = format!("{target_id}@{expected_uid}:{pod_name}:delete");
        Self {
            target_id,
            pod_name,
            expected_uid,
            identity_signature,
        }
    }

    pub fn has_valid_identity_signature(&self) -> bool {
        self.identity_signature
            == format!(
                "{}@{}:{}:delete",
                self.target_id, self.expected_uid, self.pod_name
            )
    }
}

pub fn validate_remove_replica_action_kind(
    kind: crate::crd::DurableActionKind,
    action: &DurableReplicaAction,
) -> bool {
    kind == crate::crd::DurableActionKind::RemoveReplicaIntent
        && matches!(action, DurableReplicaAction::RemoveReplicaIntent { .. })
        && replica_action_matches_kind(kind, action)
}

pub fn validate_remove_replica_dispatch_authority(
    operation: &DurableOperationStatus,
    observed: &ReplicaStatusInfo,
    addressed_instance: &ReplicaInstanceId,
    action: &DurableReplicaAction,
) -> bool {
    let Some(intent) = operation.remove_intent.as_ref() else {
        return false;
    };
    let Some(pending) = operation.pending_action.as_ref() else {
        return false;
    };
    let observed_epoch = EpochStatus {
        data_loss_number: observed.epoch.data_loss_number,
        configuration_number: observed.epoch.configuration_number,
    };
    pending.kind == crate::crd::DurableActionKind::RemoveReplicaIntent
        && pending.target_id == operation.old_primary_id
        && pending.target_instance_id == intent.primary_instance_id
        && addressed_instance.as_str() == intent.primary_instance_id
        && observed.instance_id.as_str() == intent.primary_instance_id
        && observed.agent.generation.as_str() == intent.primary_agent_generation
        && observed.agent.control_version.value() == intent.primary_agent_control_version
        && observed.agent.protocol_version
            == kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION
        && observed_epoch == pending.expected_epoch
        && action.signature() == intent.input_signature
}

pub fn prepare_remove_label_effect_command(
    operation: &DurableOperationStatus,
    target_id: ReplicaId,
    expected_uid: &str,
    role: &str,
    pod_identities: &super::OperationPodIdentities,
) -> Result<LabelEffectCommand, String> {
    if operation.target_replica_id != Some(target_id)
        || operation.target_pod_uid.as_deref() != Some(expected_uid)
        || operation.target_instance_id.as_deref() != Some(expected_uid)
        || role != "retired"
    {
        return Err("remove label command differs from frozen target identity".to_string());
    }
    let pod_name = operation
        .target_pod_name
        .as_deref()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| "remove label command has no frozen pod name".to_string())?;
    if pod_identities.get(&target_id).map(String::as_str) != Some(expected_uid) {
        return Err("remove label target UID changed before preparation".to_string());
    }
    Ok(LabelEffectCommand::new(
        target_id,
        pod_name.to_string(),
        expected_uid.to_string(),
        role.to_string(),
    ))
}

pub fn prepare_remove_delete_effect_command(
    operation: &DurableOperationStatus,
    pod_name: &str,
    expected_uid: &str,
    pod_identities: &super::OperationPodIdentities,
) -> Result<DeleteEffectCommand, String> {
    let target_id = operation
        .target_replica_id
        .ok_or_else(|| "remove delete command has no frozen target ID".to_string())?;
    if operation.target_pod_name.as_deref() != Some(pod_name)
        || operation.target_pod_uid.as_deref() != Some(expected_uid)
        || operation.target_instance_id.as_deref() != Some(expected_uid)
    {
        return Err("remove delete command differs from frozen target identity".to_string());
    }
    if pod_identities.get(&target_id).map(String::as_str) != Some(expected_uid) {
        return Err("remove delete target UID changed before preparation".to_string());
    }
    Ok(DeleteEffectCommand::new(
        target_id,
        pod_name.to_string(),
        expected_uid.to_string(),
    ))
}

pub fn remove_label_postcondition_satisfied(
    command: &LabelEffectCommand,
    pod_identities: &super::OperationPodIdentities,
    role_label: Option<&str>,
) -> bool {
    pod_identities
        .get(&command.target_id)
        .is_none_or(|uid| uid != &command.expected_uid)
        || role_label == Some(command.role.as_str())
}

pub fn remove_delete_postcondition_satisfied(
    command: &DeleteEffectCommand,
    pod_identities: &super::OperationPodIdentities,
) -> bool {
    pod_identities
        .get(&command.target_id)
        .is_none_or(|uid| uid != &command.expected_uid)
}

pub async fn execute_delete_command(
    api: &dyn ClusterApi,
    namespace: &str,
    command: &DeleteEffectCommand,
) {
    let _ = api
        .delete_pod(namespace, &command.pod_name, &command.expected_uid)
        .await;
}

fn bounded(value: &str) -> String {
    value.chars().take(MAX_EFFECT_DIAGNOSTIC_BYTES).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kuberic_core::types::{
        AccessStatus, ReplicaAgentStatus, ReplicaSetConfig, ReplicaSetQuorumMode, Role,
    };
    use std::sync::{Arc, Mutex};

    struct RecordingHandle {
        requests: Arc<Mutex<Vec<CorrelatedControlActionRequest>>>,
    }

    #[async_trait::async_trait]
    impl ReplicaHandle for RecordingHandle {
        fn id(&self) -> ReplicaId {
            2
        }

        fn instance_id(&self) -> ReplicaInstanceId {
            ReplicaInstanceId::new("replica-2-uid")
        }

        fn current_progress(&self) -> i64 {
            10
        }

        fn catch_up_capability(&self) -> i64 {
            10
        }

        fn control_address(&self) -> String {
            "http://replica-2".to_string()
        }

        fn replicator_address(&self) -> String {
            "http://replica-2-data".to_string()
        }

        async fn get_status(&self) -> kuberic_core::Result<ReplicaStatusInfo> {
            Ok(observed_command_target())
        }

        async fn execute_correlated_control_action(
            &self,
            request: CorrelatedControlActionRequest,
        ) -> kuberic_core::Result<CorrelatedControlActionAcknowledgement> {
            self.requests.lock().unwrap().push(request);
            Err(KubericError::AgentBusy)
        }
    }

    fn pending_command() -> PendingActionStatus {
        PendingActionStatus {
            action_id: "switchover:7:effect".to_string(),
            sequence: 7,
            kind: crate::crd::DurableActionKind::RevokeWrite,
            target_id: 2,
            target_instance_id: "replica-2-uid".to_string(),
            expected_epoch: EpochStatus {
                data_loss_number: 4,
                configuration_number: 9,
            },
            desired_postcondition: crate::crd::DurablePostconditionStatus {
                kind: crate::crd::DurablePostconditionKind::WriteRevoked,
                role: None,
            },
            attempts: 0,
            deadline_unix_seconds: 200,
            last_error: None,
            dispatch_authorized: true,
            dispatch_agent_generation: None,
            dispatch_agent_control_version: None,
            dispatch_observed_runtime_epoch: None,
            dispatch_action_payload: String::new(),
        }
    }

    fn observed_command_target() -> ReplicaStatusInfo {
        ReplicaStatusInfo {
            instance_id: ReplicaInstanceId::new("replica-2-uid"),
            role: Role::Primary,
            epoch: Epoch::new(4, 8),
            current_progress: 10,
            catch_up_capability: Some(10),
            committed_lsn: 10,
            healthy: true,
            write_status: AccessStatus::Granted,
            configuration: None,
            election_configuration: None,
            deactivation_info: None,
            active_replica_connections: Vec::new(),
            build_observation: None,
            agent: ReplicaAgentStatus {
                protocol_version: kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
                lifecycle_peer_protocol_version:
                    kuberic_core::replica_lifecycle::REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION,
                generation: AgentGeneration::parse("0123456789abcdef0123456789abcdef").unwrap(),
                control_version: AgentControlVersion::new(11),
                current_action: None,
                retained_terminal_actions: Vec::new(),
                local_faults: Vec::new(),
            },
        }
    }

    fn config() -> ReplicaSetConfig {
        ReplicaSetConfig {
            members: Vec::new(),
            write_quorum: 1,
        }
    }

    #[test]
    fn durable_effect_outcome_kinds_are_distinct_and_bounded() {
        let applied = DurableEffectOutcome::Applied(7_u8);
        let rejected = DurableEffectOutcome::<u8>::definite_failure("rejected", &"x".repeat(700));
        let no_admission = DurableEffectOutcome::<u8>::proven_no_admission(&"n".repeat(700));
        let pending = DurableEffectOutcome::<u8>::Pending;
        let unknown = DurableEffectOutcome::<u8>::unknown(&"u".repeat(700));

        for outcome in [applied, rejected, no_admission, pending, unknown] {
            let encoded = serde_json::to_vec(&outcome).unwrap();
            assert_eq!(
                serde_json::from_slice::<DurableEffectOutcome<u8>>(&encoded).unwrap(),
                outcome
            );
            assert!(encoded.len() < 640);
        }
    }

    #[test]
    fn dispatch_errors_do_not_alias_unknown_with_non_admission() {
        assert_eq!(
            classify_dispatch_failure(&KubericError::AgentBusy),
            DispatchFailureDisposition::ProvenNoAdmission
        );
        assert_eq!(
            classify_dispatch_failure(&KubericError::RemoteAgentConflict("conflict".into())),
            DispatchFailureDisposition::DefiniteFailure
        );
        assert_eq!(
            classify_dispatch_failure(&KubericError::Closed),
            DispatchFailureDisposition::Unknown
        );
    }

    #[tokio::test]
    async fn all_seven_switchover_replica_actions_prepare_and_dispatch_exact_fenced_commands() {
        let actions = [
            DurableReplicaAction::RevokeWriteStatus,
            DurableReplicaAction::ChangeRole {
                epoch: Epoch::new(4, 9),
                role: Role::ActiveSecondary,
            },
            DurableReplicaAction::ChangeRole {
                epoch: Epoch::new(4, 9),
                role: Role::Primary,
            },
            DurableReplicaAction::UpdateEpoch {
                epoch: Epoch::new(4, 9),
            },
            DurableReplicaAction::UpdateCatchUpConfiguration {
                current: config(),
                previous: config(),
            },
            DurableReplicaAction::WaitForCatchUpQuorum {
                mode: ReplicaSetQuorumMode::Write,
            },
            DurableReplicaAction::UpdateCurrentConfiguration { current: config() },
        ];
        for action in actions {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let handle = RecordingHandle {
                requests: requests.clone(),
            };
            let pending = pending_command();
            let observed = observed_command_target();
            let (planned, command) = prepare_replica_effect_command(
                &pending,
                &observed,
                &ReplicaInstanceId::new("replica-2-uid"),
                &action,
            )
            .unwrap();
            assert_eq!(
                command,
                ReplicaEffectCommand::from_pending(&planned).unwrap()
            );
            assert_eq!(command.action_id, pending.action_id);
            assert_eq!(command.action_signature, action.signature());
            assert_eq!(command.target_id, pending.target_id);
            assert_eq!(command.target_instance_id, pending.target_instance_id);
            assert_eq!(command.expected_epoch, pending.expected_epoch);
            assert_eq!(command.desired_postcondition, pending.desired_postcondition);
            assert_eq!(
                command.expected_agent_generation,
                observed.agent.generation.to_string()
            );
            assert_eq!(
                command.expected_control_version,
                observed.agent.control_version.value()
            );
            assert_eq!(
                command.observed_runtime_epoch,
                EpochStatus {
                    data_loss_number: observed.epoch.data_loss_number,
                    configuration_number: observed.epoch.configuration_number,
                }
            );
            let decoded = kuberic_core::grpc::convert::decode_direct_correlated_action_payload(
                &command.action_payload,
            )
            .unwrap();
            assert_eq!(decoded.signature(), action.signature());
            assert!(matches!(
                execute_replica_command(&handle, &command).await,
                Err(KubericError::AgentBusy)
            ));
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].action.signature(), action.signature());
            assert_eq!(requests[0].action_id, command.action_id);
            assert_eq!(
                requests[0].target_instance_id.as_str(),
                command.target_instance_id
            );
        }
    }

    #[test]
    fn switchover_preparation_rejects_incarnation_protocol_and_action_identity_drift() {
        let pending = pending_command();
        let action = DurableReplicaAction::RevokeWriteStatus;
        assert_eq!(
            prepare_replica_effect_command(
                &pending,
                &observed_command_target(),
                &ReplicaInstanceId::new("replacement-uid"),
                &action,
            ),
            Err(DurableEffectPreparationError::WaitForExactIncarnation)
        );

        let mut replacement = observed_command_target();
        replacement.instance_id = ReplicaInstanceId::new("replacement-uid");
        assert_eq!(
            prepare_replica_effect_command(
                &pending,
                &replacement,
                &ReplicaInstanceId::new("replica-2-uid"),
                &action,
            ),
            Err(DurableEffectPreparationError::WaitForExactIncarnation)
        );

        let mut unsupported = observed_command_target();
        unsupported.agent.protocol_version = 0;
        assert_eq!(
            prepare_replica_effect_command(
                &pending,
                &unsupported,
                &ReplicaInstanceId::new("replica-2-uid"),
                &action,
            ),
            Err(DurableEffectPreparationError::WaitForSupportedProtocol)
        );

        let (frozen, _) = prepare_replica_effect_command(
            &pending,
            &observed_command_target(),
            &ReplicaInstanceId::new("replica-2-uid"),
            &action,
        )
        .unwrap();
        let mut changed_generation = observed_command_target();
        changed_generation.agent.generation =
            AgentGeneration::parse("fedcba9876543210fedcba9876543210").unwrap();
        assert_eq!(
            prepare_replica_effect_command(
                &frozen,
                &changed_generation,
                &ReplicaInstanceId::new("replica-2-uid"),
                &action,
            ),
            Err(DurableEffectPreparationError::WaitForExactIncarnation)
        );
        let mut changed_control = observed_command_target();
        changed_control.agent.control_version = AgentControlVersion::new(12);
        assert_eq!(
            prepare_replica_effect_command(
                &frozen,
                &changed_control,
                &ReplicaInstanceId::new("replica-2-uid"),
                &action,
            ),
            Err(DurableEffectPreparationError::WaitForExactIncarnation)
        );
        let mut changed_epoch = observed_command_target();
        changed_epoch.epoch = Epoch::new(4, 9);
        assert_eq!(
            prepare_replica_effect_command(
                &frozen,
                &changed_epoch,
                &ReplicaInstanceId::new("replica-2-uid"),
                &action,
            ),
            Err(DurableEffectPreparationError::WaitForExactIncarnation)
        );
        assert_eq!(
            prepare_replica_effect_command(
                &frozen,
                &observed_command_target(),
                &ReplicaInstanceId::new("replica-2-uid"),
                &DurableReplicaAction::Close,
            ),
            Err(DurableEffectPreparationError::InvalidCommand)
        );
    }
}
