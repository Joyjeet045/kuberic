//! Execution-keyed checkpoint storage and measurements for operator workflows.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kuberic_durable_execution::{
    CasOutcome, CheckpointEnvelope, CheckpointPayload, CheckpointState, CheckpointStore,
    ExactBytes, ExecutionId, HostOutcome, InMemoryCheckpointStore, KubernetesCheckpointStore,
    PersistenceBoundary, StorageRevision, StoreError, StoreErrorKind, StoredCheckpoint,
    TerminalOutcome,
};
use tracing::info;

const MAX_RECENT_CHECKPOINT_EVENTS: usize = 64;

// COMPLEXITY-BOUNDARY: shared-operator-checkpoint-support:start
/// Workflow-independent checkpoint provider used by operator-hosted workflows.
#[derive(Clone)]
pub enum DurableCheckpointStore {
    Kubernetes(Box<KubernetesCheckpointStore>),
    InMemory(InMemoryCheckpointStore),
}

#[async_trait]
impl CheckpointStore for DurableCheckpointStore {
    async fn load(
        &self,
        execution_id: ExecutionId,
    ) -> Result<Option<StoredCheckpoint>, StoreError> {
        match self {
            Self::Kubernetes(store) => store.load(execution_id).await,
            Self::InMemory(store) => store.load(execution_id).await,
        }
    }

    async fn compare_and_swap(
        &self,
        execution_id: ExecutionId,
        expected: Option<StorageRevision>,
        checkpoint: CheckpointEnvelope,
    ) -> Result<CasOutcome, StoreError> {
        match self {
            Self::Kubernetes(store) => {
                store
                    .compare_and_swap(execution_id, expected, checkpoint)
                    .await
            }
            Self::InMemory(store) => {
                store
                    .compare_and_swap(execution_id, expected, checkpoint)
                    .await
            }
        }
    }
}

/// Classification of one accepted activity for terminal measurement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableActivityClass {
    ExternalEffect,
    PassiveObservation,
}

/// Workflow-neutral terminal activity accounting.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DurableActivityAccounting {
    pub external_effect_count: u64,
    pub passive_observation_count: u64,
}

/// Workflow-provided decoding hooks for checkpoint measurement.
#[derive(Clone, Copy)]
pub struct CheckpointMeasurementDecoder {
    workflow_name: &'static str,
    activity: fn(&ExactBytes) -> Option<DurableActivityClass>,
    terminal: fn(&TerminalOutcome, u64) -> Option<DurableActivityAccounting>,
}

impl CheckpointMeasurementDecoder {
    pub const fn new(
        workflow_name: &'static str,
        activity: fn(&ExactBytes) -> Option<DurableActivityClass>,
        terminal: fn(&TerminalOutcome, u64) -> Option<DurableActivityAccounting>,
    ) -> Self {
        Self {
            workflow_name,
            activity,
            terminal,
        }
    }

    fn workflow_name(self) -> &'static str {
        self.workflow_name
    }

    fn activity(self, input: &ExactBytes) -> Option<DurableActivityClass> {
        (self.activity)(input)
    }

    fn terminal(
        self,
        outcome: &TerminalOutcome,
        completed_activity_count: u64,
    ) -> Option<DurableActivityAccounting> {
        (self.terminal)(outcome, completed_activity_count)
    }
}
// COMPLEXITY-BOUNDARY: shared-operator-checkpoint-support:end

// COMPLEXITY-BOUNDARY: pilot-store:start
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PilotCheckpointMeasurementsSnapshot {
    pub load_attempts: u64,
    pub write_attempts: u64,
    pub accepted_writes: u64,
    pub conflicts: u64,
    pub unknown_outcomes: u64,
    pub definite_failures: u64,
    pub latest_authoritative_checkpoint_bytes: Option<usize>,
    pub maximum_authoritative_checkpoint_bytes: usize,
    pub latest_active_checkpoint_bytes: Option<usize>,
    pub minimum_active_checkpoint_bytes: Option<usize>,
    pub maximum_active_checkpoint_bytes: usize,
    pub latest_terminal_checkpoint_bytes: Option<usize>,
    pub minimum_terminal_checkpoint_bytes: Option<usize>,
    pub maximum_terminal_checkpoint_bytes: usize,
    pub completed_activity_count: Option<u64>,
    pub completed_external_effect_count: Option<u64>,
    pub completed_passive_observation_count: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PilotCheckpointEventResult {
    Accepted,
    Conflict,
    OutcomeUnknown,
    DefiniteFailure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PilotCheckpointEvent {
    pub sequence: u64,
    pub execution_id: ExecutionId,
    pub result: PilotCheckpointEventResult,
    pub attempted_checkpoint_bytes: usize,
    pub authoritative_checkpoint_bytes: Option<usize>,
    pub boundary: Option<PersistenceBoundary>,
}

#[derive(Clone, Default)]
pub struct PilotCheckpointEventCollector {
    events: Arc<Mutex<Vec<PilotCheckpointEvent>>>,
}

impl PilotCheckpointEventCollector {
    pub fn events(&self) -> Vec<PilotCheckpointEvent> {
        self.events
            .lock()
            .expect("pilot checkpoint event collector lock poisoned")
            .clone()
    }

    fn push(&self, event: PilotCheckpointEvent) {
        let mut events = self
            .events
            .lock()
            .expect("pilot checkpoint event collector lock poisoned");
        if events.len() == MAX_RECENT_CHECKPOINT_EVENTS {
            events.remove(0);
        }
        events.push(event);
    }

    fn correlate_latest(&self, boundary: PersistenceBoundary) {
        if let Some(event) = self
            .events
            .lock()
            .expect("pilot checkpoint event collector lock poisoned")
            .iter_mut()
            .rev()
            .find(|event| event.boundary.is_none())
        {
            event.boundary = Some(boundary);
        }
    }
}

#[derive(Clone)]
pub struct MeasuredPilotCheckpointStore {
    execution_id: ExecutionId,
    inner: DurableCheckpointStore,
    decoder: CheckpointMeasurementDecoder,
    measurements: Arc<Mutex<PilotCheckpointMeasurementsSnapshot>>,
    collector: PilotCheckpointEventCollector,
}

impl MeasuredPilotCheckpointStore {
    #[cfg(feature = "durable-switchover-pilot")]
    pub fn new(execution_id: ExecutionId, inner: DurableCheckpointStore) -> Self {
        Self::with_decoder(
            execution_id,
            inner,
            super::pilot::checkpoint_measurement_decoder(),
        )
    }

    #[cfg(all(
        feature = "durable-remove-replica-pilot",
        not(feature = "durable-switchover-pilot")
    ))]
    pub fn new(execution_id: ExecutionId, inner: DurableCheckpointStore) -> Self {
        Self::with_decoder(
            execution_id,
            inner,
            super::remove_replica_pilot::checkpoint_measurement_decoder(),
        )
    }

    pub fn with_decoder(
        execution_id: ExecutionId,
        inner: DurableCheckpointStore,
        decoder: CheckpointMeasurementDecoder,
    ) -> Self {
        Self::with_collector_and_decoder(
            execution_id,
            inner,
            PilotCheckpointEventCollector::default(),
            decoder,
        )
    }

    #[cfg(feature = "durable-switchover-pilot")]
    pub fn with_collector(
        execution_id: ExecutionId,
        inner: DurableCheckpointStore,
        collector: PilotCheckpointEventCollector,
    ) -> Self {
        Self::with_collector_and_decoder(
            execution_id,
            inner,
            collector,
            super::pilot::checkpoint_measurement_decoder(),
        )
    }

    #[cfg(all(
        feature = "durable-remove-replica-pilot",
        not(feature = "durable-switchover-pilot")
    ))]
    pub fn with_collector(
        execution_id: ExecutionId,
        inner: DurableCheckpointStore,
        collector: PilotCheckpointEventCollector,
    ) -> Self {
        Self::with_collector_and_decoder(
            execution_id,
            inner,
            collector,
            super::remove_replica_pilot::checkpoint_measurement_decoder(),
        )
    }

    pub fn with_collector_and_decoder(
        execution_id: ExecutionId,
        inner: DurableCheckpointStore,
        collector: PilotCheckpointEventCollector,
        decoder: CheckpointMeasurementDecoder,
    ) -> Self {
        Self {
            execution_id,
            inner,
            decoder,
            measurements: Arc::new(Mutex::new(PilotCheckpointMeasurementsSnapshot::default())),
            collector,
        }
    }

    pub fn measurements(&self) -> PilotCheckpointMeasurementsSnapshot {
        *self
            .measurements
            .lock()
            .expect("pilot checkpoint measurements lock poisoned")
    }

    pub fn collector(&self) -> PilotCheckpointEventCollector {
        self.collector.clone()
    }

    pub fn correlate_host_outcome(&self, outcome: &HostOutcome) {
        let boundary = match outcome {
            HostOutcome::ScheduleAccepted { .. } => Some(PersistenceBoundary::Schedule),
            HostOutcome::DispatchPermitted { boundary, .. } => Some(*boundary),
            HostOutcome::ObservationAccepted { .. } => Some(PersistenceBoundary::Observation),
            HostOutcome::WorkflowCompleted { boundary, .. } => Some(*boundary),
            HostOutcome::ReloadRequired { boundary, .. } => Some(*boundary),
            HostOutcome::StoreFailed {
                operation: kuberic_durable_execution::StoreOperation::CompareAndSwap(boundary),
                ..
            } => Some(*boundary),
            _ => None,
        };
        if let Some(boundary) = boundary {
            self.collector.correlate_latest(boundary);
            let measurements = self.measurements();
            info!(
                execution_id = %self.execution_id,
                persistence_boundary = ?boundary,
                latest_authoritative_checkpoint_bytes =
                    ?measurements.latest_authoritative_checkpoint_bytes,
                maximum_authoritative_checkpoint_bytes =
                    measurements.maximum_authoritative_checkpoint_bytes,
                maximum_active_checkpoint_bytes = measurements.maximum_active_checkpoint_bytes,
                latest_terminal_checkpoint_bytes =
                    ?measurements.latest_terminal_checkpoint_bytes,
                completed_activity_count = ?measurements.completed_activity_count,
                workflow = self.decoder.workflow_name(),
                "durable workflow checkpoint boundary"
            );
        }
    }

    fn require_execution(&self, execution_id: ExecutionId) -> Result<(), StoreError> {
        if execution_id == self.execution_id {
            Ok(())
        } else {
            Err(StoreError::new(
                StoreErrorKind::Other,
                "durable checkpoint store received another execution identity",
            ))
        }
    }

    fn record_authoritative_checkpoint(&self, checkpoint: &CheckpointEnvelope, bytes: usize) {
        let mut measurements = self
            .measurements
            .lock()
            .expect("pilot checkpoint measurements lock poisoned");
        measurements.latest_authoritative_checkpoint_bytes = Some(bytes);
        measurements.maximum_authoritative_checkpoint_bytes = measurements
            .maximum_authoritative_checkpoint_bytes
            .max(bytes);
        let Ok(payload) =
            serde_json::from_slice::<CheckpointPayload>(checkpoint.payload().as_slice())
        else {
            return;
        };
        match payload.state() {
            CheckpointState::Active { activities, .. } => {
                measurements.latest_active_checkpoint_bytes = Some(bytes);
                measurements.minimum_active_checkpoint_bytes = Some(
                    measurements
                        .minimum_active_checkpoint_bytes
                        .map_or(bytes, |minimum| minimum.min(bytes)),
                );
                measurements.maximum_active_checkpoint_bytes =
                    measurements.maximum_active_checkpoint_bytes.max(bytes);
                let mut external_effects = 0_u64;
                let mut passive_observations = 0_u64;
                for activity in activities {
                    match self.decoder.activity(activity.input()) {
                        Some(DurableActivityClass::PassiveObservation) => {
                            passive_observations = passive_observations.saturating_add(1);
                        }
                        Some(DurableActivityClass::ExternalEffect) => {
                            external_effects = external_effects.saturating_add(1);
                        }
                        None => continue,
                    }
                }
                measurements.completed_external_effect_count = Some(external_effects);
                measurements.completed_passive_observation_count = Some(passive_observations);
            }
            CheckpointState::Terminal {
                outcome,
                completed_activity_count,
                ..
            } => {
                measurements.latest_terminal_checkpoint_bytes = Some(bytes);
                measurements.minimum_terminal_checkpoint_bytes = Some(
                    measurements
                        .minimum_terminal_checkpoint_bytes
                        .map_or(bytes, |minimum| minimum.min(bytes)),
                );
                measurements.maximum_terminal_checkpoint_bytes =
                    measurements.maximum_terminal_checkpoint_bytes.max(bytes);
                measurements.completed_activity_count = Some(*completed_activity_count);
                let accounting = self.decoder.terminal(outcome, *completed_activity_count);
                if let Some(accounting) = accounting {
                    measurements.completed_external_effect_count =
                        Some(accounting.external_effect_count);
                    measurements.completed_passive_observation_count =
                        Some(accounting.passive_observation_count);
                } else {
                    measurements.completed_external_effect_count = None;
                    measurements.completed_passive_observation_count = None;
                }
            }
        }
    }
}

#[async_trait]
impl CheckpointStore for MeasuredPilotCheckpointStore {
    async fn load(
        &self,
        execution_id: ExecutionId,
    ) -> Result<Option<StoredCheckpoint>, StoreError> {
        self.require_execution(execution_id)?;
        {
            let mut measurements = self
                .measurements
                .lock()
                .expect("pilot checkpoint measurements lock poisoned");
            measurements.load_attempts = measurements.load_attempts.saturating_add(1);
        }
        let result = self.inner.load(execution_id).await;
        match &result {
            Ok(Some(stored)) => {
                let bytes = stored.checkpoint().encoded_len().map_err(|error| {
                    StoreError::new(
                        StoreErrorKind::MalformedResponse,
                        format!("measure loaded durable checkpoint: {error}"),
                    )
                })?;
                self.record_authoritative_checkpoint(stored.checkpoint(), bytes);
                info!(
                    execution_id = %execution_id,
                    operation = "load",
                    result = "authoritative",
                    checkpoint_bytes = bytes,
                    workflow = self.decoder.workflow_name(),
                    "durable workflow checkpoint"
                );
            }
            Ok(None) => info!(
                execution_id = %execution_id,
                operation = "load",
                result = "absent",
                workflow = self.decoder.workflow_name(),
                "durable workflow checkpoint"
            ),
            Err(error) => info!(
                execution_id = %execution_id,
                operation = "load",
                result = "definite_failure",
                error_kind = %error.kind(),
                workflow = self.decoder.workflow_name(),
                "durable workflow checkpoint"
            ),
        }
        result
    }

    async fn compare_and_swap(
        &self,
        execution_id: ExecutionId,
        expected: Option<StorageRevision>,
        checkpoint: CheckpointEnvelope,
    ) -> Result<CasOutcome, StoreError> {
        self.require_execution(execution_id)?;
        let attempted_bytes = checkpoint.encoded_len().map_err(|error| {
            StoreError::new(
                StoreErrorKind::Other,
                format!("measure proposed durable checkpoint: {error}"),
            )
        })?;
        let attempted_checkpoint = checkpoint.clone();
        {
            let mut measurements = self
                .measurements
                .lock()
                .expect("pilot checkpoint measurements lock poisoned");
            measurements.write_attempts = measurements.write_attempts.saturating_add(1);
        }
        let result = self
            .inner
            .compare_and_swap(execution_id, expected, checkpoint)
            .await;
        let result_name = match &result {
            Ok(CasOutcome::Accepted(_)) => {
                let mut measurements = self
                    .measurements
                    .lock()
                    .expect("pilot checkpoint measurements lock poisoned");
                measurements.accepted_writes = measurements.accepted_writes.saturating_add(1);
                drop(measurements);
                self.record_authoritative_checkpoint(&attempted_checkpoint, attempted_bytes);
                "accepted"
            }
            Ok(CasOutcome::Conflict) => {
                let mut measurements = self
                    .measurements
                    .lock()
                    .expect("pilot checkpoint measurements lock poisoned");
                measurements.conflicts = measurements.conflicts.saturating_add(1);
                "conflict"
            }
            Ok(CasOutcome::OutcomeUnknown) => {
                let mut measurements = self
                    .measurements
                    .lock()
                    .expect("pilot checkpoint measurements lock poisoned");
                measurements.unknown_outcomes = measurements.unknown_outcomes.saturating_add(1);
                "outcome_unknown"
            }
            Err(_) => {
                let mut measurements = self
                    .measurements
                    .lock()
                    .expect("pilot checkpoint measurements lock poisoned");
                measurements.definite_failures = measurements.definite_failures.saturating_add(1);
                "definite_failure"
            }
        };
        let snapshot = self.measurements();
        self.collector.push(PilotCheckpointEvent {
            sequence: snapshot.write_attempts,
            execution_id,
            result: match &result {
                Ok(CasOutcome::Accepted(_)) => PilotCheckpointEventResult::Accepted,
                Ok(CasOutcome::Conflict) => PilotCheckpointEventResult::Conflict,
                Ok(CasOutcome::OutcomeUnknown) => PilotCheckpointEventResult::OutcomeUnknown,
                Err(_) => PilotCheckpointEventResult::DefiniteFailure,
            },
            attempted_checkpoint_bytes: attempted_bytes,
            authoritative_checkpoint_bytes: matches!(result, Ok(CasOutcome::Accepted(_)))
                .then_some(attempted_bytes),
            boundary: None,
        });
        info!(
            execution_id = %execution_id,
            operation = "compare_and_swap",
            result = result_name,
            attempted_checkpoint_bytes = attempted_bytes,
            authoritative = matches!(result, Ok(CasOutcome::Accepted(_))),
            workflow = self.decoder.workflow_name(),
            "durable workflow checkpoint"
        );
        result
    }
}

pub type MeasuredDurableCheckpointStore = MeasuredPilotCheckpointStore;
pub type PilotCheckpointStore = DurableCheckpointStore;
pub type DurableCheckpointMeasurementsSnapshot = PilotCheckpointMeasurementsSnapshot;
pub type DurableCheckpointEvent = PilotCheckpointEvent;
pub type DurableCheckpointEventCollector = PilotCheckpointEventCollector;
pub type DurableCheckpointEventResult = PilotCheckpointEventResult;

// COMPLEXITY-BOUNDARY: pilot-store:end
#[cfg(test)]
mod durable_switchover_pilot_tests {
    use super::*;
    use kuberic_durable_execution::{
        ActivityName, ActivityRecord, ActivitySequence, ActivitySpec, ExactBytes,
        ExecutionContract, ExecutionSpec, InMemoryFault, ReloadReason, TerminalOutcome,
    };

    fn checkpoint(value: &[u8]) -> CheckpointEnvelope {
        CheckpointEnvelope::new(3, ExactBytes::new(value))
    }

    fn fixture_activity_decoder(input: &ExactBytes) -> Option<DurableActivityClass> {
        match input.as_slice() {
            b"effect" => Some(DurableActivityClass::ExternalEffect),
            b"observation" => Some(DurableActivityClass::PassiveObservation),
            _ => None,
        }
    }

    fn fixture_terminal_decoder(
        outcome: &TerminalOutcome,
        completed_activity_count: u64,
    ) -> Option<DurableActivityAccounting> {
        (outcome.payload().as_slice() == b"fixture" && completed_activity_count == 3).then_some(
            DurableActivityAccounting {
                external_effect_count: 2,
                passive_observation_count: 1,
            },
        )
    }

    #[test]
    fn workflow_provided_decoder_classifies_activity_and_terminal_accounting() {
        let decoder = CheckpointMeasurementDecoder::new(
            "fixture",
            fixture_activity_decoder,
            fixture_terminal_decoder,
        );
        assert_eq!(
            decoder.activity(&ExactBytes::new(b"effect")),
            Some(DurableActivityClass::ExternalEffect)
        );
        assert_eq!(
            decoder.activity(&ExactBytes::new(b"observation")),
            Some(DurableActivityClass::PassiveObservation)
        );
        assert_eq!(decoder.activity(&ExactBytes::new(b"unknown")), None);
        assert_eq!(
            decoder.terminal(&TerminalOutcome::succeeded(ExactBytes::new(b"fixture")), 3),
            Some(DurableActivityAccounting {
                external_effect_count: 2,
                passive_observation_count: 1,
            })
        );
    }

    #[tokio::test]
    async fn measured_store_uses_workflow_provided_decoder() {
        let execution_id = ExecutionId::from_bytes([31; 16]);
        let contract = ExecutionContract::new(
            ExecutionSpec::new(execution_id, ExactBytes::new(b"workflow"), 128),
            100_000,
        );
        let spec = |input: &'static [u8]| {
            ActivitySpec::new(
                ActivityName::new("fixture.activity", 1).unwrap(),
                ExactBytes::new(input),
                32,
            )
        };
        let checkpoint = CheckpointEnvelope::encode(&CheckpointPayload::active(
            contract,
            vec![
                ActivityRecord::completed(
                    ActivitySequence::new(0),
                    spec(b"effect"),
                    ExactBytes::new(b"ok"),
                ),
                ActivityRecord::completed(
                    ActivitySequence::new(1),
                    spec(b"effect"),
                    ExactBytes::new(b"ok"),
                ),
                ActivityRecord::completed(
                    ActivitySequence::new(2),
                    spec(b"observation"),
                    ExactBytes::new(b"ok"),
                ),
            ],
        ))
        .unwrap();
        let store = MeasuredPilotCheckpointStore::with_decoder(
            execution_id,
            DurableCheckpointStore::InMemory(InMemoryCheckpointStore::new()),
            CheckpointMeasurementDecoder::new(
                "fixture",
                fixture_activity_decoder,
                fixture_terminal_decoder,
            ),
        );

        assert!(matches!(
            store
                .compare_and_swap(execution_id, None, checkpoint)
                .await
                .unwrap(),
            CasOutcome::Accepted(_)
        ));
        let measurements = store.measurements();
        assert_eq!(measurements.completed_external_effect_count, Some(2));
        assert_eq!(measurements.completed_passive_observation_count, Some(1));
    }

    async fn terminal_accounting_measurements(
        seed: u8,
        terminal_payload: &[u8],
        completed_activity_count: u64,
    ) -> PilotCheckpointMeasurementsSnapshot {
        let execution_id = ExecutionId::from_bytes([seed; 16]);
        let contract = ExecutionContract::new(
            ExecutionSpec::new(execution_id, ExactBytes::new(b"workflow"), 128),
            100_000,
        );
        let store = MeasuredPilotCheckpointStore::new(
            execution_id,
            PilotCheckpointStore::InMemory(
                kuberic_durable_execution::InMemoryCheckpointStore::new(),
            ),
        );
        let terminal = CheckpointEnvelope::encode(&CheckpointPayload::terminal(
            contract,
            TerminalOutcome::succeeded(ExactBytes::new(terminal_payload)),
            completed_activity_count,
        ))
        .unwrap();
        assert!(matches!(
            store
                .compare_and_swap(execution_id, None, terminal)
                .await
                .unwrap(),
            CasOutcome::Accepted(_)
        ));
        store.measurements()
    }

    fn terminal_accounting_payload(
        compensated: bool,
        phase: &str,
        frozen_lsn: Option<i64>,
        next_secondary_index: u32,
        last_error: Option<&str>,
        external_effect_count: u64,
        passive_observation_count: u64,
    ) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "status": "complete",
            "state": {
                "phase": phase,
                "frozenLsn": frozen_lsn,
                "nextSecondaryIndex": next_secondary_index,
                "phaseDeadlineUnixSeconds": 100,
                "pendingAction": null,
                "lastError": last_error,
            },
            "snapshot": {
                "epoch": {
                    "dataLossNumber": 4,
                    "configurationNumber": 9,
                },
                "primaryId": 2,
                "members": [
                    {
                        "id": 1,
                        "instanceId": "pod-1-uid",
                        "role": "activeSecondary",
                    },
                    {
                        "id": 2,
                        "instanceId": "pod-2-uid",
                        "role": "primary",
                    },
                    {
                        "id": 3,
                        "instanceId": "pod-3-uid",
                        "role": "activeSecondary",
                    },
                ],
                "writeQuorum": 2,
            },
            "compensated": compensated,
            "accounting": {
                "externalEffectCount": external_effect_count,
                "passiveObservationCount": passive_observation_count,
            },
        }))
        .unwrap()
    }

    fn two_member_terminal_accounting_payload() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "status": "complete",
            "state": {
                "phase": "completed",
                "frozenLsn": 42,
                "nextSecondaryIndex": 0,
                "phaseDeadlineUnixSeconds": 100,
                "pendingAction": null,
                "lastError": null,
            },
            "snapshot": {
                "epoch": {
                    "dataLossNumber": 4,
                    "configurationNumber": 9,
                },
                "primaryId": 2,
                "members": [
                    {
                        "id": 1,
                        "instanceId": "pod-1-uid",
                        "role": "activeSecondary",
                    },
                    {
                        "id": 2,
                        "instanceId": "pod-2-uid",
                        "role": "primary",
                    },
                ],
                "writeQuorum": 2,
            },
            "compensated": false,
            "accounting": {
                "externalEffectCount": 8,
                "passiveObservationCount": 3,
            },
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn measurements_distinguish_authoritative_and_unknown_bytes() {
        let execution_id = ExecutionId::from_bytes([9; 16]);
        let backend = kuberic_durable_execution::InMemoryCheckpointStore::new();
        let store = MeasuredPilotCheckpointStore::new(
            execution_id,
            PilotCheckpointStore::InMemory(backend.clone()),
        );
        let accepted = store
            .compare_and_swap(execution_id, None, checkpoint(b"first"))
            .await
            .unwrap();
        let CasOutcome::Accepted(revision) = accepted else {
            panic!("first write must be accepted");
        };
        store.correlate_host_outcome(&HostOutcome::WorkflowCompleted {
            outcome: TerminalOutcome::succeeded(ExactBytes::new(b"terminal")),
            revision: revision.clone(),
            boundary: PersistenceBoundary::Completion,
        });
        let accepted_bytes = store
            .load(execution_id)
            .await
            .unwrap()
            .unwrap()
            .checkpoint()
            .encoded_len()
            .unwrap();

        backend.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownWithoutApply);
        assert_eq!(
            store
                .compare_and_swap(
                    execution_id,
                    Some(revision),
                    checkpoint(b"unconfirmed-larger-payload")
                )
                .await
                .unwrap(),
            CasOutcome::OutcomeUnknown
        );
        store.correlate_host_outcome(&HostOutcome::ReloadRequired {
            boundary: PersistenceBoundary::Exposure,
            reason: ReloadReason::OutcomeUnknown,
        });
        let after_unknown = store.measurements();
        assert_eq!(after_unknown.write_attempts, 2);
        assert_eq!(after_unknown.accepted_writes, 1);
        assert_eq!(after_unknown.unknown_outcomes, 1);
        assert_eq!(
            after_unknown.latest_authoritative_checkpoint_bytes,
            Some(accepted_bytes)
        );

        let unchanged = store.load(execution_id).await.unwrap().unwrap();
        assert_eq!(
            store.measurements().latest_authoritative_checkpoint_bytes,
            Some(accepted_bytes)
        );

        assert_eq!(
            store
                .compare_and_swap(
                    execution_id,
                    Some(StorageRevision::new("stale").unwrap()),
                    checkpoint(b"conflict")
                )
                .await
                .unwrap(),
            CasOutcome::Conflict
        );
        store.correlate_host_outcome(&HostOutcome::ReloadRequired {
            boundary: PersistenceBoundary::Schedule,
            reason: ReloadReason::Conflict,
        });

        backend.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownAfterApply);
        let applied = checkpoint(b"applied-unknown");
        let applied_bytes = applied.encoded_len().unwrap();
        assert_eq!(
            store
                .compare_and_swap(execution_id, Some(unchanged.revision().clone()), applied)
                .await
                .unwrap(),
            CasOutcome::OutcomeUnknown
        );
        store.correlate_host_outcome(&HostOutcome::ReloadRequired {
            boundary: PersistenceBoundary::Observation,
            reason: ReloadReason::OutcomeUnknown,
        });
        assert_eq!(
            store.measurements().latest_authoritative_checkpoint_bytes,
            Some(accepted_bytes)
        );
        store.load(execution_id).await.unwrap();
        let final_measurements = store.measurements();
        assert_eq!(final_measurements.write_attempts, 4);
        assert_eq!(final_measurements.accepted_writes, 1);
        assert_eq!(final_measurements.conflicts, 1);
        assert_eq!(final_measurements.unknown_outcomes, 2);
        assert_eq!(
            final_measurements.latest_authoritative_checkpoint_bytes,
            Some(applied_bytes)
        );
        let events = store.collector().events();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].boundary, Some(PersistenceBoundary::Completion));
        assert_eq!(events[1].boundary, Some(PersistenceBoundary::Exposure));
        assert_eq!(events[2].boundary, Some(PersistenceBoundary::Schedule));
        assert_eq!(events[3].boundary, Some(PersistenceBoundary::Observation));
    }

    #[tokio::test]
    async fn terminal_only_reload_recovers_authoritative_activity_accounting() {
        let execution_id = ExecutionId::from_bytes([10; 16]);
        let execution = ExecutionSpec::new(execution_id, ExactBytes::new(b"workflow"), 128);
        let contract = ExecutionContract::new(execution, 100_000);
        let backend = kuberic_durable_execution::InMemoryCheckpointStore::new();
        let store = MeasuredPilotCheckpointStore::new(
            execution_id,
            PilotCheckpointStore::InMemory(backend.clone()),
        );
        let active =
            CheckpointEnvelope::encode(&CheckpointPayload::active(contract.clone(), Vec::new()))
                .unwrap();
        let active_bytes = active.encoded_len().unwrap();
        let CasOutcome::Accepted(revision) = store
            .compare_and_swap(execution_id, None, active)
            .await
            .unwrap()
        else {
            panic!("active checkpoint was not accepted");
        };
        let terminal_payload =
            terminal_accounting_payload(false, "completed", Some(42), 1, None, 9, 3);
        let terminal = CheckpointEnvelope::encode(&CheckpointPayload::terminal(
            contract,
            TerminalOutcome::succeeded(ExactBytes::new(terminal_payload)),
            12,
        ))
        .unwrap();
        let terminal_bytes = terminal.encoded_len().unwrap();
        assert!(matches!(
            store
                .compare_and_swap(execution_id, Some(revision), terminal)
                .await
                .unwrap(),
            CasOutcome::Accepted(_)
        ));

        let measurements = store.measurements();
        assert_eq!(measurements.maximum_active_checkpoint_bytes, active_bytes);
        assert_eq!(
            measurements.latest_terminal_checkpoint_bytes,
            Some(terminal_bytes)
        );
        assert_eq!(
            measurements.maximum_terminal_checkpoint_bytes,
            terminal_bytes
        );
        assert_eq!(measurements.completed_activity_count, Some(12));
        assert_eq!(measurements.completed_external_effect_count, Some(9));
        assert_eq!(measurements.completed_passive_observation_count, Some(3));

        let restarted = MeasuredPilotCheckpointStore::new(
            execution_id,
            PilotCheckpointStore::InMemory(backend),
        );
        restarted.load(execution_id).await.unwrap();
        let reloaded = restarted.measurements();
        assert_eq!(reloaded.completed_activity_count, Some(12));
        assert_eq!(
            reloaded.latest_terminal_checkpoint_bytes,
            Some(terminal_bytes)
        );
        assert_eq!(reloaded.completed_external_effect_count, Some(9));
        assert_eq!(reloaded.completed_passive_observation_count, Some(3));

        let mismatched_execution_id = ExecutionId::from_bytes([11; 16]);
        let mismatched_store = MeasuredPilotCheckpointStore::new(
            mismatched_execution_id,
            PilotCheckpointStore::InMemory(
                kuberic_durable_execution::InMemoryCheckpointStore::new(),
            ),
        );
        let mismatched_contract = ExecutionContract::new(
            ExecutionSpec::new(mismatched_execution_id, ExactBytes::new(b"workflow"), 128),
            100_000,
        );
        let mismatched_terminal = CheckpointEnvelope::encode(&CheckpointPayload::terminal(
            mismatched_contract,
            TerminalOutcome::succeeded(ExactBytes::new(terminal_accounting_payload(
                false,
                "completed",
                Some(42),
                1,
                None,
                9,
                3,
            ))),
            11,
        ))
        .unwrap();
        assert!(matches!(
            mismatched_store
                .compare_and_swap(mismatched_execution_id, None, mismatched_terminal)
                .await
                .unwrap(),
            CasOutcome::Accepted(_)
        ));
        let mismatched = mismatched_store.measurements();
        assert_eq!(mismatched.completed_activity_count, Some(11));
        assert_eq!(mismatched.completed_external_effect_count, None);
        assert_eq!(mismatched.completed_passive_observation_count, None);

        let wrong_split_payload =
            terminal_accounting_payload(false, "completed", Some(42), 1, None, 10, 2);
        let wrong_split = terminal_accounting_measurements(12, &wrong_split_payload, 12).await;
        assert_eq!(wrong_split.completed_activity_count, Some(12));
        assert_eq!(wrong_split.completed_external_effect_count, None);
        assert_eq!(wrong_split.completed_passive_observation_count, None);

        let successful_redelivery_payload =
            terminal_accounting_payload(false, "completed", Some(42), 1, None, 10, 3);
        let successful_redelivery =
            terminal_accounting_measurements(13, &successful_redelivery_payload, 13).await;
        assert_eq!(
            successful_redelivery.completed_external_effect_count,
            Some(10)
        );
        assert_eq!(
            successful_redelivery.completed_passive_observation_count,
            Some(3)
        );

        let two_member_payload = two_member_terminal_accounting_payload();
        let two_member = terminal_accounting_measurements(14, &two_member_payload, 11).await;
        assert_eq!(two_member.completed_external_effect_count, Some(8));
        assert_eq!(two_member.completed_passive_observation_count, Some(3));

        let compensation_payload =
            terminal_accounting_payload(true, "failed", Some(42), 2, None, 8, 5);
        let compensation = terminal_accounting_measurements(15, &compensation_payload, 13).await;
        assert_eq!(compensation.completed_activity_count, Some(13));
        assert_eq!(compensation.completed_external_effect_count, Some(8));
        assert_eq!(compensation.completed_passive_observation_count, Some(5));

        let compensation_redelivery_payload =
            terminal_accounting_payload(true, "failed", Some(42), 2, None, 16, 5);
        let compensation_redelivery =
            terminal_accounting_measurements(16, &compensation_redelivery_payload, 21).await;
        assert_eq!(
            compensation_redelivery.completed_external_effect_count,
            Some(16)
        );
        assert_eq!(
            compensation_redelivery.completed_passive_observation_count,
            Some(5)
        );

        let impossible_compensation_payload =
            terminal_accounting_payload(true, "failed", Some(42), 2, None, 21, 3);
        let impossible_compensation =
            terminal_accounting_measurements(17, &impossible_compensation_payload, 24).await;
        assert_eq!(
            impossible_compensation.completed_external_effect_count,
            None
        );
        assert_eq!(
            impossible_compensation.completed_passive_observation_count,
            None
        );

        let impossible_split_payload =
            terminal_accounting_payload(true, "failed", Some(42), 2, None, 1, 1);
        let impossible_split =
            terminal_accounting_measurements(18, &impossible_split_payload, 2).await;
        assert_eq!(impossible_split.completed_external_effect_count, None);
        assert_eq!(impossible_split.completed_passive_observation_count, None);

        let phase_inconsistent_payload =
            terminal_accounting_payload(true, "failed", Some(42), 0, None, 8, 5);
        let phase_inconsistent =
            terminal_accounting_measurements(19, &phase_inconsistent_payload, 13).await;
        assert_eq!(phase_inconsistent.completed_external_effect_count, None);
        assert_eq!(phase_inconsistent.completed_passive_observation_count, None);

        let uncompensated_failure_payload =
            terminal_accounting_payload(true, "failed", None, 0, Some("revoke failed"), 1, 1);
        let uncompensated_failure =
            terminal_accounting_measurements(20, &uncompensated_failure_payload, 2).await;
        assert_eq!(uncompensated_failure.completed_external_effect_count, None);
        assert_eq!(
            uncompensated_failure.completed_passive_observation_count,
            None
        );
    }

    #[tokio::test]
    async fn production_event_history_is_bounded() {
        let execution_id = ExecutionId::from_bytes([10; 16]);
        let backend = kuberic_durable_execution::InMemoryCheckpointStore::new();
        let store = MeasuredPilotCheckpointStore::new(
            execution_id,
            PilotCheckpointStore::InMemory(backend.clone()),
        );
        for sequence in 1..=70_u64 {
            backend.fail_next_compare_and_swap(InMemoryFault::ConflictWithoutApply);
            assert_eq!(
                store
                    .compare_and_swap(execution_id, None, checkpoint(b"bounded"))
                    .await
                    .unwrap(),
                CasOutcome::Conflict
            );
            assert_eq!(store.measurements().write_attempts, sequence);
        }
        let events = store.collector().events();
        assert_eq!(events.len(), MAX_RECENT_CHECKPOINT_EVENTS);
        assert_eq!(events.first().unwrap().sequence, 7);
        assert_eq!(events.last().unwrap().sequence, 70);
    }
}
