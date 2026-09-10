use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
};

use async_trait::async_trait;

use crate::{
    CasOutcome, CheckpointEnvelope, CheckpointStore, ExecutionId, StorageRevision, StoreError,
    StoreErrorKind, StoredCheckpoint,
};

/// One-shot behavior for the next compare-and-swap whose expected revision matches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InMemoryFault {
    FailBeforeRequest(StoreErrorKind),
    ConflictWithoutApply,
    OutcomeUnknownWithoutApply,
    OutcomeUnknownAfterApply,
}

/// Cloneable, process-local checkpoint store used by the conformance harness.
#[derive(Clone, Default)]
pub struct InMemoryCheckpointStore {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    checkpoints: BTreeMap<ExecutionId, StoredCheckpoint>,
    next_cas_fault: Option<InMemoryFault>,
    matching_cas_before_fault: usize,
    next_load_error: Option<StoreError>,
    revision_counter: u64,
}

impl InMemoryCheckpointStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Arm a fault for one matching compare-and-swap. A newly armed fault replaces
    /// an older unconsumed fault.
    pub fn fail_next_compare_and_swap(&self, fault: InMemoryFault) {
        self.fail_compare_and_swap_after(0, fault);
    }

    /// Arm a one-shot fault after a bounded number of matching CAS operations.
    pub fn fail_compare_and_swap_after(
        &self,
        matching_cas_before_fault: usize,
        fault: InMemoryFault,
    ) {
        let mut state = self
            .state
            .lock()
            .expect("in-memory checkpoint store mutex poisoned");
        state.matching_cas_before_fault = matching_cas_before_fault;
        state.next_cas_fault = Some(fault);
    }

    /// Arm one provider error for the next load.
    pub fn fail_next_load(&self, error: StoreError) {
        self.state
            .lock()
            .expect("in-memory checkpoint store mutex poisoned")
            .next_load_error = Some(error);
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, State>, StoreError> {
        self.state.lock().map_err(|error| {
            StoreError::new(
                StoreErrorKind::Other,
                format!("in-memory checkpoint store mutex poisoned: {error}"),
            )
        })
    }
}

#[async_trait]
impl CheckpointStore for InMemoryCheckpointStore {
    async fn load(
        &self,
        execution_id: ExecutionId,
    ) -> Result<Option<StoredCheckpoint>, StoreError> {
        let mut state = self.lock_state()?;
        if let Some(error) = state.next_load_error.take() {
            return Err(error);
        }
        Ok(state.checkpoints.get(&execution_id).cloned())
    }

    async fn compare_and_swap(
        &self,
        execution_id: ExecutionId,
        expected: Option<StorageRevision>,
        checkpoint: CheckpointEnvelope,
    ) -> Result<CasOutcome, StoreError> {
        let mut state = self.lock_state()?;
        let actual = state
            .checkpoints
            .get(&execution_id)
            .map(StoredCheckpoint::revision);
        if actual != expected.as_ref() {
            return Ok(CasOutcome::Conflict);
        }

        let fault = if state.matching_cas_before_fault == 0 {
            state.next_cas_fault.take()
        } else {
            state.matching_cas_before_fault -= 1;
            None
        };
        match fault {
            Some(InMemoryFault::FailBeforeRequest(kind)) => Err(StoreError::new(
                kind,
                "in-memory fault injected before compare-and-swap request",
            )),
            Some(InMemoryFault::ConflictWithoutApply) => Ok(CasOutcome::Conflict),
            Some(InMemoryFault::OutcomeUnknownWithoutApply) => Ok(CasOutcome::OutcomeUnknown),
            fault @ (None | Some(InMemoryFault::OutcomeUnknownAfterApply)) => {
                state.revision_counter =
                    state.revision_counter.checked_add(1).ok_or_else(|| {
                        StoreError::new(
                            StoreErrorKind::Other,
                            "in-memory checkpoint revision counter exhausted",
                        )
                    })?;
                let revision = StorageRevision::new(format!(
                    "memory-revision-{:016x}",
                    state.revision_counter
                ))
                .expect("rendered in-memory revision is nonempty");
                state.checkpoints.insert(
                    execution_id,
                    StoredCheckpoint::new(revision.clone(), checkpoint),
                );
                if matches!(fault, Some(InMemoryFault::OutcomeUnknownAfterApply)) {
                    Ok(CasOutcome::OutcomeUnknown)
                } else {
                    Ok(CasOutcome::Accepted(revision))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::*;
    use crate::{CheckpointPayload, ExactBytes, ExecutionContract, ExecutionSpec};

    fn checkpoint(execution_id: ExecutionId, marker: u8) -> CheckpointEnvelope {
        CheckpointEnvelope::encode(&CheckpointPayload::active(
            ExecutionContract::new(
                ExecutionSpec::new(execution_id, ExactBytes::new([marker]), 16),
                1_000_000,
            ),
            Vec::new(),
        ))
        .unwrap()
    }

    #[test]
    fn compare_and_swap_is_atomic_and_unknown_outcomes_hide_apply_state() {
        block_on(async {
            let store = InMemoryCheckpointStore::new();
            let execution_id = ExecutionId::from_bytes([1; 16]);

            let CasOutcome::Accepted(first) = store
                .compare_and_swap(execution_id, None, checkpoint(execution_id, 1))
                .await
                .unwrap()
            else {
                panic!("initial creation was not accepted");
            };
            assert_eq!(
                store
                    .compare_and_swap(execution_id, None, checkpoint(execution_id, 2))
                    .await
                    .unwrap(),
                CasOutcome::Conflict
            );

            store.fail_next_compare_and_swap(InMemoryFault::FailBeforeRequest(
                StoreErrorKind::Unavailable,
            ));
            let error = store
                .compare_and_swap(
                    execution_id,
                    Some(first.clone()),
                    checkpoint(execution_id, 3),
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind(), StoreErrorKind::Unavailable);
            assert_eq!(
                store.load(execution_id).await.unwrap().unwrap().revision(),
                &first
            );

            store.fail_next_compare_and_swap(InMemoryFault::ConflictWithoutApply);
            assert_eq!(
                store
                    .compare_and_swap(
                        execution_id,
                        Some(first.clone()),
                        checkpoint(execution_id, 31),
                    )
                    .await
                    .unwrap(),
                CasOutcome::Conflict
            );
            assert_eq!(
                store.load(execution_id).await.unwrap().unwrap().revision(),
                &first
            );

            store.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownWithoutApply);
            assert_eq!(
                store
                    .compare_and_swap(
                        execution_id,
                        Some(first.clone()),
                        checkpoint(execution_id, 4),
                    )
                    .await
                    .unwrap(),
                CasOutcome::OutcomeUnknown
            );
            assert_eq!(
                store.load(execution_id).await.unwrap().unwrap().revision(),
                &first
            );

            store.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownAfterApply);
            assert_eq!(
                store
                    .compare_and_swap(
                        execution_id,
                        Some(first.clone()),
                        checkpoint(execution_id, 5),
                    )
                    .await
                    .unwrap(),
                CasOutcome::OutcomeUnknown
            );
            let second = store
                .load(execution_id)
                .await
                .unwrap()
                .unwrap()
                .revision()
                .clone();
            assert_ne!(first, second);
        });
    }

    #[test]
    fn load_distinguishes_absence_from_each_portable_error_class() {
        block_on(async {
            let store = InMemoryCheckpointStore::new();
            let execution_id = ExecutionId::from_bytes([2; 16]);
            assert_eq!(store.load(execution_id).await.unwrap(), None);

            for kind in [
                StoreErrorKind::Authorization,
                StoreErrorKind::Unavailable,
                StoreErrorKind::Timeout,
                StoreErrorKind::MalformedResponse,
                StoreErrorKind::Other,
            ] {
                store.fail_next_load(StoreError::new(kind, format!("{kind} detail")));
                let error = store.load(execution_id).await.unwrap_err();
                assert_eq!(error.kind(), kind);
                assert!(error.description().contains("detail"));
            }
        });
    }
}
