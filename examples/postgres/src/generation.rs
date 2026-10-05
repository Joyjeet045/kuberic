use std::sync::{Arc, Mutex, Weak};

use crate::durable::PgDurableStore;
use crate::instance::PgError;

#[derive(Default)]
struct ClockState {
    value: u64,
    store: Option<Weak<PgDurableStore>>,
    error: Option<PgError>,
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use crate::durable::{PgDurableIdentity, StorageMode};
    use crate::testing::{TestDataDir, native_identity};
    use kuberic_runtime::protocol::types::ResourceUid;

    fn identity() -> PgDurableIdentity {
        PgDurableIdentity {
            resource_uid: ResourceUid::new("clock"),
            replica: native_identity(1, "clock"),
        }
    }

    #[tokio::test]
    async fn process_clock_never_reuses_ownership_after_reopen_or_exhaustion() {
        let root = TestDataDir::new("clock-max");
        let store = Arc::new(
            PgDurableStore::open(root.path(), identity(), StorageMode::Fresh)
                .await
                .unwrap(),
        );
        let old = GenerationClock::default();
        old.bind(&store).await.unwrap();
        assert_eq!(old.allocate().unwrap(), 1);
        drop(store);
        let store = Arc::new(
            PgDurableStore::open(root.path(), identity(), StorageMode::Established)
                .await
                .unwrap(),
        );
        let clock = GenerationClock::default();
        clock.bind(&store).await.unwrap();
        assert_eq!(clock.allocate().unwrap(), 2);
        assert!(old.allocate().is_err());
        clock.advance_for_test(u64::MAX - 1).unwrap();
        assert_eq!(clock.allocate().unwrap(), u64::MAX);
        assert!(matches!(
            clock.allocate(),
            Err(PgError::GenerationExhausted(_))
        ));
        drop(store);
        let store = Arc::new(
            PgDurableStore::open(root.path(), identity(), StorageMode::Established)
                .await
                .unwrap(),
        );
        let reopened = GenerationClock::default();
        reopened.bind(&store).await.unwrap();
        assert!(matches!(
            reopened.allocate(),
            Err(PgError::GenerationExhausted(_))
        ));
        assert!(matches!(
            reopened.allocate(),
            Err(PgError::GenerationExhausted(_))
        ));
    }

    #[tokio::test]
    async fn initialized_process_clock_is_not_recreated_after_loss_or_corruption() {
        for corrupt in [false, true] {
            let root = TestDataDir::new("clock-lost");
            let store = Arc::new(
                PgDurableStore::open(root.path(), identity(), StorageMode::Fresh)
                    .await
                    .unwrap(),
            );
            let clock = GenerationClock::default();
            clock.bind(&store).await.unwrap();
            clock.allocate().unwrap();
            drop(store);
            let path = root.path().join("process-generation-v2");
            if corrupt {
                std::fs::write(path, b"invalid").unwrap();
            } else {
                std::fs::remove_file(path).unwrap();
            }
            let store = Arc::new(
                PgDurableStore::open(root.path(), identity(), StorageMode::Established)
                    .await
                    .unwrap(),
            );
            assert!(GenerationClock::default().bind(&store).await.is_err());
        }
    }
}

/// Published process identities consume a durable high-water mark before use.
/// Standalone instances retain the same checked clock in their private owner.
#[derive(Default)]
pub(crate) struct GenerationClock(Mutex<ClockState>);

impl GenerationClock {
    pub(crate) fn is_retired(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .store
            .as_ref()
            .is_some_and(|store| store.strong_count() == 0)
    }

    pub(crate) async fn bind(&self, store: &Arc<PgDurableStore>) -> Result<(), PgError> {
        let value = store.prepare_process_clock().await?;
        let mut state = self.0.lock().unwrap();
        if state.value != 0 || state.store.is_some() {
            return Err(PgError::Process(
                "process generation clock already used".into(),
            ));
        }
        state.value = value;
        state.store = Some(Arc::downgrade(store));
        Ok(())
    }

    pub(crate) fn allocate(&self) -> Result<u64, PgError> {
        let mut state = self.0.lock().unwrap();
        if let Some(error) = &state.error {
            return Err(error.clone());
        }
        let result: Result<u64, PgError> = (|| {
            let next = state
                .value
                .checked_add(1)
                .ok_or_else(|| PgError::GenerationExhausted("process generation".into()))?;
            if let Some(store) = &state.store {
                store
                    .upgrade()
                    .ok_or_else(|| PgError::Process("process generation store retired".into()))?
                    .advance_process_clock(state.value, next)?;
            }
            state.value = next;
            Ok(next)
        })();
        if let Err(error) = &result {
            state.error = Some(error.clone());
        }
        result
    }

    #[cfg(feature = "testing")]
    pub(crate) fn advance_for_test(&self, value: u64) -> Result<(), PgError> {
        let mut state = self.0.lock().unwrap();
        assert!(value >= state.value);
        if let Some(store) = &state.store {
            store
                .upgrade()
                .unwrap()
                .advance_process_clock(state.value, value)?;
        }
        state.value = value;
        Ok(())
    }
}
