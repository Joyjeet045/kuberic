use std::time::{SystemTime, UNIX_EPOCH};

use tokio::time::timeout;

use crate::executor::SqlExecutor;
use crate::observation::{InstanceSnapshot, observe_session};
use crate::runtime_config::ObserverConfig;
use crate::runtime_error::RuntimeError;
use crate::{Observation, ObservationFailureKind};

pub struct SqlServerInstanceManager<E> {
    executor: E,
    config: ObserverConfig,
}

impl<E: SqlExecutor> SqlServerInstanceManager<E> {
    pub fn new(executor: E, config: ObserverConfig) -> Self {
        Self { executor, config }
    }

    pub fn config(&self) -> &ObserverConfig {
        &self.config
    }

    pub async fn observe(&self) -> Result<Observation<InstanceSnapshot>, RuntimeError> {
        let observed_at_unix_millis = unix_millis()?;
        let result = timeout(self.config.sample_timeout(), async {
            let mut session = self.executor.connect().await?;
            observe_session(
                session.as_mut(),
                self.config.target(),
                observed_at_unix_millis,
            )
            .await
        })
        .await
        .unwrap_or_else(|_| {
            Err(RuntimeError::new(
                ObservationFailureKind::TimedOut,
                "snapshot",
                "complete observation exceeded its deadline",
            ))
        });
        Ok(match result {
            Ok(value) => Observation::Present {
                value,
                observed_at_unix_millis,
            },
            Err(error) => Observation::Failed(error.into_failure(observed_at_unix_millis)),
        })
    }
}

pub fn unix_millis() -> Result<u64, RuntimeError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .ok_or_else(|| {
            RuntimeError::new(
                ObservationFailureKind::Unsupported,
                "clock",
                "system time cannot be represented as Unix milliseconds",
            )
        })
}
