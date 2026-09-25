use serde::Serialize;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::executor::SqlExecutor;
use crate::instance::{SqlServerInstanceManager, unix_millis};
use crate::observation::InstanceSnapshot;
use crate::runtime_config::ObserverConfig;
use crate::runtime_error::RuntimeError;
use crate::{
    AvailabilityGroupName, Observation, ObservationFailureKind, ReplicaIdentity, ServerName,
};

pub const OBSERVATION_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ObservationSource {
    pub host: String,
    pub port: u16,
    pub availability_group: AvailabilityGroupName,
    pub expected_server_name: ServerName,
    pub replica: ReplicaIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ObservationReport {
    pub schema_version: u16,
    pub source: ObservationSource,
    pub evaluated_at_unix_millis: u64,
    pub max_age_millis: u64,
    pub fresh: bool,
    pub observation: Observation<InstanceSnapshot>,
}

impl ObservationReport {
    pub fn new(
        config: &ObserverConfig,
        observation: Observation<InstanceSnapshot>,
        evaluated_at_unix_millis: u64,
    ) -> Self {
        let max_age_millis = config.max_age_millis();
        Self {
            schema_version: OBSERVATION_SCHEMA_VERSION,
            source: ObservationSource {
                host: config.connection().endpoint().host().to_owned(),
                port: config.connection().endpoint().port(),
                availability_group: config.target().availability_group.clone(),
                expected_server_name: config.target().expected_server_name.clone(),
                replica: config.target().replica.clone(),
            },
            evaluated_at_unix_millis,
            max_age_millis,
            fresh: observation.is_fresh_at(evaluated_at_unix_millis, max_age_millis),
            observation,
        }
    }

    pub fn is_fresh_at(&self, now_unix_millis: u64) -> bool {
        self.observation
            .is_fresh_at(now_unix_millis, self.max_age_millis)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MonitorSummary {
    pub had_failed_or_stale_sample: bool,
}

pub struct SqlServerMonitor<E> {
    manager: SqlServerInstanceManager<E>,
}

impl<E: SqlExecutor> SqlServerMonitor<E> {
    pub fn new(manager: SqlServerInstanceManager<E>) -> Self {
        Self { manager }
    }

    pub async fn run(
        &self,
        publisher: watch::Sender<Option<ObservationReport>>,
        cancellation: CancellationToken,
    ) -> Result<MonitorSummary, RuntimeError> {
        let mut summary = MonitorSummary::default();
        loop {
            let observation = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(summary),
                result = self.manager.observe() => result?,
            };
            let report = ObservationReport::new(self.manager.config(), observation, unix_millis()?);
            publish_report(&publisher, &mut summary, report)?;
            // Delay after each attempt rather than accumulating missed timer ticks.
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(summary),
                _ = tokio::time::sleep(self.manager.config().poll_interval()) => {}
            }
        }
    }
}

fn publish_report(
    publisher: &watch::Sender<Option<ObservationReport>>,
    summary: &mut MonitorSummary,
    report: ObservationReport,
) -> Result<(), RuntimeError> {
    // Latest-value output can lose reports, but not the monitor's lifetime result.
    summary.had_failed_or_stale_sample |= !report.fresh;
    publisher.send(Some(report)).map_err(|_| {
        RuntimeError::new(
            ObservationFailureKind::Unreachable,
            "monitor",
            "observation subscriber closed",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_summary_retains_overwritten_failures_and_stale_samples() {
        let config = ObserverConfig::from_json(include_bytes!("../observer.example.json")).unwrap();
        let observed_at = 100_000;
        let now = observed_at + config.max_age_millis() + 1;
        let stale = ObservationReport::new(
            &config,
            Observation::Absent {
                observed_at_unix_millis: observed_at,
            },
            now,
        );
        let failed = ObservationReport::new(
            &config,
            Observation::Failed(
                RuntimeError::new(
                    ObservationFailureKind::PermissionDenied,
                    "test",
                    "permission missing",
                )
                .into_failure(now),
            ),
            now,
        );
        let fresh = ObservationReport::new(
            &config,
            Observation::Absent {
                observed_at_unix_millis: now,
            },
            now,
        );
        assert!(fresh.fresh);
        for report in [failed, stale] {
            assert!(!report.fresh);
            let (publisher, receiver) = watch::channel(None);
            let mut summary = MonitorSummary::default();
            publish_report(&publisher, &mut summary, report).unwrap();
            publish_report(&publisher, &mut summary, fresh.clone()).unwrap();
            assert_eq!(receiver.borrow().as_ref(), Some(&fresh));
            assert!(summary.had_failed_or_stale_sample);
        }
    }
}
