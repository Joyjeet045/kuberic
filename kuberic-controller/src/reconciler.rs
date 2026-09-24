use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kuberic_protocol::evaluator::{EvaluationConfig, evaluate};
use kuberic_protocol::observation::{ReplicaObservationKey, ReportWatermark};
use tokio::sync::Mutex;

use crate::cluster_api::ClusterApi;
use crate::executor::{ExecutionKind, ExecutionOutcome, execute_plan};
use crate::normalize::{normalize, report_watermarks};
use crate::{ControllerError, Result};

type Watermarks = BTreeMap<ReplicaObservationKey, ReportWatermark>;

pub struct Reconciler {
    api: Arc<dyn ClusterApi>,
    evaluation: EvaluationConfig,
    locks: Mutex<BTreeMap<String, Arc<Mutex<()>>>>,
    watermarks: Mutex<BTreeMap<String, Watermarks>>,
}

impl Reconciler {
    pub fn new(api: Arc<dyn ClusterApi>, evaluation: EvaluationConfig) -> Self {
        Self {
            api,
            evaluation,
            locks: Mutex::new(BTreeMap::new()),
            watermarks: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn reconcile(&self, namespace: &str, name: &str) -> Result<ReconcileAction> {
        let key = format!("{namespace}/{name}");
        let lock = {
            let mut locks = self.locks.lock().await;
            locks
                .entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = lock.lock().await;
        let raw = self.api.observe(namespace, name).await?;
        let previous = self
            .watermarks
            .lock()
            .await
            .get(&key)
            .cloned()
            .unwrap_or_default();
        let snapshot = normalize(raw.clone(), previous.clone())?;
        let next_watermarks = merge_watermarks(previous, report_watermarks(&snapshot));
        self.watermarks
            .lock()
            .await
            .insert(key.clone(), next_watermarks);
        let plan = evaluate(&snapshot, &self.evaluation);
        let outcome = match execute_plan(self.api.as_ref(), &raw, &snapshot, plan).await {
            Ok(outcome) => outcome,
            Err(ControllerError::ObservationStale) => {
                return Ok(ReconcileAction {
                    requeue_after: Duration::ZERO,
                    kind: ReconcileKind::ObservationStale,
                });
            }
            Err(ControllerError::AgentUnavailable(_)) => {
                return Ok(ReconcileAction {
                    requeue_after: Duration::from_secs(self.evaluation.wait_requeue_seconds),
                    kind: ReconcileKind::Waiting,
                });
            }
            Err(error) => return Err(error),
        };
        Ok(action(outcome))
    }
}

fn merge_watermarks(mut previous: Watermarks, observed: Watermarks) -> Watermarks {
    for (key, watermark) in observed {
        let replace = previous.get(&key).is_none_or(|existing| {
            existing.process_session_id != watermark.process_session_id
                || watermark.report_sequence > existing.report_sequence
        });
        if replace {
            previous.insert(key, watermark);
        }
    }
    previous
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileKind {
    Stable,
    Applied,
    Executed,
    Waiting,
    Unsafe,
    ObservationStale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileAction {
    pub requeue_after: Duration,
    pub kind: ReconcileKind,
}

fn action(outcome: ExecutionOutcome) -> ReconcileAction {
    ReconcileAction {
        requeue_after: outcome.requeue_after,
        kind: match outcome.kind {
            ExecutionKind::Stable => ReconcileKind::Stable,
            ExecutionKind::Applied => ReconcileKind::Applied,
            ExecutionKind::Executed => ReconcileKind::Executed,
            ExecutionKind::Waiting => ReconcileKind::Waiting,
            ExecutionKind::Unsafe => ReconcileKind::Unsafe,
        },
    }
}
