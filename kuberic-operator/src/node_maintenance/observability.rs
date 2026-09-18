use axum::{
    Router,
    extract::State,
    http::{StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use k8s_openapi::{api::core::v1::ObjectReference, jiff::Timestamp};
use kube::runtime::events::{Event, EventType};
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder,
};

use super::api::{
    MaintenanceDesiredState, MaintenancePhase, NodeMaintenanceRequestSpec,
    NodeMaintenanceRequestStatus,
};
use super::controller::ReconcileOutcome;

#[derive(Clone)]
pub struct MaintenanceMetrics {
    registry: Registry,
    transitions: IntCounterVec,
    errors: IntCounterVec,
    preparation_seconds: HistogramVec,
    release_seconds: HistogramVec,
}

impl MaintenanceMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new_custom(Some("kuberic_node_maintenance".to_string()), None)?;
        let transitions = IntCounterVec::new(
            Opts::new(
                "status_transitions_total",
                "Persisted maintenance phase or blocked-reason transitions",
            ),
            &["operation", "phase", "reason"],
        )?;
        let errors = IntCounterVec::new(
            Opts::new(
                "errors_total",
                "Maintenance reconcile or Event publication errors",
            ),
            &["stage"],
        )?;
        let buckets = vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 900.0, 3600.0];
        let preparation_seconds = HistogramVec::new(
            HistogramOpts::new(
                "preparation_duration_seconds",
                "Elapsed time from durable preparation start when Prepared is reached",
            )
            .buckets(buckets.clone()),
            &["operation"],
        )?;
        let release_seconds = HistogramVec::new(
            HistogramOpts::new(
                "release_duration_seconds",
                "Elapsed time from durable release start until Released",
            )
            .buckets(buckets),
            &["operation"],
        )?;
        registry.register(Box::new(transitions.clone()))?;
        registry.register(Box::new(errors.clone()))?;
        registry.register(Box::new(preparation_seconds.clone()))?;
        registry.register(Box::new(release_seconds.clone()))?;
        Ok(Self {
            registry,
            transitions,
            errors,
            preparation_seconds,
            release_seconds,
        })
    }

    pub fn observe(
        &self,
        spec: &NodeMaintenanceRequestSpec,
        previous: &NodeMaintenanceRequestStatus,
        outcome: &ReconcileOutcome,
    ) -> Option<Event> {
        let status = &outcome.status;
        if !outcome.persisted
            || (previous.phase == status.phase && previous.blocked_reason == status.blocked_reason)
        {
            return None;
        }
        let operation = format!("{:?}", spec.operation);
        let phase = format!("{:?}", status.phase);
        let reason = status
            .blocked_reason
            .map(|reason| format!("{reason:?}"))
            .unwrap_or_else(|| "None".to_string());
        self.transitions
            .with_label_values(&[&operation, &phase, &reason])
            .inc();
        if status.phase == MaintenancePhase::Prepared {
            if let Some(seconds) = elapsed(&status.preparation_started_at, &status.prepared_at) {
                self.preparation_seconds
                    .with_label_values(&[&operation])
                    .observe(seconds);
            }
        }
        if status.phase == MaintenancePhase::Released {
            if let Some(seconds) = elapsed(&status.release_started_at, &status.released_at) {
                self.release_seconds
                    .with_label_values(&[&operation])
                    .observe(seconds);
            }
        }
        let warning = status.phase.requires_reason() || status.blocked_reason.is_some();
        let reason = if status.phase == MaintenancePhase::Released {
            if status.observed_desired_state == Some(MaintenanceDesiredState::Cancel) {
                "Cancelled"
            } else {
                "Completed"
            }
            .to_string()
        } else if status.blocked_reason.is_some() {
            reason
        } else {
            phase
        };
        let note = status.message.as_ref().map(|message| {
            let mut end = message.len().min(1024);
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            message[..end].to_string()
        });
        Some(Event {
            type_: if warning {
                EventType::Warning
            } else {
                EventType::Normal
            },
            reason,
            action: if matches!(
                status.phase,
                MaintenancePhase::Releasing | MaintenancePhase::Released
            ) {
                "Release"
            } else {
                "Prepare"
            }
            .to_string(),
            note,
            secondary: Some(ObjectReference {
                api_version: Some("v1".to_string()),
                kind: Some("Node".to_string()),
                name: Some(spec.node_name.clone()),
                uid: status
                    .released_node_uid
                    .clone()
                    .or_else(|| status.node_uid.clone()),
                ..Default::default()
            }),
        })
    }

    pub fn reconcile_error(&self) {
        self.errors.with_label_values(&["reconcile"]).inc();
    }

    pub fn event_error(&self) {
        self.errors.with_label_values(&["event"]).inc();
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/metrics", get(scrape))
            .with_state(self.clone())
    }
}

fn elapsed(start: &Option<String>, end: &Option<String>) -> Option<f64> {
    let start: Timestamp = start.as_deref()?.parse().ok()?;
    let end: Timestamp = end.as_deref()?.parse().ok()?;
    let nanoseconds = end.as_nanosecond() - start.as_nanosecond();
    (nanoseconds >= 0).then_some(nanoseconds as f64 / 1_000_000_000.0)
}

async fn scrape(
    State(metrics): State<MaintenanceMetrics>,
) -> Result<impl IntoResponse, StatusCode> {
    let encoder = TextEncoder::new();
    let mut body = Vec::new();
    encoder
        .encode(&metrics.registry.gather(), &mut body)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok((
        [(header::CONTENT_TYPE, encoder.format_type().to_string())],
        body,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_maintenance::MaintenanceBlockedReason;
    use serde_json::json;

    fn spec() -> NodeMaintenanceRequestSpec {
        serde_json::from_value(json!({"nodeName": "worker-04", "operation": "Reboot"})).unwrap()
    }

    fn outcome(phase: MaintenancePhase) -> ReconcileOutcome {
        ReconcileOutcome {
            status: NodeMaintenanceRequestStatus {
                phase,
                ..Default::default()
            },
            persisted: true,
        }
    }

    #[test]
    fn only_persisted_meaningful_transitions_emit_events_and_metrics() {
        let metrics = MaintenanceMetrics::new().unwrap();
        let previous = NodeMaintenanceRequestStatus::default();
        let mut prepared = outcome(MaintenancePhase::Prepared);
        prepared.persisted = false;
        assert!(metrics.observe(&spec(), &previous, &prepared).is_none());
        prepared.persisted = true;
        let event = metrics.observe(&spec(), &previous, &prepared).unwrap();
        assert_eq!(event.type_, EventType::Normal);
        assert_eq!(event.reason, "Prepared");
        assert!(
            metrics
                .observe(&spec(), &prepared.status, &prepared)
                .is_none()
        );
        assert_eq!(
            metrics
                .transitions
                .with_label_values(&["Reboot", "Prepared", "None"])
                .get(),
            1
        );
    }

    #[test]
    fn blocked_reason_changes_are_reported_with_bounded_labels() {
        let metrics = MaintenanceMetrics::new().unwrap();
        let mut blocked = outcome(MaintenancePhase::Blocked);
        blocked.status.blocked_reason = Some(MaintenanceBlockedReason::BlockedByQuorum);
        let event = metrics
            .observe(&spec(), &NodeMaintenanceRequestStatus::default(), &blocked)
            .unwrap();
        assert_eq!(event.type_, EventType::Warning);
        assert_eq!(event.reason, "BlockedByQuorum");
        assert_eq!(
            metrics
                .transitions
                .with_label_values(&["Reboot", "Blocked", "BlockedByQuorum"])
                .get(),
            1
        );
        let previous = blocked.status.clone();
        blocked.status.blocked_reason = Some(MaintenanceBlockedReason::NoEligibleTarget);
        assert!(metrics.observe(&spec(), &previous, &blocked).is_some());
    }

    #[test]
    fn latency_uses_persisted_timestamps_after_restart() {
        let metrics = MaintenanceMetrics::new().unwrap();
        let mut prepared = outcome(MaintenancePhase::Prepared);
        prepared.status.preparation_started_at = Some("2026-09-18T10:00:00Z".to_string());
        prepared.status.prepared_at = Some("2026-09-18T10:00:10Z".to_string());
        metrics.observe(&spec(), &NodeMaintenanceRequestStatus::default(), &prepared);
        assert_eq!(
            metrics
                .preparation_seconds
                .with_label_values(&["Reboot"])
                .get_sample_sum(),
            10.0
        );
        let mut released = outcome(MaintenancePhase::Released);
        released.status.release_started_at = Some("2026-09-18T10:01:00Z".to_string());
        released.status.released_at = Some("2026-09-18T10:01:05Z".to_string());
        released.status.observed_desired_state = Some(MaintenanceDesiredState::Cancel);
        released.status.released_node_uid = Some("replacement-uid".to_string());
        let event = metrics
            .observe(&spec(), &prepared.status, &released)
            .unwrap();
        assert_eq!(event.reason, "Cancelled");
        assert_eq!(
            event.secondary.unwrap().uid.as_deref(),
            Some("replacement-uid")
        );
        assert_eq!(
            metrics
                .release_seconds
                .with_label_values(&["Reboot"])
                .get_sample_sum(),
            5.0
        );
    }

    #[test]
    fn event_notes_respect_the_api_byte_limit_and_invalid_latency_is_ignored() {
        let metrics = MaintenanceMetrics::new().unwrap();
        let mut blocked = outcome(MaintenancePhase::Blocked);
        blocked.status.message = Some("\u{e9}".repeat(800));
        let event = metrics
            .observe(&spec(), &NodeMaintenanceRequestStatus::default(), &blocked)
            .unwrap();
        assert_eq!(event.note.unwrap().len(), 1024);
        assert!(elapsed(&None, &None).is_none());
        assert!(elapsed(&Some("invalid".to_string()), &Some("invalid".to_string())).is_none());
        assert!(
            elapsed(
                &Some("2026-09-18T10:00:10Z".to_string()),
                &Some("2026-09-18T10:00:00Z".to_string())
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn metrics_are_exposed_in_prometheus_text_format() {
        let metrics = MaintenanceMetrics::new().unwrap();
        metrics.reconcile_error();
        metrics.event_error();
        let response = scrape(State(metrics)).await.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers()[header::CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("text/plain")
        );
        let body = axum::body::to_bytes(response.into_body(), 1_048_576)
            .await
            .unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        assert!(text.contains("kuberic_node_maintenance_errors_total{stage=\"reconcile\"} 1"));
        assert!(text.contains("kuberic_node_maintenance_errors_total{stage=\"event\"} 1"));
        assert!(!text.contains("worker-04"));
    }
}
