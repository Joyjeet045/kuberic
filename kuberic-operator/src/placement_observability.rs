//! Best-effort transition Events and scrape-time gauges from durable KubericSet status.
//!
//! No process-local counters are used: restarting the operator cannot reset totals or
//! count a replayed reconciliation twice. Missing observations are omitted, not zeros.

use std::{net::SocketAddr, time::Duration};

use axum::{
    Router,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::ListParams,
    runtime::events::{Event, EventType, Recorder},
};
use prometheus::{IntGaugeVec, Opts, Registry, TextEncoder};
use tokio::{net::TcpListener, time::timeout};
use tracing::{info, warn};

use crate::{
    crd::{DurableOperationKind, DurableOperationPhase, KubericSet},
    primary_placement::PrimaryBalancingPolicy,
};

const API_TIMEOUT: Duration = Duration::from_secs(10);
const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Bind before starting the controllers, so invalid configuration fails startup.
/// `KUBERIC_METRICS_BIND=disabled` turns off the unauthenticated HTTP listener.
pub async fn metrics_listener_from_env() -> std::io::Result<Option<TcpListener>> {
    let value = match std::env::var("KUBERIC_METRICS_BIND") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "0.0.0.0:8081".to_string(),
        Err(error) => return Err(std::io::Error::other(error)),
    };
    let Some(address) = metrics_bind_address(&value)? else {
        info!("placement metrics endpoint disabled");
        return Ok(None);
    };
    let listener = TcpListener::bind(address).await?;
    info!(address = %listener.local_addr()?, "serving placement metrics at /metrics");
    Ok(Some(listener))
}

fn metrics_bind_address(value: &str) -> std::io::Result<Option<SocketAddr>> {
    if value.eq_ignore_ascii_case("disabled") {
        return Ok(None);
    }
    value.parse().map(Some).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid KUBERIC_METRICS_BIND: {error}"),
        )
    })
}

pub async fn serve_metrics(client: Client, listener: Option<TcpListener>) -> std::io::Result<()> {
    match listener {
        Some(listener) => {
            axum::serve(
                listener,
                Router::new()
                    .route("/metrics", get(placement_metrics))
                    .with_state(client),
            )
            .await
        }
        None => std::future::pending().await,
    }
}

async fn list_sets(client: Client) -> Result<Vec<KubericSet>, kube::Error> {
    let api: Api<KubericSet> = Api::all(client);
    let mut params = ListParams::default().limit(500);
    let mut sets = Vec::new();
    loop {
        let page = api.list(&params).await?;
        sets.extend(page.items);
        match page.metadata.continue_.filter(|token| !token.is_empty()) {
            Some(token) => params = params.continue_token(&token),
            None => return Ok(sets),
        }
    }
}

async fn placement_metrics(State(client): State<Client>) -> Response {
    let sets = match timeout(API_TIMEOUT, list_sets(client)).await {
        Ok(Ok(sets)) => sets,
        Ok(Err(error)) => {
            warn!(%error, "placement metrics Kubernetes list failed");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Kubernetes placement status unavailable\n",
            )
                .into_response();
        }
        Err(error) => {
            warn!(%error, "placement metrics Kubernetes list timed out");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Kubernetes placement status unavailable\n",
            )
                .into_response();
        }
    };
    match encode_metrics(&sets, k8s_openapi::jiff::Timestamp::now().as_second()) {
        Ok(body) => ([(header::CONTENT_TYPE, METRICS_CONTENT_TYPE)], body).into_response(),
        Err(error) => {
            warn!(%error, "placement metrics encoding failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Placement metrics encoding failed\n",
            )
                .into_response()
        }
    }
}

fn gauge(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> Result<IntGaugeVec, prometheus::Error> {
    let metric = IntGaugeVec::new(Opts::new(name, help), labels)?;
    registry.register(Box::new(metric.clone()))?;
    Ok(metric)
}

/// Encode only the latest durable observations. Unknown optional values have no
/// sample; info/decision/outcome gauges have value 1 for their current labels.
pub fn encode_metrics(sets: &[KubericSet], now: i64) -> Result<String, prometheus::Error> {
    let registry = Registry::new();
    let identity = &["namespace", "set"];
    let available = gauge(
        &registry,
        "kuberic_placement_status_available",
        "Whether durable placement status exists (1 or 0).",
        identity,
    )?;
    let replicas = gauge(
        &registry,
        "kuberic_placement_replicas",
        "Last observed replica count in a topology domain.",
        &["namespace", "set", "domain"],
    )?;
    let missing = gauge(
        &registry,
        "kuberic_placement_missing_topology_replicas",
        "Last observed replicas without a usable topology domain.",
        identity,
    )?;
    let unschedulable = gauge(
        &registry,
        "kuberic_placement_unschedulable_replicas",
        "Last observed replicas with PodScheduled=False and reason Unschedulable.",
        identity,
    )?;
    let primary = gauge(
        &registry,
        "kuberic_placement_primary_info",
        "Current primary location, with value 1; empty domain means unknown.",
        &["namespace", "set", "node", "domain"],
    )?;
    let decision = gauge(
        &registry,
        "kuberic_placement_decision",
        "Current persisted balancing decision, with value 1; not an event counter.",
        &["namespace", "set", "reason"],
    )?;
    let scheduling_decision = gauge(
        &registry,
        "kuberic_placement_scheduling_decision",
        "Current persisted replica scheduling diagnostic, with value 1; omitted if absent.",
        &["namespace", "set", "reason"],
    )?;
    let improvement = gauge(
        &registry,
        "kuberic_placement_improvement",
        "Current candidate post-move domain density improvement.",
        identity,
    )?;
    let cooldown = gauge(
        &registry,
        "kuberic_placement_cooldown_remaining_seconds",
        "Remaining policy cooldown computed from the durable last rebalance timestamp.",
        identity,
    )?;
    let target = gauge(
        &registry,
        "kuberic_placement_target_info",
        "Desired candidate location, with value 1; not evidence of a completed move.",
        &["namespace", "set", "node", "domain"],
    )?;
    let last_rebalance = gauge(
        &registry,
        "kuberic_placement_last_rebalance_timestamp_seconds",
        "Persisted last rebalance Unix timestamp, not a count.",
        identity,
    )?;
    let outcome = gauge(
        &registry,
        "kuberic_placement_rebalance_outcome",
        "Latest retained rebalance outcome, with value 1; absent when evidence is unavailable.",
        &["namespace", "set", "reason"],
    )?;
    let topology_ready = gauge(
        &registry,
        "kuberic_placement_topology_ready",
        "ReplicaTopologyReady condition: True=1, False=0, Unknown=-1; omitted if missing.",
        identity,
    )?;
    let primary_balanced = gauge(
        &registry,
        "kuberic_placement_primary_balanced",
        "PrimaryBalanced condition: True=1, False=0, Unknown=-1; omitted if missing.",
        identity,
    )?;

    for set in sets {
        let namespace = set.namespace().unwrap_or_default();
        let name = set.name_any();
        let labels = &[namespace.as_str(), name.as_str()];
        let placement = set
            .status
            .as_ref()
            .and_then(|status| status.placement.as_ref());
        available
            .with_label_values(labels)
            .set(i64::from(placement.is_some()));
        if let Some(status) = &set.status {
            for (name, metric) in [
                ("ReplicaTopologyReady", &topology_ready),
                ("PrimaryBalanced", &primary_balanced),
            ] {
                if let Some(condition) = status.conditions.iter().find(|c| c.type_ == name) {
                    let value = match condition.status.as_str() {
                        "True" => 1,
                        "False" => 0,
                        _ => -1,
                    };
                    metric.with_label_values(labels).set(value);
                }
            }
        }
        let Some(placement) = placement else { continue };
        for (domain, count) in &placement.replica_domains {
            replicas
                .with_label_values(&[&namespace, &name, domain])
                .set(i64::from(*count));
        }
        missing
            .with_label_values(labels)
            .set(i64::from(placement.missing_topology_replicas));
        unschedulable
            .with_label_values(labels)
            .set(i64::from(placement.unschedulable_replicas));
        if let Some(node) = &placement.primary_node {
            primary
                .with_label_values(&[
                    &namespace,
                    &name,
                    node,
                    placement.primary_domain.as_deref().unwrap_or(""),
                ])
                .set(1);
        }
        decision
            .with_label_values(&[&namespace, &name, bounded_reason(&placement.reason)])
            .set(1);
        if let Some(reason) = placement.scheduling_reason.as_deref() {
            scheduling_decision
                .with_label_values(&[&namespace, &name, bounded_scheduling_reason(reason)])
                .set(1);
        }
        if let Some(value) = placement.improvement {
            improvement.with_label_values(labels).set(value);
        }
        if let Some(timestamp) = placement.last_rebalance_at {
            last_rebalance.with_label_values(labels).set(timestamp);
            let seconds = set.spec.primary_balancing.as_ref().map_or_else(
                || PrimaryBalancingPolicy::default().cooldown_seconds,
                |p| p.cooldown_seconds,
            );
            cooldown.with_label_values(labels).set(
                timestamp
                    .saturating_add(i64::from(seconds))
                    .saturating_sub(now)
                    .max(0),
            );
        }
        if let Some(node) = &placement.target_node {
            target
                .with_label_values(&[
                    &namespace,
                    &name,
                    node,
                    placement.target_domain.as_deref().unwrap_or(""),
                ])
                .set(1);
        }
        if let Some((_, reason)) = rebalance_outcome(set) {
            outcome
                .with_label_values(&[&namespace, &name, reason])
                .set(1);
        }
    }
    TextEncoder::new().encode_to_string(&registry.gather())
}

fn bounded_reason(reason: &str) -> &'static str {
    match reason {
        "Disabled" => "Disabled",
        "TieBreakOnly" => "TieBreakOnly",
        "InitialPlacement" => "InitialPlacement",
        "NoEligiblePrimary" => "NoEligiblePrimary",
        "FailoverPlacement" => "FailoverPlacement",
        "DensityUnavailable" => "DensityUnavailable",
        "MaintenanceExclusion" => "MaintenanceExclusion",
        "WaitingForReplicas" => "WaitingForReplicas",
        "ObservationUnavailable" => "ObservationUnavailable",
        "MissingTopology" => "MissingTopology",
        "ConflictingOperation" => "ConflictingOperation",
        "UnsafeCandidate" => "UnsafeCandidate",
        "InsufficientImprovement" => "InsufficientImprovement",
        "Cooldown" => "Cooldown",
        "Stabilizing" => "Stabilizing",
        "Scheduled" => "Scheduled",
        "Completed" => "Completed",
        "Compensated" => "Compensated",
        "Failed" => "Failed",
        "Poisoned" => "Poisoned",
        _ => "Unknown",
    }
}

fn bounded_scheduling_reason(reason: &str) -> &'static str {
    match reason {
        "RequiredTopologyViolation" => "RequiredTopologyViolation",
        "RequiredTopologyUnverified" => "RequiredTopologyUnverified",
        "Unschedulable" => "Unschedulable",
        "SchedulingPending" => "SchedulingPending",
        _ => "Unknown",
    }
}

fn rebalance_outcome(set: &KubericSet) -> Option<(&str, &'static str)> {
    let status = set.status.as_ref()?;
    let placement = status.placement.as_ref()?;
    let id = placement.operation_id.as_deref()?;
    let operation = status.operation.as_ref()?;
    if id != operation.operation_id || operation.kind != DurableOperationKind::Switchover {
        return None;
    }
    let reason = match operation.phase {
        DurableOperationPhase::Completed => {
            // A completed workflow may have compensated back to the old primary.
            match status
                .stable_snapshot
                .as_ref()
                .map(|snapshot| snapshot.primary_id)
            {
                Some(primary) if primary == operation.target_primary_id => "Completed",
                Some(primary) if primary == operation.old_primary_id => "Compensated",
                _ => "Unknown",
            }
        }
        DurableOperationPhase::Failed => "Failed",
        DurableOperationPhase::Poisoned => "Poisoned",
        _ => "InProgress",
    };
    Some((id, reason))
}

fn event(reason: &str, action: &str, note: String, warning: bool) -> Event {
    let mut note = note;
    if note.len() > 1024 {
        let mut boundary = 1024;
        while !note.is_char_boundary(boundary) {
            boundary -= 1;
        }
        note.truncate(boundary);
    }
    Event {
        type_: if warning {
            EventType::Warning
        } else {
            EventType::Normal
        },
        reason: reason.to_string(),
        action: action.to_string(),
        note: Some(note),
        secondary: None,
    }
}

/// Meaningful transitions only: no Events for message, progress, timer, resource
/// version, or unrelated condition refreshes. Both inputs must be durable snapshots.
pub fn placement_events(previous: &KubericSet, current: &KubericSet) -> Vec<Event> {
    let before = previous
        .status
        .as_ref()
        .and_then(|status| status.placement.as_ref());
    let Some(after) = current
        .status
        .as_ref()
        .and_then(|status| status.placement.as_ref())
    else {
        return Vec::new();
    };
    let mut events = Vec::new();
    let previous_scheduling = before
        .and_then(|placement| placement.scheduling_reason.as_deref())
        .map(bounded_scheduling_reason);
    let scheduling = after
        .scheduling_reason
        .as_deref()
        .map(bounded_scheduling_reason);
    if previous_scheduling != scheduling {
        let (reason, note, warning) = match scheduling {
            Some(reason) => (
                if reason == "Unknown" {
                    "SchedulingUnknown"
                } else {
                    reason
                },
                after
                    .scheduling_message
                    .clone()
                    .unwrap_or_else(|| format!("Replica scheduling diagnostic: {reason}")),
                matches!(
                    reason,
                    "RequiredTopologyViolation" | "Unschedulable" | "Unknown"
                ),
            ),
            None => (
                "ReplicaSchedulingDiagnosticCleared",
                "The previous replica scheduling diagnostic is no longer reported".to_string(),
                false,
            ),
        };
        events.push(event(reason, "EvaluateReplicaScheduling", note, warning));
    }
    if before.is_none_or(|before| {
        before.replica_domains != after.replica_domains
            || before.missing_topology_replicas != after.missing_topology_replicas
            || before.unschedulable_replicas != after.unschedulable_replicas
    }) {
        let reason = if after.unschedulable_replicas > 0 {
            "UnschedulableReplicas"
        } else if after.missing_topology_replicas > 0 {
            "MissingTopology"
        } else {
            "TopologyObserved"
        };
        events.push(event(
            reason,
            "ObserveReplicaPlacement",
            format!(
                "{} topology domains; {} replicas without topology; {} unschedulable replicas",
                after.replica_domains.len(),
                after.missing_topology_replicas,
                after.unschedulable_replicas,
            ),
            after.missing_topology_replicas > 0 || after.unschedulable_replicas > 0,
        ));
    }
    if after.primary_node.is_some()
        && before.is_none_or(|before| {
            before.primary_node != after.primary_node
                || before.primary_domain != after.primary_domain
        })
    {
        events.push(event(
            "PrimaryPlacementChanged",
            "ObservePrimaryPlacement",
            format!(
                "Primary node {}; domain {}",
                after.primary_node.as_deref().unwrap_or("unknown"),
                after.primary_domain.as_deref().unwrap_or("unknown")
            ),
            false,
        ));
    }
    let current_outcome = rebalance_outcome(current);
    if current_outcome != rebalance_outcome(previous) {
        if let Some((id, outcome)) = current_outcome {
            let reason = match outcome {
                "InProgress" => "PrimaryRebalanceStarted",
                "Completed" => "PrimaryRebalanceCompleted",
                "Compensated" => "PrimaryRebalanceCompensated",
                "Failed" => "PrimaryRebalanceFailed",
                "Poisoned" => "PrimaryRebalancePoisoned",
                _ => "PrimaryRebalanceOutcomeUnknown",
            };
            events.push(event(
                reason,
                "RebalancePrimary",
                format!("Durable rebalance {id}: {outcome}"),
                matches!(outcome, "Compensated" | "Failed" | "Poisoned" | "Unknown"),
            ));
        }
    }
    let reason = bounded_reason(&after.reason);
    let decision_changed = before.is_none_or(|before| {
        bounded_reason(&before.reason) != reason
            || before.target_pod != after.target_pod
            || before.target_node != after.target_node
            || before.target_domain != after.target_domain
    });
    if decision_changed
        && !matches!(
            reason,
            "Unknown" | "Scheduled" | "Completed" | "Compensated" | "Failed" | "Poisoned"
        )
    {
        let (event_reason, action) = match reason {
            "InitialPlacement"
            | "NoEligiblePrimary"
            | "FailoverPlacement"
            | "DensityUnavailable"
            | "MaintenanceExclusion" => (reason.to_string(), "EvaluatePrimaryPlacement"),
            _ => (
                format!("PrimaryBalancing{reason}"),
                "EvaluatePrimaryBalance",
            ),
        };
        events.push(event(
            &event_reason,
            action,
            after.message.clone(),
            matches!(
                reason,
                "ObservationUnavailable"
                    | "MissingTopology"
                    | "UnsafeCandidate"
                    | "NoEligiblePrimary"
                    | "DensityUnavailable"
            ),
        ));
    }
    events
}

/// Called only after successful reconciliation. Event read/publish failures never
/// fail reconciliation or roll back the status that made the transition durable.
pub async fn publish_persisted_placement_events(
    client: &Client,
    recorder: &Recorder,
    previous: &KubericSet,
) {
    let Some(namespace) = previous.namespace() else {
        warn!(set = %previous.name_any(), "cannot publish placement Events without namespace");
        return;
    };
    let name = previous.name_any();
    let api: Api<KubericSet> = Api::namespaced(client.clone(), &namespace);
    let latest = match timeout(API_TIMEOUT, api.get(&name)).await {
        Ok(Ok(latest)) => latest,
        Ok(Err(error)) => {
            warn!(%namespace, set = %name, %error, "cannot read durable placement status for Events");
            return;
        }
        Err(error) => {
            warn!(%namespace, set = %name, %error, "durable placement status read for Events timed out");
            return;
        }
    };
    if latest.metadata.uid != previous.metadata.uid {
        warn!(%namespace, set = %name, "skipping placement Events for a replaced KubericSet");
        return;
    }
    for event in placement_events(previous, &latest) {
        match timeout(
            API_TIMEOUT,
            recorder.publish(&event, &latest.object_ref(&())),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                warn!(%namespace, set = %name, reason = %event.reason, %error, "placement Event publication failed; durable status retained")
            }
            Err(error) => {
                warn!(%namespace, set = %name, reason = %event.reason, %error, "placement Event publication timed out; durable status retained")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{body::to_bytes, extract::Request};
    use kube::runtime::events::Reporter;
    use serde_json::json;

    use super::*;
    use crate::{
        crd::{DurableOperationStatus, KubericSetStatus, StatusCondition},
        primary_placement::PlacementStatus,
    };

    fn set() -> KubericSet {
        serde_json::from_value(json!({
            "apiVersion": "kuberic.io/v1",
            "kind": "KubericSet",
            "metadata": {
                "name": "example",
                "namespace": "default",
                "uid": "set-uid",
                "resourceVersion": "1"
            },
            "spec": { "image": "example:latest" }
        }))
        .unwrap()
    }

    fn placed_set() -> KubericSet {
        let mut set = set();
        set.status = Some(KubericSetStatus {
            placement: Some(PlacementStatus {
                primary_node: Some("node-a".into()),
                primary_domain: Some("zone-a".into()),
                replica_domains: [("zone-a".into(), 2), ("zone-b".into(), 1)].into(),
                missing_topology_replicas: 1,
                unschedulable_replicas: 1,
                reason: "Stabilizing".into(),
                message: "waiting for stable candidate".into(),
                target_pod: Some("example-2".into()),
                target_node: Some("node-b".into()),
                target_domain: Some("zone-b".into()),
                improvement: Some(2),
                candidate_since: Some(900),
                last_rebalance_at: Some(800),
                ..Default::default()
            }),
            ..Default::default()
        });
        set
    }

    fn placement_mut(set: &mut KubericSet) -> &mut PlacementStatus {
        set.status.as_mut().unwrap().placement.as_mut().unwrap()
    }

    fn operation(id: &str) -> DurableOperationStatus {
        serde_json::from_value(json!({
            "operationId": id,
            "executionId": "execution",
            "version": 1,
            "kind": "switchover",
            "phase": "revoke",
            "targetSnapshot": {
                "epoch": { "dataLossNumber": 0, "configurationNumber": 2 },
                "primaryId": 2,
                "members": [],
                "writeQuorum": 1
            },
            "oldPrimaryId": 1,
            "targetPrimaryId": 2,
            "phaseDeadlineUnixSeconds": 1000
        }))
        .unwrap()
    }

    fn metric<'a>(text: &'a str, name: &str) -> Option<&'a str> {
        text.lines()
            .find(|line| line.starts_with(&format!("kuberic_placement_{name}{{")))
    }

    #[test]
    fn exposition_has_gauges_and_bounded_labels() {
        let text = encode_metrics(&[placed_set()], 1000).unwrap();
        for (name, value) in [
            ("status_available", 1),
            ("missing_topology_replicas", 1),
            ("unschedulable_replicas", 1),
            ("primary_info", 1),
            ("decision", 1),
            ("improvement", 2),
            ("cooldown_remaining_seconds", 100),
            ("target_info", 1),
            ("last_rebalance_timestamp_seconds", 800),
        ] {
            assert!(
                metric(&text, name).unwrap().ends_with(&format!(" {value}")),
                "{name}: {text}"
            );
            assert!(text.contains(&format!("# TYPE kuberic_placement_{name} gauge")));
        }
        assert_eq!(
            text.lines()
                .filter(|line| line.starts_with("kuberic_placement_replicas{"))
                .count(),
            2
        );
        assert!(
            metric(&text, "decision")
                .unwrap()
                .contains("reason=\"Stabilizing\"")
        );
        assert!(!text.contains("example-2"));
        assert!(!text.contains("waiting for stable candidate"));
        assert!(!text.contains("operation_id"));
        assert!(
            text.lines()
                .filter(|line| line.starts_with("# TYPE "))
                .all(|line| line.ends_with(" gauge"))
        );
        assert!(metric(&text, "rebalance_outcome").is_none());
    }

    #[test]
    fn prometheus_encoder_escapes_label_values() {
        let mut set = placed_set();
        placement_mut(&mut set).replica_domains = [("zone\\\"a\nb".into(), 3)].into();
        let text = encode_metrics(&[set], 1000).unwrap();
        let line = metric(&text, "replicas").unwrap();
        assert!(line.contains(r#"domain="zone\\\"a\nb""#), "{line}");
        assert!(line.ends_with(" 3"));
    }

    #[test]
    fn missing_status_and_optional_observations_are_not_invented() {
        let text = encode_metrics(&[set()], 1000).unwrap();
        assert!(metric(&text, "status_available").unwrap().ends_with(" 0"));
        for name in [
            "missing_topology_replicas",
            "unschedulable_replicas",
            "decision",
            "primary_info",
            "target_info",
            "improvement",
            "cooldown_remaining_seconds",
            "last_rebalance_timestamp_seconds",
            "rebalance_outcome",
            "topology_ready",
            "primary_balanced",
        ] {
            assert!(metric(&text, name).is_none(), "{name}");
        }
        let mut set = set();
        set.status = Some(KubericSetStatus {
            placement: Some(PlacementStatus::default()),
            ..Default::default()
        });
        let text = encode_metrics(&[set], 1000).unwrap();
        assert!(metric(&text, "status_available").unwrap().ends_with(" 1"));
        assert!(metric(&text, "primary_info").is_none());
        assert!(metric(&text, "improvement").is_none());
        assert!(metric(&text, "target_info").is_none());
        assert!(metric(&text, "last_rebalance_timestamp_seconds").is_none());
        assert!(metric(&text, "cooldown_remaining_seconds").is_none());
    }

    fn condition(type_: &str, status: &str) -> StatusCondition {
        StatusCondition {
            type_: type_.into(),
            status: status.into(),
            reason: "Observed".into(),
            message: "observed condition".into(),
            last_transition_time: "2026-01-01T00:00:00Z".into(),
        }
    }

    #[test]
    fn conditions_distinguish_true_false_unknown_and_absent() {
        let mut set = placed_set();
        let text = encode_metrics(&[set.clone()], 1000).unwrap();
        assert!(metric(&text, "topology_ready").is_none());
        assert!(metric(&text, "primary_balanced").is_none());
        set.status.as_mut().unwrap().conditions = vec![
            condition("ReplicaTopologyReady", "False"),
            condition("PrimaryBalanced", "Unknown"),
        ];
        let text = encode_metrics(&[set.clone()], 1000).unwrap();
        assert!(metric(&text, "topology_ready").unwrap().ends_with(" 0"));
        assert!(metric(&text, "primary_balanced").unwrap().ends_with(" -1"));
        set.status.as_mut().unwrap().conditions[0].status = "True".into();
        let text = encode_metrics(&[set], 1000).unwrap();
        assert!(metric(&text, "topology_ready").unwrap().ends_with(" 1"));
    }

    #[test]
    fn stale_series_disappear_and_unknown_reasons_are_bounded() {
        let mut set = placed_set();
        let first = encode_metrics(&[set.clone()], 1000).unwrap();
        placement_mut(&mut set).reason = "unbounded error or operation ID".into();
        placement_mut(&mut set).target_node = None;
        placement_mut(&mut set).replica_domains.remove("zone-b");
        let second = encode_metrics(&[set], 1200).unwrap();
        assert!(metric(&first, "target_info").is_some());
        assert!(metric(&second, "target_info").is_none());
        assert!(
            metric(&second, "decision")
                .unwrap()
                .contains("reason=\"Unknown\"")
        );
        assert!(!second.contains("unbounded error"));
        assert!(
            metric(&second, "cooldown_remaining_seconds")
                .unwrap()
                .ends_with(" 0")
        );
        assert_eq!(
            second
                .lines()
                .filter(|line| line.starts_with("kuberic_placement_replicas{"))
                .count(),
            1
        );
        assert!(encode_metrics(&[], 1200).unwrap().is_empty());
    }

    #[test]
    fn status_only_updates_do_not_repeat_events() {
        let before = placed_set();
        let mut after = before.clone();
        after.metadata.resource_version = Some("2".into());
        after.status.as_mut().unwrap().ready_replicas += 1;
        after
            .status
            .as_mut()
            .unwrap()
            .conditions
            .push(condition("PrimaryBalanced", "False"));
        let placement = placement_mut(&mut after);
        placement.message = "new progress description".into();
        placement.improvement = Some(3);
        placement.candidate_since = Some(950);
        placement.last_rebalance_at = Some(850);
        assert!(placement_events(&before, &after).is_empty());
        assert!(placement_events(&after, &after).is_empty());
    }

    #[test]
    fn placement_and_suppression_transitions_emit_events() {
        let before = placed_set();
        let mut after = before.clone();
        let placement = placement_mut(&mut after);
        placement.unschedulable_replicas = 2;
        placement.reason = "MissingTopology".into();
        let events = placement_events(&before, &after);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].reason, "UnschedulableReplicas");
        assert!(matches!(events[0].type_, EventType::Warning));
        assert_eq!(events[1].reason, "PrimaryBalancingMissingTopology");
        assert!(placement_events(&after, &after).is_empty());
    }

    #[test]
    fn initial_and_failover_reasons_have_metrics_and_transition_events() {
        let before = placed_set();
        for (reason, warning) in [
            ("InitialPlacement", false),
            ("NoEligiblePrimary", true),
            ("FailoverPlacement", false),
            ("DensityUnavailable", true),
            ("MaintenanceExclusion", false),
        ] {
            let mut after = before.clone();
            let placement = placement_mut(&mut after);
            placement.reason = reason.into();
            placement.improvement = None;
            let text = encode_metrics(&[after.clone()], 1000).unwrap();
            assert!(
                metric(&text, "decision")
                    .unwrap()
                    .contains(&format!("reason=\"{reason}\""))
            );
            assert!(metric(&text, "improvement").is_none());
            let events = placement_events(&before, &after);
            assert_eq!(events.len(), 1, "{reason}");
            assert_eq!(events[0].reason, reason);
            assert_eq!(events[0].action, "EvaluatePrimaryPlacement");
            assert_eq!(matches!(events[0].type_, EventType::Warning), warning);
            assert!(placement_events(&after, &after).is_empty());
        }
    }

    #[test]
    fn independent_scheduling_diagnostic_transitions_have_metrics_and_events() {
        let mut previous = placed_set();
        let text = encode_metrics(&[previous.clone()], 1000).unwrap();
        assert!(metric(&text, "scheduling_decision").is_none());
        for (reason, readiness, value, warning) in [
            ("RequiredTopologyUnverified", "Unknown", -1, false),
            ("RequiredTopologyViolation", "False", 0, true),
            ("SchedulingPending", "Unknown", -1, false),
            ("Unschedulable", "False", 0, true),
        ] {
            let mut current = previous.clone();
            let placement = placement_mut(&mut current);
            placement.scheduling_reason = Some(reason.into());
            placement.scheduling_message = Some(format!("Replica scheduling: {reason}"));
            current.status.as_mut().unwrap().conditions =
                vec![condition("ReplicaTopologyReady", readiness)];
            let text = encode_metrics(&[current.clone()], 1000).unwrap();
            assert!(
                metric(&text, "scheduling_decision")
                    .unwrap()
                    .contains(&format!("reason=\"{reason}\""))
            );
            assert!(
                metric(&text, "decision")
                    .unwrap()
                    .contains("reason=\"Stabilizing\"")
            );
            assert!(
                metric(&text, "topology_ready")
                    .unwrap()
                    .ends_with(&format!(" {value}"))
            );
            let events = placement_events(&previous, &current);
            assert_eq!(events.len(), 1, "{reason}");
            assert_eq!(events[0].reason, reason);
            assert_eq!(events[0].action, "EvaluateReplicaScheduling");
            assert_eq!(matches!(events[0].type_, EventType::Warning), warning);
            let mut refreshed = current.clone();
            placement_mut(&mut refreshed).scheduling_message = Some("Message refresh only".into());
            assert!(placement_events(&current, &refreshed).is_empty());
            previous = current;
        }
        let mut cleared = previous.clone();
        placement_mut(&mut cleared).scheduling_reason = None;
        placement_mut(&mut cleared).scheduling_message = None;
        cleared.status.as_mut().unwrap().conditions =
            vec![condition("ReplicaTopologyReady", "True")];
        let text = encode_metrics(&[cleared.clone()], 1000).unwrap();
        assert!(metric(&text, "scheduling_decision").is_none());
        assert!(metric(&text, "topology_ready").unwrap().ends_with(" 1"));
        let events = placement_events(&previous, &cleared);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].reason, "ReplicaSchedulingDiagnosticCleared");
        assert!(matches!(events[0].type_, EventType::Normal));
        assert!(placement_events(&cleared, &cleared).is_empty());
    }

    #[test]
    fn arbitrary_pod_scheduling_reasons_never_become_metric_labels_or_event_reasons() {
        let previous = placed_set();
        let mut current = previous.clone();
        placement_mut(&mut current).scheduling_reason = Some("CustomSchedulerReason-123".into());
        placement_mut(&mut current).scheduling_message =
            Some("arbitrary PodScheduled message".into());
        let text = encode_metrics(&[current.clone()], 1000).unwrap();
        assert!(
            metric(&text, "scheduling_decision")
                .unwrap()
                .contains("reason=\"Unknown\"")
        );
        assert!(!text.contains("CustomSchedulerReason-123"));
        assert!(!text.contains("arbitrary PodScheduled message"));
        let events = placement_events(&previous, &current);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].reason, "SchedulingUnknown");
        assert!(matches!(events[0].type_, EventType::Warning));
        let mut refreshed = current.clone();
        placement_mut(&mut refreshed).scheduling_reason = Some("CustomSchedulerReason-456".into());
        assert!(placement_events(&current, &refreshed).is_empty());
    }

    #[test]
    fn new_candidates_and_primary_locations_are_meaningful() {
        let before = placed_set();
        let mut after = before.clone();
        placement_mut(&mut after).target_node = Some("node-c".into());
        assert_eq!(
            placement_events(&before, &after)[0].reason,
            "PrimaryBalancingStabilizing"
        );
        let before = after.clone();
        placement_mut(&mut after).primary_node = Some("node-b".into());
        assert_eq!(
            placement_events(&before, &after)[0].reason,
            "PrimaryPlacementChanged"
        );
    }

    #[test]
    fn rebalance_events_ignore_phase_progress_but_report_terminal_outcomes() {
        let before = placed_set();
        let mut started = before.clone();
        placement_mut(&mut started).operation_id = Some("operation-1".into());
        placement_mut(&mut started).reason = "Scheduled".into();
        started.status.as_mut().unwrap().operation = Some(operation("operation-1"));
        let events = placement_events(&before, &started);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].reason, "PrimaryRebalanceStarted");
        let mut progressing = started.clone();
        progressing
            .status
            .as_mut()
            .unwrap()
            .operation
            .as_mut()
            .unwrap()
            .phase = DurableOperationPhase::PreCatchUp;
        assert!(placement_events(&started, &progressing).is_empty());
        let mut completed = progressing.clone();
        let status = completed.status.as_mut().unwrap();
        status.operation.as_mut().unwrap().phase = DurableOperationPhase::Completed;
        status.stable_snapshot = Some(status.operation.as_ref().unwrap().target_snapshot.clone());
        status.placement.as_mut().unwrap().reason = "Completed".into();
        let events = placement_events(&progressing, &completed);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].reason, "PrimaryRebalanceCompleted");
        assert!(placement_events(&completed, &completed).is_empty());
        let mut cooldown = completed.clone();
        placement_mut(&mut cooldown).reason = "Cooldown".into();
        let text = encode_metrics(&[cooldown], 1000).unwrap();
        assert!(
            metric(&text, "rebalance_outcome")
                .unwrap()
                .contains("reason=\"Completed\"")
        );
        assert!(!text.contains("operation-1"));

        for (phase, expected) in [
            (DurableOperationPhase::Failed, "PrimaryRebalanceFailed"),
            (DurableOperationPhase::Poisoned, "PrimaryRebalancePoisoned"),
        ] {
            let mut terminal = progressing.clone();
            terminal
                .status
                .as_mut()
                .unwrap()
                .operation
                .as_mut()
                .unwrap()
                .phase = phase;
            assert_eq!(
                placement_events(&progressing, &terminal)[0].reason,
                expected
            );
        }
        completed
            .status
            .as_mut()
            .unwrap()
            .stable_snapshot
            .as_mut()
            .unwrap()
            .primary_id = 1;
        assert_eq!(
            placement_events(&progressing, &completed)[0].reason,
            "PrimaryRebalanceCompensated"
        );
        completed
            .status
            .as_mut()
            .unwrap()
            .operation
            .as_mut()
            .unwrap()
            .operation_id = "unrelated".into();
        assert!(rebalance_outcome(&completed).is_none());
    }

    #[test]
    fn event_notes_are_utf8_and_within_kubernetes_limit() {
        let event = event("Test", "Test", "界".repeat(500), false);
        assert_eq!(event.note.unwrap().len(), 1023);
    }

    #[test]
    fn bind_configuration_is_explicit() {
        assert!(metrics_bind_address("disabled").unwrap().is_none());
        assert!(metrics_bind_address("DISABLED").unwrap().is_none());
        assert_eq!(
            metrics_bind_address("127.0.0.1:8081")
                .unwrap()
                .unwrap()
                .port(),
            8081
        );
        assert!(metrics_bind_address("").is_err());
        assert!(metrics_bind_address("0.0.0.0:not-a-port").is_err());
    }

    async fn mock_client(router: Router) -> (Client, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = kube::Config::new(
            format!("http://{}", listener.local_addr().unwrap())
                .parse()
                .unwrap(),
        );
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (Client::try_from(config).unwrap(), task)
    }

    fn json_response(value: serde_json::Value) -> Response {
        (
            [(header::CONTENT_TYPE, "application/json")],
            value.to_string(),
        )
            .into_response()
    }

    fn forbidden() -> Response {
        (
            StatusCode::FORBIDDEN,
            [(header::CONTENT_TYPE, "application/json")],
            json!({
                "apiVersion": "v1", "kind": "Status", "status": "Failure",
                "reason": "Forbidden", "message": "not permitted", "code": 403
            })
            .to_string(),
        )
            .into_response()
    }

    #[tokio::test]
    async fn list_failure_returns_503_instead_of_empty_success() {
        let (client, task) = mock_client(Router::new().fallback(|| async { forbidden() })).await;
        let response = placement_metrics(State(client)).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(&body[..], b"Kubernetes placement status unavailable\n");
        task.abort();
    }

    #[tokio::test]
    async fn scrape_reads_all_pages_of_durable_status() {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let router = Router::new().fallback(move |request: Request| {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                let continued = request
                    .uri()
                    .query()
                    .unwrap_or("")
                    .contains("continue=next");
                let mut set = placed_set();
                if continued {
                    set.metadata.name = Some("second".into());
                }
                json_response(json!({
                    "apiVersion": "kuberic.io/v1", "kind": "KubericSetList",
                    "metadata": { "continue": if continued { "" } else { "next" } },
                    "items": [set]
                }))
            }
        });
        let (client, task) = mock_client(router).await;
        let response = placement_metrics(State(client)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            METRICS_CONTENT_TYPE
        );
        let body = to_bytes(response.into_body(), 65536).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        assert!(text.contains("set=\"example\""));
        assert!(text.contains("set=\"second\""));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        task.abort();
    }

    #[tokio::test]
    async fn failed_later_page_does_not_publish_partial_metrics() {
        let router = Router::new().fallback(|request: Request| async move {
            if request
                .uri()
                .query()
                .unwrap_or("")
                .contains("continue=next")
            {
                return forbidden();
            }
            json_response(json!({
                "apiVersion": "kuberic.io/v1", "kind": "KubericSetList",
                "metadata": { "continue": "next" }, "items": [placed_set()]
            }))
        });
        let (client, task) = mock_client(router).await;
        let response = placement_metrics(State(client)).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        assert!(
            !std::str::from_utf8(&body)
                .unwrap()
                .contains("kuberic_placement_")
        );
        task.abort();
    }

    #[tokio::test]
    async fn axum_metrics_listener_serves_http() {
        let (client, api_task) = mock_client(Router::new().fallback(|| async {
            json_response(json!({
                "apiVersion": "kuberic.io/v1", "kind": "KubericSetList",
                "metadata": {}, "items": [placed_set()]
            }))
        }))
        .await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = Client::try_from(kube::Config::new(
            format!("http://{}", listener.local_addr().unwrap())
                .parse()
                .unwrap(),
        ))
        .unwrap();
        let metrics_task = tokio::spawn(serve_metrics(client, Some(listener)));
        let request = axum::http::Request::get("/metrics")
            .body(Vec::new())
            .unwrap();
        let text = http.request_text(request).await.unwrap();
        assert!(metric(&text, "status_available").unwrap().ends_with(" 1"));
        let request = axum::http::Request::get("/other").body(Vec::new()).unwrap();
        assert!(http.request_text(request).await.is_err());
        metrics_task.abort();
        api_task.abort();
    }

    #[tokio::test]
    async fn publication_reads_persisted_status_and_write_failures_do_not_propagate() {
        let previous = placed_set();
        let mut persisted = previous.clone();
        placement_mut(&mut persisted).reason = "Cooldown".into();
        let writes = Arc::new(AtomicUsize::new(0));
        let seen = writes.clone();
        let router = Router::new().fallback(move |request: Request| {
            let seen = seen.clone();
            let persisted = persisted.clone();
            async move {
                if request.method() == axum::http::Method::GET {
                    json_response(serde_json::to_value(persisted).unwrap())
                } else {
                    seen.fetch_add(1, Ordering::SeqCst);
                    forbidden()
                }
            }
        });
        let (client, task) = mock_client(router).await;
        let recorder = Recorder::new(
            client.clone(),
            Reporter {
                controller: "kuberic-operator".into(),
                instance: None,
            },
        );
        publish_persisted_placement_events(&client, &recorder, &previous).await;
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        assert_eq!(
            previous.status.unwrap().placement.unwrap().reason,
            "Stabilizing"
        );
        task.abort();
    }

    #[tokio::test]
    async fn unchanged_persisted_status_does_not_publish_events() {
        let previous = placed_set();
        let persisted = previous.clone();
        let writes = Arc::new(AtomicUsize::new(0));
        let seen = writes.clone();
        let router = Router::new().fallback(move |request: Request| {
            let persisted = persisted.clone();
            let seen = seen.clone();
            async move {
                if request.method() != axum::http::Method::GET {
                    seen.fetch_add(1, Ordering::SeqCst);
                    return forbidden();
                }
                json_response(serde_json::to_value(persisted).unwrap())
            }
        });
        let (client, task) = mock_client(router).await;
        let recorder = Recorder::new(
            client.clone(),
            Reporter {
                controller: "kuberic-operator".into(),
                instance: None,
            },
        );
        publish_persisted_placement_events(&client, &recorder, &previous).await;
        assert_eq!(writes.load(Ordering::SeqCst), 0);
        task.abort();
    }
}
