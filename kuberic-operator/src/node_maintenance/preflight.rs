use k8s_openapi::jiff::Timestamp;

use super::api::{
    MaintenanceBlockedReason, MaintenancePhase, NodeMaintenanceRequestSpec,
    NodeMaintenanceRequestStatus,
};
use super::discovery::finish;

pub enum Preflight {
    Settled(NodeMaintenanceRequestStatus),
    Release(NodeMaintenanceRequestStatus),
    Discover,
}

pub fn preflight(
    spec: &NodeMaintenanceRequestSpec,
    generation: Option<i64>,
    previous: &NodeMaintenanceRequestStatus,
    now: Timestamp,
) -> Preflight {
    let mut status = previous.clone();
    status.observed_generation = generation;
    status.observed_desired_state = Some(spec.desired_state);

    if previous.phase == MaintenancePhase::Released {
        return Preflight::Settled(status);
    }

    if spec.desired_state.releases_request() {
        if previous.phase == MaintenancePhase::Releasing {
            return Preflight::Release(status);
        }
        status
            .release_started_at
            .get_or_insert_with(|| now.to_string());
        return Preflight::Settled(finish(
            status,
            MaintenancePhase::Releasing,
            None,
            Some(format!(
                "{:?} requested; waiting for safe placement release",
                spec.desired_state
            )),
            now,
        ));
    }

    if previous.phase.is_terminal() || previous.phase == MaintenancePhase::Releasing {
        return Preflight::Settled(status);
    }

    let not_before = match parse_optional(spec.not_before.as_deref()) {
        Ok(value) => value,
        Err(reported) => {
            return Preflight::Settled(finish(
                status,
                MaintenancePhase::Blocked,
                Some(MaintenanceBlockedReason::InvalidNotBefore),
                Some(format!(
                    "notBefore is not a valid RFC 3339 timestamp: {reported}"
                )),
                now,
            ));
        }
    };

    let deadline = match parse_optional(spec.deadline.as_deref()) {
        Ok(value) => value,
        Err(reported) => {
            return Preflight::Settled(finish(
                status,
                MaintenancePhase::Blocked,
                Some(MaintenanceBlockedReason::InvalidDeadline),
                Some(format!(
                    "deadline is not a valid RFC 3339 timestamp: {reported}"
                )),
                now,
            ));
        }
    };

    if deadline.is_some_and(|deadline| now > deadline) && !previous.phase.is_safe_to_drain() {
        return Preflight::Settled(finish(
            status,
            MaintenancePhase::Expired,
            Some(MaintenanceBlockedReason::DeadlineExceeded),
            Some("deadline exceeded before preparation completed".to_string()),
            now,
        ));
    }

    if let Some(not_before) = not_before
        && now < not_before
    {
        return Preflight::Settled(finish(
            status,
            MaintenancePhase::Requested,
            None,
            Some(format!("waiting until {not_before}")),
            now,
        ));
    }

    Preflight::Discover
}

fn parse_optional(value: Option<&str>) -> Result<Option<Timestamp>, String> {
    match value {
        None => Ok(None),
        Some(raw) => raw.parse::<Timestamp>().map(Some).map_err(|_| quote(raw)),
    }
}

fn quote(raw: &str) -> String {
    const LIMIT: usize = 64;
    let mut end = raw.len().min(LIMIT);
    while !raw.is_char_boundary(end) {
        end -= 1;
    }
    if end == raw.len() {
        format!("{raw:?}")
    } else {
        format!("{:?}...", &raw[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_maintenance::api::{MaintenanceDesiredState, MaintenanceOperation};

    fn at(text: &str) -> Timestamp {
        text.parse().expect("timestamp")
    }

    const NOW: &str = "2026-09-06T20:00:00Z";

    fn now() -> Timestamp {
        at(NOW)
    }

    fn spec() -> NodeMaintenanceRequestSpec {
        NodeMaintenanceRequestSpec {
            node_name: "worker-04".to_string(),
            operation: MaintenanceOperation::Reboot,
            desired_state: MaintenanceDesiredState::Prepare,
            provider: Some("Manual".to_string()),
            provider_event_id: Some("event-123".to_string()),
            not_before: None,
            deadline: None,
            release_node_uid: None,
        }
    }

    fn settled(result: Preflight) -> NodeMaintenanceRequestStatus {
        match result {
            Preflight::Settled(status) => status,
            Preflight::Discover | Preflight::Release(_) => panic!("expected a settled decision"),
        }
    }

    fn assert_discovers(result: Preflight) {
        assert!(
            matches!(result, Preflight::Discover),
            "expected discovery to be required"
        );
    }

    #[test]
    fn an_open_window_requires_discovery() {
        let mut spec = spec();
        spec.not_before = Some("2026-09-06T19:00:00Z".to_string());
        spec.deadline = Some("2026-09-06T21:00:00Z".to_string());
        assert_discovers(preflight(
            &spec,
            Some(1),
            &NodeMaintenanceRequestStatus::default(),
            now(),
        ));
    }

    #[test]
    fn absent_timestamps_require_discovery() {
        assert_discovers(preflight(
            &spec(),
            Some(1),
            &NodeMaintenanceRequestStatus::default(),
            now(),
        ));
    }

    #[test]
    fn a_malformed_not_before_blocks_instead_of_failing_open() {
        let mut spec = spec();
        spec.not_before = Some("tomorrow".to_string());
        let status = settled(preflight(
            &spec,
            Some(1),
            &NodeMaintenanceRequestStatus::default(),
            now(),
        ));
        assert_eq!(status.phase, MaintenancePhase::Blocked);
        assert_eq!(
            status.blocked_reason,
            Some(MaintenanceBlockedReason::InvalidNotBefore)
        );
        assert!(status.message.unwrap().contains("tomorrow"));
    }

    #[test]
    fn a_malformed_deadline_blocks_instead_of_failing_open() {
        let mut spec = spec();
        spec.deadline = Some("2026-13-45T99:00:00Z".to_string());
        let status = settled(preflight(
            &spec,
            Some(1),
            &NodeMaintenanceRequestStatus::default(),
            now(),
        ));
        assert_eq!(status.phase, MaintenancePhase::Blocked);
        assert_eq!(
            status.blocked_reason,
            Some(MaintenanceBlockedReason::InvalidDeadline)
        );
    }

    #[test]
    fn a_malformed_not_before_is_reported_before_the_deadline() {
        let mut spec = spec();
        spec.not_before = Some("not-a-time".to_string());
        spec.deadline = Some("also-not-a-time".to_string());
        let status = settled(preflight(
            &spec,
            Some(1),
            &NodeMaintenanceRequestStatus::default(),
            now(),
        ));
        assert_eq!(
            status.blocked_reason,
            Some(MaintenanceBlockedReason::InvalidNotBefore)
        );
    }

    #[test]
    fn an_oversized_malformed_value_is_not_echoed_in_full() {
        let mut spec = spec();
        spec.deadline = Some("x".repeat(4096));
        let status = settled(preflight(
            &spec,
            Some(1),
            &NodeMaintenanceRequestStatus::default(),
            now(),
        ));
        let message = status.message.expect("message");
        assert!(message.len() < 200);
        assert!(message.ends_with("\"..."));
    }

    #[test]
    fn an_exceeded_deadline_expires_without_discovery() {
        let mut spec = spec();
        spec.deadline = Some("2026-09-06T19:00:00Z".to_string());
        let status = settled(preflight(
            &spec,
            Some(1),
            &NodeMaintenanceRequestStatus::default(),
            now(),
        ));
        assert_eq!(status.phase, MaintenancePhase::Expired);
        assert_eq!(
            status.blocked_reason,
            Some(MaintenanceBlockedReason::DeadlineExceeded)
        );
    }

    #[test]
    fn an_unreached_not_before_waits_without_discovery() {
        let mut spec = spec();
        spec.not_before = Some("2026-09-06T23:00:00Z".to_string());
        let status = settled(preflight(
            &spec,
            Some(1),
            &NodeMaintenanceRequestStatus::default(),
            now(),
        ));
        assert_eq!(status.phase, MaintenancePhase::Requested);
        assert!(status.affected_sets.is_empty());
        assert!(status.node_uid.is_none());
    }

    #[test]
    fn an_elapsed_window_expires_even_before_not_before() {
        let mut spec = spec();
        spec.not_before = Some("2026-09-06T23:00:00Z".to_string());
        spec.deadline = Some("2026-09-06T19:00:00Z".to_string());
        let status = settled(preflight(
            &spec,
            Some(1),
            &NodeMaintenanceRequestStatus::default(),
            now(),
        ));
        assert_eq!(status.phase, MaintenancePhase::Expired);
    }

    #[test]
    fn the_window_is_open_on_both_boundaries() {
        let mut spec = spec();
        spec.not_before = Some(NOW.to_string());
        spec.deadline = Some(NOW.to_string());
        assert_discovers(preflight(
            &spec,
            Some(1),
            &NodeMaintenanceRequestStatus::default(),
            now(),
        ));
    }

    #[test]
    fn an_expired_request_can_be_explicitly_released() {
        let mut spec = spec();
        spec.desired_state = MaintenanceDesiredState::Complete;
        let previous = NodeMaintenanceRequestStatus {
            phase: MaintenancePhase::Expired,
            blocked_reason: Some(MaintenanceBlockedReason::DeadlineExceeded),
            message: Some("deadline exceeded before preparation completed".to_string()),
            ..Default::default()
        };
        let status = settled(preflight(&spec, Some(2), &previous, now()));

        assert_eq!(status.phase, MaintenancePhase::Releasing);
        assert!(status.phase.excludes_primary_placement());
        assert_eq!(status.release_started_at.as_deref(), Some(NOW));
        assert_eq!(status.observed_generation, Some(2));
        assert_eq!(
            status.observed_desired_state,
            Some(MaintenanceDesiredState::Complete)
        );
    }

    #[test]
    fn a_release_revokes_prepared_before_checking_restoration() {
        for desired in [
            MaintenanceDesiredState::Complete,
            MaintenanceDesiredState::Cancel,
        ] {
            let mut spec = spec();
            spec.desired_state = desired;
            let previous = NodeMaintenanceRequestStatus {
                phase: MaintenancePhase::Prepared,
                prepared_at: Some(NOW.to_string()),
                message: Some("discovered 1 affected set(s)".to_string()),
                ..Default::default()
            };
            let status = settled(preflight(&spec, Some(1), &previous, now()));

            assert_eq!(status.phase, MaintenancePhase::Releasing, "{desired:?}");
            assert!(status.phase.excludes_primary_placement());
            assert!(status.prepared_at.is_none());
            assert_eq!(status.conditions[0].status, "False");
            assert_eq!(status.observed_desired_state, Some(desired));
            assert!(matches!(
                preflight(&spec, Some(1), &status, now()),
                Preflight::Release(_)
            ));
        }
    }

    #[test]
    fn cancellation_can_release_a_blocked_request() {
        let mut spec = spec();
        spec.desired_state = MaintenanceDesiredState::Cancel;
        let previous = NodeMaintenanceRequestStatus {
            phase: MaintenancePhase::Blocked,
            blocked_reason: Some(MaintenanceBlockedReason::NodeNotFound),
            ..Default::default()
        };
        let status = settled(preflight(&spec, Some(1), &previous, now()));

        assert_eq!(status.phase, MaintenancePhase::Releasing);
        assert_eq!(status.blocked_reason, None);
    }

    #[test]
    fn a_window_moved_into_the_future_parks_a_blocked_request_without_a_stale_reason() {
        let mut spec = spec();
        spec.not_before = Some("2026-09-06T23:00:00Z".to_string());
        let previous = NodeMaintenanceRequestStatus {
            phase: MaintenancePhase::Blocked,
            blocked_reason: Some(MaintenanceBlockedReason::NodeNotFound),
            message: Some("node worker-04 not found".to_string()),
            ..Default::default()
        };
        let status = settled(preflight(&spec, Some(2), &previous, now()));

        assert_eq!(status.phase, MaintenancePhase::Requested);
        assert_eq!(status.blocked_reason, None);
    }

    #[test]
    fn a_releasing_request_is_not_redriven() {
        let previous = NodeMaintenanceRequestStatus {
            phase: MaintenancePhase::Releasing,
            ..Default::default()
        };
        let status = settled(preflight(&spec(), Some(1), &previous, now()));
        assert_eq!(status.phase, MaintenancePhase::Releasing);
    }

    #[test]
    fn a_prepared_request_is_not_expired_by_its_deadline() {
        let mut spec = spec();
        spec.deadline = Some("2026-09-06T19:00:00Z".to_string());
        let previous = NodeMaintenanceRequestStatus {
            phase: MaintenancePhase::Prepared,
            ..Default::default()
        };
        assert_discovers(preflight(&spec, Some(1), &previous, now()));
    }
}
