use k8s_openapi::api::core::v1::ObjectReference;
use kube::runtime::events::{Event, EventType};

use super::api::{
    MaintenanceDesiredState, MaintenancePhase, NodeMaintenanceRequestSpec,
    NodeMaintenanceRequestStatus,
};
use super::controller::ReconcileOutcome;

pub fn transition_event(
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
    let warning = status.phase.requires_reason() || status.blocked_reason.is_some();
    let reason = if status.phase == MaintenancePhase::Released {
        if status.observed_desired_state == Some(MaintenanceDesiredState::Cancel) {
            "Cancelled"
        } else {
            "Completed"
        }
        .to_string()
    } else if let Some(reason) = status.blocked_reason {
        format!("{reason:?}")
    } else {
        format!("{:?}", status.phase)
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
    fn only_persisted_meaningful_transitions_emit_events() {
        let previous = NodeMaintenanceRequestStatus::default();
        let mut prepared = outcome(MaintenancePhase::Prepared);
        prepared.persisted = false;
        assert!(transition_event(&spec(), &previous, &prepared).is_none());
        prepared.persisted = true;
        let event = transition_event(&spec(), &previous, &prepared).unwrap();
        assert_eq!(event.type_, EventType::Normal);
        assert_eq!(event.reason, "Prepared");
        assert!(transition_event(&spec(), &prepared.status, &prepared).is_none());
    }

    #[test]
    fn blocked_reason_changes_are_reported_as_warnings() {
        let mut blocked = outcome(MaintenancePhase::Blocked);
        blocked.status.blocked_reason = Some(MaintenanceBlockedReason::BlockedByQuorum);
        let event =
            transition_event(&spec(), &NodeMaintenanceRequestStatus::default(), &blocked).unwrap();
        assert_eq!(event.type_, EventType::Warning);
        assert_eq!(event.reason, "BlockedByQuorum");
        let previous = blocked.status.clone();
        blocked.status.blocked_reason = Some(MaintenanceBlockedReason::NoEligibleTarget);
        let event = transition_event(&spec(), &previous, &blocked).unwrap();
        assert_eq!(event.type_, EventType::Warning);
        assert_eq!(event.reason, "NoEligibleTarget");
    }

    #[test]
    fn release_events_report_the_outcome_and_recovered_node() {
        for (desired, reason) in [
            (MaintenanceDesiredState::Complete, "Completed"),
            (MaintenanceDesiredState::Cancel, "Cancelled"),
        ] {
            let mut released = outcome(MaintenancePhase::Released);
            released.status.observed_desired_state = Some(desired);
            released.status.released_node_uid = Some("replacement-uid".to_string());
            let event = transition_event(
                &spec(),
                &outcome(MaintenancePhase::Releasing).status,
                &released,
            )
            .unwrap();
            assert_eq!(event.type_, EventType::Normal);
            assert_eq!(event.reason, reason);
            assert_eq!(event.action, "Release");
            assert_eq!(
                event.secondary.unwrap().uid.as_deref(),
                Some("replacement-uid")
            );
        }
    }

    #[test]
    fn event_notes_respect_the_api_byte_limit() {
        let mut blocked = outcome(MaintenancePhase::Blocked);
        blocked.status.message = Some("\u{e9}".repeat(800));
        let event =
            transition_event(&spec(), &NodeMaintenanceRequestStatus::default(), &blocked).unwrap();
        assert_eq!(event.note.unwrap().len(), 1024);
    }
}
