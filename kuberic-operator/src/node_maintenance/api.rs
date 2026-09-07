use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::crd::StatusCondition;

pub const PREPARED_CONDITION_TYPE: &str = "KubericPrepared";

#[derive(CustomResource, Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[kube(
    group = "kuberic.io",
    version = "v1alpha1",
    kind = "NodeMaintenanceRequest",
    plural = "nodemaintenancerequests",
    shortname = "nmr",
    derive = "PartialEq",
    status = "NodeMaintenanceRequestStatus",
    printcolumn = r#"{"name":"Node","type":"string","jsonPath":".spec.nodeName"}"#,
    printcolumn = r#"{"name":"Operation","type":"string","jsonPath":".spec.operation"}"#,
    printcolumn = r#"{"name":"Desired","type":"string","jsonPath":".spec.desiredState"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Deadline","type":"string","jsonPath":".spec.deadline"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct NodeMaintenanceRequestSpec {
    #[schemars(extend("minLength" = 1))]
    pub node_name: String,

    #[serde(default)]
    pub operation: MaintenanceOperation,

    #[serde(default)]
    pub desired_state: MaintenanceDesiredState,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_event_id: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("format" = "date-time", "maxLength" = 64))]
    pub not_before: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("format" = "date-time", "maxLength" = 64))]
    pub deadline: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct NodeMaintenanceRequestStatus {
    #[serde(default)]
    pub phase: MaintenancePhase,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_desired_state: Option<MaintenanceDesiredState>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_uid: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovery_completed_at: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub affected_sets: Vec<AffectedKubericSetStatus>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<MaintenanceBlockedReason>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub prepared_at: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<StatusCondition>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AffectedKubericSetStatus {
    pub namespace: String,

    pub name: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replicas: Vec<AffectedReplicaStatus>,

    #[serde(default)]
    pub hosts_primary: bool,

    #[serde(default)]
    pub primary_moved: bool,

    #[serde(default)]
    pub no_eligible_target: bool,

    #[serde(default)]
    pub quorum_without_node: bool,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AffectedReplicaStatus {
    pub pod_name: String,

    pub pod_uid: String,

    #[serde(default)]
    pub is_primary: bool,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema, Default)]
pub enum MaintenanceOperation {
    #[default]
    Reboot,
    Reimage,
    OsUpgrade,
    Replace,
    Shutdown,
}

impl MaintenanceOperation {
    pub fn discards_local_state(self) -> bool {
        matches!(self, Self::Reimage | Self::Replace)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema, Default)]
pub enum MaintenanceDesiredState {
    #[default]
    Prepare,
    Complete,
    Cancel,
}

impl MaintenanceDesiredState {
    pub fn releases_request(self) -> bool {
        matches!(self, Self::Complete | Self::Cancel)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema, Default)]
pub enum MaintenancePhase {
    #[default]
    Requested,
    Preparing,
    Prepared,
    Blocked,
    Failed,
    Expired,
    Releasing,
    Released,
}

impl MaintenancePhase {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Failed | Self::Expired | Self::Released)
    }

    pub fn is_safe_to_drain(self) -> bool {
        matches!(self, Self::Prepared)
    }

    pub fn excludes_primary_placement(self) -> bool {
        matches!(self, Self::Preparing | Self::Prepared | Self::Blocked)
    }

    pub fn requires_reason(self) -> bool {
        matches!(self, Self::Blocked | Self::Failed | Self::Expired)
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        if self == next {
            return !self.is_terminal();
        }
        match self {
            Self::Requested => matches!(
                next,
                Self::Preparing | Self::Blocked | Self::Failed | Self::Expired | Self::Releasing
            ),
            Self::Preparing => matches!(
                next,
                Self::Requested
                    | Self::Prepared
                    | Self::Blocked
                    | Self::Failed
                    | Self::Expired
                    | Self::Releasing
            ),
            Self::Prepared => matches!(
                next,
                Self::Requested
                    | Self::Preparing
                    | Self::Blocked
                    | Self::Failed
                    | Self::Expired
                    | Self::Releasing
            ),
            Self::Blocked => matches!(
                next,
                Self::Requested | Self::Preparing | Self::Failed | Self::Expired | Self::Releasing
            ),
            Self::Releasing => matches!(next, Self::Released | Self::Failed),
            Self::Failed | Self::Expired | Self::Released => false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema)]
pub enum MaintenanceBlockedReason {
    NodeNotFound,
    NodeIncarnationChanged,
    InvalidNotBefore,
    InvalidDeadline,
    BlockedByQuorum,
    NoEligibleTarget,
    SwitchoverFailed,
    DeadlineExceeded,
    ConflictingOperation,
    ApplicationCloseIncomplete,
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt;

    fn all_phases() -> [MaintenancePhase; 8] {
        [
            MaintenancePhase::Requested,
            MaintenancePhase::Preparing,
            MaintenancePhase::Prepared,
            MaintenancePhase::Blocked,
            MaintenancePhase::Failed,
            MaintenancePhase::Expired,
            MaintenancePhase::Releasing,
            MaintenancePhase::Released,
        ]
    }

    #[test]
    fn defaults_are_requested_and_prepare() {
        assert_eq!(MaintenancePhase::default(), MaintenancePhase::Requested);
        assert_eq!(
            MaintenanceDesiredState::default(),
            MaintenanceDesiredState::Prepare
        );
        assert_eq!(
            NodeMaintenanceRequestStatus::default().phase,
            MaintenancePhase::Requested
        );
    }

    #[test]
    fn externally_owned_activity_is_not_a_kuberic_phase() {
        let generated = serde_json::to_string(&NodeMaintenanceRequest::crd()).unwrap();
        for external in ["\"Draining\"", "\"Executing\"", "\"Restoring\""] {
            assert!(
                !generated.contains(external),
                "{external} is owned by the maintenance coordinator and must not be a status phase"
            );
        }
    }

    #[test]
    fn preparation_path_transitions_are_allowed() {
        assert!(MaintenancePhase::Requested.can_transition_to(MaintenancePhase::Preparing));
        assert!(MaintenancePhase::Preparing.can_transition_to(MaintenancePhase::Prepared));
        assert!(MaintenancePhase::Prepared.can_transition_to(MaintenancePhase::Releasing));
        assert!(MaintenancePhase::Releasing.can_transition_to(MaintenancePhase::Released));
    }

    #[test]
    fn preparation_cannot_be_skipped() {
        assert!(!MaintenancePhase::Requested.can_transition_to(MaintenancePhase::Prepared));
        assert!(!MaintenancePhase::Blocked.can_transition_to(MaintenancePhase::Prepared));
    }

    #[test]
    fn an_active_request_can_return_to_requested() {
        for phase in [
            MaintenancePhase::Preparing,
            MaintenancePhase::Prepared,
            MaintenancePhase::Blocked,
        ] {
            assert!(
                phase.can_transition_to(MaintenancePhase::Requested),
                "{phase:?} must be able to wait for its window again"
            );
        }
    }

    #[test]
    fn release_can_be_requested_from_any_active_phase() {
        for phase in [
            MaintenancePhase::Requested,
            MaintenancePhase::Preparing,
            MaintenancePhase::Prepared,
            MaintenancePhase::Blocked,
        ] {
            assert!(
                phase.can_transition_to(MaintenancePhase::Releasing),
                "{phase:?} must be releasable"
            );
        }
    }

    #[test]
    fn terminal_phases_accept_no_transition() {
        for terminal in [
            MaintenancePhase::Failed,
            MaintenancePhase::Expired,
            MaintenancePhase::Released,
        ] {
            assert!(terminal.is_terminal());
            for next in all_phases() {
                assert!(
                    !terminal.can_transition_to(next),
                    "{terminal:?} must not transition to {next:?}"
                );
            }
        }
    }

    #[test]
    fn only_prepared_is_safe_to_drain() {
        for phase in all_phases() {
            assert_eq!(
                phase.is_safe_to_drain(),
                phase == MaintenancePhase::Prepared,
                "{phase:?}"
            );
        }
    }

    #[test]
    fn placement_is_excluded_only_once_preparation_has_started() {
        for phase in all_phases() {
            let expected = matches!(
                phase,
                MaintenancePhase::Preparing
                    | MaintenancePhase::Prepared
                    | MaintenancePhase::Blocked
            );
            assert_eq!(phase.excludes_primary_placement(), expected, "{phase:?}");
        }
        assert!(!MaintenancePhase::Requested.excludes_primary_placement());
    }

    #[test]
    fn unsafe_phases_require_a_reason() {
        for phase in all_phases() {
            let expected = matches!(
                phase,
                MaintenancePhase::Blocked | MaintenancePhase::Failed | MaintenancePhase::Expired
            );
            assert_eq!(phase.requires_reason(), expected, "{phase:?}");
        }
    }

    #[test]
    fn completion_and_cancellation_release_the_request() {
        assert!(MaintenanceDesiredState::Complete.releases_request());
        assert!(MaintenanceDesiredState::Cancel.releases_request());
        assert!(!MaintenanceDesiredState::Prepare.releases_request());
    }

    #[test]
    fn reimage_and_replace_discard_local_state() {
        assert!(MaintenanceOperation::Reimage.discards_local_state());
        assert!(MaintenanceOperation::Replace.discards_local_state());
        assert!(!MaintenanceOperation::Reboot.discards_local_state());
        assert!(!MaintenanceOperation::OsUpgrade.discards_local_state());
        assert!(!MaintenanceOperation::Shutdown.discards_local_state());
    }

    #[test]
    fn enums_serialize_as_pascal_case() {
        assert_eq!(
            serde_json::to_string(&MaintenancePhase::Prepared).unwrap(),
            "\"Prepared\""
        );
        assert_eq!(
            serde_json::to_string(&MaintenanceDesiredState::Cancel).unwrap(),
            "\"Cancel\""
        );
        assert_eq!(
            serde_json::to_string(&MaintenanceOperation::OsUpgrade).unwrap(),
            "\"OsUpgrade\""
        );
    }

    #[test]
    fn spec_round_trips_through_camel_case_json() {
        let spec = NodeMaintenanceRequestSpec {
            node_name: "worker-node-04".to_string(),
            operation: MaintenanceOperation::Reboot,
            desired_state: MaintenanceDesiredState::Prepare,
            provider: Some("Manual".to_string()),
            provider_event_id: Some("event-123".to_string()),
            not_before: Some("2026-09-06T20:00:00Z".to_string()),
            deadline: Some("2026-09-06T21:00:00Z".to_string()),
        };
        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(json["nodeName"], "worker-node-04");
        assert_eq!(json["desiredState"], "Prepare");
        assert_eq!(json["providerEventId"], "event-123");
        let decoded: NodeMaintenanceRequestSpec = serde_json::from_value(json).unwrap();
        assert_eq!(decoded, spec);
    }

    #[test]
    fn crd_is_cluster_scoped_and_exposes_identity_fields() {
        let crd = serde_json::to_value(NodeMaintenanceRequest::crd()).unwrap();
        assert_eq!(crd["spec"]["scope"], "Cluster");
        assert_eq!(
            crd["metadata"]["name"],
            "nodemaintenancerequests.kuberic.io"
        );

        let generated = serde_json::to_string(&crd).unwrap();
        let deployment = include_str!("../../deploy/deployment.yaml");
        for required in [
            "nodeName",
            "desiredState",
            "providerEventId",
            "notBefore",
            "deadline",
            "nodeUid",
            "observedGeneration",
            "observedDesiredState",
            "discoveryCompletedAt",
            "affectedSets",
            "podUid",
            "isPrimary",
            "primaryMoved",
            "noEligibleTarget",
            "quorumWithoutNode",
            "blockedReason",
            "InvalidNotBefore",
            "InvalidDeadline",
        ] {
            assert!(
                generated.contains(required),
                "missing generated schema {required}"
            );
            assert!(
                deployment.contains(required),
                "missing deployment schema {required}"
            );
        }

        assert!(generated.contains(r#""format":"date-time""#));
        assert!(deployment.contains("format: date-time"));
        assert!(generated.contains(r#""minLength":1"#));
        assert!(deployment.contains("minLength: 1"));
    }

    #[test]
    fn deployment_grants_consume_only_access() {
        let deployment = include_str!("../../deploy/deployment.yaml");
        assert!(deployment.contains("nodemaintenancerequests.kuberic.io"));
        assert!(deployment.contains("nodemaintenancerequests/status"));
        assert!(deployment.contains("nodemaintenancerequests/finalizers"));
        assert!(deployment.contains("nodes"));

        let rules = deployment
            .split("resources: [\"nodemaintenancerequests\"]")
            .nth(1)
            .expect("nodemaintenancerequests rule");
        let verbs = rules.lines().nth(1).unwrap_or_default();
        for forbidden in ["create", "delete"] {
            assert!(
                !verbs.contains(forbidden),
                "operator must not {forbidden} requests owned by the coordinator: {verbs}"
            );
        }
    }
}
