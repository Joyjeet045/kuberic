use kube::CustomResource;
use kuberic_protocol::types::AcceptedStatus;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const API_GROUP: &str = "operator.kuberic.io";
pub const API_VERSION: &str = "v1alpha1";
pub const CONTROLLER_NAME: &str = "kuberic-controller";
pub const SET_UID_LABEL: &str = "operator.kuberic.io/set-uid";
pub const REPLICA_ID_LABEL: &str = "operator.kuberic.io/replica-id";
pub const INSTANCE_LABEL: &str = "operator.kuberic.io/instance";
pub const CONTROL_ADDRESS_ANNOTATION: &str = "operator.kuberic.io/control-address";
pub const SCALE_UP_ALLOCATION_ANNOTATION: &str =
    "operator.kuberic.io/scale-up-allocation-operation";

#[derive(CustomResource, Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[kube(
    group = "operator.kuberic.io",
    version = "v1alpha1",
    kind = "KubericSet",
    plural = "kubericsets",
    shortname = "kset",
    derive = "PartialEq",
    namespaced,
    status = "KubericSetStatus",
    printcolumn = r#"{"name":"Replicas","type":"integer","jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Initialized","type":"boolean","jsonPath":".status.initialized"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct KubericSetSpec {
    #[schemars(range(min = 1))]
    pub replicas: u32,
    #[schemars(length(min = 1))]
    pub image: String,
    #[serde(default = "default_failover_delay_seconds")]
    pub failover_delay_seconds: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub switchover: Option<PlannedSwitchoverRequestSpec>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlannedSwitchoverRequestSpec {
    #[schemars(length(min = 1))]
    pub request_id: String,
    #[schemars(range(min = 1))]
    pub target_replica_id: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct KubericSetStatus {
    #[serde(flatten)]
    pub authority: AcceptedStatus,
}

const fn default_failover_delay_seconds() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use kube::CustomResourceExt;
    use serde_json::json;

    use super::*;

    #[test]
    fn scale_down_status_schema_and_legacy_defaults_are_explicit() {
        let schema = serde_json::to_value(KubericSet::crd()).unwrap();
        let root = &schema["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"];
        let spec = &root["spec"]["properties"];
        assert_eq!(spec["replicas"]["minimum"], 1.0);
        assert!(spec.get("removalTarget").is_none());
        assert!(spec.get("minimumReplicas").is_none());
        let status = &root["status"]["properties"];
        let transition = &status["transition"]["properties"];
        assert!(
            transition["kind"]["enum"]
                .as_array()
                .unwrap()
                .contains(&json!("secondaryScaleDown"))
        );
        let intent = &transition["secondaryScaleDown"]["properties"];
        for field in [
            "operationId",
            "resourceUid",
            "specGeneration",
            "desiredReplicas",
            "previousConfiguration",
            "currentConfiguration",
            "previousPolicy",
            "currentPolicy",
            "primary",
            "target",
            "cleanup",
        ] {
            assert!(intent.get(field).is_some(), "{field}");
        }
        assert_eq!(intent["desiredReplicas"]["minimum"], 1.0);
        for resource in ["pod", "pvc", "endpoint"] {
            let identity = &intent["cleanup"]["properties"][resource];
            assert!(
                identity["properties"]["present"]["properties"]
                    .get("name")
                    .is_some()
            );
            assert!(
                identity["properties"]["present"]["properties"]
                    .get("uid")
                    .is_some()
            );
            assert!(
                identity["properties"]["absent"]["properties"]
                    .get("name")
                    .is_some()
            );
            assert!(identity["oneOf"].as_array().unwrap().len() >= 2);
        }
        let cleanup = &status["secondaryScaleDownCleanup"]["properties"];
        assert!(cleanup.get("evidence").is_some());
        assert!(cleanup.get("currentOnlyWriteQuorum").is_some());
        assert!(cleanup.get("retirement").is_some());
        let old: KubericSetStatus = serde_json::from_value(json!({
            "initialized": false, "observedGeneration": 0, "conditions": []
        }))
        .unwrap();
        assert!(old.authority.secondary_scale_down_cleanup.is_none());
        assert!(
            serde_json::from_value::<KubericSetSpec>(json!({"replicas": -1, "image": "db"}))
                .is_err()
        );
    }

    #[test]
    fn scale_up_status_schema_is_typed_and_zero_boundaries_are_allowed() {
        let schema = serde_json::to_value(KubericSet::crd()).unwrap();
        let status = &schema["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["status"]
            ["properties"];
        let transition = &status["transition"]["properties"];
        assert!(
            transition["kind"]["enum"]
                .as_array()
                .unwrap()
                .contains(&json!("scaleUp"))
        );
        let intent = &transition["scaleUp"]["properties"];
        for field in [
            "operationId",
            "resourceUid",
            "specGeneration",
            "desiredReplicas",
            "previousConfiguration",
            "currentConfiguration",
            "previousPolicy",
            "currentPolicy",
            "primary",
            "target",
            "buildId",
            "snapshotBoundaryLsn",
            "catchUpBoundaryLsn",
        ] {
            assert!(intent.get(field).is_some(), "{field}");
        }
        assert_eq!(intent["snapshotBoundaryLsn"]["minimum"], 0.0);
        assert_eq!(intent["catchUpBoundaryLsn"]["minimum"], 0.0);
        assert_eq!(intent["target"]["properties"]["replicaId"]["minimum"], 1.0);
        for policy in ["previousPolicy", "currentPolicy"] {
            for field in ["replicaSetSize", "writeQuorum", "readQuorum"] {
                assert_eq!(intent[policy]["properties"][field]["minimum"], 1.0);
            }
        }
        assert!(status.get("scaleUpCleanup").is_some());
        assert!(status.get("lastScaleUp").is_some());
        let failover = &transition["scaleUpFailover"]["properties"];
        for field in [
            "intent",
            "provisionalConfiguration",
            "previousReadQuorum",
            "currentReadQuorum",
            "finalElection",
        ] {
            assert!(failover.get(field).is_some(), "{field}");
        }
        let final_election = &failover["finalElection"]["properties"];
        for field in [
            "selectedPrimaryReplicaId",
            "witnesses",
            "previousReadQuorum",
            "currentReadQuorum",
        ] {
            assert!(final_election.get(field).is_some(), "{field}");
        }
        assert!(final_election.get("finalConfiguration").is_none());
        assert_eq!(final_election["selectedPrimaryReplicaId"]["minimum"], 1.0);
        let final_witness = &final_election["witnesses"]["items"]["properties"];
        for field in [
            "replicaId",
            "processSessionId",
            "reportSequence",
            "currentProgress",
            "committedLsn",
            "deactivatedLsn",
            "fenceOperationId",
        ] {
            assert!(final_witness.get(field).is_some(), "{field}");
        }
        for duplicated in [
            "resourceUid",
            "identity",
            "role",
            "epoch",
            "previousConfigurationId",
            "currentConfigurationId",
            "deactivationEpoch",
            "writeStatus",
            "writeClosed",
            "pendingOperationId",
            "retainedOperationId",
        ] {
            assert!(final_witness.get(duplicated).is_none(), "{duplicated}");
        }
        assert_eq!(final_witness["replicaId"]["minimum"], 1.0);
        assert_eq!(final_witness["reportSequence"]["minimum"], 1.0);
        assert_eq!(final_witness["currentProgress"]["minimum"], 0.0);
        assert_eq!(final_witness["committedLsn"]["minimum"], 0.0);
        assert_eq!(final_witness["deactivatedLsn"]["minimum"], 0.0);
        for quorum in ["previousReadQuorum", "currentReadQuorum"] {
            assert_eq!(final_election[quorum]["items"]["minimum"], 1.0);
        }
        let receipt_failover =
            &status["lastScaleUp"]["properties"]["failoverEvidence"]["properties"];
        for field in [
            "provisionalPrimaryReplicaId",
            "previousReadQuorum",
            "currentReadQuorum",
            "finalElection",
        ] {
            assert!(receipt_failover.get(field).is_some(), "{field}");
        }
        assert!(receipt_failover.get("intent").is_none());
        assert!(receipt_failover.get("provisionalConfiguration").is_none());
        assert_eq!(
            receipt_failover["provisionalPrimaryReplicaId"]["minimum"],
            1.0
        );
        let allocation = &status["scaleUpAllocation"]["properties"];
        for field in [
            "resourceUid",
            "specGeneration",
            "desiredReplicas",
            "previousConfigurationId",
            "acceptedConfigurationId",
            "targetReplicaId",
            "operationId",
            "previousOperationId",
            "scaffoldingRequested",
            "podUid",
            "pvcUid",
            "cancellationStarted",
        ] {
            assert!(allocation.get(field).is_some(), "{field}");
        }
        assert_eq!(allocation["targetReplicaId"]["minimum"], 1.0);

        let provisioning = &status["provisioning"]["properties"]["purpose"]["properties"];
        assert_eq!(
            provisioning["kind"]["enum"],
            json!(["replacement", "scaleUp"])
        );
        assert!(
            provisioning["scaleUp"]["properties"]
                .get("targetReplicaId")
                .is_some()
        );
        assert_eq!(
            provisioning["scaleUp"]["properties"]["targetReplicaId"]["minimum"],
            1.0
        );

        let generated = serde_json::to_string_pretty(&KubericSet::crd()).unwrap();
        assert!(
            generated.len() < 350_000,
            "generated CRD unexpectedly grew to {} bytes",
            generated.len()
        );
        assert!(
            generated.len() <= 345_000,
            "compact final-election schema lost its reviewed headroom at {} bytes",
            generated.len()
        );
    }

    #[test]
    fn crd_uses_only_the_level_triggered_api_group() {
        let crd = KubericSet::crd();
        assert_eq!(crd.spec.group, API_GROUP);
        assert_eq!(crd.spec.versions.len(), 1);
        assert_eq!(crd.spec.versions[0].name, API_VERSION);
        assert_eq!(crd.spec.names.plural, "kubericsets");
    }

    #[test]
    fn schema_rejects_zero_replicas_and_malformed_status() {
        let schema = serde_json::to_value(KubericSet::crd()).unwrap();
        let replicas = &schema["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["replicas"];
        assert_eq!(replicas["minimum"].as_f64(), Some(1.0));
        let switchover = &schema["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["switchover"]["properties"];
        assert_eq!(switchover["requestId"]["minLength"].as_u64(), Some(1));
        assert_eq!(switchover["targetReplicaId"]["minimum"].as_f64(), Some(1.0));

        let status =
            &schema["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["status"];
        let required = status["required"].as_array().unwrap();
        assert!(required.iter().any(|value| value == "initialized"));
        assert!(required.iter().any(|value| value == "observedGeneration"));
        assert!(status["properties"].get("authority").is_none());
        let topology = &status["properties"]["topology"];
        assert!(topology["properties"].get("configuration").is_none());
        assert!(topology["properties"].get("members").is_some());
        assert!(topology["properties"].get("primaryId").is_none());
        let member = &topology["properties"]["members"]["items"];
        assert!(member["properties"].get("identity").is_none());
        assert!(member["properties"].get("replicaId").is_some());
        assert!(member["properties"].get("instanceId").is_some());
        assert!(member["properties"].get("agentGeneration").is_some());
        let provisioning = &status["properties"]["provisioning"]["properties"];
        assert_eq!(
            provisioning
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from(["operationId", "podUid", "purpose", "pvcUid"])
        );
        let purpose = &provisioning["purpose"]["properties"];
        assert_eq!(purpose["kind"]["enum"], json!(["replacement", "scaleUp"]));
        assert!(purpose.get("replaces").is_some());
        assert!(purpose.get("scaleUp").is_some());
        assert!(
            status["properties"]["transition"]["properties"]
                .get("startedAtUnixSeconds")
                .is_none()
        );
        assert!(status["properties"].get("primaryFailure").is_some());
        assert!(status["properties"].get("quorumLoss").is_some());
        assert!(status["properties"].get("lastSwitchover").is_some());
        assert!(
            !required
                .iter()
                .any(|value| value == "pendingReplacementCleanup")
        );
        assert_eq!(
            status["properties"]["pendingReplacementCleanup"]["properties"],
            status["properties"]["lastReplacement"]["properties"]
        );
        assert!(
            status["properties"]["quorumLoss"]["properties"]
                .get("startedAtUnixSeconds")
                .is_none()
        );
        assert!(
            status["properties"]["transition"]["properties"]
                .get("repair")
                .is_some()
        );
        assert!(
            status["properties"]["transition"]["properties"]
                .get("electionLsn")
                .is_some()
        );
        assert!(
            status["properties"]["transition"]["properties"]
                .get("switchover")
                .is_some()
        );
        assert!(
            status["properties"]["transition"]["properties"]["currentConfiguration"]["properties"]
                .get("primaryId")
                .is_none()
        );
    }

    #[test]
    fn malformed_status_does_not_deserialize() {
        let value = json!({
            "apiVersion": "operator.kuberic.io/v1alpha1",
            "kind": "KubericSet",
            "metadata": {"name": "db"},
            "spec": {"replicas": 3, "image": "example/db:latest"},
            "status": {"initialized": "yes"}
        });
        assert!(serde_json::from_value::<KubericSet>(value).is_err());
    }

    #[test]
    fn deploy_assets_are_isolated_from_the_classic_operator() {
        let crd: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(include_str!("../deploy/crd.json")).unwrap();
        let generated = serde_json::to_value(KubericSet::crd()).unwrap();
        let checked_in: serde_json::Value =
            serde_json::from_str(include_str!("../deploy/crd.json")).unwrap();
        assert_eq!(checked_in, generated);
        assert_eq!(crd["spec"]["group"].as_str(), Some("operator.kuberic.io"));
        assert_eq!(
            crd["spec"]["versions"][0]["name"].as_str(),
            Some("v1alpha1")
        );
        assert_eq!(
            crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
                ["properties"]["replicas"]["minimum"]
                .as_f64(),
            Some(1.0)
        );

        for manifest in [
            include_str!("../deploy/rbac.yaml"),
            include_str!("../deploy/service-account.yaml"),
            include_str!("../deploy/deployment.yaml"),
            include_str!("../deploy/kustomization.yaml"),
        ] {
            assert!(!manifest.contains("apiGroups:\n      - kuberic.io"));
            assert!(!manifest.contains("kuberic-operator"));
        }
        assert!(include_str!("../Dockerfile").contains("protobuf-compiler"));
    }
}
