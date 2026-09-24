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
            std::collections::BTreeSet::from(["operationId", "podUid", "pvcUid", "replaces"])
        );
        assert!(
            status["properties"]["transition"]["properties"]
                .get("startedAtUnixSeconds")
                .is_none()
        );
        assert!(status["properties"].get("primaryFailure").is_some());
        assert!(status["properties"].get("quorumLoss").is_some());
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
