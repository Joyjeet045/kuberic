use std::collections::BTreeMap;
use std::process::Command;

use kube::CustomResourceExt;
use kuberic_controller::control::{WireError, proto};
use kuberic_controller::crd::{KubericSet, KubericSetSpec, KubericSetStatus};
use kuberic_controller::normalize::normalize;
use kuberic_controller::observation::RawObservation;
use kuberic_controller::protocol::PROTOCOL_VERSION;
use kuberic_controller::protocol::command::KubernetesChange;
use kuberic_controller::protocol::observation::ObservationSnapshot;
use kuberic_controller::protocol::types::AcceptedStatus;
use kuberic_controller::protocol::validation::validate_snapshot;
use kuberic_controller::{EvaluationConfig, Plan, evaluate, production_evaluation_config};
use serde_json::json;

#[test]
fn controller_exports_observation_and_evaluation_contracts() {
    let mut set = KubericSet::new(
        "db",
        KubericSetSpec {
            replicas: 1,
            image: "example/db:latest".into(),
            failover_delay_seconds: 30,
            switchover: None,
        },
    );
    set.metadata.uid = Some("set-uid".into());
    set.metadata.resource_version = Some("1".into());
    set.metadata.generation = Some(1);
    set.status = Some(KubericSetStatus {
        authority: AcceptedStatus::default(),
    });
    let raw = RawObservation {
        set,
        pods: Vec::new(),
        pvcs: Vec::new(),
        services: Vec::new(),
        secrets: Vec::new(),
        agents: BTreeMap::new(),
        exact_resources: Vec::new(),
        failures: Vec::new(),
        now_unix_seconds: 100,
    };
    let snapshot: ObservationSnapshot = normalize(raw, BTreeMap::new()).unwrap();
    validate_snapshot(&snapshot).unwrap();
    let config: EvaluationConfig = production_evaluation_config(31, 7, 13);
    assert_eq!(config.supported_protocol_version, PROTOCOL_VERSION);
    assert!(matches!(
        evaluate(&snapshot, &config),
        Plan::Apply { changes }
            if changes == vec![KubernetesChange::EnsureReplicaSupport]
    ));
}

#[test]
fn controller_control_exports_preserve_protocol_fencing() {
    let unsupported = PROTOCOL_VERSION + 1;
    let expected = WireError::UnsupportedProtocolVersion {
        expected: PROTOCOL_VERSION,
        observed: unsupported,
    };
    assert_eq!(
        kuberic_controller::control::validate_agent_status_report(&proto::AgentStatusReport {
            protocol_version: unsupported,
            ..Default::default()
        }),
        Err(expected.clone())
    );
    assert_eq!(
        kuberic_controller::control::validate_execute_request(&proto::ExecuteCommandRequest {
            protocol_version: unsupported,
            ..Default::default()
        }),
        Err(expected)
    );
    assert!(kuberic_controller::control::ensure_supported_version(PROTOCOL_VERSION).is_ok());
}

#[test]
fn crdgen_preserves_schema_and_flattened_status() {
    let crdgen = std::env::var_os("CARGO_BIN_EXE_crdgen")
        .expect("test runner did not provide the crdgen binary");
    let output = Command::new(crdgen).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let generated: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let checked_in: serde_json::Value =
        serde_json::from_str(include_str!("../deploy/crd.json")).unwrap();
    assert_eq!(generated, checked_in);
    assert_eq!(generated, serde_json::to_value(KubericSet::crd()).unwrap());

    let serialized = json!({
        "initialized": false,
        "observedGeneration": 0,
        "conditions": []
    });
    let status: KubericSetStatus = serde_json::from_value(serialized).unwrap();
    assert_eq!(status.authority, AcceptedStatus::default());
    assert_eq!(
        serde_json::to_value(status).unwrap(),
        json!({
            "initialized": false,
            "observedGeneration": 0,
            "effectivePolicy": null,
            "topology": null,
            "transition": null,
            "provisioning": null,
            "primaryFailure": null,
            "quorumLoss": null,
            "lastSwitchover": null,
            "conditions": []
        })
    );
}
