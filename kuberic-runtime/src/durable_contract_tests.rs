use bytes::Bytes;
use serde_json::{Value, json};

use crate::application::OpenMode;
use crate::authority::{
    AdmittedAuthority, DurableBuildProgress, DurableLocalWrite, LocalWritePhase,
};
use crate::effects::{RoleTransition, RuntimeEffect, RuntimeEffectAction};
use crate::protocol::types::*;
use crate::receipts::{CertifiedPrefixReceipt, NativeOperationToken, TopologyReceipt};
use crate::transport::{CopyItem, ReplicationItem};

fn authority() -> AdmittedAuthority {
    let intent = crate::removal_fixture::intent(&[1, 2], 1);
    AdmittedAuthority {
        local_identity: intent.primary.clone(),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: intent.previous_configuration.clone(),
        switchover_handoff: None,
        secondary_removal: None,
        scale_up: None,
    }
}

#[test]
fn authority_nullable_evidence_requires_explicit_durable_fields() {
    let authority = authority();
    let value = serde_json::to_value(&authority).unwrap();
    assert_eq!(
        value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        [
            "current_configuration",
            "local_identity",
            "previous_configuration",
            "scale_up",
            "secondary_removal",
            "switchover_handoff",
            "transition_kind",
        ],
    );
    assert_eq!(
        serde_json::from_value::<AdmittedAuthority>(value.clone()).unwrap(),
        authority
    );
    for key in [
        "transition_kind",
        "previous_configuration",
        "switchover_handoff",
        "secondary_removal",
        "scale_up",
    ] {
        let mut missing = value.clone();
        assert!(missing.as_object_mut().unwrap().remove(key).is_some());
        assert!(serde_json::from_value::<AdmittedAuthority>(missing).is_err());
    }
}

#[test]
fn native_receipts_preserve_external_tags_and_snake_case_durable_keys() {
    let receipt = TopologyReceipt::CertifiedPrefix(Box::new(CertifiedPrefixReceipt {
        token: NativeOperationToken {
            authority: None,
            engine_session_id: "engine-session".into(),
            engine_generation: 7,
        },
        verified_lsn: 11,
        settled_lsn: 11,
        committed_lsn: 10,
    }));
    let expected = json!({
        "CertifiedPrefix": {
            "token": {
                "authority": null,
                "engine_session_id": "engine-session",
                "engine_generation": 7,
            },
            "verified_lsn": 11,
            "settled_lsn": 11,
            "committed_lsn": 10,
        }
    });
    assert_eq!(serde_json::to_value(&receipt).unwrap(), expected);
    assert_eq!(
        serde_json::from_value::<TopologyReceipt>(expected).unwrap(),
        receipt
    );
}

#[test]
fn effects_and_open_modes_preserve_durable_enum_representation() {
    let effect = RuntimeEffect {
        operation_id: OperationId::new("open-op"),
        sequence: 1,
        action: RuntimeEffectAction::Open(OpenMode::Existing),
    };
    let expected = json!({
        "operation_id": "open-op",
        "sequence": 1,
        "action": { "Open": "Existing" },
    });
    assert_eq!(serde_json::to_value(&effect).unwrap(), expected);
    assert_eq!(
        serde_json::from_value::<RuntimeEffect>(expected).unwrap(),
        effect
    );
    assert_eq!(serde_json::to_value(OpenMode::New).unwrap(), json!("New"));
}

#[test]
fn local_write_defaults_remain_compatible_but_role_completion_is_strict() {
    let write: DurableLocalWrite = serde_json::from_value(json!({
        "operation_id": "write-op",
        "lsn": 12,
        "data": [1, 2],
        "phase": "Registered",
    }))
    .unwrap();
    assert_eq!(write.committed_lsn, 0);
    assert_eq!(write.phase, LocalWritePhase::Registered);
    assert_eq!(write.data, Bytes::from_static(&[1, 2]));
    assert!(
        serde_json::from_value::<RoleTransition>(json!({
            "completed_role": "none",
            "target_role": "primary",
            "replicator_completed": true,
            "application_completed": false,
        }))
        .is_err()
    );
}

#[test]
fn transport_preserves_camel_case_envelopes_and_copy_boundary_defaults() {
    let authority = authority();
    let receiver = authority.current_configuration.members[1].identity.clone();
    let item = ReplicationItem {
        sender: authority.local_identity.clone(),
        receiver: receiver.clone(),
        epoch: authority.current_configuration.epoch,
        previous_configuration_id: None,
        current_configuration_id: authority.current_configuration.configuration_id.clone(),
        lsn: 4,
        committed_lsn: 3,
        data: Bytes::from_static(&[1, 2]),
    };
    let expected = json!({
        "sender": item.sender,
        "receiver": receiver,
        "epoch": item.epoch,
        "previousConfigurationId": null,
        "currentConfigurationId": item.current_configuration_id,
        "lsn": 4,
        "committedLsn": 3,
        "data": [1, 2],
    });
    assert_eq!(serde_json::to_value(&item).unwrap(), expected);
    assert_eq!(
        serde_json::from_value::<ReplicationItem>(expected).unwrap(),
        item
    );

    let copy = CopyItem {
        build_id: OperationId::new("build"),
        sender: item.sender,
        receiver,
        epoch: item.epoch,
        current_configuration_id: item.current_configuration_id,
        sequence: 1,
        lsn: 4,
        committed_lsn: 3,
        replication_boundary_lsn: 3,
        catch_up_boundary_lsn: None,
        final_item: false,
        snapshot_chunk: true,
        data: item.data,
    };
    let mut value = serde_json::to_value(&copy).unwrap();
    assert_eq!(value["replicationBoundaryLsn"], json!(3));
    assert_eq!(
        value.as_object_mut().unwrap().remove("catchUpBoundaryLsn"),
        Some(Value::Null)
    );
    assert_eq!(serde_json::from_value::<CopyItem>(value).unwrap(), copy);
}

#[test]
fn build_progress_preserves_legacy_optional_catch_up_boundary() {
    let authority = authority();
    let build = BuildAuthority {
        build_id: OperationId::new("build"),
        source: authority.local_identity.clone(),
        target: authority.current_configuration.members[1].identity.clone(),
        kind: BuildAuthorityKind::Provisioning,
        current_configuration: authority.current_configuration,
        replication_boundary_lsn: 3,
    };
    let progress = DurableBuildProgress {
        authority: build,
        last_sequence: 2,
        durable_lsn: 3,
        completed: false,
        catch_up_boundary_lsn: None,
    };
    let mut value = serde_json::to_value(&progress).unwrap();
    assert_eq!(
        value
            .as_object_mut()
            .unwrap()
            .remove("catch_up_boundary_lsn"),
        Some(Value::Null)
    );
    assert_eq!(
        serde_json::from_value::<DurableBuildProgress>(value).unwrap(),
        progress
    );
}
