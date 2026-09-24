use sqlserver_replicated::{
    AvailabilityGroupIdentity, AvailabilityGroupName, AvailabilityMode, ClusterType, ContractError,
    DatabaseIdentity, DatabaseLineage, DecimalProgress, DestructiveApproval, Edition, Endpoint,
    FailoverMode, FenceReference, Guid, MutationMode, NativeRole, OPERATION_CONTRACT_VERSION,
    Observation, ObservationFailure, ObservationFailureKind, OperationEnvelope, OperationPayload,
    OperationRecord, OperationRequest, PinnedImage, ReplayDisposition, ReplicaDescriptor,
    ReplicaIdentity, SUPPORTED_REPLICA_COUNT, SUPPORTED_REPLICA_COUNT_TEXT, SecretRef, SeedingMode,
    ServerName, SqlIdentifier, SqlServerSupportConfig,
};

use std::num::NonZeroU32;

fn guid(value: u32) -> Guid {
    Guid::parse(
        "test GUID",
        format!("{value:08x}-0000-4000-8000-000000000001"),
    )
    .unwrap()
}

fn replica(value: u32) -> ReplicaIdentity {
    ReplicaIdentity::observed(
        format!("replica-{value}"),
        guid(value),
        format!("pod-uid-{value}"),
    )
    .unwrap()
}

fn desired_replica(value: u32) -> ReplicaIdentity {
    ReplicaIdentity::desired(format!("replica-{value}"), format!("pod-uid-{value}")).unwrap()
}

fn descriptor(value: u32) -> ReplicaDescriptor {
    ReplicaDescriptor {
        identity: desired_replica(value),
        server_name: ServerName::new(format!("sql-{value}")).unwrap(),
        endpoint: Endpoint::new(format!("sql-{value}.sql.default.svc"), 5022).unwrap(),
    }
}

fn pinned_image() -> PinnedImage {
    PinnedImage::new(format!(
        "mcr.microsoft.com/mssql/server:2022-CU@sha256:{}",
        "a".repeat(64)
    ))
    .unwrap()
}

fn secret(name: &str, key: &str) -> SecretRef {
    SecretRef::new("test Secret", name, key).unwrap()
}

fn supported_config() -> SqlServerSupportConfig {
    SqlServerSupportConfig {
        engine_major: 16,
        edition: Edition::Developer,
        image: pinned_image(),
        eula_accepted: true,
        cluster_type: ClusterType::External,
        failover_mode: FailoverMode::External,
        availability_mode: AvailabilityMode::SynchronousCommit,
        seeding_mode: SeedingMode::Automatic,
        replica_count: 3,
        database_count: 1,
        required_synchronized_secondaries_to_commit: 1,
        external_write_lease_seconds: NonZeroU32::new(60),
        mutation_mode: MutationMode::ObserveOnly,
        observer_credentials: secret("sqlserver-observer", "password"),
        mutation_credentials: None,
        endpoint_certificate: secret("sqlserver-endpoint", "tls.crt"),
    }
}

fn availability_group() -> AvailabilityGroupIdentity {
    AvailabilityGroupIdentity {
        name: AvailabilityGroupName::new("kuberic-ag").unwrap(),
        group_id: guid(10),
    }
}

fn database() -> DatabaseIdentity {
    DatabaseIdentity {
        name: SqlIdentifier::new("application").unwrap(),
        group_database_id: guid(20),
    }
}

fn database_lineage(value: u32) -> DatabaseLineage {
    DatabaseLineage {
        database: database(),
        recovery_fork_id: guid(value),
    }
}

fn request(
    operation_id: &str,
    source_epoch: u64,
    target_epoch: u64,
    payload: OperationPayload,
) -> OperationRequest {
    OperationRequest::new(
        "default/example",
        operation_id,
        "configuration-1",
        source_epoch,
        target_epoch,
        payload,
    )
    .unwrap()
}

fn fence(request: &OperationRequest, fenced_replica: ReplicaIdentity) -> FenceReference {
    FenceReference::new(
        "test-container-runtime",
        "fence-1",
        request.operation_id(),
        request.input_signature(),
        fenced_replica,
    )
    .unwrap()
}

fn approval(request: &OperationRequest) -> DestructiveApproval {
    DestructiveApproval::new(
        "approval-1",
        request.operation_id(),
        request.input_signature(),
    )
    .unwrap()
}

#[test]
fn supported_profile_is_explicit_and_observe_only_by_default() {
    let config = supported_config();
    assert_eq!(config.mutation_mode, MutationMode::ObserveOnly);
    assert_eq!(config.validate(), Ok(()));
}

#[test]
fn read_scale_cluster_type_is_not_accepted_as_ha() {
    let mut config = supported_config();
    config.cluster_type = ClusterType::None;

    assert!(matches!(
        config.validate(),
        Err(ContractError::UnsupportedProfile {
            field: "cluster type",
            ..
        })
    ));
}

#[test]
fn mutation_credentials_are_separate_and_explicit() {
    let mut missing = supported_config();
    missing.mutation_mode = MutationMode::Enabled;
    assert_eq!(
        missing.validate(),
        Err(ContractError::MissingField {
            field: "mutation credentials"
        })
    );

    let mut shared = supported_config();
    shared.mutation_mode = MutationMode::Enabled;
    shared.mutation_credentials = Some(shared.observer_credentials.clone());
    assert!(matches!(
        shared.validate(),
        Err(ContractError::UnsupportedProfile {
            field: "mutation credentials",
            ..
        })
    ));

    let mut separate = supported_config();
    separate.mutation_mode = MutationMode::Enabled;
    separate.mutation_credentials = Some(secret("sqlserver-mutator", "password"));
    assert_eq!(separate.validate(), Ok(()));

    let mut unused = supported_config();
    unused.mutation_credentials = Some(secret("sqlserver-mutator", "password"));
    assert!(matches!(
        unused.validate(),
        Err(ContractError::UnsupportedProfile {
            field: "mutation credentials",
            ..
        })
    ));
}

#[test]
fn image_must_be_immutable_and_eula_must_be_explicit() {
    assert_eq!(
        PinnedImage::new("mcr.microsoft.com/mssql/server:2022-latest"),
        Err(ContractError::InvalidImageDigest)
    );
    assert!(
        PinnedImage::new(format!(
            "mcr.microsoft.com/mssql/server:2022 CU@sha256:{}",
            "a".repeat(64)
        ))
        .is_err()
    );

    let mut config = supported_config();
    config.eula_accepted = false;
    assert!(matches!(
        config.validate(),
        Err(ContractError::UnsupportedProfile {
            field: "EULA acceptance",
            ..
        })
    ));
}

#[test]
fn sql_identifier_quoting_does_not_treat_content_as_sql() {
    let identifier = SqlIdentifier::new("db]; DROP DATABASE [other").unwrap();
    assert_eq!(identifier.quoted(), "[db]]; DROP DATABASE [other]");
    assert!(SqlIdentifier::new("line\nbreak").is_err());
}

#[test]
fn native_progress_preserves_values_larger_than_i64() {
    let value = DecimalProgress::parse("9999999999999999999999999").unwrap();
    assert!(value.value() > i64::MAX as u128);
    assert_eq!(value.to_string(), "9999999999999999999999999");
    assert_eq!(
        DecimalProgress::parse("10000000000000000000000000"),
        Err(ContractError::InvalidProgress)
    );
    assert_eq!(
        DecimalProgress::parse("-1"),
        Err(ContractError::InvalidProgress)
    );
}

#[test]
fn observation_does_not_collapse_absence_failure_or_staleness() {
    let present = Observation::Present {
        value: 7_u8,
        observed_at_unix_millis: 1_000,
    };
    let absent = Observation::<u8>::Absent {
        observed_at_unix_millis: 1_000,
    };
    let failed = Observation::<u8>::Failed(ObservationFailure {
        kind: ObservationFailureKind::Unreachable,
        message: "connection timed out".to_string(),
        observed_at_unix_millis: 1_000,
    });

    assert!(present.is_fresh_at(1_100, 100));
    assert!(absent.is_fresh_at(1_100, 100));
    assert!(!present.is_fresh_at(1_101, 100));
    assert!(!failed.is_fresh_at(1_000, 100));
    assert!(!present.is_fresh_at(999, 100));

    // A failure is never fresh, but it still records when the attempt was made
    // so that callers can tell how long evidence has been unavailable.
    assert_eq!(failed.observed_at_unix_millis(), 1_000);
    assert_eq!(present.observed_at_unix_millis(), 1_000);
    assert_eq!(absent.observed_at_unix_millis(), 1_000);
}

#[test]
fn canonical_signature_is_stable_for_replica_set_order() {
    let payload = |replicas| OperationPayload::EnsureAvailabilityGroup {
        name: AvailabilityGroupName::new("kuberic-ag").unwrap(),
        expected_group_id: None,
        database_name: SqlIdentifier::new("application").unwrap(),
        replicas,
    };
    let envelope = OperationEnvelope::new(
        request(
            "bootstrap-1",
            0,
            1,
            payload(vec![descriptor(3), descriptor(1), descriptor(2)]),
        ),
        None,
        None,
    )
    .unwrap();
    let reordered = OperationEnvelope::new(
        request(
            "bootstrap-1",
            0,
            1,
            payload(vec![descriptor(2), descriptor(3), descriptor(1)]),
        ),
        None,
        None,
    )
    .unwrap();

    assert_eq!(envelope.input_signature(), reordered.input_signature());
    assert_eq!(
        envelope.input_signature().to_string(),
        "sha256:a1686f50f455aaa8498daa987cd71adec7d145e059f7983a3810053d4c70a970"
    );
}

#[test]
fn duplicate_replica_identity_is_rejected() {
    let duplicate = descriptor(1);
    let result = OperationRequest::new(
        "default/example",
        "bootstrap-1",
        "configuration-0",
        0,
        1,
        OperationPayload::EnsureAvailabilityGroup {
            name: AvailabilityGroupName::new("kuberic-ag").unwrap(),
            expected_group_id: None,
            database_name: SqlIdentifier::new("application").unwrap(),
            replicas: vec![duplicate.clone(), duplicate, descriptor(2)],
        },
    );

    assert!(matches!(
        result,
        Err(ContractError::DuplicateValue {
            field: "logical replica ID",
            ..
        })
    ));
}

#[test]
fn bootstrap_uses_desired_identity_before_sql_server_generates_replica_guids() {
    let result = OperationRequest::new(
        "default/example",
        "bootstrap-1",
        "configuration-0",
        0,
        1,
        OperationPayload::EnsureAvailabilityGroup {
            name: AvailabilityGroupName::new("kuberic-ag").unwrap(),
            expected_group_id: None,
            database_name: SqlIdentifier::new("application").unwrap(),
            replicas: vec![
                ReplicaDescriptor {
                    identity: replica(1),
                    server_name: ServerName::new("sql-1").unwrap(),
                    endpoint: Endpoint::new("sql-1.sql.default.svc", 5022).unwrap(),
                },
                descriptor(2),
                descriptor(3),
            ],
        },
    );

    assert_eq!(
        result,
        Err(ContractError::UnexpectedNativeIdentity {
            field: "bootstrap replica"
        })
    );
}

#[test]
fn post_bootstrap_commands_require_observed_native_replica_identity() {
    assert_eq!(
        OperationRequest::new(
            "default/example",
            "join-1",
            "configuration-1",
            1,
            1,
            OperationPayload::EnsureReplicaJoined {
                availability_group: availability_group(),
                target: desired_replica(2),
            },
        ),
        Err(ContractError::MissingNativeIdentity {
            field: "join target replica"
        })
    );
}

#[test]
fn operation_id_reuse_with_different_input_is_rejected() {
    let first = OperationEnvelope::new(
        request(
            "join-1",
            1,
            1,
            OperationPayload::EnsureReplicaJoined {
                availability_group: availability_group(),
                target: replica(1),
            },
        ),
        None,
        None,
    )
    .unwrap();
    let duplicate = first.clone();
    let conflicting = OperationEnvelope::new(
        request(
            "join-1",
            1,
            1,
            OperationPayload::EnsureReplicaJoined {
                availability_group: availability_group(),
                target: replica(2),
            },
        ),
        None,
        None,
    )
    .unwrap();
    let other = OperationEnvelope::new(
        request(
            "join-2",
            1,
            1,
            OperationPayload::EnsureReplicaJoined {
                availability_group: availability_group(),
                target: replica(2),
            },
        ),
        None,
        None,
    )
    .unwrap();

    let record = OperationRecord::from_envelope(&first);
    assert_eq!(
        record.classify(&duplicate),
        Ok(ReplayDisposition::ExactDuplicate)
    );
    assert_eq!(
        record.classify(&conflicting),
        Err(ContractError::OperationIdReuse)
    );
    assert_eq!(
        record.classify(&other),
        Ok(ReplayDisposition::DifferentOperation)
    );
}

#[test]
fn reseed_requires_approval_and_a_fence_for_the_target() {
    let target = replica(2);
    let request = request(
        "reseed-1",
        1,
        1,
        OperationPayload::ReseedReplica {
            availability_group: availability_group(),
            database: database(),
            source: replica(1),
            target: target.clone(),
        },
    );
    let fence = fence(&request, target);

    assert_eq!(
        OperationEnvelope::new(request.clone(), None, Some(fence.clone())),
        Err(ContractError::MissingDestructiveApproval)
    );
    assert_eq!(
        OperationEnvelope::new(request.clone(), Some(approval(&request)), Some(fence),)
            .unwrap()
            .validate(),
        Ok(())
    );
}

#[test]
fn approval_is_bound_to_canonical_input() {
    let target = replica(2);
    let original = request(
        "reseed-1",
        1,
        1,
        OperationPayload::ReseedReplica {
            availability_group: availability_group(),
            database: database(),
            source: replica(1),
            target: target.clone(),
        },
    );
    let changed = request(
        "reseed-1",
        1,
        1,
        OperationPayload::ReseedReplica {
            availability_group: availability_group(),
            database: database(),
            source: replica(3),
            target: target.clone(),
        },
    );

    assert_eq!(
        OperationEnvelope::new(
            changed.clone(),
            Some(approval(&original)),
            Some(fence(&changed, target)),
        ),
        Err(ContractError::ApprovalInputMismatch)
    );
}

#[test]
fn role_transition_fence_is_bound_to_the_old_primary_incarnation() {
    let source = replica(1);
    let request = request(
        "switch-1",
        1,
        2,
        OperationPayload::PlannedSwitchover {
            availability_group: availability_group(),
            database: database_lineage(30),
            source,
            target: replica(2),
            commit_boundary: DecimalProgress::parse("123456789").unwrap(),
        },
    );

    assert_eq!(
        OperationEnvelope::new(request.clone(), None, None),
        Err(ContractError::MissingFence)
    );
    assert_eq!(
        OperationEnvelope::new(request.clone(), None, Some(fence(&request, replica(3))),),
        Err(ContractError::FenceTargetMismatch)
    );
}

#[test]
fn fence_is_bound_to_canonical_input() {
    let source = replica(1);
    let original = request(
        "switch-1",
        1,
        2,
        OperationPayload::PlannedSwitchover {
            availability_group: availability_group(),
            database: database_lineage(30),
            source: source.clone(),
            target: replica(2),
            commit_boundary: DecimalProgress::parse("123456789").unwrap(),
        },
    );
    let changed = request(
        "switch-1",
        1,
        2,
        OperationPayload::PlannedSwitchover {
            availability_group: availability_group(),
            database: database_lineage(30),
            source,
            target: replica(2),
            commit_boundary: DecimalProgress::parse("123456790").unwrap(),
        },
    );

    assert_eq!(
        OperationEnvelope::new(changed, None, Some(fence(&original, replica(1))),),
        Err(ContractError::FenceInputMismatch)
    );
}

#[test]
fn progress_proof_is_bound_to_database_recovery_lineage() {
    let source = replica(1);
    let original = request(
        "switch-1",
        1,
        2,
        OperationPayload::PlannedSwitchover {
            availability_group: availability_group(),
            database: database_lineage(30),
            source: source.clone(),
            target: replica(2),
            commit_boundary: DecimalProgress::parse("123456789").unwrap(),
        },
    );
    let changed = request(
        "switch-1",
        1,
        2,
        OperationPayload::PlannedSwitchover {
            availability_group: availability_group(),
            database: database_lineage(31),
            source,
            target: replica(2),
            commit_boundary: DecimalProgress::parse("123456789").unwrap(),
        },
    );

    assert_eq!(
        OperationEnvelope::new(changed, None, Some(fence(&original, replica(1)))),
        Err(ContractError::FenceInputMismatch)
    );
}

#[test]
fn logical_or_native_self_transition_is_rejected_across_incarnations() {
    let same_logical_new_incarnation =
        ReplicaIdentity::observed("replica-1", guid(2), "pod-uid-new").unwrap();
    assert!(matches!(
        OperationRequest::new(
            "default/example",
            "switch-1",
            "configuration-1",
            1,
            2,
            OperationPayload::PlannedSwitchover {
                availability_group: availability_group(),
                database: database_lineage(30),
                source: replica(1),
                target: same_logical_new_incarnation,
                commit_boundary: DecimalProgress::parse("123").unwrap(),
            },
        ),
        Err(ContractError::DuplicateValue {
            field: "source and target replica",
            ..
        })
    ));

    let same_native_new_logical =
        ReplicaIdentity::observed("replacement", guid(1), "pod-uid-new").unwrap();
    assert!(matches!(
        OperationRequest::new(
            "default/example",
            "switch-2",
            "configuration-1",
            1,
            2,
            OperationPayload::PlannedSwitchover {
                availability_group: availability_group(),
                database: database_lineage(30),
                source: replica(1),
                target: same_native_new_logical,
                commit_boundary: DecimalProgress::parse("123").unwrap(),
            },
        ),
        Err(ContractError::DuplicateValue {
            field: "source and target replica",
            ..
        })
    ));
}

#[test]
fn forced_failover_requires_input_bound_approval_and_source_fence() {
    let source = replica(1);
    let original = request(
        "failover-1",
        1,
        2,
        OperationPayload::ForcedFailover {
            availability_group: availability_group(),
            database: database_lineage(30),
            source: source.clone(),
            target: replica(2),
            last_known_commit: None,
        },
    );

    assert_eq!(
        OperationEnvelope::new(original.clone(), None, None),
        Err(ContractError::MissingDestructiveApproval)
    );
    assert_eq!(
        OperationEnvelope::new(original.clone(), Some(approval(&original)), None),
        Err(ContractError::MissingFence)
    );
    assert!(
        OperationEnvelope::new(
            original.clone(),
            Some(approval(&original)),
            Some(fence(&original, source)),
        )
        .is_ok()
    );

    let changed = request(
        "failover-1",
        1,
        2,
        OperationPayload::ForcedFailover {
            availability_group: availability_group(),
            database: database_lineage(30),
            source: replica(1),
            target: replica(2),
            last_known_commit: Some(DecimalProgress::parse("1").unwrap()),
        },
    );
    assert_eq!(
        OperationEnvelope::new(
            changed.clone(),
            Some(approval(&original)),
            Some(fence(&changed, replica(1))),
        ),
        Err(ContractError::ApprovalInputMismatch)
    );
}

#[test]
fn epochs_must_not_regress() {
    assert_eq!(
        OperationRequest::new(
            "default/example",
            "join-1",
            "configuration-1",
            3,
            2,
            OperationPayload::EnsureReplicaJoined {
                availability_group: availability_group(),
                target: replica(2),
            },
        ),
        Err(ContractError::EpochRegression {
            source: 3,
            target: 2
        })
    );

    assert_eq!(
        OperationRequest::new(
            "default/example",
            "switch-1",
            "configuration-1",
            3,
            3,
            OperationPayload::PlannedSwitchover {
                availability_group: availability_group(),
                database: database_lineage(30),
                source: replica(1),
                target: replica(2),
                commit_boundary: DecimalProgress::parse("123").unwrap(),
            },
        ),
        Err(ContractError::EpochNotAdvanced {
            source: 3,
            target: 3
        })
    );
}

#[test]
fn malformed_native_identities_and_endpoints_fail_closed() {
    assert!(Guid::parse("AG GUID", "not-a-guid").is_err());
    assert!(Guid::parse("AG GUID", "00000000-0000-0000-0000-000000000000").is_err());
    assert!(Endpoint::new("sql-0;shutdown", 5022).is_err());
    assert!(Endpoint::new("sql-0.default.svc", 0).is_err());
    assert!(SecretRef::new("Secret", "Uppercase_Name", "password").is_err());
}

#[test]
fn endpoints_are_dns_canonical_and_duplicate_detection_is_case_insensitive() {
    let lower = Endpoint::new("sql-1.default.svc", 5022).unwrap();
    let upper = Endpoint::new("SQL-1.DEFAULT.SVC", 5022).unwrap();
    assert_eq!(lower, upper);
    assert_eq!(upper.host(), "sql-1.default.svc");

    let result = OperationRequest::new(
        "default/example",
        "bootstrap-1",
        "configuration-0",
        0,
        1,
        OperationPayload::EnsureAvailabilityGroup {
            name: AvailabilityGroupName::new("kuberic-ag").unwrap(),
            expected_group_id: None,
            database_name: SqlIdentifier::new("application").unwrap(),
            replicas: vec![
                ReplicaDescriptor {
                    identity: desired_replica(1),
                    server_name: ServerName::new("sql-1").unwrap(),
                    endpoint: lower,
                },
                ReplicaDescriptor {
                    identity: desired_replica(2),
                    server_name: ServerName::new("sql-2").unwrap(),
                    endpoint: upper,
                },
                descriptor(3),
            ],
        },
    );
    assert!(matches!(
        result,
        Err(ContractError::DuplicateValue {
            field: "replication endpoint",
            ..
        })
    ));
}

#[test]
fn external_availability_group_name_has_a_64_character_limit() {
    assert!(AvailabilityGroupName::new("a".repeat(64)).is_ok());
    assert_eq!(
        AvailabilityGroupName::new("a".repeat(65)),
        Err(ContractError::InvalidLength {
            field: "EXTERNAL availability group name",
            max: 64
        })
    );
}

#[test]
fn server_name_case_does_not_change_the_idempotency_key() {
    // Duplicate detection treats server names case-insensitively, so the
    // canonical encoding must agree. Otherwise a controller that re-renders
    // @@SERVERNAME with different casing turns an idempotent retry into a
    // hard OperationIdReuse error.
    let bootstrap = |first: &str| {
        request(
            "bootstrap-1",
            0,
            1,
            OperationPayload::EnsureAvailabilityGroup {
                name: AvailabilityGroupName::new("kuberic-ag").unwrap(),
                expected_group_id: None,
                database_name: SqlIdentifier::new("application").unwrap(),
                replicas: vec![
                    ReplicaDescriptor {
                        identity: desired_replica(1),
                        server_name: ServerName::new(first).unwrap(),
                        endpoint: Endpoint::new("sql-1.sql.default.svc", 5022).unwrap(),
                    },
                    descriptor(2),
                    descriptor(3),
                ],
            },
        )
    };

    let lower = bootstrap("sql-1");
    let upper = bootstrap("SQL-1");
    assert_eq!(upper.payload(), lower.payload());
    assert_eq!(upper.input_signature(), lower.input_signature());

    let record =
        OperationRecord::from_envelope(&OperationEnvelope::new(lower, None, None).unwrap());
    let retry = OperationEnvelope::new(upper, None, None).unwrap();
    assert_eq!(
        record.classify(&retry),
        Ok(ReplayDisposition::ExactDuplicate)
    );
}

#[test]
fn server_names_that_differ_only_by_case_are_rejected_as_duplicates() {
    let result = OperationRequest::new(
        "default/example",
        "bootstrap-1",
        "configuration-1",
        0,
        1,
        OperationPayload::EnsureAvailabilityGroup {
            name: AvailabilityGroupName::new("kuberic-ag").unwrap(),
            expected_group_id: None,
            database_name: SqlIdentifier::new("application").unwrap(),
            replicas: vec![
                descriptor(1),
                ReplicaDescriptor {
                    identity: desired_replica(2),
                    server_name: ServerName::new("SQL-1").unwrap(),
                    endpoint: Endpoint::new("sql-2.sql.default.svc", 5022).unwrap(),
                },
                descriptor(3),
            ],
        },
    );
    assert!(matches!(
        result,
        Err(ContractError::DuplicateValue {
            field: "server name",
            ..
        })
    ));
}

#[test]
fn identical_effect_under_a_new_operation_id_is_not_treated_as_unrelated_work() {
    // A planner that crashes after dispatch but before persisting its intent
    // can regenerate the same native effect under a fresh operation ID.
    let payload = || OperationPayload::ReseedReplica {
        availability_group: availability_group(),
        database: database(),
        source: replica(1),
        target: replica(2),
    };
    let envelope = |operation_id: &str| {
        let inner = request(operation_id, 4, 4, payload());
        let approval =
            DestructiveApproval::new("approval-1", inner.operation_id(), inner.input_signature())
                .unwrap();
        let fence = fence(&inner, replica(2));
        OperationEnvelope::new(inner, Some(approval), Some(fence)).unwrap()
    };

    let record = OperationRecord::from_envelope(&envelope("reseed-1"));
    assert_eq!(
        record.classify(&envelope("reseed-1")),
        Ok(ReplayDisposition::ExactDuplicate)
    );
    assert_eq!(
        record.classify(&envelope("reseed-2")),
        Ok(ReplayDisposition::DuplicateEffectNewOperationId)
    );

    // The operation ID is part of the idempotency key but not part of the
    // effect, and the two digests are domain-separated.
    assert_ne!(
        envelope("reseed-1").input_signature(),
        envelope("reseed-2").input_signature()
    );
    assert_eq!(
        envelope("reseed-1").effect_signature(),
        envelope("reseed-2").effect_signature()
    );
    assert_ne!(
        envelope("reseed-1").canonical_input(),
        envelope("reseed-1").canonical_effect()
    );
    assert_ne!(
        envelope("reseed-1").input_signature().as_bytes(),
        envelope("reseed-1").effect_signature().as_bytes()
    );

    let unrelated = request(
        "join-9",
        4,
        4,
        OperationPayload::EnsureReplicaJoined {
            availability_group: availability_group(),
            target: replica(3),
        },
    );
    assert_eq!(
        record.classify(&OperationEnvelope::new(unrelated, None, None).unwrap()),
        Ok(ReplayDisposition::DifferentOperation)
    );
}

#[test]
fn decoded_requests_must_carry_a_supported_contract_version() {
    let payload = || OperationPayload::EnsureReplicaJoined {
        availability_group: availability_group(),
        target: replica(1),
    };
    assert!(
        OperationRequest::from_decoded_parts(
            OPERATION_CONTRACT_VERSION,
            "default/example",
            "join-1",
            "configuration-1",
            1,
            1,
            payload(),
        )
        .is_ok()
    );
    assert_eq!(
        OperationRequest::from_decoded_parts(
            OPERATION_CONTRACT_VERSION + 1,
            "default/example",
            "join-1",
            "configuration-1",
            1,
            1,
            payload(),
        ),
        Err(ContractError::UnsupportedProfile {
            field: "operation contract version",
            expected: "1",
            actual: (OPERATION_CONTRACT_VERSION + 1).to_string(),
        })
    );
}

#[test]
fn supported_profile_constants_agree() {
    assert_eq!(
        SUPPORTED_REPLICA_COUNT.to_string(),
        SUPPORTED_REPLICA_COUNT_TEXT
    );

    let mut replicas: Vec<ReplicaDescriptor> = (1..=u32::from(SUPPORTED_REPLICA_COUNT))
        .map(descriptor)
        .collect();
    replicas.pop();
    assert_eq!(
        OperationRequest::new(
            "default/example",
            "bootstrap-1",
            "configuration-1",
            0,
            1,
            OperationPayload::EnsureAvailabilityGroup {
                name: AvailabilityGroupName::new("kuberic-ag").unwrap(),
                expected_group_id: None,
                database_name: SqlIdentifier::new("application").unwrap(),
                replicas,
            },
        ),
        Err(ContractError::UnsupportedProfile {
            field: "operation replica count",
            expected: SUPPORTED_REPLICA_COUNT_TEXT,
            actual: (SUPPORTED_REPLICA_COUNT - 1).to_string(),
        })
    );
}

#[test]
fn unrecognized_native_roles_are_retained_but_validated() {
    assert_eq!(NativeRole::parse("PRIMARY"), Ok(NativeRole::Primary));
    assert_eq!(NativeRole::parse("SECONDARY"), Ok(NativeRole::Secondary));
    assert_eq!(NativeRole::parse("RESOLVING"), Ok(NativeRole::Resolving));
    assert_eq!(NativeRole::parse("NOT_JOINED"), Ok(NativeRole::NotJoined));

    // An unsupported engine state stays distinguishable from a known role ...
    assert!(matches!(
        NativeRole::parse("REVALIDATING"),
        Ok(NativeRole::Unknown(_))
    ));
    // ... but server-supplied text is still validated like any other input.
    assert_eq!(
        NativeRole::parse(""),
        Err(ContractError::MissingField {
            field: "native replica role"
        })
    );
    assert_eq!(
        NativeRole::parse("PRIMARY\u{0}"),
        Err(ContractError::InvalidCharacter {
            field: "native replica role"
        })
    );
}
