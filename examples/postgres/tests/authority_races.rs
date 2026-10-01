use kuberic_protocol::types::AccessStatus;
use kuberic_runtime_internal::effects::RuntimeEffectAction;
use postgres_replicated::testing::{PgGroup, run_pg_test};

#[test_log::test]
fn same_ordinal_replacement_rejects_old_work_without_mutating_the_replacement() {
    run_pg_test(|| async {
        let mut group = PgGroup::singleton().await;
        group.add(2).await;
        group.add(3).await;
        group.write("before replacement").await;
        group.assert_contents().await;
        let stale = group.session(2, false).await;
        let request = group
            .pod(2)
            .application
            .native_driver()
            .durable_state()
            .await
            .native_build
            .unwrap()
            .request;
        let probe = group.next_probe();
        group.replace(2).await;
        stale.rejected(probe).await;
        let before = group
            .pod(2)
            .application
            .native_driver()
            .durable_state()
            .await;
        assert!(group.pod(2).inject(&request).await.is_err());
        assert!(
            group
                .old_incarnation()
                .effect(RuntimeEffectAction::SetAccessStatus {
                    read: AccessStatus::Granted,
                    write: AccessStatus::Granted,
                })
                .await
                .is_err()
        );
        assert_eq!(
            group
                .pod(2)
                .application
                .native_driver()
                .durable_state()
                .await,
            before
        );
        group.write("after replacement").await;
        group.assert_contents().await;
        group.shutdown().await;
    });
}

#[test_log::test]
fn raw_postgres_progress_cannot_admit_an_unbuilt_candidate() {
    run_pg_test(|| async {
        use kuberic_protocol::types::*;
        use kuberic_runtime_internal::authority::AdmittedAuthority;
        let mut group = PgGroup::singleton().await;
        let mut candidate = group.candidate(2).await;
        let source = group.pod(1);
        let build = source.authorize(&candidate, "unbuilt").await;
        let raw = postgres_replicated::native::PgNativeObserver::new(
            candidate.application.instance().clone(),
        )
        .snapshot()
        .await
        .unwrap();
        assert!(
            raw.current_lsn > 0,
            "fresh engine WAL is not application build evidence"
        );
        candidate.refresh().await;
        assert!(candidate.runtime.snapshot().await.builds.is_empty());
        let current = postgres_replicated::testing::native_configuration(
            &[source.identity.clone(), candidate.identity.clone()],
            0,
            2,
        );
        let mut intent = ScaleUpIntent {
            operation_id: OperationId::default(),
            resource_uid: ResourceUid::new("postgres-native-test"),
            spec_generation: 2,
            desired_replicas: 2,
            previous_configuration: group.configuration.clone(),
            current_configuration: current.clone(),
            previous_policy: EffectivePolicy::fixed(1, 30).unwrap(),
            current_policy: EffectivePolicy::fixed(2, 30).unwrap(),
            primary: source.identity.clone(),
            target: candidate.identity.clone(),
            build_id: build.build_id.clone(),
            snapshot_boundary_lsn: build.replication_boundary_lsn,
            catch_up_boundary_lsn: build.replication_boundary_lsn,
        };
        intent.operation_id = intent.expected_operation_id();
        let before = candidate.application.native_driver().durable_state().await;
        assert!(
            candidate
                .effect(RuntimeEffectAction::AdmitAuthority(Box::new(
                    AdmittedAuthority {
                        local_identity: candidate.identity.clone(),
                        previous_configuration: Some(group.configuration.clone()),
                        current_configuration: current,
                        transition_kind: Some(TransitionKind::ScaleUp),
                        scale_up: Some(Box::new(ScaleUpConfigurationEvidence::Admission {
                            intent
                        })),
                        switchover_handoff: None,
                        secondary_removal: None,
                    }
                )))
                .await
                .is_err()
        );
        assert_eq!(
            candidate.application.native_driver().durable_state().await,
            before
        );
        assert!(
            candidate
                .application
                .instance()
                .connect_application()
                .await
                .is_err()
        );
        candidate.shutdown().await.unwrap();
        drop(candidate);
        group.shutdown().await;
    });
}
