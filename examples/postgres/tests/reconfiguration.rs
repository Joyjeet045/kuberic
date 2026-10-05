use postgres_replicated::testing::{AdmissionCut, PgGroup, RestartPart, run_pg_test};

#[test_log::test]
fn production_agent_restart_defers_persisted_grant_until_exact_discovery() {
    run_pg_test(|| async {
        use kuberic_agent::store::AgentStore;
        use kuberic_protocol::types::AccessStatus;
        use kuberic_wire::proto::{self as wire, agent_control_client::AgentControlClient};
        use postgres_replicated::testing::ProcessProbe;
        use std::os::unix::fs::MetadataExt;
        use std::time::Duration;

        let mut group = PgGroup::singleton().await;
        group.add(2).await;
        group.add(3).await;
        group.write("acknowledged before production restart").await;
        group.assert_contents().await;
        let ordinary = group.session(1, false).await;
        let admin = group.session(1, true).await;
        let ordinary_probe = group.next_probe();
        let admin_probe = group.next_probe();
        let old = group.pods.remove(&1).unwrap();
        assert_eq!(
            old.store.load_state().await.unwrap().write_status,
            AccessStatus::Granted
        );
        let data = old.application.instance().data_dir().to_path_buf();
        let inode = std::fs::metadata(&data).unwrap().ino();
        let lineage = old
            .application
            .native_driver()
            .durable_state()
            .await
            .system_identifier;
        let processes = ProcessProbe::postgres(&data);
        // This path does not reconstruct through the fixture's closed-access shortcut:
        // AgentService::serve reads the actual granted durable agent state.
        let (pod, address) = old.reopen_with_agent().await;
        processes.assert_reaped();
        ordinary.rejected(ordinary_probe).await;
        admin.rejected(admin_probe).await;
        ordinary.disconnected().await;
        admin.disconnected().await;
        group.pods.insert(1, pod);
        let mut client = AgentControlClient::connect(format!("http://{address}"))
            .await
            .unwrap();
        let request = || {
            let mut request = tonic::Request::new(wire::GetAgentStatusRequest {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: "postgres-native-test".into(),
                replica_id: 1,
                expected_instance_id: group.pod(1).identity.instance_id.to_string(),
            });
            request.metadata_mut().insert(
                "authorization",
                "Bearer host-local-native-build".parse().unwrap(),
            );
            request
        };
        for _ in 0..2 {
            let report = client.get_status(request()).await.unwrap().into_inner();
            assert_eq!(
                report.write_status,
                wire::AccessStatus::ReconfigurationPending as i32
            );
            assert!(report.healthy);
            assert_eq!(
                group.pod(1).store.load_state().await.unwrap().write_status,
                AccessStatus::Granted
            );
            assert!(
                group
                    .pod(1)
                    .application
                    .instance()
                    .connect_application()
                    .await
                    .is_err()
            );
            assert_eq!(group.contents(1).await, group.expected);
        }
        for (&id, pod) in &group.pods {
            for (&other_id, other) in &group.pods {
                if id == other_id {
                    continue;
                }
                let mut replica = kuberic_runtime::replicator::ReplicaInformation::new(
                    kuberic_protocol::types::OperationId::default(),
                    other.identity.clone(),
                    other.endpoint.clone(),
                );
                replica.process_session_id = other.session.clone();
                kuberic_agent::testing::describe_peer(&pod.runtime, replica)
                    .await
                    .unwrap();
            }
        }
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let report = client.get_status(request()).await.unwrap().into_inner();
                if report.write_status == wire::AccessStatus::Granted as i32 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(std::fs::metadata(&data).unwrap().ino(), inode);
        assert_eq!(
            group
                .pod(1)
                .application
                .native_driver()
                .durable_state()
                .await
                .system_identifier,
            lineage
        );
        group.write("production restart reconciled").await;
        group.assert_contents().await;
        group.shutdown().await;
    });
}

#[test_log::test]
fn sequential_scale_up_and_secondary_removal_preserve_complete_sql_contents() {
    run_pg_test(|| async {
        let mut group = PgGroup::singleton().await;
        group.add(2).await;
        group.write("two replicas").await;
        group.add(3).await;
        group.write("three replicas").await;
        group.assert_contents().await;
        let removed = group.session(3, false).await;
        let removed_admin = group.session(3, true).await;
        let probe = group.next_probe();
        group.remove(3).await;
        removed.rejected(probe).await;
        removed.disconnected().await;
        removed_admin.disconnected().await;
        group.write("reduced").await;
        group.assert_contents().await;
        let last_secondary = group.session(2, false).await;
        let singleton_probe = group.next_probe();
        group.remove(2).await;
        last_secondary.rejected(singleton_probe).await;
        group.write("reduced to singleton").await;
        group.assert_contents().await;
        group.restart_peer(1).await;
        group.write("singleton restarted after reductions").await;
        group.assert_contents().await;
        group.shutdown().await;
    });
}

#[test_log::test]
fn quorum_loss_keeps_secondary_reads_and_fresh_sessions_restore_writes() {
    run_pg_test(|| async {
        let mut group = PgGroup::singleton().await;
        group.add(2).await;
        group.add(3).await;
        group.write("last acknowledged before quorum loss").await;
        group.assert_contents().await;
        let ordinary = group.session(1, false).await;
        let admin = group.session(1, true).await;
        let one = group.next_probe();
        let two = group.next_probe();
        group.fence_quorum_loss().await;
        ordinary.rejected(one).await;
        admin.rejected(two).await;
        ordinary.disconnected().await;
        admin.disconnected().await;
        for id in [2, 3] {
            let reader = group.session(id, false).await;
            assert_eq!(
                reader
                    .client()
                    .query("SELECT * FROM phase6_rows", &[])
                    .await
                    .unwrap()
                    .len(),
                group.expected.len()
            );
            reader.rejected(group.next_probe()).await;
            reader
                .client()
                .simple_query("SELECT 1")
                .await
                .expect("readable secondary rejection is not a completed fence");
        }
        group.restart_peer(2).await;
        group.restart_peer(3).await;
        group.write("fresh sessions restored").await;
        group.assert_contents().await;
        group.shutdown().await;
    });
}

#[test_log::test]
fn application_process_restart_and_metadata_connection_reopen_preserve_committed_contents() {
    run_pg_test(|| async {
        let mut group = PgGroup::singleton().await;
        group.add(2).await;
        group.add(3).await;
        let session = group.pod(1).session.clone();
        let sql = group.session(1, false).await;
        let admin = group.session(1, true).await;
        let probe = group.next_probe();
        let admin_probe = group.next_probe();
        group.pod(1).restart_application().await.unwrap();
        sql.rejected(probe).await;
        admin.rejected(admin_probe).await;
        sql.disconnected().await;
        admin.disconnected().await;
        assert_eq!(group.pod(1).session, session);
        group.refresh_configuration().await;
        group.reopen_agent_metadata(1).await;
        group.write("metadata connection reopened").await;
        let retained = group.session(1, false).await;
        let retained_admin = group.session(1, true).await;
        let agent_probe = group.next_probe();
        let agent_admin_probe = group.next_probe();
        group.restart_peer(1).await;
        retained.rejected(agent_probe).await;
        retained_admin.rejected(agent_admin_probe).await;
        retained.disconnected().await;
        retained_admin.disconnected().await;
        assert_ne!(group.pod(1).session, session);
        group.write("whole agent reopened").await;
        group.assert_contents().await;
        group.shutdown().await;
    });
}

async fn scaling_reopens_at(cut: AdmissionCut, part: RestartPart) {
    tracing::info!(?cut, ?part, "restarting scale-up at a durable boundary");
    let mut group = PgGroup::singleton().await;
    group.add_with_restart(2, Some((cut, part))).await;
    group.write("after admission recovery").await;
    group.assert_contents().await;
    group.shutdown().await;
}

macro_rules! scaling_restart_test {
    ($name:ident, $cut:expr, $part:expr) => {
        #[test_log::test]
        fn $name() {
            run_pg_test(|| scaling_reopens_at($cut, $part));
        }
    };
}

scaling_restart_test!(
    scaling_reopens_application_after_built_boundary,
    AdmissionCut::Built,
    RestartPart::ApplicationProcess
);
scaling_restart_test!(
    scaling_reopens_agent_after_built_boundary,
    AdmissionCut::Built,
    RestartPart::AgentHost
);
scaling_restart_test!(
    scaling_reopens_application_after_previous_current_boundary,
    AdmissionCut::PreviousCurrent,
    RestartPart::ApplicationProcess
);
scaling_restart_test!(
    scaling_reopens_agent_after_previous_current_boundary,
    AdmissionCut::PreviousCurrent,
    RestartPart::AgentHost
);
scaling_restart_test!(
    scaling_reopens_application_after_current_only_boundary,
    AdmissionCut::CurrentOnly,
    RestartPart::ApplicationProcess
);
scaling_restart_test!(
    scaling_reopens_agent_after_current_only_boundary,
    AdmissionCut::CurrentOnly,
    RestartPart::AgentHost
);
