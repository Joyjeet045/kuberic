use postgres_replicated::testing::{PgGroup, definitive_fence_error, run_pg_test};

#[test_log::test]
fn stale_probe_oracle_rejects_unrelated_sql_failures_and_checks_secondary_reads() {
    run_pg_test(|| async {
        let mut group = PgGroup::singleton().await;
        group.add(2).await;
        group.assert_contents().await;
        let primary = group.session(1, false).await;
        let duplicate = primary
            .client()
            .execute("INSERT INTO phase6_rows VALUES(1, 'wrong')", &[])
            .await
            .unwrap_err();
        assert!(!definitive_fence_error(&duplicate));
        let syntax = primary.client().simple_query("not SQL").await.unwrap_err();
        assert!(!definitive_fence_error(&syntax));
        let secondary = group.session(2, false).await;
        let probe = group.next_probe();
        secondary.rejected(probe).await;
        assert!(
            !secondary
                .client()
                .query("SELECT * FROM phase6_rows", &[])
                .await
                .unwrap()
                .is_empty()
        );
        group.assert_contents().await;
        group.shutdown().await;
    });
}

async fn failure_case(failure: &str) {
    use kuberic_runtime::protocol::types::{AccessStatus, FaultType};
    use kuberic_runtime::testing::effects::RuntimeEffectAction;
    use postgres_replicated::testing::ProcessProbe;
    let mut group = PgGroup::singleton().await;
    group.add(2).await;
    group.assert_contents().await;
    let client = group.session(1, false).await;
    let probe_id = group.next_probe();
    let processes = ProcessProbe::postgres(group.pod(1).application.instance().data_dir());
    let path = group.pod(1).root.join("application/state-v2.json");
    let saved = std::fs::read(&path).unwrap();
    match failure {
        "storage" => std::fs::write(&path, b"invalid durable storage").unwrap(),
        "observation" => processes.signal(rustix::process::Signal::STOP),
        "fence" => {
            let data = group.pod(1).application.instance().data_dir();
            let pid = std::fs::read_to_string(data.join("postmaster.pid"))
                .unwrap()
                .lines()
                .next()
                .unwrap()
                .parse::<u32>()
                .unwrap();
            let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
            let parent = stat
                .rsplit_once(')')
                .unwrap()
                .1
                .split_whitespace()
                .nth(1)
                .unwrap()
                .parse::<u32>()
                .unwrap();
            let command = std::fs::read(format!("/proc/{parent}/cmdline")).unwrap();
            assert!(String::from_utf8_lossy(&command).contains(data.to_str().unwrap()));
            let supervisor = ProcessProbe::process(parent);
            supervisor.signal(rustix::process::Signal::QUIT);
            supervisor.assert_exited().await;
        }
        _ => unreachable!(),
    }
    let action = if failure == "fence" {
        RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::ReconfigurationPending,
            write: AccessStatus::ReconfigurationPending,
        }
    } else {
        RuntimeEffectAction::RefreshApplicationProgress
    };
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(12),
        group.pod(1).effect(action),
    )
    .await
    .expect("fault handling must be bounded");
    assert!(
        result.is_err(),
        "{failure}: failure was acknowledged as success"
    );
    assert_eq!(
        group.pod(1).runtime.partition_report().await.reported_fault,
        Some(if failure == "observation" {
            FaultType::Transient
        } else {
            FaultType::Permanent
        }),
        "{failure}: {result:?}"
    );
    client.rejected(probe_id).await;
    if failure == "fence" {
        processes.reap_adopted().unwrap();
    }
    processes.assert_reaped();
    assert_eq!(group.contents(2).await, group.expected);
    if failure == "storage" {
        std::fs::write(path, saved).unwrap();
    }
    let cleanup = group.shutdown_after_fault().await;
    if failure == "fence" {
        assert_eq!(cleanup.len(), 1, "fatal ownership errors remain retained");
        assert_eq!(cleanup[0].0, 1);
    } else {
        assert!(cleanup.is_empty(), "{cleanup:?}");
    }
}

macro_rules! failure_test {
    ($name:ident, $failure:literal) => {
        #[test_log::test]
        fn $name() {
            run_pg_test(|| failure_case($failure));
        }
    };
}

failure_test!(
    storage_failure_is_explicit_and_keeps_clients_closed,
    "storage"
);
failure_test!(
    observation_failure_is_explicit_and_keeps_clients_closed,
    "observation"
);

#[test_log::test]
fn unprovable_owned_fence_reports_permanent_and_reaps_in_an_isolated_process() {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "owned_fence_failure_child",
            "--test-threads=1",
        ])
        .env("KUBERIC_PHASE6_FENCE_CHILD", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "isolated subreaper helper executed by the fence failure parent"]
fn owned_fence_failure_child() {
    assert_eq!(
        std::env::var("KUBERIC_PHASE6_FENCE_CHILD").as_deref(),
        Ok("1")
    );
    rustix::process::set_child_subreaper(Some(rustix::process::getpid())).unwrap();
    run_pg_test(|| failure_case("fence"));
}
