use postgres_replicated::instance::PgInstanceManager;
use postgres_replicated::testing::{TestDataDir, allocate_port, find_pg_bin};
use serial_test::serial;
use test_log::test;
use tokio::sync::mpsc;

#[test(tokio::test)]
#[serial]
async fn test_instance_lifecycle() {
    let pg_bin = find_pg_bin();
    let directory = TestDataDir::new("lifecycle");
    let data_dir = directory.path().join("pgdata");
    let (fault_tx, _fault_rx) = mpsc::channel(1);

    let instance = PgInstanceManager::new(data_dir.clone(), pg_bin, allocate_port().await);

    // initdb
    instance.init_db().await.expect("initdb should succeed");
    assert!(data_dir.join("PG_VERSION").exists());
    assert!(data_dir.join("postgresql.conf").exists());

    // start
    instance
        .start_native(fault_tx)
        .await
        .expect("start should succeed");

    // connect and query
    let (client, _conn) = instance.connect().await.expect("connect should work");

    let row = client
        .query_one("SELECT 1 + 1 AS result", &[])
        .await
        .expect("query should work");
    let result: i32 = row.get("result");
    assert_eq!(result, 2);

    // create table, insert, select
    client
        .execute(
            "CREATE TABLE test_table (id SERIAL PRIMARY KEY, value TEXT NOT NULL)",
            &[],
        )
        .await
        .expect("create table");

    client
        .execute(
            "INSERT INTO test_table (value) VALUES ($1)",
            &[&"hello kuberic"],
        )
        .await
        .expect("insert");

    let row = client
        .query_one("SELECT value FROM test_table WHERE id = 1", &[])
        .await
        .expect("select");
    let value: &str = row.get("value");
    assert_eq!(value, "hello kuberic");

    // stop
    instance.stop().await.expect("stop should succeed");
}

#[test(tokio::test)]
#[serial]
async fn test_instance_restart() {
    let pg_bin = find_pg_bin();
    let directory = TestDataDir::new("restart");
    let data_dir = directory.path().join("pgdata");
    let (fault_tx, _fault_rx) = mpsc::channel(1);

    let instance = PgInstanceManager::new(data_dir.clone(), pg_bin, allocate_port().await);
    instance.init_db().await.unwrap();
    instance.start_native(fault_tx.clone()).await.unwrap();

    // Write data
    {
        let (client, _conn) = instance.connect().await.unwrap();
        client
            .execute("CREATE TABLE persist (k TEXT PRIMARY KEY, v TEXT)", &[])
            .await
            .unwrap();
        client
            .execute("INSERT INTO persist VALUES ('key1', 'value1')", &[])
            .await
            .unwrap();
    }

    // Stop
    instance.stop().await.unwrap();

    // Restart
    instance.start_native(fault_tx).await.unwrap();

    // Verify data persisted
    let (client, _conn) = instance.connect().await.unwrap();
    let row = client
        .query_one("SELECT v FROM persist WHERE k = 'key1'", &[])
        .await
        .unwrap();
    let v: &str = row.get("v");
    assert_eq!(v, "value1");

    instance.stop().await.unwrap();
}

#[test(tokio::test)]
#[serial]
async fn test_lsn_query() {
    let pg_bin = find_pg_bin();
    let directory = TestDataDir::new("lsn");
    let data_dir = directory.path().join("pgdata");
    let (fault_tx, _fault_rx) = mpsc::channel(1);

    let instance = PgInstanceManager::new(data_dir.clone(), pg_bin, allocate_port().await);
    instance.init_db().await.unwrap();
    instance.start_native(fault_tx).await.unwrap();

    let (client, _conn) = instance.connect().await.unwrap();

    // Query current WAL LSN
    let row = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap();
    let lsn_str: &str = row.get(0);
    assert!(lsn_str.contains('/'), "LSN should be in format X/X");

    // Parse it
    let lsn = postgres_replicated::monitor::parse_pg_lsn(lsn_str).unwrap();
    assert!(lsn > 0, "LSN should be positive");

    instance.stop().await.unwrap();
}

#[test(tokio::test)]
#[serial]
async fn owned_cleanup_preserves_unrelated_postgres_and_rejects_foreign_pid_file() {
    use postgres_replicated::testing::{ProcessProbe, wrapped_pg_bin};
    let unrelated_dir = TestDataDir::new("unrelated");
    let unrelated = PgInstanceManager::new(
        unrelated_dir.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    );
    let (faults, _) = mpsc::channel(8);
    unrelated.init_db().await.unwrap();
    unrelated.start_native(faults.clone()).await.unwrap();
    let (unrelated_sql, _) = unrelated.connect().await.unwrap();
    unrelated_sql
        .batch_execute("CREATE TABLE isolation_receipt(id int)")
        .await
        .unwrap();

    for foreign_pid in [false, true] {
        let root = TestDataDir::new("owned-drop");
        let instance = PgInstanceManager::new(
            root.path().join("pgdata"),
            wrapped_pg_bin(
                root.path(),
                "postgres",
                &format!(
                    "#!/bin/sh\nsetsid sh -c '\"{}/postgres\" \"$@\" &' sh \"$@\" &\n",
                    find_pg_bin().display()
                ),
            ),
            allocate_port().await,
        );
        instance.init_db().await.unwrap();
        instance.start_native(faults.clone()).await.unwrap();
        let (sql, _) = instance.connect().await.unwrap();
        let probe = ProcessProbe::postgres(instance.data_dir());
        let pid_file = instance.data_dir().join("postmaster.pid");
        std::fs::rename(&pid_file, instance.data_dir().join("postmaster.pid.hidden")).unwrap();
        if foreign_pid {
            std::fs::copy(unrelated.data_dir().join("postmaster.pid"), &pid_file).unwrap();
        }
        drop(instance);
        probe.assert_gone().await;
        assert!(
            sql.simple_query("CREATE TABLE leaked_write(id int)")
                .await
                .is_err()
        );
        unrelated_sql
            .batch_execute("INSERT INTO isolation_receipt VALUES (1)")
            .await
            .unwrap();
        assert!(unrelated.is_running().await);
    }
    let root = TestDataDir::new("foreign-ready-backend");
    let instance = PgInstanceManager::new(
        root.path().join("pgdata"),
        wrapped_pg_bin(root.path(), "postgres", "#!/bin/sh\nexec sleep 30\n"),
        unrelated.port(),
    );
    instance.init_db().await.unwrap();
    let socket = format!(".s.PGSQL.{}", unrelated.port());
    std::os::unix::fs::symlink(
        unrelated.socket_dir().join(&socket),
        instance.socket_dir().join(&socket),
    )
    .unwrap();
    let error = instance.start_native(faults).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("ready backend is not an owned descendant"),
        "{error}"
    );
    drop(instance);
    unrelated_sql
        .batch_execute("INSERT INTO isolation_receipt VALUES (2)")
        .await
        .unwrap();
    assert!(unrelated.is_running().await);
    unrelated.stop().await.unwrap();
}

#[test(tokio::test)]
#[serial]
async fn cancelled_stop_and_readiness_failure_reap_owned_children() {
    use postgres_replicated::testing::{ProcessProbe, wrapped_pg_bin};
    use std::sync::Arc;
    use std::time::Duration;
    for readiness in [false, true] {
        let root = TestDataDir::new("owned-cancel");
        let marker = root.path().join("command-started");
        let command = if readiness { "pg_isready" } else { "postgres" };
        let script = if readiness {
            format!(
                "#!/bin/sh\necho $$ > '{}'\nexec sleep 30\n",
                marker.display()
            )
        } else {
            format!(
                "#!/bin/sh\ntrap '' INT QUIT\necho $$ > '{}'\n\"{}/postgres\" \"$@\" &\nwhile :; do :; done\n",
                marker.display(),
                find_pg_bin().display()
            )
        };
        let instance = Arc::new(PgInstanceManager::new(
            root.path().join("pgdata"),
            wrapped_pg_bin(root.path(), command, &script),
            allocate_port().await,
        ));
        let (faults, _) = mpsc::channel(8);
        instance.init_db().await.unwrap();
        let cloned = instance.clone();
        let operation = if readiness {
            tokio::spawn(async move { cloned.start_native(faults).await })
        } else {
            instance.start_native(faults).await.unwrap();
            tokio::spawn(async move { cloned.stop().await })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while !marker.exists() || !instance.data_dir().join("postmaster.pid").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let processes = ProcessProbe::postgres(instance.data_dir());
        if readiness {
            assert!(
                operation
                    .await
                    .unwrap()
                    .unwrap_err()
                    .to_string()
                    .contains("pg_isready command timeout")
            );
        } else {
            // Let stop dispatch its blocking cleanup before cancelling its caller.
            tokio::time::sleep(Duration::from_millis(100)).await;
            operation.abort();
            assert!(operation.await.unwrap_err().is_cancelled());
            let error = tokio::time::timeout(Duration::from_secs(8), instance.stop())
                .await
                .unwrap()
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("fast PostgreSQL shutdown timed out"),
                "{error}"
            );
        }
        processes.assert_gone().await;
        let command_pid = std::fs::read_to_string(&marker).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while std::path::Path::new(&format!("/proc/{}", command_pid.trim())).exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cancelled PostgreSQL helper command must also be reaped");
        assert!(!instance.is_running().await);
        assert!(instance.connect().await.is_err());
        drop(instance);
    }
}

#[test(tokio::test)]
#[serial]
async fn readiness_binds_postmaster_without_pid_file() {
    use postgres_replicated::testing::{ProcessProbe, wrapped_pg_bin};
    let root = TestDataDir::new("ready-hidden-pid");
    let data = root.path().join("pgdata");
    let script = format!(
        "#!/bin/sh\n\"{}/pg_isready\" \"$@\"\nresult=$?\nif [ \"$result\" -eq 0 ] && [ -f '{}/postmaster.pid' ]; then mv '{}/postmaster.pid' '{}/postmaster.pid.hidden'; fi\nexit \"$result\"\n",
        find_pg_bin().display(),
        data.display(),
        data.display(),
        data.display()
    );
    let instance = PgInstanceManager::new(
        data,
        wrapped_pg_bin(root.path(), "pg_isready", &script),
        allocate_port().await,
    );
    let (faults, mut received) = mpsc::channel(8);
    instance.init_db().await.unwrap();
    instance.start_native(faults).await.unwrap();
    assert!(!instance.data_dir().join("postmaster.pid").exists());
    assert!(instance.is_running().await);
    let pid = std::fs::read_to_string(instance.data_dir().join("postmaster.pid.hidden"))
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let postmaster = ProcessProbe::process(pid);
    let (sql, _) = instance.connect().await.unwrap();
    sql.batch_execute("CREATE TABLE hidden_receipt(id int); INSERT INTO hidden_receipt VALUES (1)")
        .await
        .unwrap();
    assert!(received.try_recv().is_err());
    assert!(instance.stop().await.is_err());
    assert!(instance.stop().await.is_err());
    postmaster.assert_gone().await;
    assert!(
        sql.batch_execute("INSERT INTO hidden_receipt VALUES (2)")
            .await
            .is_err()
    );
    assert!(instance.connect().await.is_err());
}

#[test(tokio::test)]
#[serial]
async fn monitor_observes_postmaster_exit_even_with_live_launcher() {
    use postgres_replicated::testing::{ProcessProbe, wrapped_pg_bin};
    use std::time::Duration;
    let root = TestDataDir::new("postmaster-exit");
    let marker = root.path().join("launcher");
    let script = format!(
        "#!/bin/sh\ntrap '' INT QUIT\necho $$ > '{}'\n\"{}/postgres\" \"$@\" &\nwhile :; do :; done\n",
        marker.display(),
        find_pg_bin().display()
    );
    let instance = PgInstanceManager::new(
        root.path().join("pgdata"),
        wrapped_pg_bin(root.path(), "postgres", &script),
        allocate_port().await,
    );
    let (faults, mut received) = mpsc::channel(8);
    instance.init_db().await.unwrap();
    instance.start_native(faults).await.unwrap();
    let launcher_pid = std::fs::read_to_string(marker)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let launcher = ProcessProbe::process(launcher_pid);
    let postmaster = ProcessProbe::postgres(instance.data_dir());
    let stopped = tokio::process::Command::new(find_pg_bin().join("pg_ctl"))
        .args(["stop", "-D"])
        .arg(instance.data_dir())
        .args(["-m", "immediate", "-w", "-t", "2"])
        .output()
        .await
        .unwrap();
    assert!(stopped.status.success());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), received.recv())
            .await
            .unwrap(),
        Some(kuberic_runtime::protocol::types::FaultType::Permanent)
    );
    assert!(std::path::Path::new(&format!("/proc/{launcher_pid}")).exists());
    assert!(!instance.is_running().await);
    instance.stop().await.unwrap();
    instance.stop().await.unwrap();
    launcher.assert_gone().await;
    postmaster.assert_gone().await;
}
