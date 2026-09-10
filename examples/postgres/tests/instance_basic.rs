use postgres_replicated::instance::PgInstanceManager;
use postgres_replicated::testing::{allocate_port, cleanup_data_dir, find_pg_bin, temp_data_dir};
use serial_test::serial;
use test_log::test;
use tokio::sync::mpsc;

#[test(tokio::test)]
#[serial]
async fn test_instance_lifecycle() {
    let pg_bin = find_pg_bin();
    let data_dir = temp_data_dir("lifecycle");
    let (fault_tx, _fault_rx) = mpsc::channel(1);

    let instance = PgInstanceManager::new(data_dir.clone(), pg_bin, allocate_port().await);

    // initdb
    instance.init_db().await.expect("initdb should succeed");
    assert!(data_dir.join("PG_VERSION").exists());
    assert!(data_dir.join("postgresql.conf").exists());

    // start
    instance
        .start(fault_tx)
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

    // cleanup
    cleanup_data_dir(&data_dir);
}

#[test(tokio::test)]
#[serial]
async fn test_instance_restart() {
    let pg_bin = find_pg_bin();
    let data_dir = temp_data_dir("restart");
    let (fault_tx, _fault_rx) = mpsc::channel(1);

    let instance = PgInstanceManager::new(data_dir.clone(), pg_bin, 15433);
    instance.init_db().await.unwrap();
    instance.start(fault_tx.clone()).await.unwrap();

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
    instance.start(fault_tx).await.unwrap();

    // Verify data persisted
    let (client, _conn) = instance.connect().await.unwrap();
    let row = client
        .query_one("SELECT v FROM persist WHERE k = 'key1'", &[])
        .await
        .unwrap();
    let v: &str = row.get("v");
    assert_eq!(v, "value1");

    instance.stop().await.unwrap();
    cleanup_data_dir(&data_dir);
}

#[test(tokio::test)]
#[serial]
async fn test_lsn_query() {
    let pg_bin = find_pg_bin();
    let data_dir = temp_data_dir("lsn");
    let (fault_tx, _fault_rx) = mpsc::channel(1);

    let instance = PgInstanceManager::new(data_dir.clone(), pg_bin, allocate_port().await);
    instance.init_db().await.unwrap();
    instance.start(fault_tx).await.unwrap();

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
    cleanup_data_dir(&data_dir);
}
