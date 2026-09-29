use sqlite_replicated::testing::{SqlitePod, scratch};

#[tokio::test]
async fn v2_data_loss_callback_reports_unchanged_and_preserves_sql_and_progress() {
    let root = scratch();
    let pod = SqlitePod::singleton(root.path().join("replica")).await;
    pod.execute("CREATE TABLE data(id INTEGER)").await.unwrap();
    pod.execute("INSERT INTO data VALUES(1)").await.unwrap();
    let before = pod.application.persistence().progress().unwrap();
    let snapshot = pod
        .application
        .persistence()
        .snapshot(before.committed_lsn)
        .unwrap();
    assert!(
        !pod.runtime
            .primary_replicator()
            .await
            .unwrap()
            .on_data_loss()
            .await
            .unwrap()
    );
    assert_eq!(pod.application.persistence().progress().unwrap(), before);
    assert_eq!(
        pod.application
            .persistence()
            .snapshot(before.committed_lsn)
            .unwrap(),
        snapshot
    );
    assert_eq!(pod.count().await, 1);
}
