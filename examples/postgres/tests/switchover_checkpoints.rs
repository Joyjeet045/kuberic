use postgres_replicated::testing::{PgGroup, run_pg_test};

#[test_log::test]
fn repeated_handoffs_and_failovers_cover_every_identity_and_rebuild_former_primaries() {
    run_pg_test(|| async {
        let mut group = PgGroup::singleton().await;
        group.add(2).await;
        group.add(3).await;
        for (target, planned) in [
            (2, true),
            (3, false),
            (1, true),
            (2, false),
            (3, true),
            (1, false),
        ] {
            tracing::info!(target, planned, "rotating PostgreSQL primary");
            let previous = group.primary_id();
            let ordinary = group.session(previous, false).await;
            let administrative = group.session(previous, true).await;
            let one = group.next_probe();
            let two = group.next_probe();
            let old = group.change_primary(target, planned).await;
            ordinary.rejected(one).await;
            administrative.rejected(two).await;
            ordinary.disconnected().await;
            administrative.disconnected().await;
            group.write("new primary").await;
            tracing::info!(old, "rebuilding former PostgreSQL primary");
            group.rejoin(old).await;
        }
        group.shutdown().await;
    });
}
