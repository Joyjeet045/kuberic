# Reliable State Manager

Reliable Collections are a new layer above Kuberic's V1-style opaque replication
boundary, inspired by Service Fabric's V2 Transactional Replicator and Reliable
State Manager. They do not wrap, convert, or depend on the legacy KVStore
implementation. KVStore remains the separate `KeyValueStoreReplica`-style example.

The implementation is split into `kuberic-transaction-log`,
`kuberic-transactional-replicator`, and `kuberic-state-manager`. No operator or
reconciler changes are required. An application starts `StateManager::run`
with the existing PodRuntime lifecycle channel and exposes its own client API.

```rust,no_run
use kuberic_state_manager::StateManager;

async fn transfer(manager: &StateManager) -> kuberic_state_manager::Result<()> {
    let accounts = manager.get_or_add_dictionary::<String, i64>("accounts").await?;
    let audit = manager.get_or_add_dictionary::<String, String>("audit").await?;
    let mut transaction = manager.create_transaction().await?;
    let alice = accounts.get_or_add(&mut transaction, &"alice".into(), 100)?;
    let bob = accounts.get_or_add(&mut transaction, &"bob".into(), 100)?;
    accounts.set(&mut transaction, &"alice".into(), &(alice - 10))?;
    accounts.set(&mut transaction, &"bob".into(), &(bob + 10))?;
    audit.set(&mut transaction, &"last-transfer".into(), &"alice -> bob: 10".into())?;
    let identity = transaction.id().clone();
    let version = transaction.commit().await?;
    assert_eq!(manager.committed_result(identity).await?, Some(version));
    Ok(())
}
```

## Dictionary and Registry API

`ReliableDictionary<Key, Item>` supports get/contains, insert, set, conditional
update, remove, get-or-add, add-or-update, clear and consistent enumeration.
Staged methods are synchronous because they access private transaction state;
begin/commit use async I/O. `Transaction::abort` and dropping the transaction
discard staging without logging or replication.

Provider creation/removal can be staged in the same transaction as data changes.
`get_dictionary` rejects incompatible types/versions. `provider_names` enumerates
the transaction's registry snapshot. Dropped/recreated providers receive a new
identity, invalidating old handles; handles cannot be used with another manager.
Provider names are stable UTF-8 strings of 1-128 bytes. The MVP supports up to
1,024 dictionaries, 1,024 observed keys/mutations per transaction, 64 KiB encoded
keys, and 512 KiB staged changes. Aggregate snapshots and contexts are separately
bounded by the transactional coordinator.

`ReliableValue` requires a stable explicit `TYPE_ID` and schema `VERSION`.
String, byte-vector, i64, u64 and bool have built-in implementations. Custom
types implement the trait and serde; serialization must be deterministic across
replicas, including map order and schema evolution. Postcard binary encoding,
format IDs, checksums and ordered registry/key maps define persisted records.
Incompatible schema changes are rejected; automatic migration is not provided.

## Isolation

Transactions capture a repeatable begin-time snapshot and read their own writes.
Commit validates per-key logical versions and provider identities against current
state. Deletes retain version tombstones, so absent-key insert/delete ABA is
detected. Dictionary enumeration, clear and provider removal observe a provider
revision; registry enumeration observes registry revision. This prevents phantom
rows and write skew while allowing disjoint point writes to commit concurrently.
Conflicts return `Error::Conflict`; repeat the application logic in a new
transaction. Stale epochs, expired contexts, resource exhaustion, incompatible
providers and conflicting retry identities have distinct errors.

Each new transaction commit, including read-only commits, has one replicated record
and LSN. Atomic publication covers the registry, every affected dictionary and
the commit result. A retained `TransactionId` can query the original result
after a lost response, promotion or restart. `with_identity` supports replaying
the same request; changing its mutation payload is rejected. Queries return
`Error::UnconfirmedCommit` when recovery has retained a result without quorum
confirmation. An identical retry obtains quorum through an additional record,
without applying mutations again, and returns the original commit version.
Alternatively, commit a new read-only transaction to confirm the adopted prefix
and query the original identity again. For provider
creation retries, query the original identity before constructing new providers,
whose incarnation IDs intentionally differ.

## Lifecycle and Recovery

The coordinator owns lifecycle open, copy/replication drain, role/epoch fencing,
close and abort. Persistence runs on blocking workers rather than Tokio threads.
State providers apply against a private candidate snapshot and become visible
only at a single publication point. Checkpoint and copy contain the entire
registry, type metadata, dictionary state, tombstones, and retry outcomes.
Old log bytes are reclaimed only after durable checkpoint publication. Primary
and secondary writes automatically checkpoint confirmed prefixes near the log
budget; unconfirmed suffixes remain available for recovery or epoch rollback.

See the sibling transactional-replicator README for accepted/durable/committed/
applied/checkpointed distinctions, in-doubt outcomes, bounded retry retention,
backup/restore administration, and exact size limits.

## Scope and Verification

This implements the dictionary MVP from #52. ReliableQueue, pessimistic lock
modes, group commit, time-based checkpoint scheduling and schema migration remain
later phases, not implied capabilities. Backups are local atomic files; external
backup storage and cluster-wide restore orchestration belong to the caller.

```sh
cargo test -p kuberic-transaction-log -p kuberic-transactional-replicator -p kuberic-state-manager
cargo clippy -p kuberic-transaction-log -p kuberic-transactional-replicator -p kuberic-state-manager --all-targets -- -D warnings
```

Tests cover cross-provider atomic visibility, key/range/registry conflicts,
conditional mutations, abort/expiry/admission, lost replies and deduplication,
log corruption/torn tails, failed checkpoint publication, stale transaction
contexts, actual gRPC copy failures, three-replica copy/catch-up, promotion,
checkpoint/backup/restore, abrupt process exit across commit boundaries and
restart recovery. Existing custom providers and examples
are unchanged and remain covered by workspace CI.