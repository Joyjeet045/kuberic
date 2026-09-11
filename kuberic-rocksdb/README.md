# RocksDB State Provider

`kuberic-rocksdb` is an adapter over `kuberic-core`, independent from the
operator, reconciler and other example storage providers.

```rust,no_run
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use kuberic_rocksdb::{Mutation, RocksReplica};
let replica = RocksReplica::open("data/rocks".into()).await?;
let bundle = kuberic_core::pod::PodRuntime::builder(1).build().await?;
let runtime = tokio::spawn(bundle.runtime.serve());
let service = tokio::spawn(replica.clone().run(bundle.lifecycle_rx));
// The control plane must open/configure/promote the replica before client writes.
let lsn = replica.write(vec![Mutation::Put {
    key: b"account".to_vec(), value: b"100".to_vec(),
}]).await?;
# Ok(())
# }
```

## Durability and Ordering

A serialized commit gate sends a versioned, checksummed envelope containing a
RocksDB `WriteBatch` to Kuberic's quorum replicator before local application.
Only a successful quorum result and local RocksDB write return success.
The original batch bytes are reconstructed using `WriteBatch::from_data`.
Typed mutation metadata permits validation against the canonical batch before
any opaque native batch is applied. Client cancellation does not abandon an
in-flight commit. Replication/local failures fence the adapter and report a
transient fault; recovery or rebuild is required before further writes.

Secondaries atomically write the user mutations, Kuberic LSN and undo history
in one RocksDB write with WAL enabled and `sync=true`, then acknowledge. An
accepted network operation is not durable progress until this completes.
RocksDB sequence numbers are never used as replication LSNs. Repeated LSNs do
not apply twice; gaps fail explicitly. An unavailable write has an uncertain
outcome and must not be blindly retried if it contains a non-idempotent merge.

## Checkpoints, Copy and Rollback

Copy uses a physical RocksDB checkpoint. If the requested copy LSN precedes
the local tail, undo records reconstruct that exact boundary in a private
checkpoint. Files are transferred in bounded chunks with profile and checksum
validation. Restore writes a new database generation, syncs its files, verifies
its LSN, and atomically publishes the active-generation pointer. Copy progress
is acknowledged only after this installation, never at network EOF.

Epoch callbacks remove an unauthorized tail atomically, including its LSN and
undo records. Promotion waits for copy/recovery and rejects a failed adapter.
The first version retains undo history in RocksDB without automatic pruning;
operators must provision disk for the retained history. A missing undo record
rejects rollback/copy at that boundary and requires a full checkpoint rebuild.
Snapshots are bounded to 256 MiB; batches are limited to 1,024 mutations and a
1 MiB encoded envelope. Oversized inputs fail explicitly. Native database I/O
runs on blocking workers, not the Tokio runtime thread.

## Supported Configuration

- Pinned Rust binding `rocksdb` 0.24.0, RocksDB 10.4.2; identical binaries and
  the fixed configuration profile are required across replicas and restarts.
- Default column family only. `write_column_family` rejects other names.
  The adapter owns database access and reserves a private metadata keyspace.
- Fixed bytewise comparator, no prefix extractor, no compression, and the
  deterministic `append-v1` merge operator (append operand bytes in order).
- Only typed puts, deletes and merges are exposed. WAL-disabled writes,
  external SST ingestion, custom options/comparators/merge operators, and
  dynamic column-family changes are not exposed and cannot bypass replication.
- `TransactionDB`, WritePrepared and WriteUnprepared are unsupported. This is
  serialized `WriteBatch` replication, not physical WAL tailing or interception.
- A custom C++ Env/FileSystem shim is not required. `GetUpdatesSince` is not
  used as an authoritative protocol.

## Build and Tests

The native build requires a C++17 compiler and libclang. Linux CI installs
`clang` and `libclang-dev`. Windows requires MSVC Build Tools, Windows SDK,
`LIBCLANG_PATH`, and bindgen include paths if libclang is distributed alone.

```sh
cargo test -p kuberic-rocksdb
cargo clippy -p kuberic-rocksdb --all-targets -- -D warnings
```

Tests use real RocksDB files and real Kuberic gRPC control/data planes. They
cover batch/LSN atomicity, abrupt process exit, checkpoint restore/catch-up,
rollback, merges/deletes, malformed and incompatible records, three-replica
quorum writes, concurrent ordering, promotion, switchover and restart.