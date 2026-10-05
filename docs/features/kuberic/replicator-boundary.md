# Replicator boundary and native proof

Kuberic hosts built-in and application-owned replicators through the same
Service Fabric-shaped public interfaces. Standard lifecycle and primary
operations always cross `Replicator` or `PrimaryReplicator`; the built-in
engine additionally supplies an unpublished native capability for durable
replication proof.

## Ownership

The agent owns desired-state ordering, durable effect intent and completion,
process/session admission, peer transport, endpoint ownership, and the
externally visible read/write projection. The built-in replicator owns local
write journals, quorum commitment, copy progress, pending-write recovery,
committed-prefix reconciliation, native topology evidence, and local write
fencing.

These responsibilities do not move at a standard-operation boundary.
Successful public catch-up, build, and ordinary removal completion is the
built-in engine's durable completion contract. The agent validates that its
authority, sessions, target, and attempt are still current, then commits the
common effect; it does not fetch a second native receipt.

## Coherent interface bundle

`ReplicatorInterfaces` binds public control, optional primary/state interfaces,
and optional host-only capabilities to one creation identity. Cancellation,
failed attachment, or an Open identity mismatch abandons the whole creation.
Wrappers that retain the built-in engine must consume the original bundle with
`wrap_primary`; rebuilding a bundle with `primary` intentionally creates an
independent public bundle without the original native provenance.

Independent custom replicators return only the public interfaces they
implement. A custom `StateReplicator` does not imply the presence of the
built-in native capability.

## Standard operation routing

Open, role, epoch, Close, progress, data-loss, configuration, catch-up, build,
and removal use the public interfaces. Hosted build/remove transport remains
agent-owned:

- one admitted build invokes the returned public primary once;
- built-in copy delivery and public completion must both succeed;
- cancellation or either branch failing cancels the exact build attempt;
- native copy progress alone is not host-accepted completion;
- remove invokes the public primary before the common Remove dispatch.

The private lifecycle contract contains local write fencing/access preparation,
pending-write recovery, committed-prefix reconciliation, exact topology proof,
and reporting/recovery observation rather than duplicate standard commands.

## Native topology receipts

Only Kuberic-specific topology operations retain private durable receipts:
certified-prefix settlement, switchover preparation, secondary removal, and
retirement. `TopologyReceipt` stores those canonical payloads directly in an
effect result. There is no catch-up/build/removal receipt layer and no generic
operation-evidence wrapper duplicating the same payload.

The host revalidates topology receipts against the durable intent and current
authority. Standard-operation staleness is validated from host admission plus
the native topology fence captured around the public call.

## Access publication

Native access admission does not itself grant application access. The host
reserves one common projection generation, validates public progress, publishes
native access, and atomically updates the common and external projection. An
explicit access transaction owns native fencing and projection cleanup while it
waits for host-memory effect acceptance. If the caller is cancelled, the
already-running transaction observes the closed decision channel and performs
rollback; no `Drop` implementation starts asynchronous correctness work. The
adapter then persists applied/completed stages through the existing intent-
first protocol; startup remains write-closed and reissues or reobserves an
interrupted persistence boundary.

Cancellation, publication failure, or configuration invalidation fences native
writes and clears an unaccepted projection. Rollback is generation-scoped, so
it cannot revoke a newer accepted grant. Final effect acceptance is serialized
with projection invalidation.

## Persistence, reporting, and recovery

SQLite persists effect intent before execution and completion only after the
canonical topology receipt, when present, and the lifecycle postcondition
validate. Existing
JSON fields remain backward-readable through serde defaults; this refactor does
not introduce a database schema migration.

Reporting composes durable agent authority with current public/native
observations. It preserves `live_builds_only`, so old-session build evidence is
not resurrected. Catch-up composition retains an accepted boundary only while
the native engine still reports catch-up complete.

Recovery reconstructs authority and native stores, reissues or reobserves
pending effects, and keeps access closed until public/native proof is accepted.
A previously persisted granted snapshot is never sufficient to reopen writes.

## Compatibility

The application-facing service, partition, factory, public replicator, primary,
and optional state-replicator contracts remain SF-shaped. Native receipts are
not requirements on independent custom replicators. The intentional wrapper
migration boundary is limited to preserving the coherent factory-created
bundle.

The runtime source-public inventory and compile-fail API guards cover hidden
bundle and receipt signatures.

## Validation

The repository validates this boundary through:

```bash
cargo test -p kuberic-runtime --test public_api_inventory
scripts/check_runtime_public_api.sh
cargo nextest run --profile ordinary -p kuberic-agent --features testing \
  --test runtime --test store --test coordinator --test crash_boundaries
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Use the repository `just nextest-*` recipes for the complete ordinary,
PostgreSQL, archive-partition, and KinD matrices.
