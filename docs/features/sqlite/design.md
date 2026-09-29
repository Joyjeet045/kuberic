# SQLite: Replicated SQLite on Kuberic V2

The existing `examples/sqlite` application (`sqlite-replicated`) is migrated
in place to the level-triggered v2 stack. There is no second SQLite application
and no classic runtime/operator dependency. The primary serves SQL over gRPC;
secondaries retain durable WAL-frame history but do not serve client SQL.

This is an experimental source/runtime migration, not a deployed-data upgrade.
Use fresh v2 storage with protocol 8 / agent schema 4. There is no v1 data,
authority, or metadata import path. SQLite-specific images, manifests, published
deployment assets, and live-cluster validation remain separate distribution work.

---

## Architecture

```text
SQL client -> SqliteServer -> serialized SQLite connection
                              -> instance commit-barrier VFS
                              -> async StateReplicator::replicate
                              -> durable local acceptance + admitted quorum
                              -> durable commitment -> local WAL publication

ReplicaHost -> agent authority/effect/write/build journals
            -> StatefulServiceReplica lifecycle + StateProvider callbacks

Secondary stream -> SqlitePersistence base/history -> fsync -> explicit ACK
Promotion -> authority-certified commit -> committed database materialization
```

[`SqliteService`](../../../examples/sqlite/src/service.rs) implements the public
v2 lifecycle and creates the default replicator with a separate
[`SqlitePersistence`](../../../examples/sqlite/src/state.rs). The
[`ReplicaHost` entry point](../../../examples/sqlite/src/main.rs) owns agent
startup and recovery; the application owns the SQL listener. Applications never
select their own membership, verified prefix, or write grant.

Each service instance registers a process-unique VFS name, including in-process
restarts. Barrier sender, receipt, cancellation, and same-process fence state are
instance-owned. SQLite registrations last for the process lifetime; the example
does not provide VFS unloading.

## Quorum Before Publication

The VFS stages the transaction's WAL bytes before its commit frame becomes
recoverable. It calls the barrier when the complete commit frame arrives, not
from a post-commit WAL hook. The synchronous SQL thread waits for an asynchronous
replication worker without holding the application-persistence lock.

The worker checks v2 write access, durably records reconciliation intent, then
dispatches immutable transaction bytes to `StateReplicator`. The agent reserves
the exact operation identity/LSN before application acceptance. Successful
replication requires durable local acceptance, every admitted configuration's
write quorum, and durable application commitment. Only then does the VFS publish
the local WAL bytes. Successful SQLite publication is confirmed before returning
client success.

Transactions replicate logical page images, not SQL text or WAL-file-specific
salts. Non-deterministic SQL therefore executes only on the primary.
[`WalFrameSet`](../../../examples/sqlite/src/frames.rs) retains the exact encoded
operation, checksums, full-page geometry, and final database size. A shrinking
transaction may contain spilled frames above that final size: their bytes remain
in history, but reconstruction skips them and enforces the final image length.
Large staged transactions flush to the parent VFS in bounded writes after one
barrier decision; this does not bound total transaction memory.

## Durable Storage and Recovery

```text
<data-root>/
  .kuberic/agent.sqlite3       agent identity, authority, effects, writes, builds
  application/
    state-v2.json             checksummed progress/fences and copy retry evidence
    base-<generation>.sqlite  immutable copied committed base
    history-<generation>.log  exact post-base operations and original watermarks
    db.sqlite                 rematerialized committed SQL view
    db.sqlite-wal             live primary WAL, never independent recovery authority
    db.sqlite-shm             possible stale SQLite companion, removed on materialization
```

Application progress satisfies `0 <= base <= committed <= applied`. Applied means
durable operation acceptance; it does **not** by itself mean client success or
SQL visibility. Records are length-prefixed and checksummed. The fsynced,
atomically replaced manifest publishes progress and the accepted log length.
Reopen may truncate only a tail beyond that length. Missing or corrupt content
covered by recorded progress causes a rebuild-required fence rather than silently
regressing progress. Invalid or missing established metadata fails closed.

The live database is reconstructed from the base plus committed history. All
connections must be closed first; stale `db.sqlite-wal` and `db.sqlite-shm` are
removed and the reconstructed file is durably replaced before SQLite opens.
Changing epochs does not discard retained bytes or unresolved reservations.

| Durable state | Meaning and recovery |
|---|---|
| Healthy | SQL still requires accepted v2 access and an active primary application. |
| Reconciliation required | Dispatched write/publication outcome is unresolved, or committed history must be rematerialized. A failure makes the current instance sticky-fenced. Reopen resolves the exact agent journal and rematerializes committed state before clearing reconciliation. |
| Rebuild required | Acknowledged application content is missing/corrupt. Ordinary reopen or reconciliation cannot clear it. |
| Rebuilding | An accepted snapshot is installed but required recovery progress is not restored. Remain SQL-fenced until contiguous catch-up and commitment satisfy the retained recovery floors. |

The runtime settles only an authority-fenced verified prefix before the Primary
callback. Previously committed state never regresses; unresolved primary-local
reservations still require exact quorum recovery. Grant-time recovery can advance
commitment after that callback, so the first SQL request refreshes the committed
materialization under request/connection serialization. New writes are rejected
while an applied suffix remains unsettled; promotion alone cannot make arbitrary
applied bytes SQL-visible.

## Committed Snapshot and Retained Catch-Up

The SF-shaped callback remains `StateProvider::get_copy_state(up_to_lsn, ...)`.
The agent freezes one durable **committed snapshot boundary** in immutable build
authority before exposing any snapshot chunk. Enumeration rebuilds the exact
historical committed image without checkpointing or reading the current live
connection. Requests below the retained base or above committed progress reject.

A fresh receiver installs `base = applied = committed = up_to_lsn`. Every source
operation above that boundary, including an applied-but-uncommitted suffix,
travels separately as ordinary retained catch-up with its original bytes and
committed watermark. Build admission also requires the independent frozen
post-enumeration catch-up boundary; snapshot completion alone is insufficient.

For source applied=10/committed=9, copy contains image 9 and catch-up carries
operation 10 with watermark 9. Repeated source enumeration after commitment
advances or either store reopens returns identical snapshot bytes for the same
build. Target copy chunks and completion are durable/idempotent; conflicting
sequence contents or completion boundaries reject. Final markers require
`lsn = committed_lsn = replication_boundary_lsn`, including duplicate replay
after staging cleanup, later catch-up, and restart.

Copy installation switches the manifest to a complete new base/history
generation. Completed-copy identity and exact chunk evidence survive cleanup of
old generations. An interruption after application installation but before agent
completion replays the same completion without losing later accepted history.

## Client API and Failure Semantics

The existing [SQL protobuf service](../../../examples/sqlite/proto/sqlitestore.proto)
provides:

| RPC | Contract |
|---|---|
| Execute | One statement in one application-owned transaction; affected rows, insert row ID, and committed LSN. |
| ExecuteBatch | Statements in one transaction; one replication per committing batch and per-statement row counts. |
| Query | Read-only statement returning typed values; primary-only accepted read access. |

The connection is serialized, exclusive-locking, WAL/FULL-synchronous, with
automatic checkpoints disabled. The authorizer rejects client transaction
control, attached databases, temporary database objects, virtual tables, and
unsafe pragmas. Query cannot execute writes. SQL cannot switch journaling off
or commit through a second database; this is intentionally not unrestricted
SQLite connection access.

| Outcome | gRPC code / action |
|---|---|
| Accepted and published | `OK`; quorum durability and local publication succeeded. A singleton has no redundancy. |
| Authority/access closed or known pre-dispatch barrier rejection | `UNAVAILABLE`; a new rejected request did not reserve an operation. |
| SQL validation/constraint failure before publication | `INVALID_ARGUMENT`; no successful transaction. |
| Failure after dispatch | Conservatively `UNKNOWN`, even if application history has not advanced: the agent may have a reservation. Reopen/reconcile and verify before retrying. |
| Reconciliation/rebuild/unsettled suffix prevents SQL | `FAILED_PRECONDITION` while authority is otherwise available; closed roles/access may reject earlier. |

An unknown outcome is not proof of rollback, nor proof of quorum completion.
Transport timeout/disconnect is also ambiguous. The SQL API has no client
idempotency key: use application-level uniqueness and verify results before
retrying non-idempotent statements. Already-committed responses may arrive after
authority revocation; these are not new successful stale-primary writes.

`on_data_loss` returns **state unchanged** and performs no destructive recovery.
Close/Abort fence the service and stop barrier/drain work; they do not delete the
application directory.

## Lifecycle and Reconfiguration

Unit and in-process tests use actual SQLite WAL frames and reopened agent
`SqliteStore`/application directories with fresh process sessions. They cover
bootstrap, replacement, ordinary failover, planned switchover, sequential
scale-up, secondary-only scale-down, and quorum loss/restoration. Incoming
primaries expose the last acknowledged row before any new write, including when
the secondary's committed watermark was one operation behind.

The application fixture drives validated agent/runtime effects; it is not a
Kubernetes controller, placement engine, or end-to-end deployment test. Stale
probes use unique IDs, accept only definitive authority/access rejection, and
verify no reservation/history/progress/file/row side effects. Exact expected row
sets and per-LSN prefixes include known recovered unknown transactions. Handoff
checkpoints test an old application that remains alive but fenced while the
target accepts new writes, separately from delayed old committed responses.

## Local Validation

From the repository root, with the pinned Rust toolchain and `protoc` available:

```bash
cargo test -p sqlite-commit-barrier -p sqlite-replicated --all-features -- --test-threads=1
cargo clippy -p kuberic-agent -p sqlite-replicated --all-targets --all-features -- -D warnings
scripts/check_level_triggered_scope.sh origin/main
scripts/check_level_triggered_dependencies.sh
scripts/check_level_triggered_guards_test.sh
scripts/check_level_triggered_documentation.sh docs/features/sqlite/design.md
```

These SQLite tests require no KinD, Kubernetes API, container runtime, or child
process launched by a test. The separate `relative_root` target owns its temporary
working directory so parallel library tests are unaffected. Agent/protocol
selections excluding their existing subprocess tests are documented in the
[testing guide](../kuberic/testing.md#sqlite-v2-unit-and-in-process-validation).

`kuberic-agent`'s opt-in `testing::InProcessTransport` binds full replica identities
and sessions, separates received/applied ACKs, routes copy ACKs, and surfaces
Build/Remove/Evict outputs without granting authority. Its `pump()` idle result
means no ready or in-flight delivery, not cluster convergence. Pause/failure
controls are absent from production-default builds.

## Startup and Limitations

There is no `--demo` or application-owned self-promotion. `ReplicaHost` needs
agent-authorized fresh initialization or matching established metadata; running
the binary does not grant SQL access. Startup classifies application storage
without creating files and constructs a deferred service. Only its v2 `Open`
callback opens SQLite persistence, after `ReplicaHost` has initialized or
validated agent metadata. An interruption while waiting for
`InitializeAgentStore` therefore leaves fresh application storage empty and
retryable. Genuinely established application data without agent metadata still
reports `Unsafe` and rejects initialization; it is never reclassified as fresh.
The
[entry point](../../../examples/sqlite/src/main.rs) accepts resource/replica,
Pod/PVC identity, namespace, advertised Pod IP, bearer token, data root and
control/replication/application listener settings. Listener defaults are
50051/50052/8080; the last is SQL gRPC, not KVStore2 HTTP.

- Fresh/no-import storage only; no rolling mixed-version or v1 conversion promise.
- One SQLite connection per replica, one database/partition, no multi-writer SQL.
- Retained history and exact copy-retry evidence have no compaction/retention
  policy. Whole transactions and snapshots are buffered; large RPC/message-size
  and memory budgets are not established.
- No throughput, maximum replica-count, or outage guarantee; no group commit.
- Direct primary removal, placement policy, destructive data-loss recovery,
  PostgreSQL migration, and v1 retirement are separate work.
- Image publication, SQLite deployment assets, and SQLite live-cluster tests are
  not provided by this migration.

## References

- [V2 runtime interfaces](../../../kuberic-runtime/README.md)
- [Replica hosting and opt-in transport](../../../kuberic-agent/README.md)
- [Commit-barrier VFS](../../../sqlite-commit-barrier/src/lib.rs)
- [SQLite WAL format](https://sqlite.org/walformat.html)
- [SQLite VFS](https://sqlite.org/vfs.html)
- [V1 retirement and deferred work](../../proposal/v1-retirement-plan.md)
