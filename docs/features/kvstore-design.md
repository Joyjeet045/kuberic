# KVStore: Replicated Key-Value Store Example

Example application demonstrating kuberic-core's full replication protocol.
A gRPC key-value store that replicates writes via quorum, serves reads from
local state, handles copy/replication streams on secondaries, and implements
all StateProvider callbacks.

---

## Architecture

```
Clients (gRPC)
    │
    ▼
┌──────────────────────────────────────────────────────┐
│                    kvstore Pod                     │
│                                                        │
│  ┌──────────────┐    ┌─────────────────────────────┐  │
│  │  PodRuntime   │    │  KV Service (user app)      │  │
│  │               │    │                             │  │
│  │ control gRPC ◄┤    │  ┌───────────────────────┐  │  │
│  │ data    gRPC ◄┤    │  │  KvState              │  │  │
│  │               │    │  │  HashMap<String,String>│  │  │
│  │ lifecycle_rx ─┼───►│  │  last_applied_lsn     │  │  │
│  │ state_prov_rx─┼───►│  └───────────────────────┘  │  │
│  │               │    │                             │  │
│  └──────────────┘    │  client gRPC ◄── Clients    │  │
│                       └─────────────────────────────┘  │
└──────────────────────────────────────────────────────┘
```

**Three gRPC servers per pod:**
1. **Control** (kuberic internal) — operator → PodRuntime
2. **Data** (kuberic internal) — primary → secondary replication
3. **Client** (user-facing) — clients → KV store reads/writes

---

## Client gRPC API

```proto
service KvStore {
    rpc Get(GetRequest) returns (GetResponse);
    rpc Put(PutRequest) returns (PutResponse);
    rpc Delete(DeleteRequest) returns (DeleteResponse);
    rpc BeginTransaction(BeginTransactionRequest) returns (BeginTransactionResponse);
    rpc TransactionGet(TransactionGetRequest) returns (GetResponse);
    rpc TransactionPut(TransactionPutRequest) returns (TransactionResponse);
    rpc TransactionDelete(TransactionDeleteRequest) returns (TransactionResponse);
    rpc CommitTransaction(TransactionRequest) returns (CommitTransactionResponse);
    rpc AbortTransaction(TransactionRequest) returns (TransactionResponse);
    rpc ExecuteTransaction(ExecuteTransactionRequest) returns (CommitTransactionResponse);
}
```

- **Put**: Primary only. Serializes `KvOp::Put{key,value}` → `replicate()` →
  flushes WAL, then applies to local HashMap → returns LSN.
- **Delete**: Primary only. Serializes `KvOp::Delete{key}` → `replicate()` →
  flushes WAL, then applies to local HashMap → returns LSN.
- **Get**: Reads from local HashMap. Checks `read_status()` — returns
  `UNAVAILABLE` while not primary or reconfiguring. Primary reads remain
  available without a write quorum.

### Transactions

  `BeginTransaction` returns a server-generated transaction ID. Pass that ID to
  the transactional get/put/delete RPCs, then commit or abort. Staging never
  replicates or changes shared data. `CommitTransaction` returns the single LSN
  for the entire transaction, including read-only and empty transactions.

  The Rust wrapper exposes the same protocol:

```rust,no_run
use kvstore::client::KvTransaction;
use kvstore::proto::kv_store_client::KvStoreClient;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("http://127.0.0.1:50051").await?;
    let mut transaction = KvTransaction::begin(client).await?;
    transaction.put("account-a", "90").await?;
    transaction.put("account-b", "110").await?;
    transaction.delete("pending-transfer").await?;
    assert_eq!(transaction.get("account-a").await?.as_deref(), Some("90"));
    let lsn = transaction.commit().await?;
    assert_eq!(transaction.commit().await?, lsn);
    Ok(())
}
```

  After reconnecting to a new primary, `KvTransaction::from_id(client, id)` can
  retry a committed transaction. The wrapper does not send asynchronous work on
  drop: call `abort()` to release staging immediately, or let the handle expire.
  It deliberately does not hide conflicts or automatically retry mutations.

  `ExecuteTransaction` is the stateless alternative: supply a unique transaction
  ID and an ordered list of `TransactionMutation` puts/deletes. It uses the same
  validation, serialization, replication, and deduplication path as interactive
  commit. It does not provide interactive reads or conditional mutations.

### Isolation and Errors

  Transactions use optimistic serializable isolation for point-key operations:

  1. The first read or write of each key captures its value and version. Repeated
    reads return that captured value, overridden by any staged mutation.
  2. Commit holds a shared gate, validates every observed version, replicates one
    `KvOp::Transaction`, flushes one WAL entry, and applies all mutations under
    the state write lock. Deletes retain key versions, detecting an absent key
    that was inserted and deleted while the transaction was open.
  3. Ordinary `Put` and `Delete` hold the same gate through replication and local
    apply. They cannot intervene between validation and commit.

  There is no begin-time MVCC snapshot or range/predicate API. A transaction
  whose observations cannot be serialized is rejected at commit; individual
  nontransactional `Get` calls do not constitute a multi-key snapshot. Readers
  and replicas see the entire applied operation or none of it.

  | Status | Meaning / Recovery |
  |--------|--------------------|
  | `ABORTED` | An observed key changed. Begin a new transaction and repeat the application logic. |
  | `NOT_FOUND` | Interactive handle is unknown, expired, fenced, aborted, or outside retained commit history. |
  | `UNAVAILABLE` | Not writable primary, reconfiguration, quorum failure, or uncertain persistence. Reconnect/recover, then retry the same commit ID. |
  | `RESOURCE_EXHAUSTED` | Transaction capacity, key/mutation count, or byte budget exceeded. |
  | `INVALID_ARGUMENT` | Invalid timeout, batch ID, or missing mutation. |
  | `FAILED_PRECONDITION` | Abort attempted for an already committed ID. |

  Abort is idempotent for unknown or already-aborted IDs. Uncommitted handles
  are local to a primary epoch and do not survive role changes or restart.
  The default and maximum lifetime is 60 seconds; `timeout_ms` can request a
  shorter lifetime. Accesses do not extend the deadline. Cleanup runs every
  second even without requests. Each server permits at most 1,024 handles,
  1,024 observed keys and 1,024 mutations per handle, and 1 MiB of captured
  keys/values and staged mutation payload. JSON-encoded commits are additionally
  limited to 3 MiB to fit the replication transport.

### Commit Retries and Durability

  A transaction ID identifies its first retained successful commit. Concurrent
  or lost-response retries return that commit's original LSN without replicating
  or applying again, even if the retried batch contains different mutations.
  Commit execution continues if its RPC caller disconnects. An uncertain write
  blocks further writes until the replica is restarted/recovered and reports
  a transient replica fault. An epoch change alone does not clear that block.
  The client must not assume an unavailable response means the write aborted.

  Each replica retains the latest 1,024 transaction IDs, ordered by commit LSN.
  These records are part of WAL recovery, checkpoints, and copy snapshots.
  Ordinary writes do not consume this history. Checkpointing does not discard
  retained IDs. Rollback removes the result and all mutations of a discarded
  transaction together. This is **bounded idempotency**, not an unlimited
  exactly-once guarantee: after eviction, an interactive commit returns
  `NOT_FOUND`, and submitting that old ID to `ExecuteTransaction` is a new batch.
  Use fresh unique IDs and resolve uncertain commits within the retention window.

  Copy transfers a snapshot at the replicator's requested LSN in bounded chunks.
  The receiver stages the chunks and persists/installs one snapshot only when it
  is complete; incomplete copies never expose partial data. WAL replay likewise
  stops before a truncated transaction record, including torn UTF-8 values.
  Actual WAL I/O errors fail recovery rather than truncating valid history.
  WAL failures never publish any of that operation's mutations and prevent
  later operations from passing it. Copy failures remain failed when promotion
  is retried; a failed completion cannot be mistaken for a completed copy.

  Configuration-only epoch changes preserve accepted progress: the final quorum
  ACK can precede propagation of the primary's commit watermark. Truncating to
  that lagging watermark would lose a successful transaction during immediate
  failover. Secondary election progress is published only after the application
  acknowledges WAL persistence, not on network receipt. Data-loss epoch changes
  may still request rollback to the known commit boundary. Failed epoch updates
  retain the previous epoch so a retry uses the same rollback boundary. As with
  ordinary operations, a commit with an unknown outcome may be retained by the
  elected primary.

  Existing Get/Put/Delete messages and old WAL/snapshot files remain readable.
  Transaction records and chunked copy require all participating KVStore replicas
  to run the new code; mixed-version replication is not supported.

### Service Fabric Comparison

  The handle lifecycle follows Reliable Collections and `KeyValueStoreReplica`:
  create a transaction, perform reads and mutations, commit atomically, or abort.
  All affected keys share one commit result, and uncommitted writes remain private.

  This example uses optimistic version validation instead of Service Fabric's
  lock-based transaction coordination. Conflicts return `ABORTED` at commit;
  applications must repeat their read/modify/write logic in a new transaction.
  It is not a Reliable State Manager, a distributed transaction coordinator,
  or a wire-compatible Service Fabric API. It provides one string-keyed store
  per partition, without named collections, cross-partition transactions, range
  locking, or configurable isolation levels.

---

## Replicated Data Format

Operations are serialized as JSON for simplicity (a production app would
use protobuf or a binary format):

```rust
#[derive(Serialize, Deserialize)]
enum KvOp {
    Put { key: String, value: String },
    Delete { key: String },
    Transaction { transaction_id: String, mutations: Vec<KvMutation> },
    Snapshot(SnapshotData),
}
```

Each `replicate()` call sends one `KvOp`. The LSN is assigned by the
replicator actor (monotonic counter in its single `select!` loop) and
returned to the caller. Users never provide LSNs — this is a deliberate
design choice matching SF's `IReplicator::Replicate()`:

- **Total ordering:** The actor serializes LSN assignment, guaranteeing
  monotonic, gap-free sequences. User-generated LSNs could arrive out of
  order from concurrent callers, breaking QuorumTracker, ReplicationQueue,
  and secondary WAL replay which all assume monotonic LSNs.
- **No gaps or duplicates:** The counter guarantees contiguity. Users
  could produce gaps (1, 2, 5) or duplicates (1, 2, 2), corrupting
  replication queue lookups and quorum tracking.
- **Epoch fencing:** The replicator gates LSN assignment on primary role.
  User-generated LSNs would bypass this — a stale primary could generate
  conflicting LSNs during a network partition.

LSN generation is a core replicator responsibility. Moving it to the
user would require the user to solve distributed total-order broadcast
— which is the problem the replicator exists to solve.

---

## Shared State

```rust
struct KvState {
    data: HashMap<String, String>,
    last_applied_lsn: Lsn,
    committed_lsn: Lsn,
    wal_writer: BufWriter<tokio::fs::File>,
    data_dir: PathBuf,
}
```

Wrapped in `Arc<RwLock<KvState>>` and shared between:
- The gRPC client server (reads + writes on primary)
- The lifecycle/state_provider event loop
- Copy/replication stream drain tasks on secondary

---

## Lifecycle Event Handling

| Event | Action |
|-------|--------|
| **Open** | `KvState::open(data_dir)` — load snapshot + replay WAL |
| **ChangeRole(IdleSecondary)** | Spawn `drain_copy_stream` — stage chunks and install the complete snapshot through `apply_op()` |
| **ChangeRole(ActiveSecondary)** | Wait for copy drain to finish naturally, checkpoint, then spawn `drain_stream` for `replication_stream` |
| **ChangeRole(Primary)** | Start client gRPC server |
| **Close** | Cancel drains, stop client server, checkpoint for fast recovery |
| **Abort** | Cancel drains, stop client server (no checkpoint) |

## StateProvider Callbacks

| Callback | Behavior |
|----------|----------|
| **GetLastCommittedLsn** | Return `state.last_applied_lsn` |
| **GetCopyContext** | Send secondary's `last_applied_lsn` as copy context |
| **GetCopyState** | Produce a snapshot at `up_to_lsn`, including versions and commit IDs; send bounded chunks after releasing the read lock |
| **UpdateEpoch** | Fence active transactions and roll back when `previous_epoch_last_lsn < current_lsn`, including rollback to zero; propagate rollback failures |
| **OnDataLoss** | Accept state as-is (return `false`) |

---

## Data Flow

**Primary write:**
Client → KvServer → check `write_status()` → serialize `KvOp` →
`replicator.replicate()` (quorum) → `apply_op()` (WAL flush, then memory) →
return LSN.

**Secondary replication:**
Primary replicator → gRPC ReplicationItem → SecondaryReceiver →
OperationStream → `drain_stream` → `apply_op()` (WAL flush, then memory) →
`acknowledge()` → ACK back to primary → quorum gate released.

---

## File Structure

```
examples/kvstore/
├── Cargo.toml
├── build.rs
├── proto/kvstore.proto           # Client API: Get/Put/Delete and transactions
├── src/
│   ├── lib.rs                    # Module declarations
│   ├── client.rs                 # Rust KvTransaction wrapper
│   ├── transactions.rs           # Optimistic staging, validation and expiry
│   ├── main.rs                   # Binary entry point + CLI args (--data-dir)
│   ├── state.rs                  # KvOp, KvState, SharedState, drain_stream
│   ├── persistence.rs            # WAL/snapshot helpers: load, replay, checkpoint, rollback
│   ├── server.rs                 # Client-facing KV gRPC server (KvServer)
│   ├── service.rs                # Lifecycle + StateProvider event loop
│   ├── testing.rs                # KvPod helper (feature = "testing")
│   └── demo.rs                   # Operator/client simulators for --demo mode
└── tests/
  ├── transactions.rs           # Transaction API and recovery coverage
    ├── reconciler.rs             # Durable workflow integration tests
    │                              # Includes build-buffer + quorum-loss E2E
    └── durable_data_loss.rs      # Correlated data-loss/replay tests
```

## Implementation Notes

- **Thread safety**: `KvState` is `Arc<RwLock<...>>`. `tokio::sync::RwLock`
  is designed to be held across `.await`. The write lock is held across
  WAL I/O (~1ms) — this is temporary contention, not a deadlock.

- **Client gRPC server lifecycle**: Started on Primary promotion, stopped
  on Close/Abort. Bind address configurable via CLI.

- **Serialization**: JSON via serde_json. Each `replicate()` payload is
  one `KvOp`. Copy sends snapshot chunks, installed as one `KvOp::Snapshot`.

- **Stream cancellation**: `drain_stream` uses `CancellationToken` with
  biased `select!`. Copy drain finishes naturally on IdleSecondary →
  ActiveSecondary (no cancellation — prevents item loss).

---

## Persistent Storage

KvState persists to disk via snapshot + WAL. Every code path — including
tests — uses `KvState::open(dir)`. No in-memory-only mode.

### Storage Layout

```
<data-dir>/
├── state.json              # Full HashMap + last_applied_lsn (atomic checkpoint)
└── wal.log                 # Append-only NDJSON log of KvOps since last snapshot
```

### Design Decisions

**Snapshot + WAL approach:** Two files, two purposes. `state.json` is a
periodic full snapshot written atomically (write-to-tmp + fsync + rename).
`wal.log` is an append-only NDJSON log. Recovery = load snapshot + replay
WAL. This enables rollback: snapshot is always at a committed LSN, WAL
entries can be selectively replayed up to any target LSN.

**Per-op WAL writes on quorum path:** Both primary (`server.rs` put/delete)
and secondary (`drain_stream`) call `apply_op()` which writes to the WAL
and flushes before acknowledging. WAL failure prevents ACK, so the primary
will timeout and rebuild the replica. This is the correct behavior — never
lie about durability.

**Durability:** `flush()` to OS page cache (survives process crash).
`sync_data()` (fdatasync) to disk is available but commented out — per-op
fdatasync caps throughput at ~200-1000 ops/sec, acceptable for an example
app but not production. A production service would use group commit.

**No in-memory mode:** All tests use real persistence via unique temp dirs
(atomic counter + pid). This ensures test and production behavior never
diverge, and enables future restart/recovery tests.

### Recovery

```
KvState::open(data_dir):
  1. Load state.json (or empty if missing)
  2. Replay wal.log entries with lsn > snapshot LSN
  3. Truncate corrupt trailing WAL bytes (crash-safe)
  4. Reopen WAL for append
```

### Checkpoint

Snapshots only when `committed_lsn == last_applied_lsn` (no uncommitted
ops). Triggered on:
- Copy stream completion (IdleSecondary → ActiveSecondary)
- Graceful Close

### UpdateEpoch Rollback

When `UpdateEpoch(previous_epoch_last_lsn)` arrives, the service:
1. Fences interactive transactions from the old epoch
2. Calls `rollback_to(previous_epoch_last_lsn)` — reloads snapshot +
  partial WAL replay + WAL rewrite under the state write lock

Rollback fires when the supplied boundary precedes local applied state.
Transactions are indivisible at that boundary. A boundary older than the
checkpoint is rejected instead of silently retaining incorrect state.
The recovered state is built privately and must reach the requested LSN.
Only after the WAL replacement succeeds are data, key versions, and commit
results installed together. Failed replacement leaves live state untouched
and blocks writes until recovery; missing history fails instead of producing
an earlier snapshot under the requested LSN.
Replication drains share the same state lock, so they cannot apply midway
through rollback. Configuration-only failover preserves the accepted suffix
as described above, rather than treating a lagging commit watermark as a
safe truncation point.

### Transaction Verification

```sh
cargo test -p kvstore --lib
cargo test -p kvstore --test transactions
cargo test -p kvstore --test reconciler test_transaction_
cargo test -p kuberic-core --lib
```

Coverage includes mixed puts/deletes, read-your-own-writes, write skew,
read-only and blind-write conflicts, absent-key ABA, disjoint commits,
ordinary-write serialization, abort/expiry/resource limits, concurrent and
lost-response retries, WAL failure/truncation, checkpoint retention, rollback,
large atomic copies, catch-up, secondary visibility, and immediate failover.

### Remaining Work

| Feature | Status | Notes |
|---------|--------|-------|
| `KvPod::crash()` / `restart()` | Done | Abrupt abort — Pattern 2 test validates primary crash → failover → rejoin |
| B0: QuorumTracker timeout | ✅ Fixed | `replicate()` returns `NoWriteQuorum` within the configured deadline when quorum ACKs stop |
| C4: ChangeRole(None) cleanup | ✅ Fixed | Stops client server on None, deletes data dir on Close. See `design-gaps.md` C4 |
| `OpenMode::Existing` support | Gap | `KvState::open()` already loads snapshot + replays WAL from `data_dir`. But `add_replica` always uses `OpenMode::New` + full `build_replica` copy. A pod restart with PVC-preserved data could skip the copy and reattach via `OpenMode::Existing`. Requires new driver primitive (`reconnect_secondary`). See `rolling-upgrade-design.md` RF-1. |
