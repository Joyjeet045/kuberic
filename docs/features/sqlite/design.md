# SQLite: Replicated SQLite on Kuberic

A replicated SQLite database running on kuberic-core. The primary
accepts SQL reads and writes, ships WAL frames to secondaries via the
replication protocol. Secondaries exist for failover — they maintain a
durable copy of the database but do not serve client queries.

---

## Goals

1. Full SQL interface — clients issue arbitrary SQL (reads + writes)
2. Primary ships SQLite WAL frames to secondaries via kuberic quorum
3. Secondaries maintain a durable copy for failover (no client reads)
4. Survive pod restarts — secondary catches up via copy (full DB snapshot)
   or WAL replay (incremental)
5. Leverage existing kuberic-core lifecycle, replication, and failover
6. Zero consensus layer — kuberic's quorum replication IS the consensus

## Non-Goals

- Multi-writer / multi-primary (single primary, same as kvstore)
- Sharding across partitions (single partition = single SQLite DB)
- Multi-process or multi-connection access to one database (the primary
  holds an exclusive lock so the wal-index stays in heap memory)
- Sub-millisecond replication latency (WAL frames are shipped per-commit)

---

## Prior Art

| Approach | Examples | Replication Unit | Determinism | Trade-off |
|----------|----------|-----------------|-------------|-----------|
| Statement-level | rqlite, dqlite | SQL text via Raft | Required | Simple but forbids non-deterministic SQL |
| Page-level | LiteFS | Changed pages (LTX) | Not required | Correct but FUSE dependency |
| VFS-level | mvSQLite, Verneuil | Pages via custom VFS | Not required | Full control but massive complexity |
| WAL backup | Litestream | WAL frames to S3 | N/A | Simple but async-only, no live replicas |

### Our Approach: WAL Frame Shipping via Kuberic Quorum

We take the **LiteFS page-level concept** but use kuberic's existing
quorum replication instead of HTTP, and intercept in a **commit-barrier
VFS** instead of FUSE. The VFS wraps the default VFS and only takes over
the WAL file, so it stages a transaction rather than reimplementing
storage.

| Aspect | Our Design |
|--------|------------|
| Replication unit | WAL frames (committed transaction pages) |
| Interception | Read WAL file after commit |
| Consensus | Kuberic quorum replication (not Raft) |
| Determinism | Not required — shipping pages, not statements |
| Copy/rebuild | Full DB file snapshot via GetCopyState |
| Failover | Kuberic operator handles promotion |

---

## Architecture

```
Clients (SQL over gRPC)
    │
    ▼
┌──────────────────────────────────────────────────────────┐
│                  sqlite Pod                            │
│                                                           │
│  ┌──────────────┐    ┌──────────────────────────────┐    │
│  │  PodRuntime   │    │  SQLite Service (user app)    │    │
│  │               │    │                              │    │
│  │ control gRPC ◄┤    │  ┌────────────────────────┐  │    │
│  │ data gRPC    ◄┤    │  │  SQLite DB (WAL mode)  │  │    │
│  │               │    │  │  + commit barrier VFS   │  │    │
│  │ lifecycle_tx ─┼───►│  └────────────────────────┘  │    │
│  │ state_prov_tx─┼───►│                              │    │
│  └──────────────┘    │  client gRPC ◄── Clients     │    │
│                       └──────────────────────────────┘    │
└──────────────────────────────────────────────────────────┘
```

### Primary Flow

```
Client ──SQL write──► SqliteServer
                         │
                         ▼
                   BEGIN (implicit or explicit)
                   Execute SQL statements
                   COMMIT
                         │
                         ▼
                   Commit-barrier VFS holds the WAL bytes:
                     the commit frame has not reached the file,
                     so the transaction does not yet exist
                         │
                         ▼
                   replicator.replicate(WalFrameSet)
                     (quorum — blocks the commit until ACKed)
                         │
                    ┌────┴────┐
                 quorum      no quorum
                    │            │
                    ▼            ▼
            write the WAL   drop the buffer,
            bytes locally,  fail the write,
            commit stands   SQLite rolls back
                    │            │
                    ▼            ▼
Client ◄──Result──         Client ◄──Error──
```

### Secondary Flow

```
Primary Replicator ──ReplicationItem──► Secondary Replicator
                                              │
                                              ▼
                                    drain_stream receives WalFrameSet
                                              │
                                              ▼
                                    Persist to local frames.log + fsync
                                              │
                                              ▼
                                         acknowledge()

On promotion (ChangeRole Primary):
  Read frames.log → apply pages to DB file → open SQLite
```

---

## Replication Protocol

### Replication Unit: WalFrameSet

```rust
#[derive(Serialize, Deserialize)]
struct WalFrameSet {
    /// The WAL frames from this transaction.
    /// Each frame is (page_number, page_data).
    frames: Vec<WalFrame>,
    /// Database size in pages after this transaction
    /// (from the WAL commit frame's header).
    db_size_pages: u32,
    /// CRC32 checksum over all frame data for integrity verification.
    /// Computed by primary, verified by secondary before persisting.
    checksum: u32,
}

#[derive(Serialize, Deserialize)]
struct WalFrame {
    page_number: u32,
    data: Vec<u8>,  // page_size bytes (typically 4096)
}
```

**Serialization:** WalFrameSet is serialized via `serde_json` (matching
kvstore's approach). The replication channel transmits `Bytes` — the
service serializes with `serde_json::to_vec()` and deserializes on
receive. This is simple and debuggable; binary formats can be adopted
later if throughput requires it.

We do NOT ship raw WAL bytes (which include SQLite-specific salts and
checksums tied to the WAL file identity). We extract the logical
content: which pages changed and what their new content is.

### Primary: Capturing WAL Frames

The primary opens SQLite against a commit-barrier VFS. The VFS buffers the
WAL bytes of the transaction in progress, and as soon as the commit frame is
complete the barrier packages them as a `WalFrameSet` with a CRC32 checksum
and calls `replicator.replicate()`, which blocks until quorum ACK. Only then
are the bytes written to the WAL file. Auto-checkpoint is disabled
(`wal_autocheckpoint=0`); single connection enforced.

**Timing:** frames reach quorum before the commit is visible. The barrier
runs in the write that completes the commit frame, not in a later sync, so
lowering `PRAGMA synchronous` cannot bypass it. See Known Problems (KP-1).

### Secondary: Persist-then-ACK, Deferred Apply

The secondary persists frames for durability before ACK. Application
to the DB file is **deferred until promotion**.

**On receive:** Deserialize → verify CRC32 → append to `frames.log`
(length-prefixed: `[4B len][JSON payload][4B CRC32]`) → fsync → ACK.

**On promotion:** Read all frames from `frames.log` → apply pages to
DB file via direct writes at `(page_number - 1) * page_size` → open
SQLite in WAL mode with a fresh connection.

This keeps the ACK path fast (one file write + fsync, no DB page I/O).
Rollback is trivial (truncate frames.log — pages never touch DB file).

**Secondary data directory:**
```
<data-dir>/
├── db.sqlite            # DB file (pages applied on promotion only)
├── frames.log           # Length-prefixed binary log of WalFrameSets
├── meta.json            # Persisted committed_lsn for crash recovery
└── (no SQLite WAL/SHM)  # WAL mode not used on secondary
```

### Copy Protocol (Full Snapshot)

For `GetCopyState` (new replica or full rebuild):

1. Primary checkpoints WAL (`PRAGMA wal_checkpoint(PASSIVE)`)
2. Reads the entire DB file as a byte array
3. Records current LSN as `copy_lsn` in `PartitionState::copy_lsn_map`
4. Streams as a single Operation via the copy stream
5. Secondary writes to local `db.sqlite`, sets `committed_lsn = copy_lsn`
6. Ready for incremental replication (actor replays from `copy_lsn + 1`)

---

## Client API

gRPC service `SqliteStore` with three RPCs (primary only):
- **Execute** — write statements (INSERT/UPDATE/DELETE/DDL). Returns `rows_affected`, `last_insert_rowid`, `lsn`.
- **Query** — read queries (SELECT). Returns typed columns and rows.
- **ExecuteBatch** — multiple statements in a transaction. Returns per-statement `rows_affected`.

See `proto/sqlitestore.proto` for full message definitions.

### Durability contract

The barrier returning successfully is the durability boundary. Errors on
either side of it mean different things, so they carry different gRPC codes.

| Outcome | gRPC code | Meaning |
|---------|-----------|---------|
| Success | `OK` | Committed here and on enough secondaries to survive losing this replica |
| Failure before the commit | `INTERNAL` | Definitely not committed |
| Failure after the commit | `UNKNOWN` | Reached quorum, but the response could not be completed |
| Replica awaiting rebuild | `FAILED_PRECONDITION` | This replica lost a committed transaction and serves nothing until rebuilt |

- **Failure before the commit** covers a rejected barrier: the write failed,
  SQLite rolled the transaction back and no commit frame was written, so
  nothing is visible to queries, checkpoints, copy snapshots or recovery.
- **Failure after the commit** covers a lost response, a cancelled request,
  or a `meta.json` confirmation write that failed. The transaction did commit
  and did reach quorum. An error here does **not** prove a rollback.
- **Retries** are only safe for a statement that is itself idempotent. The
  API has no request-level idempotency key, so a retried `INSERT` after an
  `UNKNOWN` result can produce a second row.
- **Reads** observe only quorum-confirmed state. A transaction that has not
  reached quorum is not visible on the primary, so no client can read a value
  the cluster has not agreed on.

---

## Lifecycle Integration

Same two-channel pattern as kvstore (`LifecycleEvent` + `StateProviderEvent`).

| Event | Action |
|-------|--------|
| **Open** | Create data dir, load meta.json. Return `ReplicatorHandle` |
| **ChangeRole(Idle)** | Open frame log, drain copy stream (full DB snapshot) |
| **ChangeRole(Active)** | Drain replication stream — persist frames, ACK |
| **ChangeRole(Primary)** | Apply frames from log to DB, open SQLite (WAL mode, auto-checkpoint off), start gRPC server |
| **Close** | Checkpoint WAL (primary), close connections |
| **Abort** | Cancel drains, close |
| **UpdateEpoch** | Truncate frames.log beyond `previous_epoch_last_lsn` |
| **GetCopyState** | Checkpoint WAL, send full DB file as snapshot |
| **GetLastCommittedLsn** | Return `committed_lsn` from meta.json |

---

## Key Design Decisions

- **WAL frame shipping (not statement replication):** Eliminates
  determinism requirement. `random()`, `datetime('now')`, triggers all
  work — we ship the result of execution, not the instructions.
- **Commit-barrier VFS (not a post-commit hook):** `sqlite3_wal_hook`
  runs after the local transaction is already committed and recoverable,
  which is the wrong side of the durability boundary. A VFS that stages
  the WAL bytes can hold the commit frame until quorum confirms, so a
  transaction never exists locally without existing in the cluster.
- **Stage the whole transaction (not stream frames):** Releasing the
  buffer in one write keeps the barrier out of SQLite's wal-index
  ordering, and the commit frame is inside the buffer rather than
  already recoverable on disk.
- **Logical page content (not raw WAL bytes):** WAL files contain
  file-specific salts and checksums. We extract `(page_number, data)`
  pairs — simpler and portable across WAL file instances.
- **Direct page writes on secondary (not sqlite3_backup):** Secondary
  doesn't have SQLite open, so no page cache to invalidate. On
  promotion, SQLite opens the file fresh with a clean page cache.
- **Persist-then-ACK (not memory buffer):** ACK counts toward quorum.
  `frames.log + fsync` before ACK ensures crash recovery.
- **Deferred apply (not per-receive):** Pages applied only on promotion.
  Keeps ACK path fast, rollback trivial (truncate frames.log).
- **tokio::sync::Mutex (not RwLock):** rusqlite `Connection` is `Send`
  but not `Sync`. Mutex only requires `Send`; RwLock requires both.

---

## Rollback and Consistency

**UpdateEpoch rollback:** Truncate `frames.log` beyond target LSN.
DB file is untouched (deferred apply means uncommitted pages were
never applied).

**Rollback past committed data:** If `previous_epoch_last_lsn <
committed_lsn`, applied pages can't be un-applied. Service detects
this and signals `OnDataLoss` for full copy rebuild.

**Crash recovery:** Read `committed_lsn` from `meta.json`, replay
`frames.log` up to that LSN, truncate corrupt trailing entries.

---

## File Structure

```
examples/sqlite/
├── Cargo.toml
├── build.rs
├── proto/sqlitestore.proto
├── src/
│   ├── lib.rs, main.rs, demo.rs
│   ├── state.rs         # SqliteState: primary connection, page apply (secondary)
│   ├── barrier.rs       # ReplicationBarrier: quorum gate + rebuild fence
│   ├── server.rs        # Client gRPC server (Execute/Query/ExecuteBatch)
│   ├── service.rs       # Lifecycle + StateProvider event loop
│   ├── frames.rs        # WalFrameSet/WalFrame types, staged-frame decoding
│   ├── framelog.rs      # frames.log + meta.json persistence
│   └── testing.rs       # SqlitePod helper (feature = "testing")
└── tests/
    ├── durable_data_loss.rs        # Correlated typed data-loss completion
    ├── quorum_durability.rs        # Commit barrier, crash injection, fencing
    └── correlated_replication.rs   # WAL shipping, schema, switchover, failover

sqlite-commit-barrier/
├── src/wal.rs           # WAL framing, commit detection, staging buffer
├── src/vfs.rs           # VFS shim over the default VFS
└── tests/               # Real-SQLite and raw-VFS staging tests
```

Key dependency: `rusqlite = { features = ["bundled"] }` — statically
links SQLite. `sqlite-commit-barrier` registers the VFS. Also
`crc32fast` for checksums.

---

## Comparison with KVStore

| Aspect | KVStore | SQLite |
|--------|---------|--------|
| Data model | HashMap<String, String> | Full SQL |
| Replication unit | Serialized KvOp | WalFrameSet (page-level) |
| Determinism | N/A | Not required |
| State | All in memory | Disk-backed |
| Secondary ACK | Apply to HashMap + WAL | Persist to frames.log only |
| Promotion apply | N/A (already in memory) | Replay frames.log → DB file |
| Rollback | Reload snapshot + partial WAL | Truncate frames.log |
| Checkpoint | Atomic JSON snapshot | `PRAGMA wal_checkpoint(PASSIVE)` |

---

## Test Plan

Tests use in-process pods with real gRPC via `SqlitePod` helper.
All tests use `#[serial]` (port contention).

### Implemented

`sqlite_durable_data_loss_result_uses_real_runtime_path` opens and promotes a
real SQLite pod through `ExecuteCorrelatedControlAction`, executes the
epoch-fenced data-loss callback, and verifies its retained typed terminal
result.

`correlated_replication.rs` builds real three-pod partitions through the
single correlated control path and covers multi-page WAL shipping, schema
changes, switchover data survival, and failover data survival. Cross-workflow
checkpoint integration is exercised by the durable KV reconciler suite.

### Planned (Phase 2+)

| # | Test | Category |
|---|------|----------|
| 6 | test_failover_uncommitted_lost | Failover |
| 7 | test_copy_full_rebuild | Copy protocol |
| 8 | test_copy_then_incremental | Copy protocol |
| 9 | test_update_epoch_truncates_frames_log | Rollback |
| 10 | test_update_epoch_past_committed_triggers_rebuild | Rollback |
| 11 | test_secondary_crash_recovery | Crash recovery |
| 15 | test_checkpoint_after_commit | Checkpoint |

---

## Open Questions

1. **Large transactions:** A transaction modifying 10,000 pages creates
   a ~40MB `WalFrameSet`. Tonic's default gRPC limit is 4MB. May need
   `max_encoding_message_size` / `max_decoding_message_size` config.
   The commit barrier also stages the whole transaction in memory, so
   the same transactions are the ones worth bounding.

2. **Group commit:** Every commit costs one quorum round trip, which
   bounds write throughput at roughly the inverse of quorum latency.
   Batching several transactions into one round trip is the main
   remaining throughput lever.

---

## Known Problems

Constraints the design has to work around, and how it does so. Each entry
states what remains after the mitigation.

### KP-1: Commit ordering

SQLite offers no pre-commit hook that can block on external I/O, so a hook
based design has to ship frames after the local commit and cannot avoid a
window in which the primary holds a transaction the cluster never
confirmed.

The primary therefore opens SQLite against a commit-barrier VFS instead of
using a hook. SQLite publishes a WAL transaction by writing a commit
frame, the frame whose header carries a non-zero database page count, and
recovery, checkpointing and readers all stop at the last valid commit
frame. The VFS buffers the WAL bytes of the transaction in progress and
releases them only once the frames have reached durable quorum, serving
reads from the buffer meanwhile so SQLite observes a file that behaves
normally.

**Result:** replication happens before the commit is visible, matching the
kvstore's replicate-then-apply ordering. A transaction that cannot reach
quorum fails its write, SQLite rolls it back, and no commit frame is left
behind, so the transaction is invisible to queries, to checkpoints, to
copy snapshots and to recovery after a crash.

**Cost:** a synchronous transaction waits a quorum round trip inside the
commit, and the transaction is buffered in memory until it is published.
Group commit would amortise the round trip and is not implemented.

**Requirements:** the primary opens with `locking_mode=EXCLUSIVE`, which
keeps the wal-index in heap memory rather than a shared-memory file, and
`synchronous=FULL`, which keeps a published commit durable locally. The
barrier itself does not depend on `synchronous`: it runs in the write that
completes the commit frame, so a client lowering the pragma weakens local
durability but cannot commit without quorum.

Covered by `sqlite-commit-barrier/tests/barrier.rs` and
`examples/sqlite/tests/quorum_durability.rs`.

**Residual limitation — a commit the cluster keeps but this replica loses.**
Once the barrier confirms quorum the transaction is durable on the cluster,
so the local write that follows is the last step that can still fail. If it
does, this replica is missing a transaction the cluster has and cannot be
trusted to serve or to be promoted. The barrier then fences the replica: it
stops accepting commits, refuses reads, refuses copy snapshots, refuses
promotion, writes a `rebuild-required` marker so the fence survives a
restart, and reports `FaultType::Permanent` so the operator rebuilds it.
The fence is cleared only by `restore_from_snapshot`, which replaces the
local database with quorum-confirmed state.

**Client retry semantics:** retrying a timed-out write is **not**
automatically safe. Page-level *frame replay* is idempotent, but the
client's *SQL* is not — a retried `INSERT` after a commit that did land
produces a second row. Clients that retry need request-level
idempotency (a deduplicating key), not page-level idempotency.

### KP-2: Per-Commit Synchronous Replication Throughput

Every SQL COMMIT pays a full network RTT for quorum replication.
With page-level payloads (~16KB per simple INSERT vs. ~100 bytes
in KV), throughput is limited to ~500-1000 TPS at 1ms RTT.

**Mitigation:** Acceptable for an example app. Production systems
would use group commit (batch multiple transactions' frames into
one `replicate()` call) — this is a future optimization, not a
design change.

### KP-3: ChangeRole(None) Does Not Delete Data or Stop Client Server — ✅ Fixed

`ChangeRole(Role::None)` now stops the client server immediately.
`Close` after `ChangeRole(None)` deletes the data directory.
`Close` from any other role preserves data for restart recovery.
See `design-gaps.md` C4.

### KP-4: Durable Replacement Always Forces Full Rebuild

The durable add/rebuild workflow uses `OpenMode::New` plus full
`BuildReplica` copy,
even when the secondary's `data_dir` has valid persisted state (db.sqlite
+ frames.log) from a previous run. A pod restart with PVC-preserved data
could skip the full DB snapshot copy and reattach using
`OpenMode::Existing` — `SqliteState::open()` already handles loading
from existing data (loads DB + replays frames.log).

This becomes significant for rolling upgrades where the pod image
changes but the data is preserved on PVC. Without a
`reconnect_secondary` driver primitive, every upgrade copies the entire
database — potentially minutes for large databases.

See `rolling-upgrade-design.md` RF-1 for the full analysis and
recommended fix (`reconnect_secondary` with `OpenMode::Existing` that
skips `build_replica`).

---

## References

- [SQLite WAL Mode](https://sqlite.org/wal.html)
- [SQLite WAL File Format](https://sqlite.org/walformat.html)
- [LiteFS Architecture](https://github.com/superfly/litefs)
- [LiteFS WAL Mode Blog](https://fly.io/blog/wal-mode-in-litefs/)
- [rqlite Design](https://rqlite.io/docs/design/)
- [dqlite Replication](https://canonical.com/dqlite/docs/explanation/replication)
- [Litestream How It Works](https://litestream.io/how-it-works/)
- [mvSQLite Design](https://su3.io/posts/mvsqlite)
- [SQLite VFS](https://www.sqlite.org/vfs.html)
