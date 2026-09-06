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
- Custom VFS implementation (use standard SQLite WAL mode, intercept
  at the application layer)
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
quorum replication instead of HTTP, and intercept at the **application
layer** (WAL file reads after commit) instead of FUSE.

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
│  │               │    │  │  + WAL hook callback    │  │    │
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
                   WAL hook fires:
                     read new WAL frames from file
                     serialize as WalFrameSet { pages, db_size }
                         │
                         ▼
                   replicator.replicate(WalFrameSet)
                     (quorum — blocks until ACKed)
                         │
                         ▼
Client ◄──Result──
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

After each SQL write, the primary reads new frames from the WAL file
(tracking the last-read offset), packages them as `WalFrameSet` with
CRC32 checksum, and calls `replicator.replicate()` which blocks until
quorum ACK. Auto-checkpoint is disabled (`wal_autocheckpoint=0`);
single connection enforced.

**Timing:** WAL frames are read AFTER SQLite commits locally. See
Known Problems (KP-1) for the timing inversion trade-off.

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
- **Application-layer interception (not VFS):** WAL file read after
  commit gives us the same data as a custom VFS with ~100 lines vs
  ~2000. Uses rusqlite's WAL hook for notification.
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
│   ├── state.rs         # SqliteState: WAL capture (primary), page apply (secondary)
│   ├── server.rs        # Client gRPC server (Execute/Query/ExecuteBatch)
│   ├── service.rs       # Lifecycle + StateProvider event loop
│   ├── frames.rs        # WalFrameSet/WalFrame types, WAL file parser
│   ├── framelog.rs      # frames.log + meta.json persistence
│   └── testing.rs       # SqlitePod helper (feature = "testing")
└── tests/
    ├── durable_data_loss.rs        # Correlated typed data-loss completion
    └── correlated_replication.rs   # WAL shipping, schema, switchover, failover
```

Key dependency: `rusqlite = { features = ["bundled", "hooks"] }` —
statically links SQLite, enables WAL hook. Also `crc32fast` for checksums.

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

2. **Fault reporting on promotion failure:** If `open_as_primary` fails
   (disk full, permissions), the service currently logs a warning but
   continues — the client gRPC server starts with no connection and all
   queries panic. Should report fault via `fault_tx` to trigger operator
   failover/rebuild. Same applies to `apply_committed_frames` failure.

---

## Known Problems

These are inherent limitations that cannot be fully resolved in this
design. They are documented for awareness and future mitigation.

### KP-1: WAL Hook Timing Inversion

SQLite commits a transaction to its WAL before the frames exist to be
shipped. `replicate()` therefore runs after the local commit, inverting
the kvstore's replicate-then-apply ordering.

**Impact:** There is a window between local commit and quorum
confirmation. From the client's perspective the write failed (no response
received), but the primary's local DB has the data.

**Why the window cannot be removed:** SQLite has no pre-commit hook that
allows blocking on external I/O. `sqlite3_commit_hook` can abort a
transaction but cannot pause it while waiting for the network — it must
return synchronously. Frames only exist once the commit has happened.

**How the window is bounded:** the window cannot be removed, but it is
prevented from producing a visible divergence.

1. A write gate serializes commit → capture → replicate. WAL capture is a
   single cursor over the WAL file, so overlapping writes would otherwise
   let one request ship another's frames and return success without its
   own quorum.
2. After quorum, the confirmed WAL offset and its generation are fsynced
   to `meta.json` *before* the client is told anything. Success is never
   reported for a write whose confirmation record is not durable.
3. On primary open, WAL bytes beyond the confirmed offset are truncated
   and `-shm` is removed so SQLite rebuilds its index. A transaction that
   committed locally but never reached quorum is discarded rather than
   silently resurrected.
4. If a checkpoint restarted the WAL since the last confirmation, the
   recorded offset describes a generation that no longer exists. SQLite
   only restarts a WAL once every frame is backfilled into the database,
   so nothing in the new generation reached quorum and the whole file is
   dropped. This also clears a stale WAL left behind when a secondary
   applied frames straight to the database file.

The result is that a crash in the window yields a clean rollback, matching
the kvstore's `UpdateEpoch` behaviour, instead of state the cluster never
agreed on. Covered by `tests/quorum_durability.rs`.

**Residual limitation — graceful shutdown.** Recovery works by truncating
the WAL, so it can only discard frames that are still *in* the WAL. SQLite
checkpoints the WAL whenever the last connection closes, and that happens
regardless of `wal_autocheckpoint=0` or whether the application issues
`PRAGMA wal_checkpoint` itself — `SQLITE_FCNTL_PERSIST_WAL` suppresses only
the deletion, not the checkpoint. A write that committed locally, failed to
reach quorum, and is then followed by a graceful shutdown is therefore
folded into the database file where truncation can no longer reach it.

Exposure is limited because a restarted pod reports a mismatched epoch and
the reconciler rebuilds it by copy, discarding the local database. It
matters where a replica is re-promoted without a rebuild, notably
`replicas=1`. Closing it properly means reporting a fault when replication
fails, so the replica is rebuilt rather than resumed — an operator-contract
change, not a local fix.

**Residual limitation — copy snapshots.** `snapshot_db()` checkpoints before
reading the database file and does not take the write gate, so an in-flight
unconfirmed write can be folded into a snapshot shipped to a new secondary.
The window requires the snapshot to be taken while a write is in flight and
that write to never reach quorum.

**Residual limitation:** while the primary is *running*, a locally
committed but unconfirmed transaction is still visible to reads on that
primary, because SQLite makes a commit visible and durable at the same
instant. Only a VFS interception approach closes that. If the primary
survives, the write is retried or the replica is rebuilt; if it crashes,
recovery discards the transaction.

**Client retry semantics:** retrying a timed-out write is **not**
automatically safe. Page-level *frame replay* is idempotent, but the
client's *SQL* is not — a retried `INSERT` after a commit that did land
produces a second row. Clients that retry need request-level
idempotency (a deduplicating key), not page-level idempotency.

**VFS alternative:** A custom VFS could eliminate the window entirely by
intercepting `xWrite()` calls to the WAL file and shipping pages to the
replicator *before* letting SQLite flush locally (replicate-then-commit).
However, implementing a WAL-mode VFS requires ~2000 lines (vs ~100 for WAL
file reads), including `xShmMap`/`xShmLock`/`xShmBarrier` for shared
memory, and VFS bugs can silently corrupt the database. The current
approach is the right trade-off for an example app; a production system
would consider VFS or a purpose-built replication engine (e.g., dqlite).

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
- [rusqlite WAL Hook](https://docs.rs/rusqlite/latest/rusqlite/hooks/struct.Wal.html)
