# Kuberic: WAL Persistent Storage Design

> **Archived proposal based on the removed classic v1 replicator.** Retained
> for historical design context; it is not current implementation guidance.

Design for adding durable Write-Ahead Log (WAL) storage to the Kuberic
replicator, enabling crash recovery without full replica rebuilds.

> Part of the [archived Kuberic design](../README.md).
> Related: [Protocols](../protocols.md), [Design Gaps](../design-gaps.md),
> [SF Architecture](../../../background/service-fabric/README.md)

---

## Motivation

The current replication system is entirely in-memory:

- **Primary**: `ReplicationQueue` (BTreeMap) holds ops for replay to new
  replicas. Lost on crash — new replicas require full copy rebuild.
- **Secondary**: `SecondaryState.log` (HashMap) holds received ops. Lost
  on crash — requires full copy rebuild from primary.
- **User service**: Responsible for its own persistence via the
  `OperationStream` → `acknowledge()` pattern (matching SF's model).

**The problem**: Any pod restart — even a brief one — requires a full
copy rebuild because the replicator's in-memory queue is lost. For large
datasets, this means minutes to hours of downtime for that replica.

**The goal**: Persist the replication log to disk so that:
1. A restarted pod can recover from its local WAL instead of rebuilding
2. The primary retains pending ops across restarts for secondary replay
3. Secondaries can ACK after local persist (not just user-level persist)

---

## SF's Persistence Model (Reference)

Service Fabric has **two replicators** with different persistence models.
Understanding both is critical because our WAL design is essentially
building a lightweight version of what V2 does, on top of V1's model.

### V1 Replicator (`Reliability/Replication/`)

The V1 "Fabric Replicator" is **WAL-agnostic** — it has no disk I/O
of its own. It is pure in-memory transport + quorum tracking.

**Volatile Services** (`hasPersistedState = false`):
- Replicator auto-ACKs received operations immediately
- No disk writes anywhere in the replication path
- Any crash = full rebuild via copy stream

**Persisted Services** (`hasPersistedState = true`):
- `requireServiceAck = true` (set automatically)
- Replicator receives ops into an in-memory `OperationQueue`
- Ops dispatched to user via `GetReplicationStream()`
- **User** persists (e.g., to ESE/RocksDB/custom store)
- User calls `IFabricOperation::Acknowledge()` after persisting
- Only then does the replicator send ACK to primary

**User implements `IStateProvider`** (7 methods, raw bytes):
```
GetLastCommittedSequenceNumber()   — "what LSN are you at?"
GetCopyContext()                   — "tell primary what you have"
GetCopyState(upToLsn, context)     — "produce full state for secondary"
UpdateEpoch(epoch, prevLastLsn)    — "new epoch, truncate uncommitted"
OnDataLoss()                       — "quorum lost, accept data loss?"
```

The user receives raw opaque bytes on the operation stream and is
fully responsible for parsing, applying, persisting, and acknowledging.

**This is what Kuberic currently models** — our `LifecycleEvent` +
`StateProviderEvent` channels map to `IStatefulServiceReplica` +
`IStateProvider`.

### V2 Transactional Replicator (`data/txnreplicator/`)

V2 **wraps V1 internally** and adds its own `LoggingReplicator` +
`PhysicalLogWriter`. All V1 lifecycle methods pass through. V2 adds:

- **Transaction log** (WAL) on disk — the shared/dedicated KTL log
- **Transaction semantics** — begin/commit/abort across state providers
- **Checkpoint coordination** — triggers periodic state snapshots
- **Recovery orchestration** — replays log from last checkpoint on restart

**User implements `IStateProvider2`** (16+ methods, structured):
```
ApplyAsync(lsn, txn, context, metadata, data) — structured apply
Unlock(operationContext)          — release locks after commit
PrepareCheckpoint(lsn)           — prepare for checkpoint
PerformCheckpointAsync()         — write checkpoint to disk
RecoverCheckpointAsync()         — recover from checkpoint on restart
GetCurrentState()                — produce copy data for secondary
SetCurrentStateAsync(num, data)  — receive copy data on secondary
```

V2 handles WAL persistence, transaction semantics, epoch management,
and recovery orchestration. The user focuses on storage access methods.

**V2 is NOT hidden inside Reliable Collections.** It's accessible via
`IFabricInternalStatefulServicePartition.CreateTransactionalReplicator()`
(internal COM interface, `FabricRuntime_.h`). Users can build custom
transactional state providers without using `ReliableDictionary`.

### Quorum and Primary ACK

The **primary counts toward write quorum** and is considered to have
ACKed immediately — it generated the operation, so it already has
the data in memory.

From `ReplicaManager.cpp`:
```cpp
// The primary is included in quorum and considered as immediately ACKing,
// the committed number must be ACKed by writeQuorum - 1 remote replicas.
size_t quorumIndex = static_cast<size_t>(writeQuorum - 2);
```

For a 3-replica set with `writeQuorum = 2`:
- Primary ACK = 1 (immediate, in-memory — **no disk write**)
- Need `writeQuorum - 1 = 1` secondary ACK (user-persisted for V1)
- `replicate()` returns after 2 of 3 have the data

**The primary does NOT persist to its own disk.** Durability comes
entirely from secondary ACKs. This means a primary crash loses all
in-flight ops that haven't been ACKed by quorum yet.

### SF Persistence Architecture Summary

```
V1 Path (IStateProvider):

  partition.CreateReplicator(stateProvider)
    → V1 Replicator (in-memory queues)
    → User receives raw bytes, handles ALL persistence
    → ACK = "user persisted"

V2 Path (IStateProvider2):

  partition.CreateTransactionalReplicator(factory)
    → V2 wraps V1 (all lifecycle passes through)
    → V2 adds LoggingReplicator + PhysicalLogWriter (WAL on disk)
    → V2 adds transaction semantics + checkpoint coordination
    → User implements structured apply/checkpoint/recovery
    → ACK = "V2 logged + user applied"

V2 shared/dedicated log (platform-specific):
  Windows: kernel-mode CLFS driver, shared log across partitions
  Linux:   user-space sparse files, per-partition (more I/O overhead)
```

### What This Means for Kuberic

We currently model **V1** — which is correct for our `IStateProvider`-
style API. But V1's weakness is that every pod restart requires full
copy rebuild because the replicator has no persistent state.

SF solved this with **V2**, but V2 requires users to implement the
much larger `IStateProvider2` contract (16+ methods with checkpoint,
recovery, backup semantics). That's a heavy burden.

**Our Option C is a middle path:** add V2-like WAL persistence at the
replicator level, but keep V1's simple `IStateProvider`-style user API.
Users get crash recovery for free without implementing checkpointing.
This is something SF never offered — V1 has no WAL, V2 requires the
full `IStateProvider2` contract.

---

## Architecture Options

### Option A: V1 Model — Service-Side Only (Current)

Keep the replicator as pure transport (matching SF V1). Persistence
is the service's responsibility via `OperationStream` → `acknowledge()`.

```
┌─────────────────────────────────┐
│ User Service (IStateProvider)   │
│  ├── OperationStream.recv()     │
│  ├── persist to RocksDB/file    │  ← User's responsibility
│  └── operation.acknowledge()    │
└──────────┬──────────────────────┘
           │ ACK (after user persist)
┌──────────▼──────────────────────┐
│ Replicator (in-memory only)     │
│  ├── ReplicationQueue (BTreeMap)│
│  └── QuorumTracker              │
└─────────────────────────────────┘
```

**Pros:**
- Already implemented (persisted mode in SecondaryReceiver)
- Clean separation of concerns — matches SF V1 exactly
- No framework-level disk I/O to manage
- No double-write penalty

**Cons:**
- User must implement persistence correctly (error-prone)
- Primary crash loses ReplicationQueue → all secondaries need full
  rebuild even if ops were locally applied
- Brief pod restart = expensive full copy rebuild
- SF solved this limitation with V2, but V2 requires a much heavier
  user contract (IStateProvider2, 16+ methods)

**Verdict:** Sufficient for MVP. Matches SF V1 but shares its weakness.

### Option B: V2-Style — Full Framework Persistence

The replicator persists every operation to a local WAL before ACK,
matching SF V2's `LoggingReplicator` model. User services become
consumers of the committed log with a structured apply/checkpoint API.

```
┌─────────────────────────────────┐
│ User Service (IStateProvider2)  │
│  ├── ApplyAsync(lsn, txn, data) │
│  ├── PerformCheckpointAsync()   │  ← Structured contract
│  └── RecoverCheckpointAsync()   │
└──────────┬──────────────────────┘
           │
┌──────────▼──────────────────────┐
│ Replicator (persistent WAL)     │
│  ├── WAL: append(lsn, data)     │  ← Write before ACK
│  ├── WAL: read_from(lsn)        │  ← Replay for new replicas
│  ├── WAL: truncate(acked_lsn)   │  ← GC after quorum commit
│  ├── Checkpoint coordination    │
│  └── QuorumTracker              │
└─────────────────────────────────┘
```

**Pros:**
- Automatic durability for all services (zero user persistence effort)
- Pod restart → replay from local WAL (no copy rebuild needed)
- Primary retains ReplicationQueue across restarts
- Single persistence layer (no user-level double-write)
- Matches SF V2's proven architecture

**Cons:**
- Requires users to implement V2-style contract (apply, checkpoint,
  recover) — much heavier than V1's 7-method IStateProvider
- Users who already have a storage engine (RocksDB, SQLite) end up
  with double-write anyway (WAL + their own store)
- WAL compaction requires checkpoint coordination with user
- Forces all users into one model

**Verdict:** Too heavy. This is what SF V2 does, and it requires the
full `IStateProvider2` contract. We want the benefits without the cost.

### Option C: Lightweight V2 — Replicator WAL + V1 User API (Recommended)

The key insight: SF V2 solved the persistence problem but imposed a
heavy user contract. We can get V2's benefits (WAL, crash recovery)
while keeping V1's simple user API. **This is something SF never
offered** — V1 has no WAL, V2 requires full `IStateProvider2`.

The replicator maintains a WAL for replication protocol recovery.
Users keep the simple V1-style API (`OperationStream` + `acknowledge()`).
The WAL is the replicator's internal concern — invisible to users.

```
┌─────────────────────────────────────────────┐
│ User Service (V1-style API, unchanged)      │
│  ├── OperationStream.recv()                 │
│  ├── apply to state (RocksDB/memory/custom) │  ← Optional user persist
│  └── operation.acknowledge()                │
└──────────┬──────────────────────────────────┘
           │ ACK (user acknowledged)
┌──────────▼──────────────────────────────────┐
│ Replicator (persistent WAL, transparent)    │
│  ├── WAL: persist op BEFORE sending to user │
│  ├── WAL: persist op BEFORE sending ACK     │  ← On secondary
│  ├── WAL: replay for new replicas           │
│  ├── WAL: recover on restart                │
│  └── QuorumTracker                          │
└─────────────────────────────────────────────┘
```

**How this compares to SF:**
```
SF V1:  No WAL.        Simple API (IStateProvider).   No crash recovery.
SF V2:  Has WAL.       Heavy API (IStateProvider2).   Full crash recovery.
Ours:   Has WAL.       Simple API (V1-style).         Crash recovery.
                       ↑ Best of both worlds
```

**Primary write path:**
```
1. User calls replicate(data)
2. Assign LSN
3. WAL.append(lsn, epoch, data) + fsync     ← NEW: persist locally
4. ReplicationQueue retains in-memory copy   (for fast replay)
5. send_to_all (non-blocking)
6. Wait for quorum ACK
7. Return LSN to user → DURABLE
```

Note: SF V1 primary does NOT persist — its quorum ACK is immediate
and in-memory. Our primary WAL persist is extra safety beyond SF.
If primary crashes before quorum, the WAL lets it recover in-flight
ops when the pod restarts (SF V1 loses them permanently).

**Secondary receive path:**
```
1. Receive ReplicationItem from primary
2. WAL.append(lsn, epoch, data) + fsync     ← NEW: persist before ACK
3. Send ACK to primary                       ← ACK = "I have it on disk"
4. Dispatch to user via OperationStream
5. User applies + acknowledges (optional)
```

**Recovery path (pod restart):**
```
1. Read WAL from disk
2. Reconstruct: last_lsn, epoch, committed_lsn
3. Report last_committed_lsn to operator
4. Operator recognizes replica is alive with recent state
5. If gap is small: replay missing ops from primary's WAL
6. If gap is large or epoch mismatch: full copy rebuild
```

**Pros:**
- V2's crash recovery without V2's heavy user contract
- User services keep simple V1-style API (no checkpoint/apply/unlock)
- Fast recovery from brief restarts (replay WAL, not full copy)
- Primary retains replication queue across restarts (beyond SF V1)
- WAL is replicator-internal (not exposed as user API)
- User services can use any state store they want (or none)

**Cons:**
- Two ACK semantics to understand: replicator ACK (WAL persist)
  vs user ACK (application persist)
- Potential double-write if user also persists (WAL + user store)
- WAL management complexity (compaction/truncation)
- No checkpoint coordination with user (unlike V2) — WAL grows
  until truncated by committed_lsn advance

**Verdict:** Best balance. Gets V2's durability with V1's simplicity.
Matches how real databases work (PG, etcd, CockroachDB all have
replication-level WAL + application-level storage).

---

## Detailed Design (Option C)

### WAL Storage Trait

The WAL storage is abstracted behind a trait to allow different backends
(file-based, embedded DB, or in-memory for testing):

```rust
/// Durable storage for replication log entries.
///
/// Implementations must guarantee:
/// - append() followed by sync() makes data durable
/// - read_from() returns entries in LSN order
/// - truncate_before() is crash-safe (never exposes partial state)
pub trait WalStorage: Send + Sync + 'static {
    /// Append an entry to the log. Not durable until sync() is called.
    fn append(&mut self, entry: WalEntry) -> Result<()>;

    /// Ensure all appended entries are durable on disk.
    /// Called before sending ACK (secondary) or confirming quorum (primary).
    fn sync(&mut self) -> Result<()>;

    /// Append and sync in one call (convenience for single-entry writes).
    fn append_sync(&mut self, entry: WalEntry) -> Result<()> {
        self.append(entry)?;
        self.sync()
    }

    /// Read all entries with LSN >= from_lsn, in order.
    fn read_from(&self, from_lsn: Lsn) -> Result<Vec<WalEntry>>;

    /// Remove all entries with LSN <= up_to_lsn.
    /// Called during GC when all replicas have ACKed past this point.
    fn truncate_before(&mut self, up_to_lsn: Lsn) -> Result<()>;

    /// Get the highest LSN in the log, or 0 if empty.
    fn last_lsn(&self) -> Result<Lsn>;

    /// Get the stored epoch (set on each write, recovered on restart).
    fn last_epoch(&self) -> Result<Epoch>;

    /// Clear all entries (role change, epoch reset).
    fn clear(&mut self) -> Result<()>;
}

pub struct WalEntry {
    pub lsn: Lsn,
    pub epoch: Epoch,
    pub data: Bytes,
}
```

### WAL Entry Format (On-Disk)

For file-based WAL implementations:

```
┌─────────────────────────────────────────────┐
│ WAL Entry (variable length)                 │
├─────────────────────────────────────────────┤
│ magic: u32         (0x4B554245 = "KUBE")    │
│ version: u8        (1)                      │
│ lsn: i64           (operation LSN)          │
│ epoch_data_loss: i64                        │
│ epoch_config: i64                           │
│ data_len: u32      (payload length)         │
│ data: [u8]         (payload bytes)          │
│ crc32: u32         (CRC32C of all above)    │
└─────────────────────────────────────────────┘

Header: 4 + 1 + 8 + 8 + 8 + 4 = 33 bytes
Footer: 4 bytes (CRC32)
Total overhead: 37 bytes per entry
```

### Segment Files

Large WAL files are split into segments for efficient truncation:

```
wal/
  segment-000001.wal    (LSN 1 - 10000)      ← can be deleted after GC
  segment-000002.wal    (LSN 10001 - 20000)   ← active tail
  wal.meta              (last_lsn, epoch, segment info)
```

- Segment size: configurable (default 64 MB)
- GC: delete entire segment files when all entries are below committed_lsn
- `wal.meta`: atomically updated (write-rename) with current state

### Integration Points

#### Primary Side (WalReplicatorActor)

Changes to `actor.rs` data path:

```rust
// Current (in-memory only):
replication_queue.push(lsn, req.data.clone());
quorum_tracker.register(lsn, self.replica_id, req.reply);
sender.send_to_all(lsn, &req.data);

// New (with WAL):
wal.append_sync(WalEntry { lsn, epoch, data: req.data.clone() })?;
replication_queue.push(lsn, req.data.clone());  // keep in-memory for fast replay
quorum_tracker.register(lsn, self.replica_id, req.reply);
sender.send_to_all(lsn, &req.data);
```

The in-memory `ReplicationQueue` is retained as a hot cache. The WAL is
the durable backing store. On restart, the queue is rebuilt from WAL.

#### Secondary Side (SecondaryReceiver)

Changes to `secondary.rs` replication_stream handler:

```rust
// Current (ACK after in-memory insert):
state.accept_item(&item)?;
// ... dispatch to user or auto-ACK

// New (persist before ACK):
wal.append_sync(WalEntry { lsn, epoch, data })?;
state.accept_item(&item)?;
// ... dispatch to user or auto-ACK
```

The secondary persists to its local WAL BEFORE sending ACK to the
primary. This means:
- ACK = "I have this op durable on my disk"
- Even if the secondary's user service hasn't processed it yet
- User service processes asynchronously (apply + acknowledge later)

#### Two-Level ACK Semantics

```
                ACK to primary               User acknowledge()
                (WAL persisted)              (applied to state)
                     │                              │
  ┌──────────────────▼──────────────────────────────▼──────────┐
  │              DURABLE ON DISK             APPLIED TO STATE  │
  │              (replication safe)           (query-visible)   │
  │                                                            │
  │  Quorum of WAL ACKs = replicate() returns to user          │
  │  User acknowledge = op visible in application state        │
  └────────────────────────────────────────────────────────────┘
```

This is a design change from the current model where secondary ACK
means "user has persisted." With framework WAL, ACK means "replicator
has persisted." The user's `acknowledge()` becomes a state application
signal, not a durability signal.

**Impact on quorum semantics:**
- Current: quorum = N/2+1 user-level persists (strong but slow)
- With WAL: quorum = N/2+1 WAL persists (strong and fast)
- User apply happens asynchronously after ACK

**Trade-off:** Slightly weaker in the sense that user state may lag
behind replicator state. But durability is the same — ops are on disk
on N/2+1 replicas. On crash, the user service replays from WAL.

### Recovery Protocol

When a pod restarts with an existing WAL:

```
Pod Restart Recovery:
  1. WalStorage::open(path) → reads WAL from disk
  2. last_lsn = wal.last_lsn()
  3. last_epoch = wal.last_epoch()
  4. Report to operator: "I'm alive, last_lsn=X, epoch=Y"
  5. Operator decides:
     a. If epoch matches and gap is small:
        → Replay missing ops from primary's WAL/queue
        → Rejoin quorum directly (no full copy needed!)
     b. If epoch mismatch or gap too large:
        → Full copy rebuild (current behavior)
     c. If primary restarted:
        → Rebuild ReplicationQueue from WAL
        → Resume serving (secondaries reconnect)

User State Recovery (after replicator recovers):
  1. User service receives ChangeRole(role)
  2. User reads from WAL replay stream (new API)
  3. User rebuilds in-memory state from WAL entries
  4. User acknowledges each entry (marks as applied)
  5. Service is ready
```

#### Operator Changes (PartitionDriver)

```rust
// Current: pod restart = full rebuild
// New: pod restart with WAL = check if catchup is possible

fn reconcile_replica(&self, replica: &ReplicaInfo) -> ReconcileAction {
    if replica.is_alive() && replica.wal_lsn > 0 {
        let gap = primary_lsn - replica.wal_lsn;
        if replica.epoch == current_epoch && gap <= max_catchup_gap {
            // Fast path: replay from primary's queue/WAL
            ReconcileAction::CatchUp { from_lsn: replica.wal_lsn + 1 }
        } else {
            // Slow path: full copy rebuild
            ReconcileAction::Rebuild
        }
    } else {
        ReconcileAction::Rebuild
    }
}
```

### WAL Compaction

The WAL grows unbounded without compaction. Two strategies:

#### 1. Truncation (Simple, Recommended for V1)

Delete entries below the globally committed LSN (all replicas have ACKed):

```rust
// In WalReplicatorActor, after UpdateCurrentConfiguration:
let committed = state.committed_lsn();
replication_queue.gc(committed);
wal.truncate_before(committed)?;  // Delete old segment files
```

For secondaries:
```rust
// Triggered when primary confirms committed_lsn advance:
wal.truncate_before(committed_lsn)?;
```

#### 2. Checkpointing (Future, For Large State)

Periodically snapshot the full state, then truncate the WAL:

```
1. User service creates checkpoint (snapshot of state at LSN X)
2. Checkpoint persisted to disk
3. WAL entries before LSN X can be deleted
4. On recovery: load checkpoint + replay WAL from LSN X
```

This is how PostgreSQL and etcd work. Defer to V2.

### Configuration

```rust
/// WAL persistence configuration per partition.
pub struct WalConfig {
    /// WAL storage directory. Each partition gets a subdirectory.
    /// Default: /var/lib/kuberic/wal/<partition-id>/
    pub wal_dir: PathBuf,

    /// Maximum segment file size before rotation.
    /// Default: 64 MB
    pub segment_size: u64,

    /// Sync mode: how aggressively to fsync.
    pub sync_mode: SyncMode,

    /// Whether WAL is enabled. When false, behaves like current
    /// in-memory-only mode (for testing or volatile services).
    /// Default: true
    pub enabled: bool,
}

pub enum SyncMode {
    /// fsync after every append (safest, slowest).
    /// Guarantees: crash after ACK = data is on disk.
    EveryWrite,

    /// fsync in batches (group commit). Multiple appends share one
    /// fsync call. Latency-throughput trade-off.
    /// Guarantees: data durable within batch_interval.
    /// Default mode.
    Batched { batch_interval: Duration },

    /// No explicit fsync — rely on OS page cache.
    /// WARNING: Data may be lost on power failure.
    /// Use only for volatile/test workloads.
    None,
}
```

---

## Implementation Options (Rust)

### Option 1: redb (Recommended)

Pure-Rust ACID embedded database with B-tree storage.

**Why it fits:**
- `BTreeMap`-like API → drop-in for our `ReplicationQueue` semantics
- Range queries: `table.range(from_lsn..)` → `ops_from()`
- Prefix deletion: `table.drain(..=acked_lsn)` → `gc()`
- ACID transactions with configurable durability
- Single-file database, easy to manage per-partition
- Pure Rust, zero C dependencies, ~300KB binary overhead
- Stable file format (1.0+ since 2023)
- Active maintenance (cberner/redb, 3k+ GitHub stars)

**API mapping:**
```rust
use redb::{Database, TableDefinition, ReadableTable};

const WAL_TABLE: TableDefinition<i64, &[u8]> = TableDefinition::new("wal");

struct RedbWalStorage {
    db: Database,
}

impl WalStorage for RedbWalStorage {
    fn append_sync(&mut self, entry: WalEntry) -> Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(WAL_TABLE)?;
            table.insert(entry.lsn, entry.data.as_ref())?;
        }
        txn.commit()?;  // fsync happens here
        Ok(())
    }

    fn read_from(&self, from_lsn: Lsn) -> Result<Vec<WalEntry>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(WAL_TABLE)?;
        let entries: Vec<_> = table.range(from_lsn..)?
            .map(|r| {
                let (k, v) = r.unwrap();
                WalEntry { lsn: k.value(), data: Bytes::copy_from_slice(v.value()), .. }
            })
            .collect();
        Ok(entries)
    }

    fn truncate_before(&mut self, up_to_lsn: Lsn) -> Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(WAL_TABLE)?;
            // drain_filter equivalent
            let to_remove: Vec<_> = table.range(..=up_to_lsn)?
                .map(|r| r.unwrap().0.value())
                .collect();
            for lsn in to_remove {
                table.remove(lsn)?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    fn last_lsn(&self) -> Result<Lsn> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(WAL_TABLE)?;
        Ok(table.last()?.map(|(k, _)| k.value()).unwrap_or(0))
    }
}
```

**Performance:**
- Write: ~100K-200K single-entry transactions/sec (with fsync)
- Read: ~500K range reads/sec
- Group commit (batch multiple entries in one txn): ~500K entries/sec

### Option 2: DIY File WAL

Custom append-only log with segment files.

**Why it might fit:**
- Zero external dependencies
- Absolute control over fsync, format, segment rotation
- Minimal code (~500-800 lines for basic implementation)
- Can implement group commit natively

**Risks:**
- Must implement CRC validation, crash recovery, segment rotation
- fsync correctness is tricky (fdatasync vs fsync, directory fsync
  after file creation, write barriers)
- Bug surface area is larger than using a tested library
- No ACID transactions (must handle partial writes manually)

**When to choose:** If zero-dependency is a hard requirement.

### Option 3: raft-engine (Alternative)

TiKV's Raft log storage engine. Designed specifically for consensus
log persistence — the closest existing solution to our use case.

**Why it fits:**
- Built for exactly this pattern: append, range-read, prefix-truncate
- Per-write `fdatasync()` control (not transaction-level like redb)
- Multi-partition support (region_id maps to partition_id)
- Write batching via leader barrier (batches concurrent writers)
- Collaborative GC: `compact_entries()` + `purge_expired_files()`
- Production-proven at TiKV scale (PB-scale deployments)

**Trade-offs:**
- C dependency (`lz4-sys`) — not pure Rust
- Raft-specific API (region/index model, needs adapter)
- Heavier dependency (~500KB, 15-20s compile)
- Less commonly used outside TiKV ecosystem

**When to choose:** If write throughput requirements exceed 200K ops/sec
or if multiple partitions per pod need shared I/O management.

### Option 4: fjall / sled

LSM-tree based embedded stores.

**Why NOT recommended:**
- LSM write amplification is wasteful for sequential append workload
- Compaction overhead unnecessary (we GC by truncation, not compaction)
- sled maintenance status uncertain (alpha quality)
- fjall API less mature than redb

### Comparison Summary

| Aspect | redb | raft-engine | DIY File | fjall/sled |
|--------|------|-------------|----------|------------|
| Dependencies | 1 (pure Rust) | 1 (has C dep) | 0 | 1 |
| Correctness | Proven ACID | Production TiKV | Must validate | Alpha |
| Range queries | Native B-tree | Scan API | Must build index | Native LSM |
| Prefix delete | Native | compact_entries | Delete segments | Native |
| Fsync control | Per-txn commit | Per-write fdatasync | Full control | No control |
| Write perf | 100-200K/s | 100-500K/s | ~300K/s (raw) | 100-200K/s |
| Binary size | ~300KB | ~500KB | 0 | ~200KB |
| Maintenance | Active (3k+ ★) | Active (TiKV) | Self | Uncertain |
| Crash safety | ACID | CRC + replay | Must implement | Alpha |
| Fit for WAL | ★★★★★ | ★★★★☆ | ★★★☆☆ | ★★☆☆☆ |

**Recommendation: redb** for V1 — best balance of correctness, simplicity,
pure-Rust purity, and API fit for our sequential-write + range-read workload.
Consider raft-engine if we outgrow redb's throughput or need multi-partition
I/O management.

**Fsync note:** redb's `txn.commit()` calls fsync. Since our pattern is
one `append_sync()` = one transaction, we get per-write fsync. The
"transaction-level only" limitation only matters if batching multiple
entries per txn — which is actually desirable for group commit (Phase 4).

---

## Comparison with Other Systems

### Service Fabric V2 (Transactional Replicator)
- V2 wraps V1 replicator, adds `LoggingReplicator` + `PhysicalLogWriter`
- Uses shared log (Windows/CLFS) or per-partition files (Linux/InProc)
- Requires `IStateProvider2` contract (apply, checkpoint, recover)
- **Our design differs:** we add WAL at the replicator level but keep
  V1's simple `IStateProvider`-style API — no checkpoint contract needed
- **Trade-off:** V2 can coordinate checkpoints with user (smaller WAL).
  We can't (WAL grows until committed_lsn advances). Acceptable for
  typical workloads; defer checkpointing to Phase 3 if needed.

### PostgreSQL / CloudNative-PG
- PG has its own WAL (Write-Ahead Log) for crash recovery
- PG streams WAL to standbys for replication
- CloudNative-PG operator manages PG's built-in WAL streaming
- Our design is analogous: replicator WAL = PG WAL for replication

### etcd
- Uses a WAL (bbolt/bolt) for Raft log persistence
- Every Raft entry is persisted to WAL before sending to followers
- Followers persist to WAL before ACK
- Periodic snapshots compact the WAL
- Our Option C is essentially the same model

### CockroachDB / TiKV
- Use RocksDB WAL for Raft log persistence
- Each Raft group has its own log in RocksDB
- Raft entries persisted before ACK to leader
- Snapshots (SST files) sent for large state transfers

### Summary

| System | Replicator WAL | User API complexity | Crash recovery |
|--------|---------------|-------------------|----------------|
| SF V1 | None | Simple (7 methods) | Full rebuild only |
| SF V2 | Shared/dedicated log | Heavy (16+ methods) | Checkpoint + replay |
| etcd | Per-node WAL | N/A (built-in KV) | Snapshot + replay |
| PG | WAL segments | N/A (built-in SQL) | Checkpoint + replay |
| **Kuberic (Option C)** | **Per-partition redb** | **Simple (V1-style)** | **WAL replay** |

All production systems persist the log before ACK. Our current
in-memory-only model (V1-style) is appropriate for development but
not production. Option C fills the gap SF left between V1 and V2.

---

## Migration Path

### Phase 1: WAL Trait + redb Backend
1. Define `WalStorage` trait
2. Implement `RedbWalStorage`
3. Implement `InMemoryWalStorage` (for tests, wraps current BTreeMap)
4. Wire into `WalReplicatorActor` — append on data path
5. Wire into `SecondaryReceiver` — append before ACK
6. Tests: verify crash recovery with WAL

### Phase 2: Recovery Protocol
1. Add `GetWalState` RPC (pod reports last_lsn + epoch on restart)
2. Operator `reconcile_replica` checks WAL state before deciding rebuild
3. Implement catch-up replay from primary's WAL (skip full copy)
4. Add user-side WAL replay stream for state recovery

### Phase 3: Compaction + Checkpointing
1. Segment rotation with configurable size
2. Truncation after global committed_lsn advance
3. User checkpointing API (snapshot state → truncate WAL)
4. Checkpoint-based recovery (load snapshot + replay WAL tail)

### Phase 4: Group Commit
1. Batch multiple appends into single fsync (SyncMode::Batched)
2. Latency-throughput trade-off tuning
3. Per-partition WAL configuration

---

## Open Questions

1. **Should secondary WAL ACK replace user ACK for quorum?**
   With framework WAL, the secondary can ACK after local WAL persist
   (fast) rather than after user applies + acknowledges (slow). This
   changes quorum semantics: durability is WAL-level, not user-level.
   Recommended: yes, WAL ACK for quorum (matches etcd/PG model).

2. **Should the primary persist before or after sending to secondaries?**
   Before: safer (primary has local copy even if all secondaries fail)
   After: lower latency (send to secondaries while writing locally)
   Recommended: before (safety first, group commit mitigates latency).

3. **WAL per-partition or per-pod?**
   Per-partition: cleaner isolation, easier cleanup on partition move.
   Per-pod: shared I/O, more complex GC.
   Recommended: per-partition (one redb file per partition).

4. **Maximum WAL size / retention policy?**
   Unbounded WAL is dangerous. Options:
   - Size-based: truncate when WAL exceeds N GB
   - Time-based: retain last N hours of ops
   - LSN-based: retain last N ops
   Recommended: LSN-based (retain ops since min(replica_lsn) for
   replay, plus a configurable buffer).

5. **What happens if WAL is corrupted?**
   Options: treat as full data loss (rebuild from primary),
   or attempt partial recovery (skip corrupt entries).
   Recommended: treat corrupt WAL as empty → full rebuild.
   Log the corruption event for operator visibility.
