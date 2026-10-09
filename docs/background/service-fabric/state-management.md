# Service Fabric: State Management and Persistence

Reliable Collections, state provider interfaces (V1 vs V2),
persistence architecture, and ReadStatus/WriteStatus semantics.

> Part of the [SF Architecture Reference](README.md).

---

## Reliable Collections and the State Manager

For services using the Reliable Services programming model, the **State
Manager** and **Reliable Collections** provide a high-level abstraction over
the replicator:

| Collection | Analogous To |
|---|---|
| **Reliable Dictionary** | `ConcurrentDictionary<K,V>` — replicated key-value store |
| **Reliable Queue** | `ConcurrentQueue<T>` — replicated FIFO queue |
| **Reliable Concurrent Queue** | Best-effort ordering for high throughput |

### How They Use the Replicator

```
   User Code                 State Manager              Replicator
      │                           │                         │
      │── tx.AddAsync(k,v) ──────►│                         │
      │                           │── Replicate(op) ───────►│
      │                           │                         │── Op ──► Secondaries
      │                           │                         │◄─ ACKs ─┘
      │                           │◄── Quorum committed ───│
      │◄── tx.CommitAsync() ─────│                         │
      │    returns                │                         │
```

- **All reads are local** (from the replica's own state)
- **All writes replicate** through the replicator before commit returns
- **Transactions** can span multiple Reliable Collections atomically
- **Persisted mode:** Data written to disk AND replicated (survives full
  cluster restart)
- **Volatile mode:** Data replicated but kept in-memory only (survives
  individual node failures, not full cluster loss)

### Primary Demotion and Write Revocation

When a primary is demoted:
1. Write access to Reliable Collections is **immediately revoked**
2. Any in-flight transactions get `FabricNotPrimaryException`
3. The service must handle this — transient exceptions can be retried on the
   new primary
4. Secondary replicas have **read-only access** to Reliable Collections

### Persistence Architecture: V1 vs V2 Replicator

SF has **two distinct replicators** with different persistence models:

**V1 Replicator** (`Reliability/Replication/`) — the "Fabric Replicator":
- Pure in-memory operation queues (`OperationQueue`, `ReplicationQueueManager`)
- **No disk I/O of its own** — persistence is the user's responsibility
- `hasPersistedState` flag controls whether it waits for user `acknowledge()`
- Volatile services: auto-ACK (no disk writes anywhere)
- Persisted services: defers ACK until user calls `acknowledge()`
- **This is what Kuberic models**

**V2 Transactional Replicator** (`data/txnreplicator/`) — powers Reliable Collections:
- Wraps V1 replicator and adds its own `LoggingReplicator` + `PhysicalLogWriter`
- Maintains a **transaction log** on disk (the "KTL log")
- All Reliable Collection mutations are logged before replication
- This is where the shared/dedicated log architecture lives

See [V2 Transactional Replicator](v2-transactional-replicator.md) for the
component design, transaction and recovery paths, copy protocol, and complete
API ownership model.

### V1 Restart and Recovery Ownership

The V1 Fabric Replicator is stateless across process restart in the narrow
durability sense: it does not reopen a replicator-owned log, checkpoint, or
metadata database. Its live role engine, operation queues, peer sessions,
replica-set configuration, and queue progress disappear with the process.
Recovery works because Service Fabric divides the required facts between the
service's state provider and the Reconfiguration Agent (RA).

| Recovery fact | Durable or authoritative owner |
|---|---|
| Applied service data and last committed LSN | `IStateProvider` |
| Epoch metadata needed by the service | `IStateProvider::BeginUpdateEpoch` / `EndUpdateEpoch` |
| Copy representation and retained application history | `IStateProvider::GetCopyContext` / `GetCopyState` |
| Partition membership, previous/current configuration, replica roles and reconfiguration phase | RA and Failover Manager state |
| Live replication queue, peer ACKs, send windows and sessions | V1 Replicator memory only |

On restart, the responsibilities compose as follows:

1. The RA recreates the service and V1 Replicator and drives the required role,
   epoch, replica-set configuration and catch-up operations again.
2. When creating an initial primary, the Replicator asks the state provider for
   `GetLastCommittedSequenceNumber()`. It uses that value to initialize the
   next replication LSN; it does not recover the value from replicator-owned
   storage
   (`src/prod/src/Reliability/Replication/Replicator.ChangeRoleAsyncOperation.cpp:128-184`).
3. Epoch changes are forwarded to the state provider together with the last
   LSN from the previous epoch. Successful completion means the provider has
   accepted whatever durable fencing or epoch work its implementation requires
   (`src/prod/src/Reliability/Replication/PrimaryReplicator.UpdateEpochAsyncOperation.cpp:25-87`;
   `src/prod/src/Reliability/Replication/ComProxyStateProvider.cpp:63-150`).
4. The RA supplies previous/current membership and quorum configuration through
   the primary-replicator configuration APIs. The V1 Replicator reconstructs
   its in-memory membership and sessions from those calls rather than loading
   them from disk.
5. If a replica cannot catch up from retained operations, the Replicator asks
   the primary state provider for copy context/state and streams that
   application-defined snapshot to the rebuilding secondary.

The constructor's `hasPersistedState` argument does **not** make the V1
Replicator persistent. It tells the replication protocol whether the service
has durable state and therefore affects acknowledgement, copy, and queue
retention behavior. The durable write boundary remains the state provider:
for a persisted service, it applies and persists an operation before
acknowledging the operation stream item. For a volatile service, no disk
recovery is promised.

This division also explains why the V1 Replicator can discard its process-local
metadata safely:

- the state provider is the durable authority for application progress;
- the RA is the authority for topology, role, and reconfiguration intent;
- the Replicator is an executable, reconstructable data-plane component
  between those two owners.

It must not be interpreted as the whole Service Fabric replica being
stateless. The **Replicator component** is restart-reconstructable, while the
state provider and RA retain the durable facts needed to rebuild it.

### Shared Log vs Dedicated Log (Windows vs Linux)

The V2 Transactional Replicator uses a two-tier log architecture, with
a critical platform difference:

**Windows (`PLATFORM_UNIX` not defined) — OutOfProc mode:**
- Uses a **kernel-mode log driver** (`KtlLogManager::CreateDriver`)
- Backed by CLFS (Common Log File System), a Windows kernel subsystem
- Multiple partitions on the same node share a single **shared log** file
- Each partition also has a **dedicated log** for overflow
- The kernel driver batches writes from many partitions into one fsync
  (like group commit across services) — very efficient I/O

**Linux (`PLATFORM_UNIX` defined) — InProc mode:**
- Uses a **user-space sparse file** implementation (`KtlLogManager::CreateInproc`)
- Each partition writes to its own file — no shared log multiplexing
- Every partition pays its own fsync cost (more I/O overhead)
- Falls back to InProc even on Windows if the driver is not loaded

From `LogManager.cpp`:
```cpp
if (ktlLoggerMode == KtlLoggerMode::Default) {
#if !defined(PLATFORM_UNIX)
    ktlLoggerMode = KtlLoggerMode::OutOfProc;   // Windows: kernel driver
#else
    ktlLoggerMode = KtlLoggerMode::InProc;       // Linux: user-space
#endif
}
```

**Key insight:** This shared/dedicated log architecture exists entirely
within the V2 Transactional Replicator (Reliable Collections), NOT in
the V1 Replicator. The V1 Replicator has no log of its own on either
platform. Custom services using V1 directly (with `IStateProvider`)
handle their own persistence regardless of OS.

**Relevance to Kuberic:** Since we model V1, the Windows/Linux
logging difference doesn't affect us. Our Option C (framework WAL)
is conceptually similar to what V2 does — adding a replicator-level
log — but much simpler: per-partition file, no shared log, no kernel
driver. A per-partition redb file is the appropriate model for K8s.

### API Surface: CreateReplicator vs CreateTransactionalReplicator

The partition object exposes two different factory methods for creating
replicators. They live on different COM interfaces — one public, one
internal:

| API | COM Interface | Header | Returns |
|-----|---------------|--------|---------|
| `CreateReplicator()` | `IFabricStatefulServicePartition` | `FabricRuntime.h` (public SDK) | V1 Replicator |
| `CreateTransactionalReplicator()` | `IFabricInternalStatefulServicePartition` | `FabricRuntime_.h` (**internal**) | V2 wrapping V1 |

**`CreateReplicator(stateProvider, settings)` — V1 path:**
```
partition → ReplicatorFactory.CreateReplicator()
  → ComReplicator (V1, in-memory queues)
  → Returns IFabricReplicator + IFabricStateReplicator
```
This is the documented public API. The user provides an `IStateProvider`
and receives raw operation bytes on the replication/copy streams. The
user is fully responsible for persistence. This is what Kuberic models.

**`CreateTransactionalReplicator(factory, dataLossHandler, ...)` — V2 path:**
```
partition → TransactionalReplicatorFactory.CreateReplicator()
  → ComTransactionalReplicator
    → internally creates V1 via ReplicatorFactory.CreateReplicator()
    → wraps V1 with LoggingReplicator + PhysicalLogWriter
    → adds transaction protocol, shared/dedicated log
  → Returns IFabricPrimaryReplicator + TransactionalReplicator handle
```

From `ComTransactionalReplicator.cpp`:
```cpp
// Line 205: Creates V1 internally
hr = replicatorFactory.CreateReplicator(...)

// Line 52: V1 is a required inner component
"V1 replicator com object must implement IFabricStateReplicator"

// Line 489: V2 delegates lifecycle to V1
"All the below methods are just a pass through to the V1 replicator"
```

**V2 is NOT hidden inside Reliable Collections.** It's accessible through
the internal COM interface `IFabricInternalStatefulServicePartition`.
Users can QI (QueryInterface) for it and call
`CreateTransactionalReplicator()` with their own
`IFabricStateProvider2Factory`. This enables building **custom
transactional state providers** without using `ReliableDictionary` or
`ReliableQueue` — they plug into V2's transaction protocol and get the
shared/dedicated log for free.

**In practice, who uses what:**
- **C#/.NET services** call `StatefulServiceBase.CreateReliableStateManager()`
  which internally calls `CreateTransactionalReplicator()` — they never
  see V2 directly
- **C++ services** using `IStateProvider` call `CreateReplicator()` (V1)
- **C++ services** wanting transactions call
  `CreateTransactionalReplicator()` with a custom `IStateProvider2Factory`
- **The key difference:** V1's `IStateProvider` receives raw opaque bytes.
  V2's `IStateProvider2` participates in a structured transaction protocol
  with the LoggingReplicator (begin/commit/abort semantics)

### IStateProvider vs IStateProvider2: Choosing an Interface

The two interfaces have fundamentally different contracts. The choice
determines how much work the user does vs how much the framework handles.

**IStateProvider (V1)** — raw bytes, user handles everything:
```
User implements:
  GetLastCommittedSequenceNumber()     — report current LSN
  GetCopyContext()                     — tell primary what you have
  GetCopyState(upToLsn, context)       — produce full state for secondary
  UpdateEpoch(epoch, prevLastLsn)      — handle epoch change
  OnDataLoss()                         — accept data loss decision

User consumes:
  IFabricOperationStream::GetOperation()  — receive raw bytes with LSN
  IFabricOperation::Acknowledge()         — signal "I persisted this op"

User is responsible for:
  ✗ Parsing raw bytes (own serialization format)
  ✗ Applying ops to state store
  ✗ Persisting to disk before calling acknowledge()
  ✗ Building copy streams (serializing entire state)
  ✗ Recovery from copy streams
  ✗ Transaction semantics (if any)
  ✗ Checkpoint management
```

**IStateProvider2 (V2)** — structured transactions, V2 handles the log:
```
User implements:
  ApplyAsync(lsn, txn, context, metadata, data)  — structured apply
  Unlock(operationContext)             — release locks after commit
  PrepareCheckpoint(lsn)              — prepare for checkpoint
  PerformCheckpointAsync()            — write checkpoint to disk
  CompleteCheckpointAsync()           — finalize checkpoint
  RecoverCheckpointAsync()            — recover from checkpoint on restart
  BackupCheckpointAsync(dir)          — backup for disaster recovery
  RestoreCheckpointAsync(dir)         — restore from backup
  GetCurrentState()                   — produce copy data for secondary
  BeginSettingCurrentState()          — start receiving copy
  SetCurrentStateAsync(recordNum, data) — receive copy data
  EndSettingCurrentState()            — copy complete

V2 handles for user:
  ✓ Transaction log (LoggingReplicator + PhysicalLogWriter)
  ✓ Transaction semantics (begin/commit/abort across state providers)
  ✓ Replication (V1 replicator underneath)
  ✓ Epoch management and truncation
  ✓ WAL persistence (shared/dedicated log)
  ✓ Checkpoint coordination (triggers PrepareCheckpoint/PerformCheckpoint)
  ✓ Recovery orchestration (replay log from last checkpoint)
```

**When to choose which:**

| Choose V1 (IStateProvider) | Choose V2 (IStateProvider2) |
|---|---|
| Simple replication of opaque data | Need multi-collection transactions |
| Already have a storage engine (RocksDB, SQLite) | Want framework-managed WAL and checkpointing |
| Want full control over persistence | Want the framework to handle recovery |
| Minimal API surface (7 methods) | Willing to implement larger API (16+ methods) |
| Lower overhead, fewer abstractions | More correctness guarantees from framework |

**Analogy:** V1 is like building a database on raw block storage — you
handle everything. V2 is like building a storage engine plugin for
PostgreSQL — you implement storage access methods, the framework handles
WAL, transactions, and recovery.

**Relevance to Kuberic:** We model V1's `IStateProvider` contract
(our `LifecycleEvent` + `StateProviderEvent` channels). Our WAL design
(Option C) adds V2-like log persistence at the replicator level without
requiring users to adopt the full V2 checkpoint/apply/unlock contract.

---

## StateReplicator and StateProvider Interfaces

Below Reliable Collections sits a lower-level contract between the **Fabric
Replicator** (system-provided) and the **State Provider** (user/system-
implemented). Understanding this layer is critical for building custom
replication — it's the interface our kuberic design is modeled after.

**Source:** `service-fabric-apps-rs/crates/libs/mssf-ext/src/traits.rs`,
Microsoft documentation for `IStateReplicator` and `IStateProvider`.

### IStateProvider — What the Service Implements

The state provider is the service's state management layer. The Fabric
Replicator calls it to manage state during replication and reconfiguration.

```
trait StateProvider {
    // Epoch barrier — segments operations by primary configuration
    async fn update_epoch(epoch, previous_epoch_last_lsn, token) -> Result<()>;

    // Returns the last committed (applied + persisted) LSN
    fn get_last_committed_sequence_number() -> Result<i64>;

    // Called on data loss (quorum lost). Returns true if state was modified.
    async fn on_data_loss(token) -> Result<bool>;

    // Secondary → Primary: send context about what state this secondary has
    fn get_copy_context() -> Result<OperationDataStream>;

    // Primary → Secondary: send state needed to build the secondary
    // up_to_lsn: everything up to this LSN goes via copy; after this via replication
    fn get_copy_state(up_to_lsn, copy_context_stream) -> Result<OperationDataStream>;
}
```

**Key details:**
- `update_epoch()` receives `previous_epoch_last_lsn` — the last LSN from
  the previous primary. The state provider uses this to fence out stale
  operations.
- `get_copy_context()` / `get_copy_state()` form a **bidirectional exchange**:
  the secondary sends its context (what it already has), and the primary
  responds with only the missing state. For empty secondaries (in-memory
  services), `get_copy_context()` returns empty — the primary sends
  everything.
- `get_last_committed_sequence_number()` is called by the replicator to
  determine where the copy stream should start and to report progress for
  primary selection.

### IStateReplicator — What the Service Calls

The state replicator is the Fabric Replicator's interface exposed to the
service. The primary uses it to replicate; secondaries use it to receive.

```
trait StateReplicator {
    // Primary: replicate data to quorum. Returns (lsn, future<lsn>).
    // lsn is assigned immediately; future completes on quorum ACK.
    fn replicate(data, token) -> (i64, Future<i64>);

    // Secondary: get the stream of copy operations (full state transfer)
    fn get_copy_stream() -> OperationStream;

    // Secondary: get the stream of replication operations (incremental)
    fn get_replication_stream() -> OperationStream;

    // Modify replicator settings at runtime (e.g., security credentials)
    fn update_replicator_settings(settings) -> Result<()>;
}
```

**Key details:**
- `replicate()` returns **both** an immediate LSN (for local bookkeeping)
  **and** a future that completes when quorum ACKs the operation. The service
  can prepare a local write immediately using the LSN, then commit when the
  future completes.
- `get_copy_stream()` and `get_replication_stream()` return `OperationStream`
  objects. The secondary **pulls** operations from these streams by calling
  `get_operation()`. This is a pull model, not push.

### IOperation — Individual Replicated Operations

Each operation from the copy or replication stream:

```
trait Operation {
    fn get_metadata() -> OperationMetadata;  // type + sequence number
    fn get_data() -> Buf;                    // the replicated bytes
    fn acknowledge() -> Result<()>;          // ACK: "I've applied this"
}
```

**The `acknowledge()` method is the critical ACK mechanism:**
- For **persisted services**, calling `acknowledge()` is **mandatory**. The
  Fabric Replicator will NOT release subsequent operations until the current
  one is acknowledged. This creates backpressure — the secondary controls
  the pace.
- For **volatile services**, operations are implicitly acknowledged on
  receipt (unless `isRequireServiceAck` is set to true).
- An operation must be acknowledged by a **quorum** of replicas before the
  primary's `replicate()` future completes.

### Data Flow: Primary Side

```
User Code          StateProvider        StateReplicator       FabricReplicator
   │                    │                    │                      │
   │── write(data) ────►│                    │                      │
   │                    │── replicate(data) ►│                      │
   │                    │   returns (lsn,    │                      │
   │                    │    future)         │                      │
   │                    │                    │── send to secondaries │
   │                    │                    │◄── quorum ACKs ──────│
   │                    │◄── future done ───│                      │
   │◄── commit ────────│                    │                      │
```

### Data Flow: Secondary Side (Pull Model)

```
FabricReplicator         StateReplicator              User/StateProvider
      │                       │                              │
      │                       │◄── get_copy_stream() ───────│
      │── copy ops ──────────►│                              │
      │                       │──── Operation ──────────────►│
      │                       │                        apply + persist
      │                       │◄──── acknowledge() ─────────│
      │                       │                              │
      │   (copy done)         │                              │
      │                       │◄── get_replication_stream() ─│
      │── repl ops ──────────►│                              │
      │                       │──── Operation ──────────────►│
      │                       │                        apply + persist
      │                       │◄──── acknowledge() ─────────│
      │◄── ACK ──────────────│                              │
```

**Key insight — `Acknowledge()` semantics depend on the service mode:**

The behavior of `Operation.Acknowledge()` depends on the
`RequireServiceAck` replicator setting:

**Persisted services** (`RequireServiceAck = true`, the default):
- `Acknowledge()` IS in the quorum path
- The primary's `replicate()` does NOT complete until a quorum of
  secondaries have called `Acknowledge()`
- This means the secondary's apply speed directly affects primary write
  latency
- The durability guarantee is strong: quorum commit = quorum of services
  have applied the operation
- From the .NET docs: *"An operation must be acknowledged by a quorum of
  replicas before the Primary replica receives the ReplicateAsync operation
  complete responses."*

**Volatile services** (`RequireServiceAck = false`):
- `Acknowledge()` is NOT in the quorum path
- The replicator auto-ACKs to the primary on receive
- `Acknowledge()` only releases the operation from the replicator's queue
  (memory management / backpressure)
- The primary's `replicate()` completes faster (on replicator receive, not
  service apply)

**Internally** (from C++ source, `ReplicationAckMessageBody.h`):

The ACK message sent from secondary to primary contains **four values**:

```
ReplicationAckMessageBody {
    replicationReceivedLSN   // what replicator received (auto-tracked)
    replicationQuorumLSN     // what service acknowledged via Acknowledge()
    copyReceivedLSN          // copy received
    copyQuorumLSN            // copy acknowledged
}
```

The variable name `replicationQuorumLSN` is definitive — this IS the value
used for quorum evaluation. In persisted mode, it advances only when the
service calls `Acknowledge()`. In volatile mode, the replicator auto-advances
it on receive (making it equal to `replicationReceivedLSN`).

The primary's `PrimaryReplicator::ReplicationAckMessageHandler()` passes
both values to `session->UpdateAckProgress()`, which evaluates quorum
against the QuorumLSN fields.

**Implications for volatile mode:**

Volatile mode (`RequireServiceAck = false`) trades durability for latency:

| Aspect | Persisted | Volatile |
|---|---|---|
| Quorum gated on | Service `Acknowledge()` | Replicator receive |
| Write latency | Includes secondary apply time | Replicator-to-replicator only |
| On failover | Promoted secondary has applied all quorum-ACKed ops | Promoted secondary may NOT have applied all ops yet — user must drain stream to catch up |
| Data loss risk | None (quorum = applied) | Possible gap between received and applied |
| When to use | Default. Databases, KV stores. | In-memory caches, volatile state |

**The critical safety implication:** In volatile mode, after failover the
new primary's user state may lag behind what was quorum-committed. The
replicator has the data (received), but the user hasn't applied it yet. The
user MUST drain the replication stream before accepting new writes — otherwise
the new primary's state is inconsistent with what clients saw as committed.

### Copy Stream vs Replication Stream

| Stream | When | Content | Purpose |
|---|---|---|---|
| **Copy** | During `build_replica` | Full state snapshot | Brings new/empty secondary up to a point-in-time |
| **Replication** | After copy completes | Incremental operations | Ongoing stream of new writes from the primary |

The copy stream has an `up_to_lsn` boundary. Everything ≤ `up_to_lsn` comes
via copy; everything > `up_to_lsn` comes via the replication stream. The
replicator buffers operations with LSN > `up_to_lsn` during the copy phase
and delivers them once the secondary starts consuming the replication stream.

**Recommended ordering:** Drain copy stream first, then replication stream.
Parallel consumption is supported but complex.

---

## ReadStatus and WriteStatus

Service Fabric exposes **ReadStatus** and **WriteStatus** on the stateful
service partition. These are the mechanism by which the runtime communicates to
user code whether it is safe to accept client requests. Services **must** check
these before processing reads or writes.

### PartitionAccessStatus Enum

| Value | Meaning | Service Action |
|---|---|---|
| **Granted** | The operation is allowed. The replica is primary and has quorum. | Process the request. |
| **ReconfigurationPending** (TryAgain) | A reconfiguration is in progress. The status will change when it completes. | Return a retryable error to the client. |
| **NotPrimary** | This replica is not the primary. It cannot accept reads or writes. | Redirect the client to the primary (via Naming Service). |
| **NoWriteQuorum** | The primary does not have a write quorum. Reads may still be allowed. | Return a retryable error for writes; reads may succeed. |

### How ReadStatus and WriteStatus Are Computed

**Source:** `src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.ReadWriteStatusCalculator.cpp`

The RA computes ReadStatus and WriteStatus as a function of **two dimensions**:

1. **Lifecycle state** — Is the replica opening, ready, or closing?
2. **Reconfiguration state** — What phase of reconfiguration is in progress,
   and what type?

The calculator maintains separate state tables for four reconfiguration
scenarios, each with five sub-phases:

```
Reconfiguration Sub-Phases:
  PreWriteStatusCatchup   — Before the initial catchup (swap primary only)
  TransitioningRole       — Role change in progress
  CatchupInProgress       — Secondaries catching up
  CatchupCompleted        — Catchup done, deactivation/activation pending
  Completed               — Reconfiguration finished
```

### Status During Normal Operation (No Reconfiguration)

| Lifecycle State | ReadStatus | WriteStatus |
|---|---|---|
| **Opening Primary** | TryAgain | TryAgain |
| **Ready Primary** | Granted | Granted (if write quorum in CC) |
| **Other** (secondary, closing, closed) | NotPrimary | NotPrimary |

For a ready primary, WriteStatus is **dynamically** evaluated: it checks
`HasMinReplicaSetAndWriteQuorum()` in real time. If enough secondaries go
down that write quorum is lost, WriteStatus transitions to **NoWriteQuorum**
without any reconfiguration — the primary remains primary but cannot accept
writes until quorum is restored.

ReadStatus for a ready primary is unconditionally **Granted** — reads are
always served from local state.

### Status During Failover (Secondary Promoted to Primary)

Reconfiguration type: **PromoteToPrimary** (S→P, I→P, U→P)  
Phase order: Role Transition → Catchup → Deactivate → Activate

| Phase | ReadStatus | WriteStatus |
|---|---|---|
| TransitioningRole | TryAgain | TryAgain |
| CatchupInProgress | TryAgain | TryAgain |
| **CatchupCompleted** | **Granted** | TryAgain |
| **Completed** | Granted | **Granted** (if write quorum) |

**Key insight:** ReadStatus is granted **after catchup completes** (Phase 2
done) but WriteStatus is only granted **after the full reconfiguration
completes** (Phase 4 done). This means the new primary can serve reads before
it can accept writes, minimizing read unavailability.

### Status During Swap Primary (Primary Demoted to Secondary)

Reconfiguration type: **DemoteToSecondary** (P→S)  
Phase order: PreWriteStatusCatchup → Catchup → Role Transition → Complete

| Phase | ReadStatus | WriteStatus |
|---|---|---|
| **PreWriteStatusCatchup** | Granted | **Granted** (if quorum in PC AND CC) |
| CatchupInProgress | Granted | **TryAgain** (revoked) |
| CatchupCompleted | **TryAgain** (revoked) | TryAgain |
| TransitioningRole | NotPrimary | NotPrimary |
| Completed | NotPrimary | NotPrimary |

**Key insight:** During swap, the demoting primary goes through a carefully
staged revocation:

1. **PreWriteStatusCatchup** — Both read and write are still granted. The
   replicator performs an initial catchup while clients can still write. This
   is the "double catchup" optimization — catching up once with writes flowing
   reduces the final catchup window.
2. **WriteStatus revoked first** — Once the pre-catchup completes, write
   status is revoked (TryAgain) but reads continue.
3. **ReadStatus revoked after catchup** — Once the full catchup completes,
   reads are also revoked.
4. **NotPrimary after role transition** — Once the old primary becomes a
   secondary, both statuses become NotPrimary permanently.

This staged revocation minimizes the window where clients cannot read, and
ensures the new primary has all committed data before it starts accepting
writes.

### Status During No-Primary-Change Reconfiguration (P→P)

When secondaries are added/removed but the primary stays the same:

| Phase | ReadStatus | WriteStatus |
|---|---|---|
| CatchupInProgress | Granted | Granted (if quorum in PC AND CC) |
| CatchupCompleted | Granted | Granted (if quorum in PC AND CC) |
| Completed | Granted | Granted (if quorum in CC only) |

**Key insight:** Read and write remain granted throughout — there is no
disruption to client requests. The only change is that during the transition,
writes must be acknowledged by quorum from **both** the old and new
configurations (dual quorum). Once complete, only the new configuration's
quorum is needed.

### Status for Idle-to-Active Promotion (I→S)

| Phase | ReadStatus | WriteStatus |
|---|---|---|
| TransitioningRole | NotPrimary | NotPrimary |
| Completed | NotPrimary | NotPrimary |

Idle and active secondaries never get read or write access through this
mechanism (they are not primary).

### Dynamic Quorum Checks

When WriteStatus is marked as "dynamic" in the calculator, the actual value
is computed at query time:

```cpp
// From ReadWriteStatusCalculator.cpp
if (isDynamic_) {
    return fup.HasMinReplicaSetAndWriteQuorum(fupLock, needsPC_)
        ? AccessStatus::Granted
        : AccessStatus::NoWriteQuorum;
}
```

The `needsPC_` flag controls whether the check requires quorum from both
Previous Configuration and Current Configuration (during reconfiguration) or
just the Current Configuration (normal operation).

### How Services Should Use ReadStatus/WriteStatus

Services should check status **before** processing each request:

```csharp
// C# example pattern
public async Task<string> HandleReadRequest()
{
    // Check read access
    if (this.Partition.ReadStatus != PartitionAccessStatus.Granted)
        throw new FabricNotReadableException();

    // Proceed with read from Reliable Collections
    using (var tx = this.StateManager.CreateTransaction())
    {
        var value = await myDictionary.TryGetValueAsync(tx, key);
        return value.Value;
    }
}

public async Task HandleWriteRequest(string key, string value)
{
    // Check write access
    if (this.Partition.WriteStatus != PartitionAccessStatus.Granted)
        throw new FabricNotPrimaryException();

    // Proceed with write to Reliable Collections
    using (var tx = this.StateManager.CreateTransaction())
    {
        await myDictionary.SetAsync(tx, key, value);
        await tx.CommitAsync();  // Replicates to quorum
    }
}
```

**Important caveats:**
- The status can change **between the check and the actual operation**. The
  replicator may still throw `FabricNotPrimaryException` or similar even
  after a successful WriteStatus check.
- When a primary is demoted, write access to Reliable Collections is
  **immediately revoked** — in-flight transactions get `FabricNotPrimaryException`.
- Services must handle both `FabricNotPrimaryException` (permanent, redirect
  client) and `FabricTransientException` (transient, retry).

### ReadWriteStatusRevokedNotification

When read/write status is revoked during reconfiguration, the RA sends a
**ReadWriteStatusRevokedNotification** message to the failover unit. This
triggers the partition to update its cached status and unblock any threads
waiting on `GetReadStatus()` / `GetWriteStatus()` calls with the new value.

This notification mechanism ensures that long-running operations don't
continue operating under stale status — they are promptly informed of the
status change.

### Summary: Status Across the Full Failover Timeline

```
Time ──────────────────────────────────────────────────────────────►

OLD PRIMARY (P→S swap):
  Read:   Granted──────────────────┐TryAgain──┐NotPrimary──────────
  Write:  Granted──────┐TryAgain───┘           │
                       │                       │
           PreCatchup  Catchup     Completed   Role Changed
           (double     (write
            catchup)    revoked)

NEW PRIMARY (S→P failover):
  Read:   NotPrimary──┐TryAgain──────┐Granted──────────────────────
  Write:  NotPrimary──┘              │TryAgain──────┐Granted───────
                                     │              │
                      Role Changed   Catchup Done   Activate Done
```

### How the Replicator Reacts to ReadStatus/WriteStatus

The Fabric Replicator (the built-in replicator) checks WriteStatus **before
every replicate call**. This is the gate that prevents writes from being
accepted when the partition is in a transitional state.

**Source:** `src/prod/src/Reliability/Replication/Replicator.ReplicateAsyncOperation.cpp`

When `BeginReplicate()` is called by user code (via Reliable Collections or
directly), the replicator:

1. Acquires a read lock on its internal state
2. Calls `VerifyAccessGranted()` which reads `partition_.GetWriteStatus()`
3. Maps the status to an error code:

```
WriteStatus                        → Error Returned to Caller
─────────────────────────────────────────────────────────────
GRANTED                            → (proceed with replication)
RECONFIGURATION_PENDING (TryAgain) → ReconfigurationPending
NO_WRITE_QUORUM                    → NoWriteQuorum
NOT_PRIMARY                        → NotPrimary
```

4. If access is granted, verifies the replicator is actually in primary state
   (belt-and-suspenders check — if WriteStatus is Granted but the replicator
   is not primary, this is a test assertion failure)
5. Only then delegates to `PrimaryReplicator::BeginReplicate()` to enqueue the
   operation for quorum replication

**Key design:** The WriteStatus check happens **inside the replicator**, not
just in user code. This means even if user code forgets to check WriteStatus,
the replicator itself will reject the operation with the appropriate error.
User code checking first is an optimization to avoid the overhead of entering
the replicator's lock and constructing operation objects.

### How User Services Must React

User services interact with ReadStatus/WriteStatus at two levels: checking
the status proactively before operations, and handling exceptions from
operations that fail due to status changes.

#### Level 1: Proactive Status Checks

Before processing any client request, the service should check the
appropriate status on the partition object:

```
Client Request Type     Check                     On Non-Granted
──────────────────────────────────────────────────────────────────
Read                    Partition.ReadStatus       Return retryable error
                                                   or redirect to primary
Write                   Partition.WriteStatus      Return retryable error
                                                   or redirect to primary
Read-Write              Both ReadStatus AND        Handle each independently
                        WriteStatus
```

The check is cheap (reads a cached value on the partition object) and prevents
unnecessary work like deserializing request bodies, creating transactions, or
acquiring locks on Reliable Collections.

#### Level 2: Exception Handling from Reliable Collections / Replicator

Even after a successful status check, the status can change before the
operation completes. Services must handle these exceptions:

| Exception | Cause | Action |
|---|---|---|
| **FabricNotPrimaryException** | Replica lost primary role (WriteStatus became NotPrimary during operation) | **Permanent for this replica.** Close/abort any pending work. Client should resolve the new primary via Naming Service and retry there. |
| **FabricNotReadableException** | Replica lost read access (ReadStatus became NotPrimary or TryAgain) | **Permanent for this replica.** Client should resolve and retry. |
| **FabricTransientException** | Temporary condition (NoWriteQuorum, ReconfigurationPending) | **Transient.** Retry on the same replica after a brief delay. The condition may resolve when quorum is restored or reconfiguration completes. |
| **TimeoutException** | Operation waited too long for quorum acknowledgment | **Ambiguous.** The write may or may not have been committed. Client should re-read to check, then retry if needed. |
| **FabricObjectClosedException** | Partition/replica is shutting down | **Permanent.** Re-resolve the service and retry. |

#### Level 3: RunAsync Cancellation

When a primary is demoted, SF cancels the `CancellationToken` passed to
`RunAsync()`. Background work loops must honor this:

```
RunAsync(cancellationToken):
  while not cancellationToken.IsCancellationRequested:
    try:
      // Check write status before doing work
      if partition.WriteStatus != Granted:
        sleep(backoff)
        continue

      // Do work with Reliable Collections
      using tx = stateManager.CreateTransaction():
        await dictionary.SetAsync(tx, key, value)
        await tx.CommitAsync()

    catch FabricNotPrimaryException:
      // We lost primary, stop the loop
      return

    catch FabricTransientException:
      // Transient, retry after backoff
      sleep(backoff)
```

**Critical rule:** If `RunAsync()` does not exit promptly after cancellation,
the entire reconfiguration stalls. SF will eventually force-terminate the
service after a timeout (default 15 minutes), causing a health warning and
potentially a failed upgrade.

#### Level 4: ReportFault — Service-Initiated Failover

Services can also **proactively trigger failover** via `ReportFault()` on the
partition when they detect an unrecoverable condition:

| Fault Type | Effect |
|---|---|
| **Transient** | The replica is restarted on the same node. State is preserved (for persisted services). Equivalent to a process crash. |
| **Permanent** | The replica is dropped and rebuilt elsewhere. Local state is deleted. Use when the local state is corrupted. |

```
// Example: Service detects corrupted state
if stateIsCorrupted:
    partition.ReportFault(FaultType.Permanent)
    // SF will drop this replica and build a new one from another replica

// Example: Service detects transient issue (e.g., port conflict)
if needsRestart:
    partition.ReportFault(FaultType.Transient)
    // SF will restart this replica on the same node
```

`ReportFault(Permanent)` is particularly important for data integrity — it
tells SF that this replica's state cannot be trusted and must be rebuilt from
a healthy replica.

### Complete Request Processing Flow

Putting it all together, here is the full flow for a write request:

```
                Client
                  │
                  ▼
         ┌────────────────┐
         │ Service Replica │
         │ (Primary)       │
         └───────┬─────────┘
                 │
    ① Check Partition.WriteStatus
                 │
         ┌───────┴───────────────────┐
         │ Granted?                  │ No → return error to client
         │                           │      (NotPrimary / TryAgain /
         │                           │       NoWriteQuorum)
         ▼                           │
    ② Create Transaction             │
    ③ Perform writes on              │
       Reliable Collections          │
    ④ tx.CommitAsync()               │
         │                           │
         ▼                           │
  ┌──────────────────┐               │
  │ State Manager    │               │
  │ (Reliable Svcs)  │               │
  └──────┬───────────┘               │
         │                           │
         ▼                           │
  ┌──────────────────┐               │
  │ Replicator       │               │
  │                  │               │
  │ ⑤ VerifyAccess-  │               │
  │   Granted()      │               │
  │   (checks Write- │               │
  │    Status again)  │               │
  │                  │               │
  │ ⑥ If Granted:    │               │
  │   PrimaryRepl.   │               │
  │   BeginReplicate │               │
  │                  │               │
  │ ⑦ Send operation │               │
  │   to secondaries │               │
  │                  │               │
  │ ⑧ Wait for       │               │
  │   write quorum   │               │
  │   ACKs           │               │
  │                  │               │
  │ ⑨ Commit locally │               │
  └──────┬───────────┘               │
         │                           │
         ▼                           │
    Return success                   │
    to client                        │
```

**Key observation:** WriteStatus is checked **twice** — once by the service
(step ①) and once by the replicator (step ⑤). The service check is an
optimization; the replicator check is the authoritative gate. Between these
two checks, a reconfiguration could change the status, which is why exception
handling (Level 2 above) is essential even after a successful service-level
check.

---
