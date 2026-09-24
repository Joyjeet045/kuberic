# Service Fabric: Replication Protocol

The replicator protocol, quorum mechanics, and replica building
for Azure Service Fabric stateful services.

> Part of the [SF Architecture Reference](README.md).

---

## The Replicator Protocol

The **Replicator** (also called the Fabric Replicator when using the built-in
implementation) sits between the RA and the service's state provider. It
handles:

1. **Quorum-based replication** of operations from primary to secondaries
2. **Copy stream** for seeding new replicas with full state
3. **Replication stream** for incremental operations
4. **LSN tracking** for progress and catchup capability
5. **Configuration management** during reconfigurations

### Two Streams

| Stream | Direction | Purpose |
|---|---|---|
| **Copy Stream** | Primary → Idle Secondary | Full state transfer during `build_replica()`. Brings a new/empty replica up to the current committed state. |
| **Replication Stream** | Primary → Active Secondaries | Incremental operations. Every write on the primary is sent as an operation with a monotonically increasing LSN. |

### LSN (Log Sequence Number)

Every operation gets a unique, monotonically increasing LSN. Two LSN values
are tracked per replica:

| LSN | Meaning | API |
|---|---|---|
| **Current Progress** (end of log) | The highest LSN this replica has received/applied | `get_current_progress()` |
| **Catch-Up Capability** (beginning of log) | The oldest LSN this replica still has available | `get_catch_up_capability()` |

**Primary selection uses Current Progress** — the replica with the highest
current progress (most data) is the best candidate.

**Catch-up feasibility uses Catch-Up Capability** — if a secondary's current
progress is less than a candidate primary's catch-up capability, that secondary
cannot catch up incrementally and must be rebuilt from scratch.

### Quorum Acknowledgment

```
    Primary                Secondary-1            Secondary-2
       │                       │                       │
       │──── Op(LSN=42) ──────►│                       │
       │──── Op(LSN=42) ──────────────────────────────►│
       │                       │                       │
       │◄─── ACK(LSN=42) ─────│                       │
       │                       │                       │
       │   WriteQuorum met (2 of 3)                    │
       │   Commit LSN=42                               │
       │                       │                       │
       │◄─── ACK(LSN=42) ────────────────────────────│
       │                       │                       │
```

- **Write Quorum** = ⌊ReplicaCount / 2⌋ + 1 (majority including primary)
- An operation is **committed** once acknowledged by a write quorum
- During reconfiguration with both current and previous configurations,
  writes must be acknowledged by a write quorum of **both** configurations

### Async Dispatch to Secondaries (from C++ source)

The primary replicator **never blocks** on any individual secondary when
sending operations. The write path is fully asynchronous:

**Source:** `ReplicaManager.cpp`, `ReplicationSession.cpp`

```cpp
// In AddReplicateOperationPrivate:

// 1. Enqueue op into the replication queue (in-memory, fast)
replicationQueueManager_.Enqueue(move(comOperationPointer), epoch_, opComPtr);

// 2. Dispatch to sessions via a JOB QUEUE (async, non-blocking)
beginReplicateJobQueueUPtr_->Enqueue(
    ComReplicateJobPtr(move(opRawPtr), move(sessions), currentCompletedLsn));

// 3. Return immediately — primary's write path is not blocked
return ErrorCode(Common::ErrorCodeValue::Success);
```

The `beginReplicateJobQueueUPtr_` is an async job queue that processes
sends to each session in a separate thread. Each `SendReplicateOperation`
creates a transport message and calls `SendTransportMessage`, which is a
**fire-and-forget datagram send** — no waiting for the message to be
delivered.

```cpp
// In ReplicationSession::SendReplicateOperation:
MessageUPtr message = ReplicationTransport::CreateReplicationOperationMessage(...);
return SendTransportMessage(replicationOperationHeadersSPtr_, move(message), true);

// SendTransportMessage calls transport_->SendMessage() which is non-blocking
```

**Key design points:**
- The primary's `replicate()` call enqueues the op and returns immediately
- Sending to each secondary happens in a background job queue
- A slow secondary doesn't block the primary or other secondaries
- Backpressure is handled at the quorum level: `replicate()` returns a
  future that completes when write quorum ACKs arrive. The primary can
  continue accepting new writes while waiting for ACKs.
- If a secondary's send queue fills up (`REQueueFull`), the replicator
  logs a health warning but continues operating

**Contrast with our implementation:** Our `send_to_all` awaits
`item_tx.send()` sequentially for each secondary. A slow secondary
blocks the entire actor loop, preventing new writes from being processed.
The fix is to use unbounded (or very large) per-secondary channels so
`send_to_all` never blocks, matching SF's fire-and-forget dispatch model.

### Retry and Flow Control (ReliableOperationSender)

Each secondary connection is wrapped in a `ReliableOperationSender`
that handles retries and flow control independently.

**Source:** `ReliableOperationSender.h/cpp`

**Mechanism:**
- Each sender maintains a **send window** (`sendWindowSize_`, starts at
  a configurable value, max bounded by `maxSendWindowSize_`).
- Ops are sent up to the window size. Unsent ops queue in `pendingOperations_`.
- A **retry timer** (`retrySendTimer_`) fires every `RetryInterval`
  (configurable). On each tick:
  1. If no ACK received since last tick, **halve the send window**
     (backoff for overwhelmed secondaries)
  2. Collect up to `sendWindowSize_` pending ops
  3. Send them via `opCallback_` (which calls `SendReplicateOperation`)
  4. If `SendReplicateOperation` returns false (`TransportSendQueueFull`),
     stop sending more ops in this tick

**Error handling in `SendTransportMessage`:**

| Error | Action |
|-------|--------|
| Success | Continue normally |
| `TransportSendQueueFull` | Return `false` — sender stops retrying this tick, will retry on next timer |
| `MessageTooLarge` | Report fault — fatal error, replica marked unhealthy |
| Other errors | Ignored — transport layer handles retries |

**Flow control features:**
- **Adaptive window:** Window shrinks on no-ACK (secondary overloaded),
  grows when ACKs resume (via `ackDurationList_` tracking average ACK
  latency)
- **No data loss:** Ops stay in `pendingOperations_` until ACKed. The
  retry timer re-sends them. A slow secondary eventually catches up.
- **No blocking:** The primary's write path (`AddReplicateOperationPrivate`)
  enqueues to the replication queue and returns immediately. The
  `ReliableOperationSender` drains ops from the queue asynchronously.
- **Per-secondary isolation:** Each secondary has its own sender with its
  own window and timer. A slow secondary doesn't affect others.

---

### Committed LSN Propagation to Secondaries (from C++ source)

The primary propagates its committed/completed LSN to secondaries so
they can GC their operation queues and know which operations are
globally committed.

**Source:** `ReplicaManager.cpp`, `ReplicationSession.cpp`,
`ReliableOperationSender.cpp`, `ReplicationOperationHeader.h`,
`SecondaryReplicationReceiver.cpp`

**Two LSN concepts (SF distinguishes these):**

| Name | Meaning | Who computes | Who uses |
|------|---------|-------------|----------|
| `committedLSN` | Quorum-acknowledged by write quorum replicas | Primary (`ReplicaManager::GetProgress`, sorted ACKs at `quorumIndex`) | Primary queue management, user `replicate()` completion |
| `completedLSN` | Acknowledged by ALL replicas (min of all receive ACKs, capped at committedLSN) | Primary (`ReplicaManager::GetProgress`) | Secondary queue GC |

**Propagation mechanism — piggybacked on replication messages:**

The primary includes `completedSequenceNumber` in the
`ReplicationOperationHeader` of every replication message:

```
ReplicationSession::SendReplicateOperation(op, completedSeqNumber)
  → ReplicationTransport::CreateReplicationOperationMessage(op, ..., completedSequenceNumber)
    → ReplicationOperationHeader { ..., completedSequenceNumber_ }
```

The secondary extracts it in `ProcessReplicationOperation`:

```
SecondaryReplicationReceiver::ProcessReplicationOperation(batchOp, completedSequenceNumber)
  if completedSequenceNumber != InvalidLSN:
    replicationQueue_->UpdateCompleteHead(completedSequenceNumber + 1)
    → GC: remove ops with LSN <= completedSequenceNumber
```

**How the cached value stays fresh — ACK-driven updates:**

The `ReliableOperationSender` caches `completedSeqNumber_` (initialized
to `InvalidLSN`). It's updated from the ACK processing path:

```
ACK arrives
  → PrimaryReplicator::ReplicationAckMessageHandler
    → session->UpdateAckProgress(receivedLSN, quorumLSN, ...)
      → callback: UpdateProgressAndCatchupOperation
        → ReplicaManager::UpdateProgress
          → GetProgressCallerHoldsLock → computes new committedLSN, completedLSN
          → UpdateProgressInQueueCallerHoldsLock → updates queue heads + commits
            → ReliableOperationSender::Add/Open with new completedSeqNumber
```

At send time, the sender reads the cached `completedSeqNumber_` and
includes it in the outbound message.

**One-item lag:** The piggybacked value is read at send time. For the
last op in a burst (no subsequent op to piggyback on), the committed
value may lag by one. SF mitigates this with the `RequestAck` timer.

**RequestAck periodic heartbeat:**

The `ReliableOperationSender` has a retry timer that fires
periodically. When it fires, it reads the latest `completedSeqNumber_`
and can send a `RequestAck` message to the secondary even without new
data ops. This ensures secondaries eventually learn the latest
committed progress.

```
ReliableOperationSender::OnTimerCallback
  → reads completedSeqNumber_
  → if requestAck needed: opCallback_(nullptr, true, completedSeqNumber)
    → ReplicationSession::SendRequestAck()
    → secondary receives RequestAck with updated completedSequenceNumber
```

Without this heartbeat, the last op's committed status is only
propagated when the next data op is sent.

---


## Quorum and Consistency

### Write Quorum

```
Write Quorum = ⌊N/2⌋ + 1

Where N = total replicas in the configuration

Examples:
  3 replicas → Write Quorum = 2  (tolerates 1 failure)
  5 replicas → Write Quorum = 3  (tolerates 2 failures)
  7 replicas → Write Quorum = 4  (tolerates 3 failures)
```

### During Reconfiguration (Two Configurations)

When both a Previous Configuration (PC) and Current Configuration (CC) exist:

> At any time the replicator can have one or two configurations. There is
> always a current configuration which represents the set of replicas that are
> participating in replication along with the current write quorum. In addition
> there can be a previous configuration which represents the set of replicas
> that were in the previous configuration. When there is both a current and
> previous configuration the replicator must ensure that writes are
> acknowledged by a write quorum of **both** configurations.
>
> — `stateful_traits.rs`

This dual-quorum requirement ensures no committed data is lost during
configuration transitions.

### Primary Counts Toward Write Quorum

The primary is **included** in the write quorum count and is considered
to have ACKed immediately (it generated the operation — it already has
the data). The committed LSN is computed by looking at the
`writeQuorum - 1` th secondary ACK in the sorted ACK list.

From `ReplicaManager.cpp`:
```cpp
// Since the primary is included in quorum and considered as immediately ACKing,
// the committed number must be ACKed by writeQuorum - 1 remote replicas.
size_t quorumIndex = static_cast<size_t>(writeQuorum - 2);
```

For a 3-replica set with `writeQuorum = 2`:
- Primary counts as 1 ACK (immediate)
- Need `writeQuorum - 1 = 1` secondary ACK
- Total: 2 of 3 = majority quorum

**The primary does NOT persist to its own disk** before returning from
`replicate()`. Its "ACK" is purely in-memory. Durability comes from the
secondary ACKs. For persisted services, each secondary ACK means the
user service has called `acknowledge()` (typically after persisting to
its own store). This means an operation is durable on `writeQuorum - 1`
secondaries' disks, plus in the primary's memory.

### Consistency Guarantees

- **Strong consistency by default:** Transaction commits only return after the
  entire transaction is logged on a majority quorum (including the primary)
- **Linearizable reads on primary:** All reads on the primary see the latest
  committed state
- **Eventual consistency option:** Applications can acknowledge before async
  commit returns for lower latency

---

## Building New Replicas

When a new replica joins the partition (scaling out, replacing a failed node,
or rebalancing):

```
    Primary                              New Idle Secondary
       │                                        │
  1.   │◄──── SF creates idle replica ──────────│
       │                                        │
  2.   │──── build_replica(idle_info) ─────────►│
       │     [Copy Stream: full state transfer] │
       │     Op(copy,1), Op(copy,2), ...        │
       │     ...until current committed state   │
       │                                        │
  3.   │     SF promotes Idle → Active Secondary │
       │                                        │
  4.   │──── update_catch_up_config() ─────────►│
       │     [Include S in current config]      │
       │                                        │
  5.   │──── wait_for_catch_up_quorum() ───────►│
       │     [S catches up to committed LSN]    │
       │                                        │
  6.   │     Replica becomes Active Secondary   │
       │     Access status remains separately   │
       │     controlled by the RA calculator    │
       │                                        │
```

**Key constraints on `build_replica()` input:**
- Role must be `IdleSecondary`
- `current_progress` = -1 (unknown)
- `catch_up_capability` = -1 (unknown)
- `must_catch_up` = false
- Replica being built does NOT count towards quorum
- A replica cannot simultaneously be in-build and in the configuration

### Adding Idle Replicas — Closing the Copy/Replication Gap

**Source:** `ReplicaManager::TryAddIdleReplica` in `ReplicaManager.cpp`

When the primary starts building a new idle replica, the critical challenge
is ensuring **zero gap** between the copy stream (full state) and the
replication stream (incremental ops). SF handles this atomically:

```cpp
// Under write lock (exclusive access to replication queue):
ErrorCode ReplicaManager::TryAddIdleReplica(...) {
    AcquireWriteLock lock(lock_);

    // 1. Create idle replica session
    session = CreateReplica(replica);
    idleReplicas_.push_back(session);

    // 2. Get replication start sequence = committed LSN + 1
    //    AND get ALL pending (uncommitted) ops from the queue
    replicationQueueManager_.GetPendingOperationsToSendDuringBuild(
        ..., replicationStartSeq, pendingOperations);

    // Release lock — new replicate() calls will include the idle replica
}

// 3. Send pending ops to idle replica IMMEDIATELY (outside lock)
session->AddReplicateOperations(pendingOperations, currentCompletedLsn);
```

**Why this works — three ranges with zero gap:**

```
[0, committed_lsn]              → Copy stream (from state provider)
(committed_lsn, highest_lsn]    → Pending ops (from replication queue)
(highest_lsn, ∞)                → Live replication (new ops via send_to_all)
```

- The copy stream produces state up to `committed_lsn` (the state provider
  only has committed data).
- The replication queue retains all ops from `committed_lsn + 1` to
  `highest_lsn` (uncommitted, in-flight). These are sent to the idle
  replica immediately at build start.
- New ops (LSN > `highest_lsn`) are sent via normal `send_to_all` because
  the idle replica's session was registered under the lock before any new
  `AddReplicateOperation` can run.

The write lock ensures that step 2 (get pending ops) and the registration
of the idle replica happen atomically. No new `AddReplicateOperation` can
sneak in between — it would need the same lock.

**The replication queue is the key enabler.** Without a queue that retains
uncommitted ops, there's no way to bridge the gap between committed state
(copy) and future ops (replication). This is why WAL persistence matters.

### Build Completion Condition (from C++ source)

**Source:** `PrimaryReplicator.BuildIdleAsyncOperation.cpp`

Build is complete when **both** of these are ACKed by the secondary:
1. The last copy operation produced by the state provider
2. The last replication operation that existed at the moment the last copy
   op was produced

From the source comments:
> "This is necessary because subsequent operations may override operations
> provided by copy, so that the state when copy is finished is inconsistent."

The primary captures `lastReplicationSequenceNumber` at the exact moment the
copy enumerator produces its final item. The build waits for the secondary to
ACK both this copy LSN and this replication LSN.

This handles **concurrent writes during build**: while the state provider is
producing the copy state, the primary may be accepting new writes. These
writes go into the replication queue. The secondary first drains the copy
stream (full state), then drains any replication operations up to the
captured boundary. Only when both are ACKed is the secondary considered built.

### Async Build: Fire-and-Retry Pattern (from C++ source)

**Source:** `ReconfigurationAgentProxy.cpp`, `FailoverUnit.cpp`

The RA (Reconfiguration Agent) does NOT block waiting for a build to
complete. Instead, it uses a fire-and-retry pattern:

1. **RA → RAProxy:** RA sends `ReplicatorBuildIdleReplica` message
2. **RAProxy → Replicator:** RAProxy calls `BeginBuildIdleReplica()`
   (COM async begin/end pattern)
3. **In-progress:** If the build is still running, the RAProxy returns
   `RAProxyBuildIdleReplicaInProgress` and **suppresses the reply**.
   ```cpp
   bool sendReply = result != ErrorCodeValue::RAProxyBuildIdleReplicaInProgress;
   ```
4. **Retry timer:** The RA has a `MessageRetryTimer` that re-sends the
   build message periodically. The RAProxy drops duplicates:
   `MessageDropInvalidWorkInProgress`
5. **Completion:** When the build finishes, the RAProxy sends the reply.
   The RA receives it and advances to the next phase.
6. **Cancellation:** RA can send `CancelCatchupReplicaSet` to abort.

**Key insight:** No progress reporting — just "done" or "still working."
The FM monitors overall health (via heartbeats and lease expiry), not
per-operation progress. The same pattern applies to `CatchupReplicaSet`.

### Catchup Mechanics (from C++ source)

**Source:** `PrimaryReplicator.CatchupAsyncOperation.cpp`

`wait_for_catch_up_quorum()` has two modes:

**Mode: All (`FABRIC_REPLICA_SET_QUORUM_ALL`)**
- Completes when ALL replicas' completed progress ≥ latest sequence number
- Legacy fallback for replicators without `must_catch_up` support
- Check: every member's `completed >= latestSequenceNumber`

**Mode: Write Quorum (`FABRIC_REPLICA_SET_WRITE_QUORUM`)**
- Completes when write quorum replicas are caught up AND every `must_catch_up`
  replica has individually caught up
- Default for modern replicators during all reconfiguration types
- Check: `committed >= previousConfigCatchupLsn` AND
  `lowestLSNAmongstMustCatchupReplicas >= committed`

Key detail: even in Write Quorum mode, the `must_catch_up` replica (e.g.,
the primary candidate during swap, or the restarted secondary) must be fully
caught up individually — quorum of other replicas catching up is not
sufficient if the must_catch_up replica itself hasn't caught up. This
prevents promoting a replica that is missing data, or finalizing a config
where a critical replica is lagging.

### Catch-Up Completion Logic (from C++ source)

**Source:** `PrimaryReplicator.CatchupAsyncOperation.cpp`, `ReplicaManager.cpp`

The `UpdateCatchupOperation` callback fires whenever any replica's progress
updates. It re-checks the catch-up condition:

**`previousConfigCatchupLsn`**: Recorded at `UpdateCatchUpConfiguration`
time as `ReplicationQueueManager::LastSequenceNumber`. This is the baseline
— only ops up to this LSN existed when the config changed.

**Write Quorum mode (`IsQuorumProgressAchieved`):**
```cpp
bool progressAchieved = (committed >= previousConfigCatchupLsn);
if (lowestLSNAmongstMustCatchupReplicas != MaxLSN) {
    // must_catch_up replicas exist
    progressAchieved = progressAchieved &&
        (lowestLSNAmongstMustCatchupReplicas >= committed);
}
```

The variable named `committed` in this path is the **Current Configuration
quorum progress** supplied to catch-up, not necessarily the dual-PC/CC client
commit position.

Two conditions:
1. **CC quorum progress ≥ `previousConfigCatchupLsn`** — the CC quorum reached
   the boundary captured when catch-up configuration was installed
2. **`lowestLSNAmongstMustCatchupReplicas` ≥ CC quorum progress** — every
   `must_catch_up` replica individually reached current CC quorum progress

If no must_catch_up replicas exist (`lowestLSN == MaxLSN`), only check 1
applies. If no new ops arrived after config change, `committed ==
previousConfigCatchupLsn` and check 1 trivially passes.

**`lowestLSNAmongstMustCatchupReplicas`** is computed from `activeReplicas_`
by calling `GetLastApplyLsn()` on each replica with `MustCatchup == Yes`.
This is the replica's **actual reported ACK from the replication channel**,
not operator-provided data.

**All mode (`IsAllProgressAchieved`):**
```cpp
bool progressAchieved = (completed >= latestSequenceNumber);
```
Simple: every replica's completed progress ≥ latest sequence number.

### Replication During Reconfiguration

`replicate()` continues to work during reconfiguration with dual-config
quorum. The primary sends to all replicas in both PC and CC. An operation
commits when it's acknowledged by write quorum in BOTH configurations.
The `wait_for_catch_up_quorum` async operation is completed by a callback
(`UpdateCatchupOperation`) that fires whenever any replica's progress
updates — it re-checks the catchup condition after each ACK.

---

## Scale-Down (Replica Removal)

**Source:** `PlacementTask.cpp`, `ReconfigurationTask.cpp`, `MovementTask.cpp`

### How FM Chooses Which Replica to Remove

The PLB (Placement & Load Balancing) decides which replicas to drop based on
load balancing heuristics (not newest/oldest). It generates movement actions
with `DropSecondary`, `DropPrimary`, or `DropInstance` types.

### Sequence: Remove a Secondary

Order is **config-first, then close**:

1. **PLB marks**: replica gets `IsToBeDroppedByPLB = true`
2. **Wait for ready**: FM waits for `IsReady` state (stable, caught up)
3. **Start reconfiguration**: `StartReconfiguration(isPrimaryChange: false)`
4. **Remove from config**: `RemoveFromCurrentConfiguration(replica)` —
   removes from CC FIRST, before closing the replica
5. **Primary broadcasts**: an `Other` reconfiguration starts at Phase 2
   catch-up, then proceeds through Phase 3 deactivation and Phase 4 activation
6. **Close replica**: replica receives close, releases resources

```cpp
// ReconfigurationTask.cpp line 418-428
for (auto it = failoverUnit.BeginIterator;
     it != failoverUnit.EndIterator &&
     count > TargetReplicaSetSize; ++it) {
    if (it->IsCurrentConfigurationSecondary &&
        it->IsToBeDropped && it->IsReady) {
        failoverUnit.RemoveFromCurrentConfiguration(it);
        count--;
    }
}
```

### Removing the Primary

SF **never drops the primary directly**. If PLB marks the primary for drop:

1. `ReconfigSwapPrimary()` is called — switchover to a stable secondary
2. Old primary becomes secondary
3. Then dropped as a normal secondary
4. Safety assert: `ASSERT_IF(newPrimary->IsToBeDropped, ...)` — new primary
   must NOT be marked for drop

### Safety Guarantees

- **MinReplicaSetSize**: removal only when `CurrentConfig.ReplicaCount >
  MinReplicaSetSize`
- **Write quorum**: maintained because removal only when `count >
  TargetReplicaSetSize`; dropped replicas excluded from quorum
- **Implicit drain**: FM waits for `IsReady` (caught up) before removing,
  allowing in-flight writes to complete

---
