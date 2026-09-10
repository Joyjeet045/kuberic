# Kuberic: Design Gaps and Required Work

Gaps identified by reviewing the design docs against the implementation.
Categorized by severity and whether additional design work is needed
vs simple implementation of existing designs.

> Related: [Status & Roadmap](status.md),
> [Failure Scenarios](../operator-failure-scenarios.md)

---

## Category A: Protocol Safety — Needs Design Work

These gaps affect data safety and correctness. The design docs don't
cover the error paths adequately. Additional design is required before
implementation.

### A1. Partial Failure in Multi-Replica Operations — ✅ Fixed

**Severity:** ✅ Resolved
**Affects:** Durable failover and switchover
**Files:** `kuberic-operator/src/durable/failover.rs`,
`kuberic-operator/src/durable/switchover.rs`

**Problem (was):** The switchover/failover had a "fence-before-promote"
pattern — `update_epoch` was called on ALL secondaries with `?`
(fail-on-first-error) BEFORE promotion. An unreachable secondary blocked
the entire operation and left the partition in an inconsistent state
(write status revoked but never rolled back).

**Root cause:** The fence-before-promote pattern was incorrect — SF doesn't
do it. In SF, epoch distribution happens in Phase 4 (ACTIVATE), AFTER
the new primary is active. Pre-promotion fencing is unnecessary because:
- **Failover:** old primary is dead → can't send ops
- **Switchover:** old primary's writes are revoked → can't send new ops

**Fix:** The CRD-backed workflows persist every transition and execute one
correlated activity at a time. Promotion is followed by durable epoch and
configuration convergence. Unavailable members are retained in the workflow
denominator and cannot be silently dropped; impossible convergence fails
closed.

**Test:** Durable switchover/failover restart matrices and lost-reply tests
exercise every persistence and runtime boundary.

---

### A2. Switchover Write Revocation — ✅ Fixed

**Severity:** ✅ Resolved
**Affects:** Durable switchover
**Files:** `kuberic-operator/src/durable/switchover.rs`,
`kuberic-core/src/pod.rs`

**Problem (was):** Switchover could demote the old primary without first
closing its write-admission window.

**Fix:** The durable switchover persists a `RevokeWriteStatus` action before
demotion. It is dispatched through `ExecuteCorrelatedControlAction`, setting
`write_status = ReconfigurationPending`. New writes are rejected immediately;
demotion fails remaining in-flight writes.

**Switchover sequence (now):**
1. Correlated `RevokeWriteStatus` → new writes rejected immediately
2. Catch up the target and persist the demotion intent
3. Correlated `ChangeRole(ActiveSecondary)` → in-flight writes failed
4. Correlated `ChangeRole(Primary)` on the target
5. Rebuild configuration + catch-up

**Remaining consideration:** SF's SwapPrimary has a double-catchup pattern
(catchup WITH writes → revoke → catchup WITHOUT writes → promote). Our
implementation skips the first catchup-with-writes phase. For most workloads
this is fine — the write revocation + catch-up at step 5 is sufficient.
Double-catchup would reduce the catch-up window after revocation for
high-throughput workloads.

**Test:** Durable switchover and lost-reply tests verify persisted write
revocation, observation-first recovery, and compensation.

---

### A3. Promotion Failure Leaves Partition Unavailable — ✅ Fixed (switchover)

**Severity:** ✅ Switchover fixed / 🟡 Failover handled by reconciler retry
**Affects:** Durable switchover and failover
**Files:** `kuberic-operator/src/durable/switchover.rs`,
`kuberic-operator/src/durable/failover.rs`

**Problem:** If `change_role(Primary)` on the target fails during
switchover, the old primary is already demoted. No replica can accept
writes. Returning an error without durable compensation would strand the
partition.

The durable reconciler instead retains pending intent, observes the result,
and follows explicit compensation or post-promotion roll-forward phases.

**SF reference:** SF has explicit `AbortPhase0Demote` and
`RevertConfiguration()` in the RA. When a swap is aborted (target dies
or higher-priority reconfig arrives), SF:
1. Cancels the catchup (`SendCancelCatchupMessage`)
2. Enters `AbortPhase0Demote` state
3. Calls `RevertConfiguration()` — restores all replicas' roles to their
   pre-swap state (CC role = PC role)
4. Reports `ReconfigurationResult::AbortSwapPrimary`

SF doesn't retry inside the reconfiguration — the FM's outer loop handles
retry by triggering a new reconfiguration.

**Fix:** Durable switchover records explicit restore/compensation phases before
the irreversible target promotion boundary. After that boundary, failures
roll forward or poison rather than publishing an inconsistent snapshot.
Failover separately confirms its candidate and commits a primary-only
snapshot before convergence.

**Test:** Promotion failure, lost reply, operator restart, compensation, and
post-commit roll-forward are covered by the durable reconciler suite.

**Operator crash recovery (related):**

Stable `Healthy` operator restart recovery is implemented from the
authoritative `stableSnapshot`, current pod logical/incarnation identities,
and runtime `GetStatus`. It does not trust live state to invent a topology and
does not issue mutating RPCs during reconstruction.

Mid-switchover and mid-failover recovery use the persisted
`status.operation` checkpoint; they are not inferred from whichever live role
happens to be visible.

---

### A4. gRPC Control Plane Ordering — ✅ Not a Problem

**Severity:** ✅ Closed (no issue)
**Affects:** N/A

**Original concern:** gRPC/HTTP2 multiplexing could reorder control RPCs.

**Analysis:** The operator issues one correlated action at a time and the
ReplicaAgent admits only one local action at a time. HTTP2 multiplexing cannot
reorder a runtime effect because no individual mutation RPC bypass exists.

**Conclusion:** No additional transport mutex is needed.

---

### A5. Synchronous BuildReplica and CatchUp Block Operator — ✅ Fixed for Add/Rebuild and Removal

**Severity:** 🔴 Critical (for large datasets) / 🟢 OK for MVP
**Affects:** Durable add/rebuild, removal, and catch-up workflow activities
**Files:** `driver.rs`, `handle.rs`, `pod.rs`, `server.rs`, `quorum.rs`

Replica add/rebuild no longer blocks the operator on these operations.
`AddReplicaIntent` is accepted by the primary agent and runs in a background
coordinator. `PodRuntime` tracks copy and catch-up-quorum waits through a
command/completion loop, so status, cancellation, and compensation remain
available.

Replica removal likewise runs behind one `RemoveReplicaIntent`; the primary
agent tracks its quorum wait asynchronously while status remains responsive.

The direct correlated BuildReplica and quorum actions remain for unchanged
creation/failover/switchover workflows. Their existing ownership is not
migrated here.

**Original problem:** Two long-running operations blocked the operator
synchronously:

1. **`build_replica`** — the operator calls a single gRPC unary RPC that
   holds open for the entire copy duration. For GBs of state, this is
   minutes to hours.

2. **`wait_for_catch_up_quorum`** — blocks until the must_catch_up replica
   ACKs all ops. B0 now applies a fixed deadline and returns
   `NoWriteQuorum` if catch-up is incomplete when it expires, but a slow
   replica can still occupy the operator synchronously until completion or
   the deadline.

Both have the same consequences:
- **Reconciler stall** — blocked on `.await`, no health checks or
  failover detection can proceed.
- **Operator restart** — drops the gRPC connection, aborts mid-operation,
  no way to resume.

**SF's approach (from C++ source):**

SF uses a **fire-and-retry** pattern, NOT progress-based polling:

1. **RA → RAProxy:** RA sends `ReplicatorBuildIdleReplica` message (async)
2. **RAProxy → Replicator:** Calls `BeginBuildIdleReplica()` (COM async)
3. **In-progress handling:** If the build is still running, the RAProxy
   returns `RAProxyBuildIdleReplicaInProgress` and **does not reply** to
   the RA. The RA's `MessageRetryTimer` re-sends the message periodically.
   The RAProxy drops duplicates (`MessageDropInvalidWorkInProgress`).
4. **Completion:** When the build finishes, the RAProxy sends the reply.
5. **Cancellation:** RA can send `CancelCatchupReplicaSet` to abort.

Key insight: **no progress reporting** — just "done" or "still working."
The FM monitors overall health (via heartbeats), not build progress.

**Implemented shape:** The operator sends one coarse action to the primary,
not BuildReplica or WaitForCatchUpQuorum individually. Primary status exposes
coordinator phase and one exact build observation. Target stages use a
dedicated peer protocol. Copy completion publishes its LSN boundary only after
target acknowledgement; tracked quorum wait supports exact cancellation.

### A6. Uncommitted Operations Not Rolled Back on Epoch Change — ✅ Fixed

**Severity:** ✅ Resolved (KV app rollback + B5 committed_lsn propagation)
**Affects:** `SecondaryReceiver`, user services, `StateProviderEvent`
**Files:** `secondary.rs`, `events.rs`, kvstore `service.rs`

**What was fixed:**

1. **B5 (committed_lsn propagation):** Primary piggybacks committed_lsn
   on every `ReplicationItem`. Secondary extracts it and updates
   `PartitionState`. `previous_epoch_last_lsn` is now correct (non-zero).

2. **KV app rollback:** `UpdateEpoch` handler calls `rollback_to()`
   which reloads from snapshot + partial WAL replay. Does NOT cancel
   drain tasks (replication stream continues during epoch change,
   matching SF behavior).

3. **Test verified:** After failover with 5 ops, secondary correctly
   rolls back the uncommitted 5th op and retains 4 committed ops.

**SF alignment:** `IStateProvider::UpdateEpoch(epoch, previousEpochLastLsn)`
callback tells the user service to truncate state beyond
`previousEpochLastLsn`. Our implementation follows this pattern.

**Future framework-level option (defer dispatch until committed):**
For apps that can't implement rollback (e.g., SQLite where page
overwrites are destructive), the framework could defer dispatch to
user's `OperationStream` until committed. This is the SQLite
app's design — see `docs/features/sqlite/design.md`.

---

These gaps affect reliability under adverse conditions. The system may
hang, leak resources, or fail to recover.

### B0. Replication Stream Failure Goes Undetected

**Severity:** 🟡 Medium (reduced — write timeout and dead-send cleanup fixed)
**Affects:** `PrimarySender`, `WalReplicatorActor`, operator reconciler
**Files:** `primary.rs`, `actor.rs`, `reconciler.rs`

**Problem:** The replication stream is a long-lived bidirectional gRPC
stream. When it breaks, detection is partial:

**What's already fixed:**
- `send_to_all()` (primary.rs:163-175) detects dead channels via
  `item_tx.send().is_err()`, logs a warning, and **removes the dead
  connection** from `PrimarySender::connections`. This was implemented
  during the non-blocking send work.
- `QuorumTracker` gives every operation a deadline (5 seconds by default).
  An operation that has not reached quorum expires with `NoWriteQuorum`;
  late ACKs are harmless and later operations have independent deadlines.
- Catch-up quorum waiters use the same bounded behavior. An expired operation
  fails the active catch-up attempt. Duplicate catch-up updates preserve the
  baseline; rollback/finalization must end the failed attempt before a later
  retry establishes a new baseline.

**No auto-reconnection (intentional, matching SF):**
When a connection is removed, the secondary silently drops out of
replication. We do NOT auto-reconnect because:
1. The pod might be truly dead — reconnection retries would be futile
2. Even if reconnected, the secondary missed ops — its data is stale
   and needs a full rebuild (copy), not just reconnection
3. Auto-reconnection would bypass the operator's lifecycle control

SF handles this the same way: the replicator stops sending, the FM
detects the replica is unreachable (via heartbeat failure), and
orchestrates a `BuildIdleReplica` rebuild. The replicator never
reconnects on its own.

**The missing piece:** We remove the dead connection but **don't notify
the operator**. The operator's reconciler checks pod readiness (K8s),
not replication stream health. A pod can be Running/Ready (process alive)
but its replication stream dead (e.g., network partition between pods).

**What's still a gap:**

1. **ACK reader exits silently** — the spawned ACK reader task
   (primary.rs:93) logs a warning and exits. No callback to the actor.
   The independent request drain can remain open, so a later
   `send_to_all()` is not guaranteed to discover the dead ACK path.

2. **Operator can't detect replication health** — pods can be Ready but
   replication broken. `GetStatus` doesn't report replication stream
   status. The reconciler only checks pod readiness.

**Remaining design (for production hardening):**

- **Replication health in GetStatus:** Add `connected_secondaries` count
  or per-replica replication status to the GetStatus response. The
  reconciler can then detect "Ready but replication broken."
  This maps to SF's RA→FM `ChangeNotification` pattern — the pod
  reports its health, the operator acts on it.

- **ACK reader death notification** (nice-to-have): When the ACK reader
  exits, notify the actor to proactively remove the connection. Without that
  signal the sender can continue to appear connected while acknowledgements
  are no longer arriving.

**SF failure detection model (for reference):**

SF does NOT monitor replication streams directly. It uses layered detection:

| Layer | Mechanism | Detects | Speed |
|-------|-----------|---------|-------|
| Federation | Lease-based heartbeats between nodes | Node failure | Seconds |
| RA → FM | `ReplicaDown` message when local process crashes | Process crash | Seconds |
| FM health | Periodic `ChangeNotification` from each RA | Silent node death | ~30s |

A broken replication stream without node/process failure (network partition
between pods) is detected indirectly: quorum loss → writes fail → user
reports fault. This is a known gap in SF too, though SF's Federation
leases catch most real failures at the node level.

**Our K8s equivalents:**
- Node-level: K8s kubelet → pods go NotReady
- Pod-level: Liveness probe → pod restart
- Replication-level: **Our gap** — no equivalent of Federation leases
  between pods. Fix: `connected_secondaries` in GetStatus (see above).

---

### B1. Copy Protocol — No Reconnection (Intentional)

**Severity:** 🟡 Medium (streaming improvement) / 🟢 Low (reconnection)
**Affects:** `handle_build_replica()`, `SecondaryReceiver`
**File:** `kuberic-core/src/pod.rs`

**No reconnection is intentional.** If the `copy_stream` gRPC call fails
mid-transfer (network error, pod crash), the error propagates up:

```
copy_stream fails (pod.rs:579)
  → handle_build_replica returns Err
    → ReplicaAgent retains a Failed correlated observation
      → durable add/rebuild remains at its pending action
        → reconciler observes failure and follows retry/compensation policy
```

This matches SF's model: `BuildReplica` is a single atomic operation.
If it fails for any reason, the operator retries the entire build. The
secondary's state is discarded (it was in `IdleSecondary` role, not yet
promoted to `ActiveSecondary`), so there's no partial-state corruption.

**Why no mid-stream reconnection:**
- The copy is orchestrated by the primary's `PodRuntime`, not the operator.
  The primary connects to the secondary as a gRPC client. If the stream
  breaks, the primary has no way to know what the secondary received.
- Reconnecting mid-copy would require the secondary to report its
  received-LSN checkpoint, which adds protocol complexity for a rare case.
- The retry path already handles it: the secondary's `GetCopyContext`
  callback can report its current LSN, and the primary's `GetCopyState`
  can produce only the delta. This is an application-level concern
  (the kvstore example currently always sends full state, but
  the protocol supports incremental).

**Remaining improvement — streaming instead of materializing:**
`collect_stream()` materializes the entire copy state into a
`Vec<proto::CopyItem>` before sending. For large datasets this causes
memory exhaustion. Should be replaced with a direct pipe:
`OperationStream` → `tokio_stream::Stream` → gRPC streaming request.

---

### B2. Timeout Enforcement for Short RPCs

**Severity:** 🟠 High
**Affects:** Short control-plane RPCs, state provider callbacks
**Files:** `handle.rs`, `pod.rs`, `actor.rs`

**Problem:** `GetStatus` and `ExecuteCorrelatedControlAction` have no
transport-level client deadline. A broken connection can therefore hold one
reconcile longer than intended, although CRD action deadlines still bound
workflow policy and runtime effects have internal reply timeouts.

**Design needed:**
- **Per-call timeout on `GrpcReplicaHandle`:** Add a timeout field and wrap
  both public control calls:
  ```rust
  async fn get_status(&self) -> Result<ReplicaStatusInfo> {
      tokio::time::timeout(self.timeout, self.client.clone().get_status(...))
          .await
          .map_err(|_| KubericError::Timeout)??;
      // strict decode...
  }
  ```
- **State provider callback timeout:** `send_state_provider()` already
  has a 30s reply timeout (line 648-651 in pod.rs), but only for the
  oneshot reply — not for slow user processing. Add cancellation token
  support (see C3).
- **Configuration:** `spec.rpcTimeout` (default 10s) in CRD spec.

---

### B3. gRPC Handle Reconnection and IP Resolution

**Severity:** 🟠 High
**Affects:** `GrpcReplicaHandle`
**File:** `kuberic-core/src/grpc/handle.rs`

**Problem:** Channel is created once with a hardcoded IP address. Pod
restart assigns a new IP. The handle becomes a zombie — all RPCs fail
but the handle isn't replaced.

**Design needed:**
- **DNS-based addressing:** Use K8s DNS names instead of pod IPs:
  `{pod-name}.{headless-service}.{namespace}.svc.cluster.local`.
  tonic's Channel resolves DNS on each connection attempt.
- **Lazy connection:** Use `Channel::from_shared(addr)` without
  `.connect().await` — tonic connects lazily on first RPC. This
  naturally handles reconnection after pod restarts.
- **Handle staleness detection:** The operator should detect when a
  handle's underlying pod has a different IP (pod recreated) and
  replace the handle. This is part of the "missing pod detection"
  design in operator-failure-scenarios.md §3.

---

### B4. Reconciler Operation Locking

**Severity:** 🟠 High
**Affects:** `reconciler.rs`
**File:** `kuberic-operator/src/reconciler.rs`

**Problem:** If a reconciliation triggers `add_replica` (which takes
seconds due to copy), and the controller requeues another reconciliation
before it completes, the second reconciliation may try to add the same
replica again or modify driver state while the first is mid-operation.

**Design needed:**
- **CRD-status-based lock:** Add `status.operationInProgress: Option<String>`
  to the CRD. Set before starting long operations (failover, add_replica,
  switchover). Clear on completion. If set on entry, skip reconciliation
  (requeue after 5s).
  ```rust
  if set.status.operation_in_progress.is_some() {
      return Ok(ReconcileAction::Requeue(Duration::from_secs(5)));
  }
  ```
- **Generation counter:** Add `status.observedGeneration` to track which
  spec generation the operator has processed. Standard K8s pattern.
- **Idempotent operations:** Ensure that re-running a partially completed
  operation is safe. For `add_replica`: check if the replica is already
  open before calling `open()` again.

---

### B5. committed_lsn Not Propagated to Secondaries — ✅ Fixed

**Severity:** ✅ Resolved
**Affects:** `WalReplicatorActor`, `PartitionState`, `SecondaryState`, user services
**Files:** `kuberic.proto`, `actor.rs`, `primary.rs`, `secondary.rs`, `pod.rs`

**What was fixed:** Added `committed_lsn` field to `ReplicationItem`
proto message. The primary includes its quorum-committed LSN with
every replication item. The secondary extracts it in
`SecondaryState::accept_item()` and propagates to `PartitionState`
via `SecondaryReceiver`. This matches SF's `completedSequenceNumber`
pattern in `ReplicationOperationHeader`.

**SF alignment:** SF sends `completedSequenceNumber` (= min of all
secondaries' receive-ACKed LSNs, capped at `committedLSN`) piggybacked
on every replication message. Our implementation follows the same
pattern — one i64 field per item, extracted and stored on receive.

**What this unblocked:**
1. **A6 rollback works on secondaries** — `previous_epoch_last_lsn` is
   now correct (non-zero). KV app rollback fires and correctly discards
   uncommitted ops.
2. **SQLite deferred application model** — Phase 2 (apply to DB) can
   now trigger when `committed_lsn` advances past frame LSNs.

**Also fixed:** UpdateEpoch handler no longer cancels drain tasks.
The replication stream continues during epoch change (matches SF).
Cancelling killed the ACK pipeline, causing `replicate()` hangs on
the new primary after failover.

**SF reference (from source analysis):**

SF sends `completedSequenceNumber` piggybacked on every replication
message (`ReplicationOperationHeader`). The secondary extracts it in
`ProcessReplicationOperation` and uses it for queue GC. SF also
distinguishes **committed** (user ACK'd, local) from **completed**
(all replicas ACK'd, global) — we currently conflate both into
`committed_lsn`. This is acceptable for our current apps but may
need separation for production use.

**One-item lag (matches SF):** The piggybacked `committed_lsn` value is
read at send time. For the last op in a burst, the secondary ACK may
arrive after the send, so committed_lsn lags by one. SF has the same
lag but mitigates it with a `RequestAck` periodic heartbeat that
carries updated progress even without new data ops. Our implementation
does not have this heartbeat — the lag means a secondary may roll back
the last committed op on failover if no subsequent op was sent. The
rolled-back op is re-replicated during catch-up from the new primary.
This is correct (conservative) behavior. A future `RequestAck`
mechanism would eliminate the lag.

---

## Category C: Correctness Refinements — Needs Design Work

Smaller issues that affect correctness in edge cases.

### C0. Replication Queue Replay — ✅ Fixed

**Severity:** ✅ Resolved
**Affects:** `WalReplicatorActor`, `ReplicationQueue`, `PartitionState`, KV service

**What was fixed (Phase 1 — ReplicationQueue):**
The in-flight ops gap is closed — all ops are retained in the `ReplicationQueue`
and replayed to new replicas at `add_secondary` time. No data loss.

**What was fixed (Phase 2 — Precise LSN boundary):**
The replay now uses the precise copy boundary LSN instead of replaying
everything from LSN 1. Two changes:

1. **Copy LSN tracking (`PartitionState.copy_lsn_map`):** After
   `run_build_replica_copy` collects the state snapshot, it extracts the
   copy boundary LSN (max LSN of copy items = `last_applied_lsn` at
   snapshot time) and stores it per-replica via `state.set_copy_lsn()`.
   At `UpdateCatchUpConfiguration` time, the actor reads it via
   `state.take_copy_lsn()` and replays only `ops_from(copy_lsn + 1)`.

2. **Copy stream full consumption:** The KV service's role change handler
   was cancelling the copy drain task when transitioning from IdleSecondary
   to ActiveSecondary, losing items still in the channel. Fixed by waiting
   for the copy drain to finish naturally (sender is already dropped by
   the gRPC handler) before starting the replication drain. This ensures
   the secondary receives the complete copy state before replication begins.

**Three ranges with zero gap (matching SF):**
```
[0, copy_lsn]              → Copy stream (from state provider snapshot)
(copy_lsn, highest_lsn]    → Replay from replication queue
(highest_lsn, ∞)           → Live replication (new ops via send_to_all)
```

**Changes made:**
1. `PartitionState` (`handles.rs`): added `copy_lsn_map: Mutex<HashMap<ReplicaId, Lsn>>`
   with `set_copy_lsn()` and `take_copy_lsn()` methods
2. `run_build_replica_copy` (`pod.rs`): extracts `copy_lsn` from collected state items,
   stores via `state.set_copy_lsn(replica.id, copy_lsn)`
3. Actor (`actor.rs`): at `UpdateCatchUpConfiguration`, reads `state.take_copy_lsn(&member.id)`
   and replays from `copy_lsn + 1` instead of `1`
4. KV service (`service.rs`): on IdleSecondary → ActiveSecondary transition,
   waits for copy drain to complete naturally instead of cancelling

**Why this is correct for non-idempotent ops:**
- The copy snapshot captures all state through `last_applied_lsn` at snapshot time
- The actor's queue LSNs match the app's LSNs (assigned by the same actor)
- `ops_from(copy_lsn + 1)` replays only ops NOT in the snapshot
- No overlap, no gap

**Remaining consideration:** User apps that handle the IdleSecondary →
ActiveSecondary role change must ensure the copy stream is fully consumed
before starting replication. The KV app demonstrates the correct pattern.
This could be enforced at the framework level in the future (e.g., the
framework waits for the copy stream to drain before delivering the
replication stream).

**Verified:** 5/5 passes at 500 initial + 200 concurrent writes.

### C1. QuorumTracker Stale ACK Entries

**Severity:** 🟡 Medium
**Affects:** `QuorumTracker`
**File:** `kuberic-core/src/replicator/quorum.rs`

**Problem:** `set_current_configuration` clears `previous_members` but
doesn't clean up `replica_acked_lsn` entries for replicas that are no
longer in any configuration. Late ACKs from removed replicas could
affect catch-up calculations.

**Fix:** In `set_current_configuration()`, retain only entries where
`replica_id ∈ current_members`:
```rust
self.replica_acked_lsn.retain(|id, _| self.current_members.contains(id));
```

---

### C2. Double set_catch_up_configuration Guard

**Severity:** 🟡 Medium
**Affects:** `QuorumTracker`
**File:** `kuberic-core/src/replicator/quorum.rs`

**Problem:** Calling `set_catch_up_configuration` twice without an
intervening `set_current_configuration` overwrites `catch_up_baseline_lsn`.
The second call's baseline is higher, making catch-up fire too early.

**Fix:** Add a guard:
```rust
pub fn set_catch_up_configuration(...) -> Result<()> {
    if !self.previous_members.is_empty() {
        return Err(KubericError::InvalidState(
            "catch-up configuration already active".into()
        ));
    }
    // ... existing logic
}
```

---

### C3. Cancellation Tokens for State Provider Callbacks

**Severity:** 🟡 Medium
**Affects:** `StateProviderEvent`, `PodRuntime`
**Files:** `events.rs`, `pod.rs`

**Problem:** `GetCopyState` and `GetCopyContext` callbacks don't pass
a `CancellationToken`. If the user's state production hangs (large
dataset, deadlock), there's no way to abort the build operation.

**Fix:** Add `CancellationToken` to the relevant `StateProviderEvent`
variants:
```rust
GetCopyState {
    up_to_lsn: Lsn,
    copy_context: OperationStream,
    token: CancellationToken,  // NEW
    reply: oneshot::Sender<Result<OperationStream>>,
},
```

---

### C4. ChangeRole(None) Does Not Delete Data or Stop Client Server — ✅ Fixed

**Severity:** ✅ Resolved
**Affects:** kvstore, sqlite, postgres examples

**Problem:** `ChangeRole(Role::None)` was a no-op in all three examples.
It didn't stop the client server (if primary) or delete persisted data.
`Role::None` signals permanent removal — the replica is being
decommissioned.

**Fix (implemented in all three examples):**

- `ChangeRole(None)`: stops client server immediately (kvstore, sqlite).
  Postgres already defers PG stop to Close.
- `Close` after `ChangeRole(None)`: deletes data directory.
- `Close` from any other role: preserves data directory (restart case).
- Tracked via `last_role` / `current_role` variable in each service.

---

## Category D: Designed Capabilities and Remaining Implementation

These items have complete designs in the existing docs. No additional
design work needed — just implementation.

| Item | Design Location | Priority |
|------|----------------|----------|
| Durable Phase-1/data loss protocol (`on_data_loss()` + `data_loss_number`) | protocols.md §Failover | ✅ Implemented |
| Stable operator restart recovery (`PartitionDriver::recover()`) | operator-failure-scenarios.md §8, A3 notes above | ✅ Implemented |
| Secondary health detection + durable rebuild/eviction | operator-failure-scenarios.md §2 | ✅ Implemented |
| Stable missing secondary detection | operator-failure-scenarios.md §3 | ✅ Implemented |
| gRPC failure tracking (per-replica counter) | operator-failure-scenarios.md §5 | P1 |
| Failover delay (`primaryFailingSince`) | operator-failure-scenarios.md §1 | ✅ Implemented |
| Durable agent-owned secondary removal | operator-failure-scenarios.md §3 | ✅ Implemented |
| CRD conditions (Ready, Degraded, QuorumAvailable) | operator-failure-scenarios.md | P1 |
| Old primary cleanup after failover | operator-failure-scenarios.md §9 | P1 |
| Primary self-fencing liveness probe | operator-failure-scenarios.md §5 | P2 |
| Node drain detection | operator-failure-scenarios.md §6 | P2 |
| Multi-primary detection | operator-failure-scenarios.md §9 | P2 |
| CrashLoop retry capping | operator-failure-scenarios.md §4 | P3 |
| Pod anti-affinity in CRD | operator-failure-scenarios.md §6 | P3 |

---

## Summary

| Category | Count | Top Priority |
|----------|-------|-------------|
| A: Protocol Safety | 6 | **A1 ✅**, **A2 ✅**, **A3 ✅ (switchover)**, **A4 ✅ (not an issue)**, **A5 ✅ (add/removal)**, **A6 ✅** |
| B: Operational Resilience (needs design) | 6 | B0 bounded writes fixed (health reporting remains), **B5 ✅**, B2 timeouts |
| C: Correctness Refinements (needs design) | 5 | **C0 ✅ fixed**, C1 stale ACKs, **C4 ChangeRole(None) cleanup** |
| D: Designed capabilities | 14 | Stable restart and durable Phase-1/data-loss failover ✅ |
| **Total** | **31** | |

**Recommended order:**
1. **B0 QuorumTracker timeout ✅** — bounded writes and catch-up waits implemented; replication health reporting remains
2. B2 (short RPC timeouts) — prevents hangs on unchanged control-plane calls
3. A3 failover candidate retry — failover resilience
4. Remaining D items (health reporting and operational integrations)
5. B3 + B4 (reconnection, operation locking) — operational maturity
6. B1 streaming improvement — large dataset support
7. C1-C3 (refinements) — can be done alongside other work

---

## Category E: Rolling Upgrade Review Findings

Discovered during review of `rolling-upgrade-design.md`. The original
mutable-driver findings below are superseded by CRD-backed durable workflows
and the read-only production `PartitionDriver`.

### E1. `add_replica` zombie on failure — ✅ Fixed

**Severity:** ✅ Resolved

The durable add workflow keeps the candidate outside the authoritative stable
snapshot until current configuration commits. Failure before that boundary
uses explicit compensation and cannot inflate failover quorum.

**Test:** Durable add compensation, lost-reply, operator-restart, and
current-configuration roll-forward tests.

### E2. Switchover missing catchup — ✅ Fixed

**Severity:** ✅ Resolved
**Affects:** Durable switchover
**Files:** `kuberic-operator/src/durable/switchover.rs`, `kuberic-core/src/pod.rs`

The original switchover sequence had no catch-up step between write revocation
and demotion of the old primary.

**Confirmed scenario** (3-replica, P=1, S=2=target, S=3):
1. Write W ACKed to client via P=1 + S=3 (quorum met)
2. W in S=2's unbounded channel, drain task hasn't delivered yet
3. `revoke_write_status()` blocks new writes
4. `change_role(ActiveSecondary)` on P=1 calls `close_all()` — drops
   unbounded senders, W lost from S=2's channel
5. S=2 promoted as primary WITHOUT W
6. W exists on S=3 (secondary) but not on primary S=2

Window: microseconds to low milliseconds. Under high write throughput
with a slow target, the window widens.

#### What SF does (from C++ source)

SF's `StatefulServiceDemoteToSecondary` action list
(`ProxyActionsList.cpp:130-142`) runs **10 ordered steps**:

```
1. ReconfigurationStarting
2. ReplicatorPreWriteStatusRevokeUpdateConfiguration
      → configure replicator with must_catch_up on swap target
3. PreWriteStatusRevokeCatchup             ← CATCHUP #1
      → wait_for_catch_up_quorum(WRITE)
      → writes still flowing (WriteStatus = Granted)
4. UpdateEpoch
5. ReplicatorUpdateCatchUpConfiguration
6. CatchupReplicaSetAll (CatchupDuringSwap) ← CATCHUP #2
      → wait_for_catch_up_quorum(WRITE or ALL)
      → writes stopped (WriteStatus = TryAgain)
7. UpdateReadWriteStatus                   ← write revocation HERE
8. ChangeReplicatorRole                    ← demote replicator P→S
9. ChangeReplicaRole                       ← demote user service P→S
10. ReconfigurationEnding
```

The `ReadWriteStatusCalculator.cpp` state machine confirms the timing:

| Reconfig phase | ReadStatus | WriteStatus |
|---|---|---|
| PreWriteStatusCatchup | Granted | **Granted** (writes flow) |
| CatchupInProgress | Granted | **TryAgain** (writes stopped) |
| CatchupCompleted | TryAgain | TryAgain |
| TransitioningRole | NotPrimary | NotPrimary |

Key: catchup #1 runs with writes granted, so the target gets nearly
caught up while clients keep writing. Then writes are revoked and
catchup #2 handles the final few ops. Only after both catchups does
the role change happen.

`PreWriteStatusRevokeCatchup` was added later as an optimization
(behind `IsPreWriteStatusCatchupEnabled` feature flag). The original
SF relied on catchup #2 alone — the post-revoke catchup is the safety
net. Catchup #1 reduces the post-revoke window but is not required
for correctness.

#### Why kuberic doesn't follow SF's full sequence

SF's double-catchup operates at the **replicator level** — the RA calls
`wait_for_catch_up_quorum()` on the replicator, which blocks until the
target's ACKed LSN reaches the required level. This requires:

1. **The replicator is still running as primary** during both catchups.
   SF doesn't demote the replicator until step 8 — after both catchups.
2. **`must_catch_up`** semantics in the quorum tracker that specifically
   target the swap secondary.
3. **Separate replicator role and service role transitions** — SF
   changes the replicator role and user service role in distinct steps.

Kuberic's `change_role(ActiveSecondary)` is a single call that demotes
the replicator AND triggers `close_all()` on the `PrimarySender` — there
is no intermediate state where the replicator is demoted but connections
are still alive. Once `close_all()` fires, any un-drained data in the
unbounded channels is lost.

#### Kuberic fix: poll target progress after revoke

Since kuberic's replicator drain tasks run independently (tokio spawned
tasks), ops already in the unbounded channels will drain to completion
as long as the channels aren't dropped. The fix is to **wait for the
target to catch up before demoting** — don't call `change_role` (which
triggers `close_all`) until the target has received all data:

```rust
// 1. Revoke write status (atomic — no new writes accepted)
revoke_write_status().await?;

// 2. Wait for target to catch up (drain tasks still running)
//    After revoke, LSN is frozen. Poll until target matches.
let primary_lsn = self.replicas[&old_primary_id]
    .handle.current_progress();
let deadline = Instant::now() + Duration::from_secs(5);
loop {
    let target_progress = self.replicas[&target_id]
        .handle.current_progress();
    if target_progress >= primary_lsn { break; }
    if Instant::now() > deadline {
        // Abort: re-grant writes, return error
        return Err(KubericError::Internal(
            "switchover catchup timeout".into()));
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
}

// 3. Now safe to demote (target has all data)
change_role(new_epoch, ActiveSecondary).await?;  // close_all() OK
change_role(new_epoch, Primary).await?;          // promote target
```

This is simpler than SF's full sequence because:
- No catchup #1 needed: kuberic's `revoke_write_status` is an atomic
  flag set, and the drain tasks continue independently. By the time
  we poll, most ops have already drained.
- No `must_catch_up` quorum interaction needed: we poll a single
  target's `current_progress`, not the full quorum tracker.
- The window between revoke and the first poll is microseconds —
  the drain tasks typically finish before the first poll iteration.

**Fix (implemented):**

- Durable switchover captures the frozen LSN, waits for the target to catch
  up, then persists correlated revocation/demotion/promotion actions. Deadline
  and compensation state live in CRD status.

- `SecondaryReceiver` now updates `PartitionState::current_progress`
  when accepting replication items. Previously, only the primary actor
  set `current_progress` — secondaries always reported 0. This made
  `GetStatus` on a secondary return stale progress.

- High-fidelity durable switchover tests exercise the production
  `ReplicaAgent → PodRuntime` path, including compensation and lost replies.

### E3. Pod restart not detected — stale handle in driver

**Severity:** ✅ Fixed
**Affects:** Reconciler Healthy phase
**File:** `kuberic-operator/src/reconciler.rs`

When K8s restarts a container (crash, OOM kill, node eviction), the Pod
stays Ready but the PodRuntime is virgin: `role = Unknown`,
`epoch = (0,0)`. Without detection, the driver holds a stale handle
to a blank runtime — replication breaks, quorum inflated, failover
may select a blank pod.

**Fix (implemented):**

- Added `Role::Unknown` variant (`#[repr(u8)]` with `TryFrom<u8>`)
  to distinguish "never assigned" from `Role::None` (explicit demotion).
  Proto updated: `ROLE_UNKNOWN = 0`, `ROLE_NONE = 4`.

- `ReplicaStatusInfo` and `ReplicaHandle::get_status()` expose strict runtime
  and required replica-agent observations through `GrpcReplicaHandle`.

- Reconciler Healthy phase probes ALL replicas (primary + secondaries)
  via `get_status()` on every reconcile cycle. Detects:
  - Epoch mismatch → stale (pod restarted)
  - `Role::Unknown` → stale (virgin runtime)
  - gRPC error → stale (pod dead)
  Primary stale → FailingOver. Secondary stale → remove handle.
  Runs before switchover check to prevent switchover to a dead target.

- Added `KvClusterApi::crash_pod()` / `restart_pod()` mock methods.
  `crash_pod` aborts tasks + preserves `data_dir` (PVC simulation).
  `restart_pod` reuses saved `data_dir` (PVC re-attach).

- Tests upgraded: `test_reconciler_detects_primary_failure_and_fails_over`
  and `test_reconciler_double_failover` now use `crash_pod()` instead
  of `mark_pod_not_ready()`. Double failover no longer needs manual
  `remove_replica_from_driver` — health check handles it automatically.

- New test: `test_reconciler_secondary_crash_and_rejoin` validates
  secondary crash → health check removes stale handle → restart →
  scale-up re-integrates.

**Remaining:** Add `restart_count` to `MemberStatus` CRD for
observability (complementary to epoch-based detection).

### E4. Switchover state lost with operator process — ✅ Fixed

**Affects:** Reconciler `Switchover` phase

Reconciler-driven switchover now persists a compact versioned operation in CRD
status. Each side effect has write-ahead intent, deterministic action identity,
exact replica incarnation/epoch targeting, observable completion, bounded
retry/deadline metadata, and resource-version-protected advancement.

The operation durably covers write revocation, frozen-LSN catch-up, demotion,
promotion, member epoch convergence, catch-up/current configuration, routing
labels, stable snapshot persistence, and compensation to the old primary.
Impossible or incompatible observations perform no runtime mutation and remain
fail closed. Failover and other operations retain their existing paths.

### E5. Replica add/rebuild state lost with operator process — ✅ Fixed

**Affects:** Reconciler scale-up and stale-secondary rejoin

Replica add and rebuild now use the same bounded CRD-backed durability model as
switchover, but its runtime sequence is now primary-agent-owned. The checkpoint
keeps previous/target/committed snapshots, exact candidate identity, one
structured frozen intent, one coarse pending action, generations/endpoints,
configuration descriptors, build key, and deadlines.

The operator dispatches one `AddReplicaIntent` only to the current primary.
That agent coordinates target Prepare/Activate/Cleanup through
`ReplicaLifecyclePeer` v2, tracked copy, catch-up/current configuration, and
quorum wait. Target process generation participates in build proof, so a
same-Pod process restart recopies rather than activating from stale evidence.

Stale-secondary rejoin removes the old exact primary connection before opening
the replacement under the same logical ID. This follows Service Fabric's
BuildIdleReplica instance checks and in-build tracking
(`FailoverUnitProxy.ReplicatorBuildIdleReplicaAsyncOperation.cpp`): duplicate
ready instances complete successfully, stale requests are rejected, and an
older instance is removed before a newer build proceeds.

Compensation is commit-aware: previous configuration and target-connection
absence must be proven before the operator deletes the pod. Failure to prove
that barrier poisons and preserves the target. Current-configuration commit is
roll-forward only; unattested committed membership becomes
`CommittedDegraded` without a serving label and normal recovery takes over.

### E6. Replica removal state lost with operator process — ✅ Fixed

**Affects:** Healthy scale-down and stale/dead-secondary eviction

The operator now freezes one remove operation v2 and dispatches one
`RemoveReplicaIntent` v1 to the exact current primary through correlated
control v3. CRD status retains desired topology, `ScaleDown`/`Force`
authorization, frozen previous/reduced descriptors, attempt/deadline state,
commit recognition, Kubernetes cleanup, and final stable publication.

The primary `ReplicaAgent` transiently installs reduced CatchUp with the
previous configuration, waits for the frozen write quorum, installs reduced
Current, removes the exact old-incarnation connection, and coordinates target
`Retire` through `ReplicaLifecyclePeer` v2. `PodRuntime` executes ordered
effects only. Primary/target phases and their 16-entry terminal ledgers are
volatile evidence, not a second durable workflow store.

Exact reduced Current is the irreversible boundary. Before current-install
dispatch, observed failure may restore previous Current. Once dispatch is
recorded, failed or ambiguous re-observation never authorizes rollback without
positive exact configuration evidence. The operator first persists the
primary commit timestamp and reduced workflow-scoped `committedSnapshot`;
`stableSnapshot` remains previous until exact connection/retirement evidence,
UID-qualified `role=retired` fencing, and UID-qualified pod deletion complete.
Every post-commit path rolls forward.

`ScaleDown` requires exact target reachability before operation persistence.
`Force` permits unavailable target retirement but does not bypass
`minReplicas`, quorum, identity, generation, epoch, or committed-configuration
checks. The 10/30/60/600-second budgets and maximum three pre-commit attempts
are frozen. Exhausted known pre-commit work, structurally impossible
post-dispatch evidence, and a complete evidence-erasing primary restart are
durably distinguished as `FailedPreCommitIncomplete`,
`InvalidRemovalState`, and `AmbiguousPrimaryRestart`.

This aligns with SF's manager-owned topology and primary/target RA-owned local
removal responsibilities. The deliberate transport difference is that SF
sends `DeleteReplica` directly FM→target, while Kuberic sends
primary-agent→target-agent `Retire` and leaves physical deletion with the
operator
(`/data/code/service-fabric/src/prod/src/Reliability/Failover/fm/PendingTask.cpp:112-160`,
`/data/code/service-fabric/src/prod/src/Reliability/Failover/fm/SendMessageAction.cpp:174-216`).
Kuberic `Force` has no SF `Obliterate` target epoch bypass
(`/data/code/service-fabric/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp:780-834`).

### E7. Initial creation state lost with operator process — ✅ Fixed

**Affects:** Reconciler `Creating` phase

Initial partition bootstrap now uses a creation-specific durable operation
rather than the monolithic driver method. The checkpoint explicitly represents
no previous topology, pins the sorted logical/incarnation target set and
lowest-ID primary, and carries an optional committed bootstrap snapshot.

Open(New), initial promotion, primary-only current configuration, secondary
epoch/idle/build/active transitions, catch-up/current configuration, quorum
wait, and routing-label publication are correlated activities. Primary-only
and every expanded secondary current configuration are persisted as partial
bootstrap authority before later work begins.

All pods start with a non-serving bootstrap label. Serving labels are published
only after the complete target satisfies `minReplicas`, then the complete
stable snapshot becomes Healthy authority. Pre-primary-commit failure can
clean up and restart from no topology; later failure preserves committed
members and cleans only the current candidate. Replacement of a committed
incarnation fails closed.

This aligns with Service Fabric's AddPrimary duplicate/stale-instance handling
and replication `CreateInitialPrimary` transition
(`Failover/ra/ReconfigurationAgent.cpp`,
`Replication/Replicator.ChangeRoleAsyncOperation.cpp`) while retaining
Kuberic's explicit incremental configuration checkpoints.

### E8. Pod runtime owns manager-protocol correlation — ✅ Fixed

**Affects:** Control server, pod-local restart fencing, durable activity
observation

`ReplicaAgent` now sits between `ControlServer` and `PodRuntime`. It owns
action ID/signature validation, active/terminal observations, duplicate replay,
local mutation serialization, bounded errors/faults, and explicit
replica-incarnation, process-generation, control-version, and runtime-epoch
fences. `PodRuntime` owns effect ordering and exact completion only; it has no
correlation ledger.

This follows the Service Fabric ownership split rather than copying its storage
model. RA routes FM messages through per-failover-unit entity scheduling,
rechecks generation/epoch/replica instance under the entity lock, and persists
failover-unit progress
(`Reliability/Failover/ra/MessageHandler.cpp`,
`MessageContext.h`, `Infrastructure.EntityScheduler.h`). RAProxy keeps a fresh
runtime-bound proxy map, admits compatible action lists, and executes ordered
service/replicator callbacks
(`ReconfigurationAgentProxy.cpp`, `FailoverUnitProxy.cpp`,
`ProxyActionsList.cpp`).

Kuberic does not add a durable local LFUM. CRD status remains the sole durable
global store. Agent state lasts one process generation and retains 16 terminal
records. A same-Pod process restart changes `AgentGeneration`; prior local
state is not inherited, and the operator observes durable postconditions
before refreshing fences or redriving. When stable secondary runtime
continuity cannot be proven, recovery enters the existing durable
force-remove/rebuild protocol.

`ExecuteCorrelatedControlAction` is the only production mutation path, and
`current_action` plus `retained_terminal_actions` is the only local
correlation ledger. Individual mutation RPCs and `ExecuteDurableAction` are
retired. Required numeric control v3 mismatch fails closed, so operator
and runtime deployment must be coordinated.

Replica add/build and removal share one deliberately narrow
`ReplicaLifecyclePeer` v2 surface on the same listener. The target
reverse-observes exact parent/sender authority and local
identity/generation/epoch before typed Prepare/Activate/Cleanup/Retire effects.
Shared peer primitives do not merge the workflows and do not create a general
RA-to-RA reconfiguration framework. Switchover remains the next
operator-sequenced local reconfiguration candidate.

### E9. PostgreSQL correlated topology integration

The retired mutable-driver Postgres suites were not recreated. PostgreSQL
instance/adapter tests remain, while durable topology checkpoint integration
is covered with real KV pods and correlated SQLite replication covers the
external data-plane shape. A future Postgres-specific `ClusterApi` harness
should exercise durable failover and switchover without restoring a public
mutation bypass.
