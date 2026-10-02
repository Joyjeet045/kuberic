# Kuberic: Peer-Repair Replication

> **Archived comparative proposal.** Its classic-operator baseline was removed.
> The document remains design history and does not describe a supported stack.

Design for a replication system where the Writer sends writes to a
quorum of replicas, and replicas **repair each other** to fill in
missing data. The Writer's job ends once W replicas ACK.
Replicas that missed the write (because they weren't in the W) or
were temporarily down catch up by pulling from peers — not from the
Writer.

This is a separate system from both the former leader-based
operator and the Writer-based design
(`writer-based-replication.md`).

> **Status:** Archived design alternative. Shares the **API contract**
> of the [In-Doubt Writer Contract](writer-based-replication.md#in-doubt-writer-contract):
> - `Ok(lsn)` / `PreSendFailure` / `InDoubt` / `WriterPoisoned`
> - Single-op-in-flight serialization (Writer caps throughput at
>   ~1/RTT per partition)
> - Replica durability contract (persisted mode required;
>   `acknowledge()` after fsync)
> - Recommended caller pattern: app restart on `InDoubt`
>
> However, peer-repair uses **different authority recovery
> mechanics** than writer-based, because its W-of-N write path
> creates inherent gaps **between `received_lsn` (highest contiguous
> LSN) and `max_received_lsn` (highest LSN observed)**. Peer-repair
> does NOT inherit "Replica Order Acceptance" from writer-based;
> it has its own gap-tolerant recovery sketch (see
> [Crash Recovery (Peer-Repair Variant)](#crash-recovery-peer-repair-variant)).
> The full peer-repair recovery protocol is a follow-up work item.
>
> Authority recovery uses `max(max_received_lsn)` across fenced-
> quorum replicas, plus a synchronous peer-repair sweep to close
> gaps before declaring recovery complete. No `committed_lsn`
> tracking, no per-commit external state.
>
> **Latency vs writer-based:** Peer-repair sends to exactly W
> replicas and must wait for the slowest of that subset. Writer-based
> sends to all N and takes the W-th fastest ACK (better tail latency).
> Peer-repair's actual benefits are: stateless Writer (no
> `ReplicationQueue`), reduced Writer bandwidth, and self-healing
> replicas via background gossip. Choose peer-repair when these
> benefits matter more than tail latency or recovery simplicity.

> **Scope:** Single writer only. **In-cluster Writer topology only**
> (matches writer-based scope). Multi-writer extensions are out of
> scope.

> Part of the [archived Kuberic design](../README.md).
> Related: [Writer-Based Replication](writer-based-replication.md),
> [Leaderless Quorum Research](../../../background/leaderless-quorum-replication.md),
> [Lazy Repair Research](../../../background/lazy-repair-replication.md)

---

## Motivation

### The Problem with Writer-Based Replication

In the Writer-based design, the Writer sends every op to **all N
replicas** and waits for W ACKs. This has two costs:

1. **The Writer fans out to all N replicas**, even though only W ACKs
   are needed. The N-W extra sends consume Writer bandwidth and add
   tail-latency sensitivity (the Writer's `send_to_all` must reach
   all N).

2. **The Writer holds a `ReplicationQueue`** to replay ops to new
   replicas during builds. If the Writer crashes during a build, the
   queue is lost (KI-2). The queue also grows unboundedly during
   builds (KI-4).

### The Peer-Repair Alternative

What if the Writer only sends to W replicas (the quorum), and the
remaining N-W replicas catch up from their peers?

```
Writer-based (current design):        Peer-repair-based:
─────────────────────────────          ──────────────────────

Writer ──► Replica₁ (ACK)             Writer ──► Replica₁ (ACK)
       ──► Replica₂ (ACK)                    ──► Replica₂ (ACK)
       ──► Replica₃ (no ACK needed)          (done — Replica₃ catches up later)
                                                         │
                                       Replica₃ ◄── pull from Replica₁ or Replica₂
```

**Writer becomes simpler:**
- No `send_to_all` — Writer sends to W replicas, waits for W ACKs,
  returns LSN. Done.
- No `ReplicationQueue` — Writer doesn't need to buffer ops for
  replay. Replicas hold their own data and share it with peers.
- Writer crash has less impact — no queue to lose. Replicas
  repair among themselves.

**Replicas become smarter:**
- Each replica knows what LSNs it has and what it's missing.
- Replicas periodically compare state with peers and pull missing ops.
- This is the **anti-entropy** model from Dynamo/Cassandra/Riak,
  adapted for totally-ordered single-writer ops.

---

## Prior Art

### Dynamo-Family Anti-Entropy

All Dynamo-family systems (Cassandra, Riak, Voldemort) use
replica-to-replica repair as a core mechanism:

| Mechanism | When | How |
|---|---|---|
| **Read repair** | On read | Coordinator compares digests from R replicas; if mismatch, pushes latest value to stale replicas |
| **Hinted handoff** | On write failure | Stand-in node stores a "hint" for the down replica; replays when it recovers |
| **Active anti-entropy (AAE)** | Background | Replicas exchange Merkle tree hashes; identify and repair divergent/missing keys |

Key differences from our design:
- Dynamo systems are **multi-writer** — any node can accept writes,
  so conflicts are possible. They use vector clocks / LWW for
  conflict resolution.
- Our system is **single-writer** with **total ordering** (LSNs).
  No conflicts are possible. Repair is simpler: a replica
  either has an op or it doesn't. No merge logic needed.

### Cassandra Read Repair and Anti-Entropy Repair

Cassandra's write path sends to all N replicas but only waits for W
ACKs (same as Writer-based). Replicas that missed the write are
healed by:

1. **Read repair:** On read, coordinator sends digest requests to
   extra replicas. If a replica is behind, the coordinator pushes
   the latest value.
2. **Anti-entropy repair:** Background Merkle tree comparison between
   replicas. Runs periodically (operator-initiated or scheduled).
   Identifies and fixes missing or divergent data.

### Riak Active Anti-Entropy (AAE)

Riak's AAE runs continuously as a background process:

1. Each vnode maintains a **persistent on-disk Merkle tree** (hash
   tree) of its key/value data.
2. Trees are updated in real-time as writes arrive.
3. Periodically, vnodes exchange tree hashes with their replicas.
4. Mismatches are drilled down level-by-level until the specific
   divergent keys are found.
5. The vnode with the more recent value pushes it to the stale vnode.

Key advantage: AAE works for "cold data" that is never read —
read repair alone can't fix data that nobody reads.

### Epidemic / Gossip Protocols

The foundational paper: Demers et al. (1987), "Epidemic Algorithms
for Replicated Database Maintenance." Defines two modes:

- **Anti-entropy:** Each node periodically picks a random peer and
  exchanges state differences. Guaranteed convergence but slower.
- **Rumor mongering:** Each node that learns new data actively pushes
  it to random peers until the rumor "dies" (enough peers already
  know). Faster but not guaranteed.

Our peer-repair design uses **anti-entropy** (periodic pairwise
exchange) because we need guaranteed convergence, not just
probabilistic spread.

### Log-Based Peer-Repair (Unique to Our Design)

Unlike Dynamo-family systems which repair **key-value pairs**
(comparing individual keys), our system repairs a **totally
ordered log**. This is simpler:

- Each replica knows its `received_lsn` — the highest contiguous LSN
  it has.
- To catch up, a replica asks any peer: "give me ops from LSN X+1
  onward."
- No Merkle trees needed for the common case — a single integer
  comparison (`received_lsn`) tells a replica exactly what it's
  missing.
- Merkle trees are only needed for detecting **corruption** (a
  replica thinks it has LSN 50 but the data is wrong), not for
  detecting **gaps** (which LSN comparison handles).

---

## Design

### Architecture

```
User App + Writer                     In-Cluster
─────────────────                     ──────────

Writer ──gRPC──► Replica₁  ◄──repair──► Replica₂
       ──gRPC──► Replica₂  ◄──repair──► Replica₃
                 Replica₃  ◄──repair──► Replica₁
                    │
                    ▼
              Coordinator ◄── watches ── Operator/CRD
```

**Three distinct data flows:**

1. **Write path** (Writer → W replicas): Writer sends to W replicas,
   waits for W ACKs. Fast, minimal.
2. **Repair path** (replica ↔ replica): Background process
   where replicas compare `received_lsn` and pull missing ops from
   peers. Continuous, asynchronous.
3. **Control path** (Coordinator ↔ Writer): Same as Writer-based
   design — epoch, config updates, lifecycle.

### Write Path (Simplified)

```rust
impl Writer {
    pub async fn replicate(&self, data: Bytes) -> Result<Lsn> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::Relaxed);

        // Send to W replicas (not all N)
        let targets = self.pick_w_replicas();
        let mut acks = 0;
        let futures = targets.iter().map(|r| r.send(lsn, &data));

        for result in join_all(futures).await {
            if result.is_ok() { acks += 1; }
        }

        if acks >= self.write_quorum {
            Ok(lsn)
        } else {
            Err(KubericError::NoWriteQuorum)
        }
    }
}
```

**Key difference from Writer-based design:** No `send_to_all`. No
`ReplicationQueue`. The Writer sends to exactly W replicas and
forgets about the op. The remaining N-W replicas will get the op
via peer-repair.

**Replica selection:** The Writer can use round-robin, random, or
latency-based selection to pick which W replicas receive each write.
This naturally load-balances writes across replicas.

### Peer-Repair Protocol

Each replica runs a background repair loop:

```
Every T seconds (e.g., 100ms):
  1. Pick a random peer replica
  2. Send PeerRepairRequest { my_received_lsn }
  3. Peer responds with ops from my_received_lsn+1 to peer.received_lsn
  4. Apply received ops to local state
  5. Update my received_lsn
```

```protobuf
service ReplicaPeerRepair {
    // Pairwise peer-repair: compare LSNs, exchange missing ops
    rpc PeerRepair(PeerRepairRequest) returns (PeerRepairResponse);
}

message PeerRepairRequest {
    int64 my_received_lsn = 1;     // requester's highest contiguous LSN
    int64 writer_epoch = 2;        // requester's current writer_epoch
    int64 data_loss_number = 3;    // requester's current data_loss_number
}

message PeerRepairResponse {
    repeated ReplicationItem ops = 1;  // ops the requester is missing
    int64 writer_epoch = 2;        // responder's current writer_epoch
    int64 data_loss_number = 3;    // responder's current data_loss_number
    int64 peer_oldest_available_lsn = 4; // lowest LSN still in op store (for snapshot trigger)
}
```

Note: `committed_lsn` is **not** included in repair messages.
Under the [In-Doubt Writer Contract](writer-based-replication.md#in-doubt-writer-contract),
all received ops are preserved (authority approach uses
`max(received_lsn)` across survivors), so replicas only need to
exchange `received_lsn`.

##### Epoch-Aware Peer-Repair

Replicas must validate epochs during repair to ensure peers agree
on the current generation:

```
On receiving PeerRepairRequest:
  if requester.writer_epoch < my.writer_epoch
     OR requester.data_loss_number < my.data_loss_number:
    → REJECT: respond with my epoch, no ops
       (requester learns it's stale and must re-sync with coordinator)

  if requester.writer_epoch > my.writer_epoch:
    → REJECT: I'm stale, don't send my ops
       (I need to re-sync with coordinator first)

  if epochs match:
    → PROCEED: exchange ops normally
```

This prevents the scenario where a partitioned replica at
`writer_epoch=1` re-injects ops into a peer at `writer_epoch=2`.
The stale peer's repair request is rejected, and it learns it
needs to contact the coordinator for the current epoch before it
can participate in repair.

##### Repair Propagates All Received Ops

Under the [In-Doubt Writer Contract](writer-based-replication.md#in-doubt-writer-contract),
all ops a replica has received are preserved (the authority
approach uses `max(received_lsn)` across surviving replicas on
Writer takeover, keeping everything ≤ authority). Therefore peer-repair
simply propagates **all** received ops:

```
Repair response filter:
  Send ops WHERE lsn > requester_received_lsn
             AND lsn <= my_received_lsn
             AND epoch matches (already validated above)
```

There is no "committed-only" filter — whether an op was
quorum-confirmed by the Writer doesn't matter for peer-repair.
If the Writer poisons after sending only to one replica, the
authority recovery protocol will preserve that op (or not, if no
survivor has it). Either outcome is consistent with the user's
in-doubt status.

##### Surviving-Replica Scenario

```
N=3, W=2. Writer sends LSN=50 to Replica₁ and Replica₂. Both ACK.
Writer returns Ok(50) to user.

Replica₂ dies BEFORE repair to Replica₃.

Surviving:
  Replica₁: has LSN=50
  Replica₃: does NOT have LSN=50

The op is safe — Replica₃ repairs with Replica₁ and pulls LSN=50.

If the Writer crashes before repair completes, the coordinator
runs authority recovery: max(received_lsn) = max(50, 49) = 50.
Replicas keep everything ≤ 50. New Writer starts at 51.
Result: LSN=50 preserved. ✓
```

A single surviving copy of a committed op is sufficient: the
authority approach + peer-repair ensures the op propagates to
the rest of the cluster.

##### All-Copies-Lost Scenario (Data Loss)

```
N=3, W=2. Writer sends LSN=50 to Replica₁ and Replica₂. Both ACK.
Writer returns Ok(50) to user.

Both Replica₁ AND Replica₂ die. Only Replica₃ survives.
Replica₃ does NOT have LSN=50.

LSN=50 is LOST — no surviving replica has it.
This is quorum loss → data loss — same as any replication system.
Operator must bump data_loss_number and run data loss recovery
(pick canonical survivor, call on_data_loss(), rebuild others).
```

This is identical to the leader-based system: if both the primary
and the one secondary that had the latest op die, the op is lost.
Quorum replication guarantees survival through F failures (where
F = N - W). It does not guarantee survival through F+1 failures.

**Properties:**

- **Convergence guaranteed:** As long as any two replicas can
  eventually communicate, all replicas converge to the same
  state. This follows from epidemic algorithm theory — pairwise
  random exchange converges in O(log N) rounds.
- **Epoch-aware:** Peer-repair validates `writer_epoch` and
  `data_loss_number` on every exchange. Stale-epoch peers are
  rejected.
- **No coordinator involvement:** Peer-repair is peer-to-peer.
- **Idempotent:** Receiving an op you already have is a no-op.
- **Bounded catch-up time:** For N=3 with 100ms repair interval,
  a lagging replica catches up within ~200-300ms.

### Replica Data Model

Each replica maintains:

```rust
struct ReplicaState {
    /// Ops stored locally: LSN → data
    /// Retains ops for peer-repair
    ops: BTreeMap<Lsn, Bytes>,
    /// Highest contiguous LSN received (no gaps below this)
    received_lsn: Lsn,
    /// Applied to user state up to this LSN
    applied_lsn: Lsn,
}
```

Note: replicas do **not** track `committed_lsn`. Under the
in-doubt + authority approach, all received ops are preserved on
Writer takeover (the authority is `max(received_lsn)` across
survivors), so the distinction between "committed" and
"received" is unnecessary at the replica level.

**Ops retention:** Replicas retain ops in their local store for
peer-repair. Ops are GC'd once all N replicas have confirmed
receipt (via repair exchanges). This replaces the Writer's
`ReplicationQueue`.

**GC protocol:** During repair, replicas exchange their
`received_lsn`. Each replica tracks the minimum `received_lsn`
across all peers (`min_peer_lsn`). Ops with LSN ≤ `min_peer_lsn`
are safe to GC — all replicas have them.

### Epoch and Writer Identity

Same as the Writer-based design:
- `data_loss_number` + `writer_epoch` (fenced on replicas)
- `config_version` (informational, not fenced)
- `session_token` for Writer authentication
- Coordinator manages epoch lifecycle
- Writer lifecycle protocol (graceful shutdown, crash fencing)

No changes to the epoch design.

### Crash Recovery (Peer-Repair Variant)

Crash recovery for peer-repair shares the **API contract** of the
[In-Doubt Writer Contract](writer-based-replication.md#in-doubt-writer-contract)
— `Ok(lsn)` / `PreSendFailure` / `InDoubt` / `WriterPoisoned`,
fail-stop semantics, app-restart caller pattern — but uses
**different recovery mechanics** because peer-repair's write path
is structurally different from writer-based:

- Writer-based sends to ALL N replicas; replicas accept LSNs in
  strict order; `received_lsn` is contiguous; authority can be
  computed directly from `max(received_lsn)`.
- Peer-repair sends to W replicas (a subset); the (N-W) skipped
  replicas inherently lack those LSNs until peer-repair runs;
  `received_lsn` cannot be strictly contiguous in steady state.

This means peer-repair **cannot inherit** writer-based's "Replica
Order Acceptance" property. Instead, peer-repair must define its
own recovery semantics consistent with gappy `received_lsn`.

#### Peer-Repair Authority Recovery (Sketch — Detailed Design Pending)

A first-pass sketch — full design is a follow-up work item:

```
On Writer poison/disconnect (coordinator detects):

  1. Coordinator advances recovery_generation + epoch in CRD
     (same as writer-based)

  2. Push FreezeWrites{generation, new_epoch} to all replicas;
     wait for ≥W ACKs (fenced quorum). Each ACK carries
     replica's (max_received_lsn, contiguous_received_lsn) pair —
     the highest LSN observed and the highest contiguous prefix.

  3. authority_lsn = max(max_received_lsn from fenced quorum).
     This may be a "gappy" value — some replicas may not have
     ops at every LSN ≤ authority_lsn.

  4. Coordinator orchestrates a synchronous peer-repair sweep
     among the fenced quorum: every replica must hold every
     LSN ≤ authority_lsn before recovery completes. This is
     analogous to "must_catch_up" in the existing leader-based
     design's reconfiguration.

  5. After the synchronous repair sweep completes:
     Push UpdateEpoch{generation, new_epoch, authority_lsn}
     (atomic truncate-above on receipt; same as writer-based).

  6. Per-replica reconciliation for unfenced/lagging replicas
     (same as writer-based, with snapshot+peer-repair build).

  7. New Writer starts at authority_lsn + 1.
```

**Key differences from writer-based recovery:**
- Step 3: `max(max_received_lsn)` may include LSNs not present
  on every fenced replica — peer-repair must close these gaps
  during recovery.
- Step 4: synchronous peer-repair sweep adds a recovery cost
  proportional to gap size (typically small, since gaps reflect
  one repair-interval worth of writes).
- The authority approach still works because `Ok(lsn)` from the
  Writer required W ACKs at write time; at least one fenced
  replica has the op even if peer-repair hasn't propagated it
  to all W yet.

**Open design question:** In step 3, what if a fenced replica
holds an op at LSN=K but no other fenced replica has it (Writer
sent to {R1, R2}, both ACKed, R2 then crashed; R1 is in fenced
quorum, R2 is not)? The op is committed (W ACKs at write time)
but only one fenced replica has it. The synchronous repair sweep
in step 4 must propagate it to the rest of the fenced quorum
before authority recovery completes — this is the equivalent of
the writer-based design's strict-order guarantee, but enforced
during recovery rather than steady state.

**Status:** This sketch is preliminary. The full peer-repair
authority recovery protocol — proving correctness, specifying
the synchronous repair sweep semantics, and characterizing
its latency cost — is a follow-up work item before peer-repair
is implementation-ready.

### Replica Building

Replica building uses **snapshot + peer-repair**:

1. **Snapshot (required for base data):** Existing replicas GC ops
   once all peers have confirmed receipt. A new replica joining with
   `received_lsn = -1` cannot repair from the beginning — those
   ops are long gone. It needs a full state snapshot first.

2. **Peer-repair (for the delta):** After the snapshot, the new
   replica has data through `snapshot_lsn`. The repair loop
   catches up the delta from `snapshot_lsn + 1` to the current head.

```
Operator                    Existing Replicas          New Replica
────────                    ─────────────────          ───────────
1. Create new pod
2. Pick snapshot source
   (highest received_lsn)

3. Snapshot (copy protocol):
   Source replica ──────────────────────────────────►
     GetCopyState(up_to_lsn) ─────────────────────►  received_lsn = -1
     CopyStream(full state)  ─────────────────────►  Apply snapshot
                                                      received_lsn = snapshot_lsn

4. Peer-repair (delta):
   (repair loop starts automatically)
                              ◄── PeerRepairRequest ──
                              ── PeerRepairResponse ──►
                                 (ops from snapshot_lsn+1)

   ... repeat until caught up ...
                                                      received_lsn ≈ current

5. Add to config (tell Writer via coordinator)
```

**The copy protocol is the same as today's** `GetCopyState` /
`CopyStream` — any replica can serve as the snapshot source. The key
difference from the Writer-based design is that **the Writer is not
involved in the build at all**. The operator picks a source replica,
runs the copy, and the new replica's repair loop handles the
rest.

**When is snapshot needed vs. peer-repair-only?**

| Scenario | Mechanism |
|---|---|
| New replica (fresh pod) | Snapshot required, then peer-repair for delta |
| Restarted replica (has local state) | Peer-repair only — pulls missing ops from peers |
| Replica down briefly | Peer-repair only — peers still have the ops |
| Replica down longer than GC window | Snapshot required — peers have GC'd the gap |

**GC must account for builds:** While a build is in progress, existing
replicas must not GC below the snapshot source's `received_lsn` at
snapshot time. The operator signals `BuildInProgress` to replicas (not
the Writer — replicas manage their own GC). After the new replica
catches up via peer-repair, `BuildComplete` resumes GC.

### Writer Takeover

Writer takeover in peer-repair uses the **peer-repair-variant
authority recovery** described above (see [Crash Recovery (Peer-Repair Variant)](#crash-recovery-peer-repair-variant)).
Summary:

1. Writer poisons (via `InDoubt`) or disconnects.
2. Coordinator advances `recovery_generation` + epoch in CRD.
3. Coordinator pushes `FreezeWrites` to all replicas; awaits ≥W
   ACKs (fenced quorum). Each ACK carries replica's
   `(max_received_lsn, contiguous_received_lsn)`.
4. `authority_lsn = max(max_received_lsn from fenced quorum)`.
5. Coordinator orchestrates synchronous peer-repair sweep among
   the fenced quorum to close any gaps ≤ `authority_lsn`.
6. Coordinator pushes `UpdateEpoch{generation, new_epoch,
   authority_lsn}` to all replicas (atomic truncate-above).
7. Per-replica reconciliation for unfenced/lagging replicas.
8. New Writer connects, starts at `authority_lsn + 1`.

Compared to writer-based: peer-repair's recovery is more complex
because of the synchronous repair sweep in step 5, but it does not
require the Writer to maintain a `ReplicationQueue`. Trade-off:
recovery latency proportional to gap size at recovery time vs.
steady-state Writer memory.

Update also reflects [Replica Durability Contract](writer-based-replication.md#replica-durability-contract)
inheritance: persisted-mode is required; replicas MUST not ACK
ops to the Writer or to peers until durably persisted.

---

## Comparison: Writer-Based vs. Peer-Repair-Based

| Aspect | Writer-based | Peer-repair-based |
|---|---|---|
| **Write fan-out** | All N replicas | W replicas only |
| **Writer state** | `ReplicationQueue` (in-memory) | None (stateless after quorum ACK) |
| **Lagging replica catch-up** | Writer replays from queue | Replica pulls from peers |
| **Replica building** | Copy protocol + queue replay via Writer | Copy protocol (snapshot) + peer-repair (delta), no Writer involvement |
| **Writer crash impact** | Queue lost, build aborted | No impact (no queue) |
| **Median write latency** | RTT to W-th-fastest of N (slow replicas naturally avoided) | RTT to slowest of W (no avoidance) |
| **Tail write latency** | Better (fastest W-th of N) | Worse (slowest of W) |
| **Recovery latency** | 2 RTT (fence + UpdateEpoch) | 2 RTT + synchronous peer-repair sweep (gap-fill cost proportional to repair-interval gap size) |
| **Convergence latency** | Immediate (Writer sends to all) | Delayed (~100-300ms for N-W replicas) |
| **Replica complexity** | Simple (receive + ACK + serve reads) | Higher (repair loop + op storage + read serving) |
| **Network traffic** | Writer → all N (write path) | Writer → W (write) + replica ↔ replica (peer-repair) |
| **Coordinator dependency** | High (build coordination) | Lower (no build coordination) |

### When to Choose Which

| Scenario | Recommended |
|---|---|
| All replicas must have data immediately | Writer-based (send to all) |
| Writer bandwidth is constrained | Peer-repair (send to W only) |
| Minimizing Writer complexity/state | Peer-repair (no queue) |
| Large datasets with frequent pod restarts | Peer-repair (self-healing) |
| Reads from any replica require latest data | Writer-based (all replicas up-to-date) |
| Reads tolerate ~100-300ms staleness | Peer-repair |

---

## Trade-Offs

### Advantages

1. **Stateless Writer** — no `ReplicationQueue`, no build
   coordination, no queue GC. Writer crash is trivial to recover
   from.
2. **Self-healing replicas** — lagging or new replicas catch up from
   peers automatically. No operator intervention for routine
   catch-up.
3. **Simpler replica building** — snapshot from any peer + automatic
   peer-repair for the delta. No Writer involvement, no queue
   replay coordination. The Writer doesn't even know a build is
   happening.
4. **Reduced Writer bandwidth** — sends to W instead of N.
5. **Eliminates KI-2 and KI-4** from Writer-based design (no queue
   to lose or grow unboundedly).

### Disadvantages

1. **Convergence delay** — N-W replicas are behind by one
   repair interval (~100-300ms). Reads from a lagging
   replica return stale data.
2. **Higher replica complexity** — each replica runs a repair
   loop, maintains an op store with GC, exchanges state with peers.
3. **More inter-replica traffic** — repair exchanges consume
   intra-cluster bandwidth. For N=3, 3 pairwise exchanges per
   interval.
4. **Op retention on replicas** — replicas must retain ops until all
   peers have them. If one replica is down for a long time, ops
   accumulate on the others (same problem as Writer's queue, but
   distributed).
5. **Snapshot still needed for new replicas** — peer-repair handles
   the delta, but a fresh replica needs a full snapshot first (same
   copy protocol as today). The simplification is that the Writer
   is uninvolved — the operator and replicas handle it directly.

---

## Open Questions

1. **Repair interval tuning:** 100ms gives fast catch-up but
   generates continuous background traffic. 1s reduces traffic but
   increases staleness. Should this be configurable per-partition?

2. **Op retention and GC:** Replicas retain ops in their local
   store for peer-repair. Two distinct operations:

   **GC (garbage collection):** Remove ops that all N replicas
   have confirmed receipt of. This is routine cleanup to bound
   memory usage. GC condition: `lsn <= min(peer_received_lsn)`
   across all replicas.

   **Authority-based truncation (on Writer takeover):** Replicas
   keep everything ≤ `authority_lsn` (received from coordinator).
   There is no rollback below authority — the
   [In-Doubt Writer Contract](writer-based-replication.md#in-doubt-writer-contract)
   ensures ops on any surviving replica are preserved.

3. **Snapshot trigger (resolved):** A replica needs a snapshot when
   the ops it's missing have been **GC'd from all peers**. This is
   a binary condition, not a threshold:

   During peer-repair, if a peer responds with ops starting at
   LSN X but the requester needs ops from LSN Y where Y < X (the
   peer has GC'd everything below X), peer-repair cannot fill
   the gap. The peer signals this via
   `peer_oldest_available_lsn` in `PeerRepairResponse`.

   If `peer_oldest_available_lsn > my_received_lsn + 1`, the gap
   is unrecoverable via peer-repair. The replica must request a
   snapshot from this peer (or another). After applying the
   snapshot, peer-repair resumes for the delta.

   **When this happens:**
   - Fresh replica (new pod): always needs snapshot
   - Restarted replica (has local state): only if down longer than
     the GC window (i.e., all peers advanced `min_peer_lsn` past
     this replica's `received_lsn` while it was down)
   - Briefly-down replica: peer-repair suffices (peers still have
     the ops)

4. **Read consistency:** Reads from a lagging replica may return
   stale data. Should the peer-repair protocol include a
   "wait for LSN" mechanism?

5. **Merkle tree for corruption detection:** For the common case
   (gap detection), LSN comparison is sufficient. Should replicas
   also maintain Merkle trees (like Riak AAE) for detecting silent
   corruption? This adds storage and CPU overhead but catches bit
   rot.

---

## Implementation Plan

### Phase 1: Peer-Repair Protocol

- Define `PeerRepairRequest` / `PeerRepairResponse` protobuf messages
- Implement `ReplicaPeerRepair` gRPC service on each replica
- Implement repair loop (periodic random peer selection)
- Implement op store with LSN-based GC

### Phase 2: Writer (Send to W)

- Reuse Writer from writer-based design, but remove `ReplicationQueue`
  and `send_to_all` — replace with `send_to_w` (pick W replicas)
- Coordinator integration (same protocol)

### Phase 3: Snapshot Fallback

- Reuse existing copy protocol (`GetCopyState` / `CopyStream`) for
  large-delta bootstrap
- Automatic snapshot trigger when repair delta exceeds
  threshold

### Phase 4: GC Coordination

- Implement distributed GC: track `min(peer_received_lsn)` across
  all replicas
- Ops below min are safe to GC
- Handle down replicas (GC must not advance past a down replica's
  last known LSN until operator removes it from config)

---

## Known Issues

Issues identified during design review (SoT premortem analysis) that
require further design work.

### KI-1: GC Watermark Disagreement (must-fix)

Replicas compute `min_peer_lsn` independently from pairwise
repair exchanges. Each replica has a potentially different
view of peers' progress, so two replicas may compute different
`min_peer_lsn` values and GC different ops.

```
Replica₁ repairs with Replica₂ → learns Replica₂ has LSN=50
Replica₁ repairs with Replica₃ → learns Replica₃ has LSN=45
Replica₁: min_peer_lsn = 45, GCs below 45

Replica₂ hasn't repaired with Replica₃ recently
Replica₂: min_peer_lsn = 40 (stale view of Replica₃)
Replica₂ GCs below 40

If Replica₃ actually needs LSN 41-44 from Replica₂, they're gone.
But Replica₁ still has them. So repair from Replica₁ works.
```

**The issue is not data loss** (as long as ANY replica retains the
ops), but it complicates the snapshot trigger — a replica may need
to try multiple peers to find one that hasn't GC'd the needed range.

**Potential fix:** Coordinator-managed GC watermark. The coordinator
collects `received_lsn` from all replicas (via repair status
reports or direct queries) and publishes a global
`safe_gc_lsn = min(all replicas' received_lsn)`. Replicas GC only
below this coordinator-published value.

### KI-2: Inter-Replica Partition Detection (must-fix)

The design assumes all replicas can reach each other for
peer-repair. If two replicas are partitioned from each other
(but both reachable by the Writer and coordinator), peer-repair
between them stalls silently. Effects:

- GC is blocked: the partitioned replica's `received_lsn` doesn't
  advance from the other's perspective, blocking GC.
- Data divergence: the partitioned replica falls behind and can
  only catch up from the Writer's W-subset.

**Potential fix:** Coordinator pushes replica health/reachability
status. Replicas skip unreachable peers in GC watermark calculation.
The coordinator can also detect partitions and trigger snapshot
rebuilds for replicas that fall too far behind.

### KI-3: Applied Ops Are Always Preserved (resolved by In-Doubt Contract)

Once an op is applied to user state via `StateProvider` and
`acknowledge()`d, it cannot be un-applied by the replication
system. Under the [In-Doubt Writer Contract](writer-based-replication.md#in-doubt-writer-contract)
+ authority approach, applied ops are always preserved:

- The `authority_lsn` is `max(received_lsn)` across surviving
  replicas.
- Any op a replica has applied is in its `received_lsn` range.
- Therefore `authority_lsn ≥ any applied op's LSN` on any
  surviving replica.
- Replicas keep everything ≤ `authority_lsn` — no rollback below.

There is no rollback below `authority_lsn`, so applied ops are
never un-applied. This is the same invariant the existing
leader-based system maintains (via highest-LSN primary selection),
expressed differently.

### KI-4: W-Subset Selection Strategy (should-fix)

If the Writer always picks the same W replicas (e.g., lowest-latency),
some replicas never receive direct writes. Effects:

- Those replicas are always one repair interval behind
- They become GC bottlenecks (their `received_lsn` advances slower)
- Write load is unbalanced across replicas

**Fix:** Round-robin or random W-selection distributes writes evenly.
The Writer should rotate which replicas are in each write's W-subset.

### KI-5: New Replica Repair Loop Before Snapshot (should-fix)

A new replica starts its repair loop immediately on creation,
before the operator initiates the snapshot. Early repair
attempts fail (peers respond with `peer_oldest_available_lsn` > 0,
gap unrecoverable). Harmless but wastes network and log noise.

**Fix:** New replica starts repair loop only after snapshot
completes (operator signals readiness via config update).

---

## References

- Demers, A. et al. (1987). *Epidemic Algorithms for Replicated
  Database Maintenance.* PODC '87.
- Riak KV. *Active Anti-Entropy.*
  [docs.riak.com](https://docs.riak.com/riak/kv/latest/learn/concepts/active-anti-entropy/index.html)
- Cassandra. *Dynamo Architecture — Replica Synchronization.*
  [cassandra.apache.org](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html)
- Writer-based replication design:
  [writer-based-replication.md](writer-based-replication.md)
- Leaderless quorum survey:
  [leaderless-quorum-replication.md](../../../background/leaderless-quorum-replication.md)
