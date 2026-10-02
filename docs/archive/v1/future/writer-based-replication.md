# Kuberic: Writer-Based Replication

> **Archived comparative proposal.** Its classic-operator baseline was removed.
> The document remains design history and does not describe a supported stack.
> Present-tense technical statements below describe the proposed system model;
> they do not indicate that either the proposal or classic baseline is available.

Design for a **new** operator and control system that decouples
quorum coordination logic into a standalone **Writer** library. The
Writer handles LSN assignment, fan-out to replicas, and quorum
tracking. It is embedded in the user application and communicates
with an in-cluster **coordinator** for replica set discovery, epoch
changes, and build coordination.

The leader-based operator used as the comparison baseline was later removed.

> **Scope:** Single writer only. **In-cluster Writer topology only**
> — the Writer must run inside the same Kubernetes cluster as the
> replicas (in-process embedded, sidecar, or dedicated pod). Out-of-
> cluster Writer (laptop, CI, cross-region) is out of scope; the
> latency benefit does not hold across WAN. Multi-writer extensions
> are out of scope.

> Part of the [archived Kuberic design](../README.md).
> Related: [Leaderless Quorum Research](../../../background/leaderless-quorum-replication.md),
> [WAL Persistence](wal-persistence.md)
>
> **Historical assumption:** This proposal originally envisioned a new system
> built alongside the leader-based classic operator. That baseline has since
> been removed; there is no current workload choice between the two.

> **Key design properties:**
> - **In-Doubt Writer Contract** (see [section](#in-doubt-writer-contract)):
>   `replicate()` returns `Ok(lsn)` / `PreSendFailure` / `InDoubt` /
>   `WriterPoisoned`. On `InDoubt`, the app SHOULD treat it as a
>   fatal error and restart, recovering through normal startup.
> - **Fence-first-then-query authority recovery**: on Writer
>   takeover, the coordinator first fences old-epoch writes
>   (synchronous quorum-confirmed), then queries surviving replicas
>   for `received_lsn`, then computes `authority_lsn = max(received_lsn)`,
>   then pushes `update_epoch{new_epoch, authority_lsn}`. Mirrors
>   Service Fabric A2 (`revoke_write_status`) and Kuberic's durable
>   failover fence-before-observe pattern.
> - **Single-op-in-flight serialization**: the Writer internally
>   serializes `replicate()` calls via an actor (mpsc channel),
>   preserving strict total ordering. Throughput is capped at
>   ~1/RTT (~1000 ops/sec at 1ms RTT).
> - **Stateless coordinator**: all coordinator state lives in the
>   `KubericWriterPartition` CRD `status`. Recovery is naturally
>   idempotent under coordinator restart.
> - **No per-commit external state**: `committed_lsn` is not tracked.
>   Authority recovery uses replica-resident `received_lsn` only,
>   plus fence-first-then-query to prevent dual-Writer races.
>
> The 1-hop write latency is preserved for the in-cluster topology
> at full correctness; trade-offs are: failover is 2 RTT (rare path),
> throughput is RTT-bound (no pipelining), `InDoubt` triggers app
> restart.

---

---

## Motivation

### Current Write Path (2 round-trips)

Today, a client write traverses:

```
Client  ──gRPC──►  Primary Pod  ──gRPC──►  Secondary₁  ──ACK──►  Primary  ──reply──►  Client
                                ──gRPC──►  Secondary₂  ──ACK──►     │
                                                                     │
                                         (wait for W ACKs)          │
                                                                     │
                                         ◄───── LSN reply ──────────┘
```

**Hops:** Client → Primary (1) → Secondaries (2) → Primary (3) →
Client (4). The critical path is **2 sequential network round-trips**:
client↔primary + primary↔fastest-secondary.

The code path is:

1. `KvServer::put()` serializes the op
   (`examples/kvstore/src/server.rs:52-78`)
2. `StateReplicatorHandle::replicate()` sends `ReplicateRequest` over
   an mpsc channel to the `WalReplicatorActor`
   (`kuberic-core/src/handles.rs:162-186`)
3. The actor assigns an LSN, stores in `ReplicationQueue`, registers
   with `QuorumTracker`, and calls `PrimarySender::send_to_all()`
   which enqueues to per-secondary unbounded channels
   (`kuberic-core/src/replicator/actor.rs:312-336`)
4. Background drain tasks bridge unbounded channels to gRPC
   `ReplicationStream` streams
   (`kuberic-core/src/replicator/primary.rs:123-130`)
5. Secondaries receive items, store them, and send `ReplicationAck`
   back (`kuberic-core/src/replicator/secondary.rs:167-240`)
6. ACK reader tasks call `QuorumTracker::ack()` — when quorum is met,
   the original `replicate()` future resolves with the LSN
   (`kuberic-core/src/replicator/quorum.rs:165-197`)

### The Bottleneck

The primary pod is a **serialization point**: every write must traverse
it, even though the primary's contribution to quorum is just "one more
ACK" — the same as any secondary. The primary adds latency (extra
network hop + channel + actor loop) and caps throughput (single actor
event loop processes all writes sequentially).

### Proposed Write Path (1 round-trip)

The Writer is embedded in the user app. It fans out directly to all
replicas and receives config from a coordinator.

```
                            ┌── Coordinator (in-cluster) ◄── watches ── Operator/CRD
                            │
User App + Writer ──config stream──┘
                  ──gRPC──►  Replica₁  ──ACK──┐
                  ──gRPC──►  Replica₂  ──ACK──┤  Writer waits for W ACKs
                  ──gRPC──►  Replica₃  ──ACK──┘
                                               │
                  ◄── LSN returned ────────────┘
```

**Critical path:** 1 network round-trip — Writer → W replicas → ACK.

**Expected latency improvement:** ~50-67% reduction for typical
3-replica deployments where primary↔secondary latency ≈ client↔pod
latency.

---

## Design

### Key Design Decisions

**Single writer → no roles.** With one Writer doing all writes, there
is no reason to distinguish Primary from Secondary. All replicas are
identical — they accept writes, validate epochs, store ops, ACK. The
Writer is the coordinator, but it is not a role assigned to a replica —
it's a library embedded in the user application.

**Single writer → trivial LSN assignment.** The multi-writer LSN
problem (range allocation, HLC, etc.) disappears entirely. The Writer
holds a simple monotonic counter: `next_lsn += 1`. Total ordering is
preserved — identical to today's primary-side counter.

**Single writer → no conflict resolution.** There is exactly one
source of writes, producing a single totally-ordered sequence of
operations. No concurrent conflicting writes, no vector clocks, no
LWW, no CRDTs. Every replica sees the same sequence.

**Writer communicates via coordinator.** The Writer runs **in-cluster**
(in-process embedded, sidecar, or dedicated pod — see
[Writer Process Model](#writer-process-model) for the supported
topologies). It must not depend on direct access to the K8s API or
the operator. Instead, the Writer connects to a **coordinator**
(a lightweight gRPC service running in-cluster) over a single
long-lived bidirectional stream. The coordinator abstracts away
CRD watches and operator interactions.

```
Writer (in-cluster) ──gRPC stream──►  Coordinator (in-cluster)  ──watches──►  CRD/Operator
                    ◄─ config events                             ◄─ updates ──
                    ── status reports ─►                         ── writes ──►
```

The coordinator can be a separate service or an extra gRPC port on
the operator itself.

### Architecture Overview

The proposal described a **new, parallel system** rather than a modification
of the then-existing classic operator. It assumed the two systems could share
low-level concepts while being deployed independently; that coexistence
baseline was later removed.

```
Historical classic baseline          Proposed writer system
─────────────────────────────        ────────────────────────────
kuberic-operator                     kuberic-writer-op
  ├─ Primary election                  ├─ Replica set management
  ├─ Failover / switchover             ├─ Epoch management
  ├─ ChangeRole lifecycle              ├─ Copy source selection
  └─ Write revocation                  └─ Coordinator (gRPC)

kuberic-core                         kuberic-writer (new crate)
  ├─ WalReplicatorActor                ├─ Writer (library)
  ├─ PrimarySender                     ├─ QuorumTracker
  ├─ QuorumTracker                     ├─ LSN counter
  └─ SecondaryReceiver                 └─ Coordinator client

Pod roles: Primary / Secondary       Pod roles: None (all equal)
                                      Writer: library in user app
                                      Coordinator: in-cluster service
```

| Component | Current location | New system location |
|---|---|---|
| **LSN assignment** | `WalReplicatorActor` (primary pod) | Writer (`next_lsn` counter) |
| **Quorum tracking** | `QuorumTracker` (primary pod) | Writer |
| **Fan-out to replicas** | `PrimarySender` (primary pod) | Writer |
| **Op application** | User service on primary | Each replica independently |
| **Replication queue** | `ReplicationQueue` (primary pod) | Each replica (local) |
| **Epoch/fencing** | Operator → primary (`data_loss` + `config` combined) | Split: `data_loss` + `writer_epoch` (fenced) + `config_version` (informational) |
| **Pod roles** | Primary / Secondary | **None** — all replicas equal |

### What the New System Does NOT Need

The following machinery from `kuberic-operator` / `kuberic-core` was
**not present** in the proposed system (the historical baseline contained all
of it):

| Not needed in new system | Exists in `kuberic-core` | Why it existed |
|---|---|---|
| `ChangeRole` lifecycle event | `actor.rs`, `pod.rs` | Transition Primary↔Secondary |
| `AccessStatus::NotPrimary` | `handles.rs:166` | Gate writes on non-primary |
| `PrimarySender` | `primary.rs` | Primary → secondary fan-out |
| `WalReplicatorActor` data path | `actor.rs:312-336` | LSN assignment, quorum, fan-out |
| Correlated `RevokeWriteStatus` action | `replica_agent.rs`, `pod.rs` | Write revocation before switchover (A2) |
| Switchover rollback (A3) | `pod.rs` | Re-promote old primary on failure |
| SF Phase 4 epoch distribution | `pod.rs` | Primary distributes epoch to secondaries after promotion |
| Primary election / failover | `kuberic-operator` | Elect new primary on failure |
| Switchover orchestration | `kuberic-operator` | Controlled primary migration |
| `write_status` / `read_status` | `PartitionState` | Per-pod access gating (4-state enum tied to primary role) |

### Component Design

#### 1. Writer Library (`kuberic-writer`)

The core replication coordinator, packaged as a Rust library that the
user application links and calls directly:

```rust
/// The Writer — replication coordinator. Embedded in the user app.
/// Connects to replicas via gRPC and to the coordinator for
/// discovery and lifecycle events.
pub struct Writer {
    /// Connections to all replicas (gRPC channels)
    replicas: Vec<ReplicaConnection>,
    /// Partition config: epoch, replica set, write quorum
    config: Arc<PartitionConfig>,
    /// Simple monotonic counter (single writer — no contention)
    next_lsn: AtomicI64,
    /// Quorum tracker (reused from kuberic-core)
    quorum: QuorumTracker,
    /// Replication queue — retains ops for replay to new replicas
    replication_queue: ReplicationQueue,
    /// Bidirectional gRPC stream to coordinator
    config_stream: CoordinatorStream,
    /// Assigned by coordinator, stored locally for reconnect identity
    writer_id: String,
    /// Opaque token from coordinator, included in every ReplicationItem
    session_token: Bytes,
}

impl Writer {
    /// Write data to quorum. Returns assigned LSN.
    ///
    /// 1. Assign LSN (next_lsn += 1)
    /// 2. Buffer in ReplicationQueue
    /// 3. Send ReplicationItem to all N replicas in parallel
    /// 4. Wait for W ACKs
    /// 5. Return LSN to caller
    pub async fn replicate(&self, data: Bytes) -> Result<Lsn> { ... }
}
```

**Writer bootstrap:** When a new Writer connects, the coordinator
provides the `authority_lsn` from the most recent authority recovery
(the value pushed to replicas via `update_epoch`). The new Writer
sets `next_lsn = authority_lsn + 1` and starts writing. There is no
need to query replicas — the coordinator already ran authority
recovery before allowing the new Writer to connect.

**LSN assignment is trivial:** Single writer means a plain atomic
counter — `next_lsn.fetch_add(1, Ordering::Relaxed)`. Identical to
today's `next_lsn += 1` in `actor.rs:315`, just moved to the Writer.
Globally sequential, contiguous, no gaps.

#### 2. Coordinator

A lightweight gRPC service running **in-cluster** (as a standalone
service or an extra port on the operator). It bridges the Writer
(in-process, sidecar, or dedicated pod — see
[Writer Process Model](#writer-process-model)) to the K8s control
plane.

```protobuf
service Coordinator {
    // Bidirectional stream: Writer sends connect + status,
    // coordinator pushes config events.
    rpc WriterSession(stream WriterMessage) returns (stream ConfigEvent);
}

message WriterMessage {
    oneof message {
        WriterConnect connect = 1;       // first message on stream open
        WriterPoisoned poisoned = 2;     // Writer poisoned itself (InDoubt)
        WriterShutdown shutdown = 3;     // cooperative graceful shutdown
    }
}

message WriterConnect {
    string writer_id = 1;               // empty on first-ever connect
    string partition_id = 2;            // which partition to write to
    int64 stored_writer_epoch = 3;      // epoch at which writer_id was
                                        // issued; 0 when writer_id is
                                        // empty. Used by the coordinator
                                        // for the epoch-comparison
                                        // anti-flap rule (see
                                        // Interaction with Coordinator
                                        // and Epoch).
    bytes auth_token = 4;               // K8s ServiceAccount token (via
                                        // gRPC metadata or projected
                                        // volume); coordinator validates
                                        // before accepting the session.
                                        // See Anti-Flap Admission Control.
}

message ConfigEvent {
    oneof event {
        ConfigUpdate config_update = 1;     // epoch, replica set, write quorum (CC+PC), authority_lsn
        AddReplica add_replica = 2;         // new replica ready, copy_lsn = X
        RemoveReplica remove_replica = 3;   // replica removed from set
        BuildInProgress build_start = 4;    // defer queue GC for build_id
        BuildComplete build_complete = 5;   // resume queue GC for build_id
        BuildAborted build_aborted = 6;     // build_id aborted (replica failed during build)
        PauseWrites pause_writes = 7;       // Writer must reject replicate() calls
        ResumeWrites resume_writes = 8;     // Writer resumes accepting writes
    }
}

message BuildInProgress {
    string build_id = 1;        // UUID, distinguishes concurrent builds
    string replica_id = 2;      // which replica is being built
    int64 freeze_gc_below = 3;  // Writer must NOT GC ops below this LSN
}
message BuildComplete {
    string build_id = 1;        // matches the BuildInProgress
    string replica_id = 2;
}
message BuildAborted {
    string build_id = 1;
    string replica_id = 2;
    string reason = 3;          // human-readable
}

// Writer responds to BuildInProgress / BuildComplete / BuildAborted
// with a synchronous ACK on the same WriterSession stream:
message WriterEventAck {
    string build_id = 1;        // if applicable
    bool acked = 2;
    string error = 3;           // if !acked
}

message ConfigUpdate {
    Epoch epoch = 1;
    ReplicaSet current_config = 2;        // CC — current members + write quorum
    ReplicaSet previous_config = 3;       // PC — non-empty during reconfiguration
    string writer_id = 4;                 // assigned by coordinator
    bytes session_token = 5;              // opaque token for replica authentication
    int64 authority_lsn = 6;              // starting point for new Writer (next_lsn = authority_lsn + 1)
}

message WriterPoisoned {
    int64 in_doubt_lsn = 1;       // the LSN that triggered InDoubt
    string reason = 2;            // human-readable reason
}

message WriterShutdown {
    int64 highest_lsn = 1;        // last LSN the Writer assigned (for clean handoff)
}
```

Note: The Writer does **not** report `committed_lsn` to the
coordinator. The In-Doubt Writer Contract eliminates the need
for `committed_lsn` tracking — see Section 6 (Crash Recovery) and
the [In-Doubt Writer Contract](#in-doubt-writer-contract) section
for the full rationale. The coordinator computes `authority_lsn`
during authority recovery by reading `received_lsn` from
`FreezeWrites` responses (see [Authority-Based Recovery](#authority-based-recovery-fence-first-then-query));
there is no separate query RPC.

##### Writer Identity and Authentication

The Writer is an external client — the coordinator must know who it
is and whether it's the same Writer reconnecting or a different one.

**Writer ID:** An opaque UUID assigned by the coordinator on first
connect. The Writer stores it locally and presents it on reconnect.

```
First connect:
  Writer → Coordinator:  WriterConnect { writer_id: "" }
  Coordinator:           Assigns writer_id = new UUID
                         Stores in CRD status
                         Sends ConfigUpdate { writer_id, session_token, ... }
  Writer:                Stores writer_id locally

Reconnect (same Writer):
  Writer → Coordinator:  WriterConnect { writer_id: "<stored UUID>",
                                          stored_writer_epoch,
                                          auth_token }
  Coordinator:           Validates auth_token (k8s ServiceAccount).
                         Matches writer_id AND compares
                         stored_writer_epoch with current_writer_epoch
                         in CRD status:
    + clean shutdown + epochs match → Same epoch, resume (no fencing)
    + crash + epochs match          → Run authority recovery
                                      (fence-first-then-query)
    + stored_writer_epoch < current → Reject with WriterRetired{
                                        current_writer_epoch};
                                      Writer must clear stored state
                                      and reconnect with empty writer_id

Takeover (different Writer):
  Writer → Coordinator:  WriterConnect { writer_id: "",
                                          stored_writer_epoch: 0,
                                          auth_token }
  Coordinator:           Validates auth_token. No matching writer_id.
                         Apply Anti-Flap Admission Control (see below)
                         before triggering a new recovery.
                         If admitted: run authority recovery
                         (full protocol below), assign new writer_id at
                         the resulting epoch.
                         Old Writer's epoch becomes stale →
                         old Writer self-poisons on next op.
```

The "authority recovery" referenced here is the full
fence-first-then-query protocol with `recovery_generation` binding,
documented in
[Authority-Based Recovery (Fence-First-Then-Query)](#authority-based-recovery-fence-first-then-query).

**Session token:** On each `ConfigUpdate`, the coordinator issues an
opaque `session_token` (e.g., HMAC of `writer_id + writer_epoch +
timestamp`, signed by the coordinator). The Writer includes this
token in every `ReplicationItem`:

```protobuf
message ReplicationItem {
    int64 data_loss_number = 1;
    int64 writer_epoch = 2;
    int64 lsn = 3;
    bytes data = 4;
    bytes session_token = 5;      // proves Writer identity to replicas
}
```

Note: `committed_lsn` is **not** included on `ReplicationItem`.
Under the In-Doubt Writer Contract, replicas only track
`received_lsn` and the coordinator runs authority recovery on
Writer takeover. See Section 6 (Crash Recovery).

Replicas receive the valid `session_token` from the coordinator via
`update_epoch()` (the coordinator is in-cluster, trusted). On each
incoming `ReplicationItem`, the replica validates:
1. Epoch check: `(data_loss_number, writer_epoch) >= current` (ordering)
2. Token check: `session_token == expected_token` (identity)

A rogue client without a valid session token is rejected even if it
guesses the correct epoch values. The token rotates on every
`writer_epoch` bump, so a stolen token from an old Writer is
invalidated when the epoch changes.

**How it works:**

1. Writer opens `WriterSession` stream and sends `WriterConnect`
   with its stored `writer_id` (or empty on first connect).
2. Coordinator validates identity (see above), assigns/confirms
   `writer_id`, generates `session_token`, pushes `ConfigUpdate`
   with `authority_lsn`.
3. Writer stores `writer_id`, sets `next_lsn = authority_lsn + 1`,
   connects to all replicas, includes `session_token` in every
   `ReplicationItem`.
4. When the operator changes config (epoch bump, add/remove replica,
   build), the coordinator pushes the relevant `ConfigEvent`.
5. Writer reacts: updates its connection pool, replays from queue for
   new replicas, adjusts quorum tracker.
6. **On `InDoubt`**: Writer sends `WriterPoisoned` to coordinator,
   refuses subsequent ops with `WriterPoisoned` error.
7. **Liveness:** If the stream disconnects without a prior
   `WriterShutdown` or `WriterPoisoned`, the coordinator treats it
   as a crash (see Writer Lifecycle below).

##### Writer Lifecycle Protocol

The coordinator distinguishes three Writer restart scenarios based
on the last message received before disconnect:

**Graceful shutdown → clean restart (no fencing):**

```
Writer                              Coordinator
──────                              ───────────
1. Drain in-flight ops
   (wait for pending replicate() to complete)
2. Send WriterShutdown{highest_lsn=X}  ──►
                                    3. Record: "clean shutdown at LSN X"
                                       No epoch bump, no update_epoch to replicas
4. Close stream, exit process

... later ...

5. Writer reconnects  ──────────────►
                                    6. Last shutdown was clean
                                       Send ConfigUpdate with SAME writer_epoch,
                                       authority_lsn = highest_lsn (from shutdown)
7. Writer resumes at authority_lsn + 1
```

**Crash or InDoubt poison → fenced restart (authority recovery):**

```
Writer                              Coordinator
──────                              ───────────
1. Process crashes / network dies / InDoubt poison
   (no WriterShutdown sent; possibly WriterPoisoned sent)
                                    2. Stream disconnect or WriterPoisoned received
                                       → treat as crash/poison
                                       → run Authority Recovery Protocol:
                                         a. Bump recovery_generation + epoch in CRD
                                         b. Push FreezeWrites to ALL replicas
                                            (need ≥W ACKs to proceed)
                                         c. fenced_quorum = replicas that ACKed
                                         d. authority_lsn = max(received_lsn
                                            from fenced_quorum responses)
                                         e. Push UpdateEpoch{generation, epoch,
                                            authority_lsn} to all replicas
                                         f. Per-replica reconciliation for
                                            replicas outside fenced_quorum or
                                            lagging behind authority_lsn

... later ...

3. New Writer reconnects  ──────────►
                                    4. Send ConfigUpdate with NEW writer_epoch,
                                       NEW recovery_generation,
                                       authority_lsn (from step 2.d)
5. Writer starts at authority_lsn + 1 with new epoch
```

See [Authority-Based Recovery (Fence-First-Then-Query)](#authority-based-recovery-fence-first-then-query)
for the full protocol with `recovery_generation` binding,
fenced-quorum-as-authority-quorum, and per-replica reconciliation.

**Takeover (different Writer connects while old is alive):**

Same as crash — the coordinator cannot prove the old Writer is dead,
so it runs authority recovery and bumps `writer_epoch`. The old
Writer's stale writes are rejected by replicas that have received
the epoch bump (which causes the old Writer to detect epoch
staleness and poison itself).

**Why no lease?** The cooperative protocol handles the graceful case
(no unnecessary fencing), and crash fencing (authority recovery +
epoch bump) handles the crash case identically to the existing
leader-based system's failover model. A lease would add recovery
latency (must wait for expiry) without meaningful safety improvement,
since authority recovery already preserves all surviving ops.

##### Reconfiguration via Coordinator

Adding or removing a replica follows the same three-step
configuration dance as today, but driven by `ConfigEvent`s instead
of direct RPC calls on the primary:

```
                                              Writer
                                              ──────
1. Operator completes copy, tells coordinator
   "add replica 3 at copy_lsn=50"

   Coordinator → Writer:  AddReplica(id=3, addr, copy_lsn=50)
                                              Writer connects to replica 3
                                              Replays queue from LSN 51
                                              Starts live send_to_all

2. Operator enters dual-config

   Coordinator → Writer:  ConfigUpdate(
                            CC={1,2,3} quorum=2,
                            PC={1,2} quorum=2,
                            must_catch_up={3})
                                              Writer enters dual-config:
                                              ops must be ACKed by quorum
                                              from BOTH CC and PC

   Writer → Coordinator:  caught_up=false (signaled via stream)
                          ...
   Writer → Coordinator:  caught_up=true
                                              Replica 3 has ACKed all ops

3. Operator finalizes

   Coordinator → Writer:  ConfigUpdate(
                            CC={1,2,3} quorum=2,
                            PC=∅)
                                              Writer exits dual-config
                                              GC queue up to min_replica_received_lsn
```

This is identical to today's `update_catch_up_configuration` →
`wait_for_catch_up_quorum` → `update_current_configuration` sequence,
just expressed as stream events instead of RPCs.

##### Write Gating

In the current system, `AccessStatus` is a 4-state enum tied to
primary role transitions:

| Status | Meaning |
|---|---|
| `Granted` | Primary, writes allowed |
| `NotPrimary` | Not the primary — reject |
| `NoWriteQuorum` | Can't reach enough secondaries |
| `ReconfigurationPending` | Mid-reconfiguration pause |

In the Writer-based system, most of these don't apply:

- **`NotPrimary`** — irrelevant (no primary role).
- **`NoWriteQuorum`** — happens naturally. If the Writer can't get W
  ACKs, `replicate()` times out / fails. No explicit status needed.
- **`ReconfigurationPending`** — the Writer continues writing during
  reconfiguration. Dual-config quorum ensures both old and new
  configurations are satisfied. No pause needed.

The only scenario requiring an explicit write pause is **data loss
recovery**, where the operator needs to freeze all replica state for
a consistent rebuild:

| Scenario | Write pause? | Why |
|---|---|---|
| Replica added | No | Writer continues, queue buffers gap ops |
| Replica removed | No | Writer drops connection, continues |
| Writer failure | No | Old Writer is dead, epoch fences stale writes |
| Normal quorum loss | No | `replicate()` naturally fails (can't get W ACKs) |
| **Data loss recovery** | **Yes** | Must freeze state for consistent rebuild from canonical |

The coordinator sends `PauseWrites` before data loss recovery begins
and `ResumeWrites` after the rebuild completes. The Writer rejects
all `replicate()` calls with an error while paused.

```
Data loss recovery:
  Coordinator → Writer:  PauseWrites
                         Writer rejects replicate() calls
  Operator rebuilds all replicas from canonical survivor
  Coordinator → Writer:  ResumeWrites(new epoch, new replica set)
                         Writer resumes with new data_loss_number
```

This replaces the entire `AccessStatus` enum with a simple boolean.

**Why not direct K8s API?** The Writer is in-cluster but may run as
a library inside the user's application pod, where K8s credentials
and CRD access would couple every user-app deployment to operator-
level RBAC. The coordinator is the single in-cluster contact point
— one gRPC endpoint to reach, no K8s permissions needed by the user
app.

#### 3. Replica Pod (Roleless)

All pods run identical code. No Primary/Secondary role assignment,
no `ChangeRole` events.

```protobuf
service ReplicaData {
    // Writer sends items, replica ACKs each one
    rpc Write(stream ReplicationItem) returns (stream ReplicationAck);

    // Copy protocol: operator designates a source replica
    rpc GetCopyContext(GetCopyContextRequest) returns (GetCopyContextResponse);
    rpc CopyStream(stream CopyItem) returns (CopyStreamResponse);
}
```

Each replica:
- Accepts `ReplicationItem` (containing epoch + LSN + data +
  session_token)
- Validates epoch (fencing: rejects stale `writer_epoch` or
  `data_loss_number`)
- **Validates session token** (identity: rejects items without a
  valid token matching the one received from coordinator via
  `update_epoch`)
- Stores in local `ReplicationQueue` / WAL
- Applies op to user state via `StateProvider`
- Sends `ReplicationAck` back to the Writer

The existing `SecondaryReceiver::accept_item()` epoch validation and
op storage logic (`secondary.rs:95-124`) is reused almost verbatim —
it already does exactly what a roleless replica needs.

#### 4. Epoch Design

##### Current Epoch (Leader-Based)

Today's epoch has two components, both fencing via a single comparison:

```rust
pub struct Epoch {
    pub data_loss_number: i64,       // quorum loss events
    pub configuration_number: i64,   // every reconfiguration
}
```

`configuration_number` is bumped on every change: failover, switchover,
add replica, remove replica. It fences the old primary after failover.
But it also gets bumped for replica membership changes that don't change
the writer — creating unnecessary write stalls.

##### New Epoch (Writer-Based)

The Writer-based system splits the epoch into three independent
numbers, because three independent concerns exist:

```rust
pub struct Epoch {
    /// Incremented when the operator declares data loss (quorum loss
    /// recovery with fewer than W surviving replicas). Signals that
    /// previously committed operations may be gone.
    pub data_loss_number: i64,

    /// Incremented when the Writer identity changes (Writer failure
    /// and takeover). Fences stale Writer's in-flight writes.
    pub writer_epoch: i64,

    /// Incremented when the replica set membership changes (add,
    /// remove, rebuild). NOT checked on writes — informational
    /// for the Writer to update its connection pool.
    pub config_version: i64,
}
```

| Number | Bumped when | Fences what | Checked by replicas on write? |
|---|---|---|---|
| `data_loss_number` | Operator declares data loss | All prior epochs | **Yes** — supersedes everything |
| `writer_epoch` | Writer dies, new Writer takes over | Stale Writer's writes | **Yes** — rejects old Writer |
| `config_version` | Replica added / removed / rebuilt | Nothing (informational) | **No** — Writer uses it to update pool |

**Replica fencing check:** Replicas compare incoming
`(data_loss_number, writer_epoch)` against their current values.
An item is rejected if either component is stale:

```rust
fn accept_item(&self, item: &ReplicationItem) -> Result<(), Status> {
    let current = self.current_epoch.lock().unwrap();
    let item_epoch = (item.data_loss_number, item.writer_epoch);
    let my_epoch = (current.data_loss_number, current.writer_epoch);

    if item_epoch < my_epoch {
        return Err(Status::failed_precondition("stale epoch"));
    }

    // Identity check: session token must match (received from coordinator)
    if item.session_token != self.expected_session_token {
        return Err(Status::unauthenticated("invalid session token"));
    }

    // ... store op
}
```

`config_version` is not part of this check — a replica addition
should not stall writes from the existing Writer.

##### When Each Number Changes

**Writer failure → `writer_epoch` bumped + authority recovery:**

```
1. Coordinator detects stream disconnect or WriterPoisoned
2. Coordinator advances recovery_generation + bumps writer_epoch in CRD
3. Coordinator pushes FreezeWrites{generation, new_epoch} to ALL replicas;
   waits for ≥W ACKs (= fenced_quorum). Each ACK carries received_lsn.
4. authority_lsn = max(received_lsn from fenced_quorum responses only)
5. Coordinator pushes UpdateEpoch{generation, new_epoch, authority_lsn}
   to all replicas (atomic truncate-above on receipt)
6. Per-replica reconciliation for unfenced/lagging replicas (see
   Authority-Based Recovery section)
7. New Writer connects, receives ConfigUpdate(epoch, generation,
   authority_lsn), starts at authority_lsn + 1
8. Old Writer's late-arriving writes → rejected with StaleEpoch
   → triggers self-poison via is_epoch_stale()
```

See [Authority-Based Recovery](#authority-based-recovery-fence-first-then-query)
for the full protocol including `recovery_generation` binding and
fenced-quorum-as-authority-quorum semantics.

**Replica added → `config_version` bumped:**

```
1. Operator completes copy to new replica
2. Operator bumps config_version
3. Coordinator pushes ConfigUpdate (new replica set) to Writer
4. Writer adds new replica to its connection pool, replays from queue
5. NO epoch push to existing replicas — no write stall
```

**Replica removed → `config_version` bumped:**

```
1. Operator removes replica from set
2. Operator bumps config_version
3. Coordinator pushes ConfigUpdate to Writer
4. Writer removes replica from its pool
5. NO epoch push to existing replicas — no write stall
```

**Quorum loss → `data_loss_number` bumped:**

```
1. Multiple replicas die, fewer than W remain
2. Operator waits (quorum loss timeout)
3. If quorum doesn't recover, operator drops dead replicas and
   declares data loss
4. Operator bumps data_loss_number (supersedes all writer_epochs)
5. Operator pushes new epoch to surviving replicas
6. Surviving replicas receive on_data_loss() callback
   → state provider can restore from backup or accept the gap
7. Coordinator pushes new epoch to Writer (or new Writer connects)
8. Writes resume from whatever LSN the survivors have
```

##### Proto Changes

The `ReplicationItem` gains a `writer_epoch` field. `config_version`
is not included in items — it's only in `ConfigEvent` messages
between coordinator and Writer.

```protobuf
message ReplicationItem {
    int64 data_loss_number = 1;   // was epoch_data_loss
    int64 writer_epoch = 2;       // was epoch_config (repurposed)
    int64 lsn = 3;
    bytes data = 4;
    bytes session_token = 5;      // identity proof
    // committed_lsn NOT included — not tracked under In-Doubt contract
    // config_version NOT included — not checked on writes
}
```

##### Data Loss Recovery

Data loss occurs when fewer than W replicas survive — the quorum
contract is broken, and previously committed operations may be gone
from the surviving replicas.

**Leader-based (today):** The operator promotes the best surviving
secondary as the new primary and calls `on_data_loss()` on that one
replica. The new primary becomes the canonical state, and all other
replicas are rebuilt from it via the copy protocol. Only one replica
handles the data loss callback.

**Writer-based:** There is no primary to promote, but the recovery
follows the same pattern — one survivor becomes the canonical state:

```
Before:  Replica₁ (received_lsn=100)  Replica₂ (received_lsn=98)  Replica₃ (dead)

1. Quorum lost (only 2 of 3 replicas alive, but the lost replica
   may have had ops the surviving two don't)

2. Operator waits for quorum loss timeout

3. Operator picks survivor with highest received_lsn → Replica₁
   (has the most complete state)

4. Operator calls on_data_loss() on Replica₁ ONLY
   → Replica₁'s state provider can:
     a. Accept the gap (do nothing — most common)
     b. Restore from external source (backup, another service)
     c. Report StateChanged if it modified state

5. Operator bumps data_loss_number

6. Operator rebuilds ALL other replicas from Replica₁:
   - Replica₂ → closed, rebuilt via copy protocol from Replica₁
   - Replica₃ → new pod, built via copy protocol from Replica₁
   (same as adding new replicas — Replica₁ is the copy source)

7. After rebuild, operator pushes new epoch (with bumped
   data_loss_number) to all replicas

8. Coordinator pushes new epoch to Writer (or new Writer connects)

9. Writer resumes with new data_loss_number, new replica set,
   LSN continues from Replica₁'s highest LSN

After:   Replica₁ (canonical, LSN=100)
         Replica₂ (rebuilt from Replica₁, LSN=100)
         Replica₃ (rebuilt from Replica₁, LSN=100)
         Writer starts at next_lsn=101
```

**Key points:**

- **Only one replica gets `on_data_loss()`** — the most-complete
  survivor, chosen by highest LSN. Same as the leader-based system.
- **All other survivors are rebuilt** from the canonical replica via
  the normal copy protocol. This ensures all replicas converge to
  the same state. A survivor at LSN=98 when the canonical is at
  LSN=100 could have divergent ops (ops the canonical saw but this
  one didn't, or vice versa). Rebuilding eliminates this ambiguity.
- **No external restore required by default.** The most common
  `on_data_loss()` implementation is "accept the gap" — return
  `StateChanged = false`. The surviving data is the new truth.
- **External restore is optional.** If the user's state provider has
  access to backups or an external source, `on_data_loss()` is the
  hook to restore from it. This is application-specific.
- **The `StateProvider` trait is unchanged** — the same
  `on_data_loss() → bool (state_changed)` callback works identically
  in both systems.

#### 5. Quorum Tracking in the Writer

The Writer embeds the same `QuorumTracker` logic from
`kuberic-core/src/replicator/quorum.rs`:

- `register(lsn, reply)` — register pending op
- `ack(lsn, replica_id)` — record ACK from a replica
- `is_quorum_met()` — check if W replicas have ACKed

**No self-ACK.** Unlike the leader-based system where the primary
counts its own local persist as one ACK (`quorum.rs:131-132`), the
Writer does NOT self-ACK — it holds no data. It waits for W out of N
**replica** ACKs. For N=3, W=2: any 2 replicas ACKing is sufficient.
This means W must be set with the understanding that all ACKs come
from replicas (the write quorum is not inflated by a phantom
self-ACK).

#### 6. Crash Recovery: The In-Doubt Writer Contract

Crash recovery is the central design challenge of separating the
Writer from the replicas. With the former leader-based design,
the primary IS a replica — its local state is authoritative on
failover, and no external tracking is needed. With a separated
Writer, the question becomes: when the Writer crashes or is fenced
mid-operation, how do replicas converge on a consistent state without
violating response consistency to clients?

The naive approach — having the Writer piggyback `committed_lsn` on
each `ReplicationItem` — fails because `committed_lsn` lags by one
write: if the Writer commits LSN=N and crashes before sending
LSN=N+1, no replica knows LSN=N was committed. Variants that send
`committed_lsn` to the coordinator either preserve a microsecond
correctness window (fire-and-forget) or sacrifice the latency
benefit (acknowledged + etcd persistence).

**Resolution: the In-Doubt Writer Contract + fence-first authority
recovery.** The Writer's `replicate()` API returns explicit failure
modes:

```rust
pub enum ReplicateError {
    PreSendFailure(ValidationError),    // No replica saw the op
    InDoubt { lsn, partial_acks },      // Op may persist; Writer poisoned
    WriterPoisoned,                     // Writer no longer accepts ops
}
```

The Writer **never** returns a "definitively failed" error after
sending to replicas. `InDoubt` makes ambiguity explicit in the
type system; the recommended caller pattern is "treat InDoubt as
fatal application error and restart" (see Caller Contract for
rationale).

**Fence-first-then-query authority recovery.** When the coordinator
detects Writer poison or disconnect, it does NOT immediately query
replicas for `received_lsn`. A naive query-first ordering races
with the old Writer's in-flight `Ok` returns. Instead:

```
1. Coordinator advances recovery_generation + bumps epoch in CRD
2. Coordinator pushes FreezeWrites{generation, new_epoch} to ALL
   replicas (synchronous; ≥W ACKs required = fenced quorum).
   FreezeWrites response carries the replica's received_lsn.
   → old Writer's in-flight ACKs fail with stale-epoch
   → old Writer self-poisons via is_epoch_stale()
3. authority_lsn = (W-1)-th highest received_lsn from fenced quorum
   (catch-up quorum: drops single-replica tails which are not
   quorum-confirmed; safe under the In-Doubt contract)
4. Coordinator pushes UpdateEpoch{generation, new_epoch,
   authority_lsn} (replicas atomically truncate ops > authority_lsn)
5. Per-replica reconciliation for unfenced/lagging replicas
   (forced UpdateEpoch + rebuild on reconnect)
6. New Writer connects, starts at authority_lsn + 1
```

This mirrors Service Fabric's `revoke_write_status` (Phase 0) and
Kuberic's durable failover fencing pattern. The two
mechanisms (in-doubt contract + fence-first recovery) together
provide response consistency without per-commit external state.

**Cost:** failover takes 2 RTT (rare path); steady-state writes
remain 1 RTT. No `committed_lsn` tracking, no per-commit CRD writes.

For the full design — InDoubt firing conditions, single-op-in-flight
serialization, replica durability contract, replica order acceptance,
caller contract with restart pattern, edge case verification, and
the full fence-first-then-query protocol — see the
[In-Doubt Writer Contract](#in-doubt-writer-contract) section later
in this document.

**Implications for `ReplicationItem`:** The Writer no longer needs
to piggyback `committed_lsn` on items. Replicas track `received_lsn`
only (defined as **highest contiguous LSN** — gaps are forbidden,
enforced by replica order acceptance).

#### 7. Replica Building (Copy + Catch-Up)

Replica building is the most nuanced part of the Writer-based system
because the responsibilities that today live on a single primary are
now split across two actors:

- **Copy source replica** — has the data (`StateProvider`), produces
  the state snapshot, streams it to the new replica.
- **Writer** — has the `ReplicationQueue` with ops written during the
  copy window, handles the replay gap and live replication.

##### How It Works Today (Leader-Based)

In the current system, the primary does everything
(`replicator/copy.rs`, `replicator/actor.rs:257-275`,
`replicator/primary.rs:62-140`):

```
[0 ─────── copy_lsn]              Copy stream (StateProvider snapshot)
           (copy_lsn ── highest]   Replay from ReplicationQueue
                       (highest ── ∞)  Live replication (send_to_all)
```

1. Operator tells primary: `build_replica(new_secondary_info)`
2. Primary spawns copy task (`copy.rs:18`):
   a. Connects to new secondary's gRPC data plane
   b. `GetCopyContext` → secondary returns its current context
   c. `GetCopyState(up_to_lsn=committed_lsn, copy_context)` →
      primary's StateProvider produces snapshot
   d. `CopyStream` → primary pushes all state items to secondary
   e. Records `copy_lsn = max(snapshot LSNs)` via
      `state.set_copy_lsn(replica_id, copy_lsn)`
3. Operator promotes secondary: `change_role(ActiveSecondary)`
4. Operator calls `update_catch_up_configuration` on primary
5. Primary actor (`actor.rs:136-221`):
   a. `add_secondary(id, addr)` → opens gRPC replication stream,
      spawns ACK reader + drain tasks
   b. `take_copy_lsn(replica_id)` → retrieves boundary
   c. `replication_queue.ops_from(copy_lsn + 1)` → gets gap ops
   d. `send_to_one()` for each gap op → replays to new secondary
   e. From now on, `send_to_all()` includes the new secondary
6. Operator calls `wait_for_catch_up_quorum(Write)`
7. Operator calls `update_current_configuration` → finalizes quorum

**Critical invariant:** The three ranges `[0, copy_lsn]`,
`(copy_lsn, highest_lsn]`, `(highest_lsn, ∞)` are gapless because
the primary is both the copy source AND the replication queue holder.
During the copy window, new `replicate()` calls keep arriving and
buffering in the queue. The `copy_lsn` is recorded atomically with
the snapshot. No ops are lost.

##### How It Works in the Writer-Based System

The copy source and the queue holder are **separate processes**. The
`copy_lsn` must be communicated between them.

```
Operator              Coordinator       Copy Source Replica     New Replica           Writer
────────              ────────────       ────────────────────    ───────────           ──────
                                                                                      (continuously accepting
                                                                                       replicate() calls,
                                                                                       buffering in queue)

1. Pick copy source                                                                   
   (highest received_lsn)
                      ◄── BuildInProgress ──────────────────────────────────── push ──►
                                                                                      (defers queue GC)
2. Tell copy source:
   build to new ──────────────────────►
                                       3. Connect to new replica's gRPC
                                       4. GetCopyContext ──────► Returns context
                                       5. GetCopyState
                                          (local StateProvider
                                           snapshot up to
                                           received_lsn)
                                       6. CopyStream ─────────► Receives full state
                                                                 via copy_stream
                                          copy_lsn = max(snapshot LSNs)
◄── done, copy_lsn ───────────────────

7. Tell Writer via
   coordinator:
   add replica,       ── AddReplica ─────────────────────────────────────────── push ──►
   copy_lsn = X                                                                       8. Record copy_lsn
                                                                                       9. Connect to new
                                                                                          replica (Write gRPC)
                                                                                       10. Replay queue from
                                                                                           copy_lsn + 1 via
                                                                                           send_to_one()
                                                                 ◄── replayed ops ────
                                                                 ◄── live ops ──────── 11. send_to_all() now
                                                                                            includes new
                                                                 ──── ACKs ───────────►

                      ◄── BuildComplete ─────────────────────────────────────── push ──►
                                                                                       (resumes queue GC)
12. Quorum catch-up    ── ConfigUpdate ──────────────────────────────────────── push ──►
    (update_catch_up)                                                                  (enters dual-config)
13. Wait for catch-up  ◄── caught_up=true (signaled via stream) ──────────────────────
14. Finalize config    ── ConfigUpdate ──────────────────────────────────────── push ──►
                                                                                       15. replication_queue.gc()
```

##### Preserving the Zero-Gap Invariant

The three ranges remain gapless:

```
[0 ─────── copy_lsn]              Copy source replica's snapshot
           (copy_lsn ── highest]   Writer's ReplicationQueue replay
                       (highest ── ∞)  Writer's live send_to_all()
```

This works because:

1. **The Writer holds all ops.** Every `replicate()` call goes through
   the Writer, which buffers in its `ReplicationQueue`. The queue
   contains all ops from the beginning (or from the last GC point).

2. **The copy source snapshots at its `received_lsn`.** The copy
   source replica has all ops up to `received_lsn` and applies them
   to user state. Its `StateProvider` produces a snapshot through
   this LSN. The highest LSN in the snapshot is `copy_lsn`.

3. **`copy_lsn ≤ Writer's highest_lsn`.** The copy source's
   `received_lsn` cannot exceed any LSN the Writer has assigned.
   Since the Writer's queue retains all ops from before the build
   started, the replay range `(copy_lsn, highest_lsn]` is fully
   covered by the queue.

4. **The Writer has not GC'd past `copy_lsn`.** The Writer must not
   GC its queue below `copy_lsn` while a build is in progress.
   **Critical synchronization protocol:**
   1. Coordinator generates `build_id = UUID` for this build.
   2. Coordinator sends `BuildInProgress{build_id, replica_id,
      freeze_gc_below}` to the Writer over the WriterSession stream.
   3. Coordinator **waits for `WriterEventAck{build_id, acked: true}`**
      before authorizing the copy source to start. This synchronous
      ACK proves the Writer has frozen its GC boundary.
   4. Writer adds `build_id` to its set of in-progress builds; queue
      GC is bounded by `min(freeze_gc_below across in-progress builds)`.
   5. Coordinator sends `BuildReplica{build_id, ...}` to the copy
      source replica. The copy source captures its snapshot.
   6. After copy + catch-up + finalization, coordinator sends
      `BuildComplete{build_id, replica_id}` to the Writer; Writer
      moves `build_id` from in-progress set into a **terminated set**
      (a tombstone-bounded LRU cache, default size 1024 most-recent
      build_ids).
   7. If the build fails (replica died, copy timeout), coordinator
      sends `BuildAborted{build_id, replica_id, reason}`; Writer
      moves `build_id` to the terminated set; partial new-replica
      state is discarded.
   8. **Tombstone semantics**: any subsequent `BuildInProgress`
      with a `build_id` that appears in the terminated set is
      rejected with `WriterEventAck{acked: false, error:
      "build_id terminated"}`. This prevents the late-duplicate
      attack: a delayed `BuildInProgress` arriving after `BuildAborted`
      cannot resurrect the defunct build_id and re-freeze the GC
      boundary indefinitely.
   9. The terminated set's LRU eviction is safe because a
      coordinator that wants to reuse a `build_id` would generate
      a fresh UUID (non-colliding) — terminated build_ids are only
      used to detect stale duplicates, never to re-add live builds.

   Without the synchronous ACK, the Writer could miss the
   `BuildInProgress` (network drop), continue GC normally, and the
   new replica would see a gap in the catch-up range — same C0
   bug the existing in-process system avoids.

   GC resumes for a `build_id` when its `BuildComplete` or
   `BuildAborted` arrives. Concurrent builds (multiple new replicas
   joining at once) are supported via the `build_id` set.

   **Writer poison during build:** If the Writer poisons while
   builds are in progress, the coordinator sends `BuildAborted`
   for each `build_id` to the affected copy source replicas. The
   new Writer (after authority recovery) restarts any pending
   builds with fresh `build_id`s.

##### Difference from Today

| Aspect | Leader-based (today) | Writer-based |
|---|---|---|
| Copy source | Primary (implicit) | Any replica (operator picks by highest `received_lsn`) |
| Queue holder | Primary | Writer |
| `copy_lsn` transfer | In-process (`state.set_copy_lsn`) | Cross-process (operator relays) |
| Concurrent writes during copy | Primary buffers in queue | Writer buffers in queue (same) |
| Replay gap ops | Primary's actor `send_to_one` | Writer's `send_to_one` |
| Live replication after build | Primary's `send_to_all` | Writer's `send_to_all` |
| GC coordination | Implicit (primary knows) | Explicit (operator signals build-in-progress) |

##### StateProvider Callbacks for Copy

Each replica still implements the same `StateProvider` callbacks:

- **`GetCopyContext`** — the new replica returns its current state
  context (what it already has, typically empty for a fresh replica).
  Called by the copy source via gRPC.
- **`GetCopyState`** — the copy source replica receives `up_to_lsn`
  + `copy_context`, produces a state snapshot. Called by the copy
  source's own build task.
- **Copy stream** — the new replica receives state items via
  `CopyStream` RPC and applies them via its `OperationStream`
  (same as today's `copy_op_tx` → `copy_stream`).

The copy source replica needs the ability to run `build_replica`
(today only the primary can). This means the `run_build_replica_copy`
logic moves from a primary-only code path to a general replica
capability — any replica can serve as copy source when asked by the
operator.

#### 8. New Operator: `kuberic-writer-op`

A new, lightweight operator — separate from `kuberic-operator` — that
includes the coordinator as a built-in gRPC endpoint:

| Responsibility | `kuberic-operator` (existing) | `kuberic-writer-op` (new) |
|---|---|---|
| Replica set membership | ✓ | ✓ |
| Epoch management | ✓ | ✓ |
| Primary election | ✓ | — |
| Failover orchestration | ✓ | — |
| Switchover orchestration | ✓ | — |
| Write revocation (A2) | ✓ | — |
| Switchover rollback (A3) | ✓ | — |
| Authority recovery on Writer takeover | — | ✓ (new) |
| Copy source selection | Implicit (primary) | Explicit (pick replica with highest `received_lsn`) |
| Config distribution | To primary → secondaries | Coordinator → Writer (stream) |
| Writer liveness | N/A | Coordinator detects stream disconnect / WriterPoisoned |
| CRD | `KubericPartition` (existing) | `KubericWriterPartition` (new CRD) |

**Failure handling:** When a replica pod dies, `kuberic-writer-op`
removes it from the replica set, bumps the epoch, and pushes
`ConfigUpdate` + `RemoveReplica` to the Writer via the coordinator.
No failover, no election, no promotion. If W replicas are still
reachable, writes continue uninterrupted.

**Writer failure:** When the coordinator detects the Writer's stream
disconnect, the operator marks the partition as writer-less. A new
Writer instance can connect and receive the current config. The
operator bumps the epoch to fence any stale writes from the old
Writer.

#### 9. Simplified StateProvider Trait

The user-facing `StateProvider` trait drops role-related callbacks but
retains copy callbacks (every replica can be a copy source):

```rust
// Today's lifecycle events:
enum LifecycleEvent {
    Open { ... },
    ChangeRole { new_role: Role, ... },  // ← REMOVED
    CloseReplica,
    Abort,
}

// Writer-based model:
enum LifecycleEvent {
    Open { ... },
    CloseReplica,
    Abort,
}

// StateProvider events (retained — any replica may serve as copy source):
enum StateProviderEvent {
    UpdateEpoch { ... },
    GetLastCommittedLsn { ... },
    GetCopyContext { ... },       // new replica returns its context
    GetCopyState { ... },         // copy source produces snapshot
    OnDataLoss { ... },
}
```

No `Role` enum needed. No `ChangeRole` transitions. Each replica
opens, accepts writes, and closes. The `OperationStream` for applying
ops works identically — replicas receive ops and acknowledge them.

---

## Proposed System vs. Historical Baseline

The proposal assumed the classic operator/runtime would remain unchanged while
the writer system was developed alongside it. Both the coexistence assumption
and classic implementation are now historical.

### New Components (to build)

| Component | Crate | Purpose |
|---|---|---|
| `Writer` | `kuberic-writer` (new) | Replication coordinator library (embedded in user app) |
| `CoordinatorClient` | `kuberic-writer` (new) | Writer-side coordinator stream client |
| `QuorumTracker` | Shared (extracted) | Reused from `kuberic-core`, made a shared dep |
| `ReplicaWriteServer` | `kuberic-writer-runtime` (new) | Roleless pod runtime accepting writes |
| `Coordinator` | `kuberic-writer-op` (new) | gRPC endpoint on operator for Writer communication |
| `kuberic-writer-op` | `kuberic-writer-op` (new) | Operator + coordinator (membership, epochs, builds) |
| `KubericWriterPartition` | CRD | New CRD for writer-based partitions |

### Not Present in New System

| Existing component | Why not needed |
|---|---|
| `PrimarySender` | Writer does fan-out |
| `WalReplicatorActor` data path | Writer assigns LSN, tracks quorum |
| `ChangeRole` lifecycle | No roles |
| `revoke_write_status()` | No primary to revoke from |
| Switchover / failover logic | No primary to fail over |
| `AccessStatus` gating | All replicas accept writes |
| Primary election | No primary to elect |

### Proposed Reuse from the Historical Baseline

| Component | How reused |
|---|---|
| `ReplicationItem` / `ReplicationAck` proto | Same message format |
| `QuorumTracker` logic | Extracted to shared crate |
| Epoch fencing logic | Same `accept_item` validation; split into `writer_epoch` + `data_loss_number` |
| Copy protocol (gRPC) | Same RPCs; any replica can be copy source |
| `run_build_replica_copy` | Same logic, generalized from primary-only to any replica |
| `ReplicationQueue` | Writer holds the queue; each replica has local storage |
| `StateProvider` trait | Open/Close/Abort + copy callbacks (minus ChangeRole) |
| `OperationStream` | Same op delivery + acknowledge pattern |

---

## Latency Analysis

### Current: 2 Round-Trips

```
t=0    Client sends to Primary
t=RTT₁ Primary receives, assigns LSN, sends to Secondaries
t=RTT₁+RTT₂  Fastest secondary ACKs → quorum met
t=RTT₁+RTT₂  Primary sends LSN reply to Client
t=2·RTT₁+RTT₂  Client receives LSN

Total ≈ 2·RTT(client↔pod) + RTT(pod↔pod)
```

Assuming RTT(client↔pod) ≈ RTT(pod↔pod) ≈ R:
**Total ≈ 3R** (best case), **4R** (if secondaries are in different
racks).

### Proposed: 1 Round-Trip (in-cluster only)

```
t=0    Writer sends to all N replicas in parallel
t=max(RTT₁..RTTw)  W replicas ACK → quorum met

Total ≈ max(RTT to W-th fastest replica) ≈ R
```

For N=3, W=2: **Total ≈ RTT to 2nd-fastest replica ≈ R** (same
cluster) or **~1.5R** (cross-rack).

### Per-Topology Latency

The "1 RTT" claim only holds for **in-cluster Writer + in-cluster
replicas** topology, which is the only supported scope (see Scope
at the top of this document).

| Topology | Historical leader-based baseline | Writer-based (in-doubt) | Win? |
|---|---|---|---|
| In-cluster Writer + in-cluster replicas | 2 RTT (intra-cluster, ~3R) | 1 RTT (intra-cluster, ~R) | ✅ ~50-67% reduction |
| In-cluster Writer + cross-AZ replicas | 2 RTT (1 intra, 1 cross-AZ) | 1 RTT (slowest of W cross-AZ) | ✅ ~30-50% reduction (depends on AZ count and replica placement) |
| ~~Out-of-cluster Writer (cross-region, laptop, CI)~~ | — | — | **Out of scope** — see Scope. The latency benefit does not hold across WAN; failure-domain coupling becomes problematic. |

**Out-of-cluster Writer was out of scope.** The original proposal referred
those workloads to the then-existing leader-based system or required an
in-cluster Writer. That leader-based implementation has since been removed.

Note: The coordinator stream is long-lived and carries only
infrequent config events — it adds no latency to the write path.

### Throughput Limits

The In-Doubt Writer Contract requires single-op-in-flight
serialization (see [Single-Op-In-Flight Serialization](#single-op-in-flight-serialization)).
Each `replicate()` call is processed strictly in order; the next
LSN cannot be assigned until the current op resolves.

**Per-partition throughput cap: ~1 / RTT.** At a typical 1ms
intra-cluster RTT, this is ~1000 ops/sec.

For workloads requiring higher throughput, partition the data
across multiple `KubericWriterPartition` instances. The single-op-
in-flight invariant is per partition, not per cluster.

If a workload requires both high throughput and the latency
benefit, partitioning is necessary. If neither single-partition
throughput nor latency were critical, the original proposal preferred
the then-existing leader-based system because it supported pipelined
writes per partition. That fallback implementation has since been removed.

---

## Applicability

The Writer-based system requires that write operations produce a
**self-contained, opaque replication payload** that any replica can
apply independently. The Writer ships bytes — it never touches the
data itself. This constraint determines which workloads fit.

### Suitable Workloads

| Workload | Why it works |
|---|---|
| **KV store** (kvstore example) | Write = serialized `KvOp::Put{key,value}`. Pure data blob. Writer ships it, each replica deserializes and applies independently. |
| **Application-level state machine** | Write = serialized command/event. Replicas are deterministic state machines that apply commands in LSN order. |
| **Event sourcing** | Write = domain event. Replicas append to their local event log. Projections are derived per-replica. |
| **Document store** | Write = serialized document mutation. Each replica applies the mutation to its local store. |

**Common trait:** The application computes the replication payload
*before* calling `writer.replicate()`. No local state is needed at the
Writer — the payload is complete and self-describing.

### Unsuitable Workloads

| Workload | Why it doesn't work |
|---|---|
| **Replicated SQLite** (sqlite example) | SQL execution is inseparable from the local DB file. WAL frames are captured by reading the local WAL *after* SQLite commits. The Writer would need to host the SQLite DB to execute SQL and capture frames — making it a primary in all but name. |
| **Any page/WAL-level replication** | Replication payload (pages, WAL frames) is produced as a side effect of local storage engine operations. Cannot be separated from the data. |
| **Workloads requiring local reads** | If the Writer must serve reads from the data it writes, it needs local state — defeating the "Writer has no data" principle. |

### Historical SQLite Recommendation

The original proposal recommended the then-existing leader-based operator for
replicated SQLite and similar workloads where the write path was coupled to
local storage. The primary hosted the SQLite DB, executed SQL, captured WAL
frames and replicated them. The classic implementation used for that
recommendation has since been removed.

### Historical Decision Rule

```
Can the app produce the replication payload WITHOUT local state?
  YES → Writer-based system (this design)
  NO  → Then-existing leader-based system
```

---

## In-Doubt Writer Contract

This section provides the full design of the contract introduced in
Section 6 of Design. The contract resolves the response-consistency
challenge of separating the Writer from replicas without external
storage or extra latency.

### The Contract

The Writer's `replicate()` API uses three error variants to make the
ambiguity of partial-failure outcomes explicit in the type system:

```rust
pub enum ReplicateError {
    /// Validation failed BEFORE any network send was attempted.
    /// Data definitely not on any replica. Caller may safely retry
    /// (with corrected input) or treat as a clean failure.
    ///
    /// Examples: payload exceeds max op size, schema validation
    /// failure, required field missing, Writer pre-flight check
    /// (e.g., < W replicas reachable per coordinator status) refuses
    /// to start the op.
    ///
    /// IMPORTANT: PreSendFailure is for failures detected BEFORE
    /// any `send_to_w` invocation. Any failure that occurs after
    /// the Writer has begun sending the op (even partial transport
    /// failures, even immediate connection refused on every
    /// replica) is reported as `InDoubt`, because the framework
    /// cannot reliably distinguish "transport REJECT before
    /// delivery" from "delivered but ACK lost." Conservative
    /// classification preserves the response-consistency guarantee
    /// of the In-Doubt Writer Contract.
    PreSendFailure(ValidationError),

    /// Writer cannot determine the outcome of this op. Data MAY exist
    /// on some replicas. After this, the Writer is poisoned — all
    /// subsequent replicate() calls return WriterPoisoned. Caller
    /// SHOULD treat this as a fatal application error and restart
    /// the application; on restart, the application reads back its
    /// persisted state from the cluster to recover. (See "Caller
    /// Contract" below.)
    InDoubt {
        lsn: Lsn,
        partial_acks: u32,  // informational
    },

    /// Writer was previously poisoned by InDoubt and is no longer
    /// accepting writes. The application should be restarting; this
    /// variant exists so concurrent in-flight calls during the brief
    /// shutdown window observe a clean error.
    WriterPoisoned,
}
```

The key property: the Writer **never** returns an error that means
"this op definitively failed and no replica has the data." That
state would create response inconsistency (the data may persist on
the replica that received it). Instead, the only ambiguous-failure
variant is `InDoubt`, whose name and contract make clear that the
data may exist.

#### Single-Op-In-Flight Serialization

The contract requires that **only one op is in-flight at a time**:
the next LSN cannot safely be assigned until the current op resolves
(either succeeds, or poisons the Writer via `InDoubt`). This is
non-negotiable for the authority approach to be correct (otherwise
ops past the poison point pollute `max(received_lsn)`).

Enforcement: the `Writer` library **internally serializes**
`replicate()` calls via an mpsc channel + actor task. Concurrent
caller tasks each push a `ReplicateRequest` onto the channel; the
actor processes them strictly in order. The user-facing API
preserves `&self` (multiple callers can share the Writer handle)
while the actor enforces the in-flight-of-one invariant.

```
[caller A] ──┐
[caller B] ──┼──► mpsc::UnboundedSender<ReplicateRequest>
[caller C] ──┘                    │
                                  ▼
                       ┌────────────────────────┐
                       │  WriterActor           │
                       │  (single tokio task)   │
                       │                        │
                       │  loop {                │
                       │    req = rx.recv();    │
                       │    process(req);       │  ← strict serialization
                       │    req.reply.send();   │
                       │  }                     │
                       └────────────────────────┘
                                  │
                                  ▼
                          send_to_w → quorum ACK
```

This pattern matches the existing `WalReplicatorActor` in
`kuberic-core/src/replicator/actor.rs`.

**Throughput consequence**: throughput is bounded by `1 / RTT(Writer↔W-th replica)`.
At a typical 1ms intra-cluster RTT, this is ~1000 ops/sec per
partition. For higher throughput, partitions must be sharded — the
single-writer guarantee is per partition, not per cluster.
Pipelining (multiple in-flight ops) is intentionally **not
supported**; it would invalidate the authority recovery proof.

#### Replica Durability Contract

The contract's correctness guarantee — `Ok(lsn) ⇒ data persists`
across F replica failures — requires that a replica does not send
`ReplicationAck` for LSN=K until LSN=K is durably persisted such
that, after any process or kernel crash, `received_lsn ≥ K` is
recoverable from disk.

The existing `SecondaryReceiver` (`kuberic-core/src/replicator/secondary.rs:200-224`)
implements this via two modes:

- **Persisted mode** (`operation_tx` set): replica forwards the op
  to the user's `StateProvider` via `Operation::new(lsn, data, user_ack_tx)`.
  The replica ACKs to the Writer only after the user code calls
  `acknowledge()` on the Operation, signaling that the op has been
  durably persisted (e.g., WAL fsync, `StateProvider::accept_op`
  returned successfully). This is the mode required by the In-Doubt
  Writer Contract.
- **Volatile mode** (`operation_tx` unset): replica auto-ACKs after
  in-memory acceptance. **This mode is NOT compatible with the
  In-Doubt contract** — it can violate `Ok(lsn) ⇒ data persists`
  under correlated power-loss failures. In the former classic implementation,
  this mode was available to workloads that explicitly opted out of durability
  (for example metrics or ephemeral caches); the proposal's correctness claims
  did not apply to that mode.

User-facing implication: a `StateProvider` for the writer-based
system MUST persist (fsync) before calling `acknowledge()`.
Reference examples (`examples/kvstore/src/state.rs` for kvstore,
`examples/sqlite/src/state.rs` for SQLite WAL persistence)
demonstrate this pattern.

#### Replica Order Acceptance (Forbid Gaps)

Replicas accept `ReplicationItem`s in **strict LSN order**. An item
with `lsn != received_lsn + 1` is rejected (with an error that
indicates the expected next LSN). This complements the
single-op-in-flight serialization and keeps `received_lsn`
contiguous by construction.

Why forbid gaps:
- The authority approach computes `authority_lsn = max(received_lsn)`.
  If `received_lsn` is allowed to be the highest *observed* LSN
  (allowing gaps), then a replica with `received_lsn = 100` but
  ops only up to LSN=98 would falsely advertise authority for ops
  it doesn't have.
- Contiguous `received_lsn` makes `Ok(lsn) ⇒ replica has all ops
  ≤ lsn` an enforceable invariant, which is what the contract's
  correctness proof requires.

Implementations: define `received_lsn := highest contiguous LSN`
across both writer-based and peer-repair docs. The existing
`SecondaryReceiver::accept_item` matches this when fed in-order
streams (which the In-Doubt Writer guarantees).

### When Does InDoubt Fire?

Because kuberic uses **strict total ordering with single-writer
semantics**, only one op is in-flight at a time (enforced by the
internal actor). When the Writer cannot resolve the current op,
the entire replication stream is blocked. `InDoubt` fires when:

1. **Quorum unreachable, retries exhausted:** Writer sent the op,
   got fewer than W ACKs, retried for the configured retry budget,
   still didn't reach quorum. The retry budget is **configurable**
   per workload (default e.g., 5-10s; can be tuned up to 60s+ for
   workloads that prefer long-tail latency over restart frequency).

2. **Epoch staleness detected:** Writer learned (via coordinator
   stream or replica reply with stale-epoch error) that another
   Writer holds the current epoch. The in-flight op's fate now
   depends on the new Writer's authority decision.

3. **Coordinator unreachable for too long:** Writer cannot confirm
   it still holds the current epoch. Cannot safely make further
   progress.

For **planned operations** (rolling updates, scale-down,
switchover), the operator integrates `PauseWrites`/`ResumeWrites`
into the workflow (see [Reconfiguration via Coordinator](#reconfiguration-via-coordinator)).
The Writer holds new ops during pause; the in-flight op completes
before the pause takes effect. This prevents InDoubt storms during
routine pod replacement.

```rust
/// Writer state machine. Distinct phases prevent the actor from
/// processing pending requests after a poison decision.
enum WriterState {
    Live,                       // accepting and processing requests
    Poisoning { in_doubt_lsn },  // poison decided; draining channel
    Poisoned,                   // drain complete; only WriterPoisoned returned
}

pub async fn replicate(&self, data: Bytes) -> Result<Lsn, ReplicateError> {
    // Fast-path rejection: if already Poisoning or Poisoned, fail
    // immediately without enqueuing.
    match self.state.load(Ordering::Acquire) {
        WriterState::Poisoning { .. } | WriterState::Poisoned =>
            return Err(WriterPoisoned),
        WriterState::Live => {}
    }

    // Pre-send validation (safe to fail cleanly)
    if let Err(e) = self.validate(&data) {
        return Err(ReplicateError::PreSendFailure(e));
    }

    // Send to actor; actor enforces single-op-in-flight
    let (reply_tx, reply_rx) = oneshot::channel();
    self.actor_tx.send(ReplicateRequest { data, reply_tx })?;
    reply_rx.await?
}

// Inside the WriterActor loop:
async fn run_actor(&mut self, mut rx: mpsc::Receiver<ReplicateRequest>) {
    while let Some(req) = rx.recv().await {
        // RE-CHECK poison state inside the actor loop. Without this
        // check, a request that was enqueued just before another
        // request triggered poison would still be processed.
        if !matches!(self.state.load(Ordering::Acquire), WriterState::Live) {
            let _ = req.reply_tx.send(Err(WriterPoisoned));
            self.drain_remaining(rx).await;  // drain rest of channel
            return;
        }

        let result = self.process_request(req).await;
        // process_request returns Err(InDoubt) only after calling
        // self.poison(), which transitioned state to Poisoning.
        if let Err(ReplicateError::InDoubt { .. }) = &result {
            // Send the InDoubt result to THIS request (the one whose
            // attempt failed); then drain remaining requests with
            // WriterPoisoned.
            let _ = req.reply_tx.send(result);
            self.drain_remaining(rx).await;
            return;
        }
        let _ = req.reply_tx.send(result);
    }
}

async fn process_request(&mut self, req: ReplicateRequest) -> Result<Lsn, ReplicateError> {
    let lsn = self.next_lsn.fetch_add(1, Ordering::Relaxed);

    // Send + bounded retry loop
    let deadline = Instant::now() + self.retry_budget;
    loop {
        let acks = self.send_to_w(&req.data, lsn).await;
        if acks >= self.write_quorum {
            return Ok(lsn);
        }
        if self.is_epoch_stale().await {
            return Err(self.poison(lsn, acks));
        }
        if Instant::now() >= deadline {
            return Err(self.poison(lsn, acks));
        }
        tokio::time::sleep(RETRY_BACKOFF).await;
    }
}

/// Transition Live → Poisoning. Stamps the in-doubt LSN so concurrent
/// callers can observe consistent state.
fn poison(&self, lsn: Lsn, partial_acks: u32) -> ReplicateError {
    self.state.store(
        WriterState::Poisoning { in_doubt_lsn: lsn },
        Ordering::Release,
    );
    self.notify_coordinator_of_poison();  // speeds up takeover
    ReplicateError::InDoubt { lsn, partial_acks }
}

/// Drain the channel sending WriterPoisoned to all pending requests.
/// Called once after the actor decides to poison.
async fn drain_remaining(&mut self, mut rx: mpsc::Receiver<ReplicateRequest>) {
    rx.close();  // refuse new sends from senders racing the drain
    while let Some(req) = rx.recv().await {
        let _ = req.reply_tx.send(Err(WriterPoisoned));
    }
    self.state.store(WriterState::Poisoned, Ordering::Release);
}
```

**Critical invariant:** once the actor reaches the `Poisoning`
state, no further request is processed (no `Ok` is returned). The
drain loop converts every pending request — including any that the
sender enqueued just before observing the state change — into
`WriterPoisoned`. The channel is closed via `rx.close()` so any
late `send()` from a sender that hasn't yet observed the
`Poisoning` state fails fast at the channel boundary.

This pattern requires the channel to be **bounded** in production:
unbounded channels can OOM under sustained over-1/RTT submission.
The bounded channel applies natural back-pressure on callers
exceeding the throughput cap.

### Why the Writer Halts After InDoubt

After `InDoubt`, the Writer is **poisoned** — it refuses all
subsequent `replicate()` calls with `WriterPoisoned`. The next op
is not sent. This is essential for correctness:

- The in-doubt op (LSN=N) may or may not be on some replicas.
- If the Writer were to proceed with LSN=N+1, replicas with LSN=N+1
  would either (a) reject it (gap forbidden by replica order
  acceptance) or (b) accept it but create non-contiguous logs.
- Halting is cleaner: the in-doubt op is resolved by the
  **coordinator's authority recovery protocol**
  (fence-first-then-query), then a new Writer starts at
  `authority_lsn + 1` with a clean slate.

This matched the former leader-based system's pattern: when a
primary cannot reach quorum, it gets demoted; the new primary's
`max(received_lsn)` defines truth.

### Why This Eliminates the Fatal Flaw

The original flaw was **Scenario B**: the user is told
`Err(NoWriteQuorum)`, but the data persists on the replica that
received the op. Response consistency is violated because the
system's post-recovery state (data present) contradicts what the
user was told (write failed).

Under the in-doubt contract, **Scenario B cannot occur**:

- Writer cannot return a "definitively failed" error after sending
  to replicas. It returns `InDoubt` (data may exist) or `Ok` (data
  exists).
- `InDoubt`'s type-level contract makes ambiguity explicit. A
  caller cannot mistake it for a clean failure.
- The system is free to keep or discard the in-doubt data; both
  outcomes are response-consistent.

### Authority-Based Recovery (Fence-First-Then-Query)

The authority recovery protocol runs when the coordinator detects
Writer disconnect or receives a `WriterPoisoned` notification.

**Critical ordering: fence FIRST, then query.** A naive
"query → max → fence" ordering would race with the old Writer's
in-flight `Ok` returns: the coordinator could compute
`authority_lsn = 99` from a stale snapshot while the old Writer is
still ACKing LSN=100 from a quorum, causing `update_epoch` to
truncate an LSN the user already received `Ok` for. This is exactly
the same response-consistency violation we set out to eliminate.

The correct ordering mirrors Service Fabric's `revoke_write_status`
(Phase 0) and Kuberic's durable failover pattern: fence the old epoch
synchronously before sampling state.

#### Recovery Generation: Binding Mechanism Across Async Boundaries

A subtle hazard with multi-coordinator HA + async RPCs is that a
clean abstract protocol can fail when two coordinators race or when
RPCs cross independent async boundaries (CRD ↔ RPC, RPC ↔ replica
state). To prevent this, every recovery action carries a
**`recovery_generation`** — a monotonically-increasing integer
stored in CRD `status` and stamped into every recovery RPC.

```
status.recovery_generation: int64    # monotonically increasing
                                     # (incremented on each recovery start)
```

**Replica enforcement:** every replica records the highest
`recovery_generation` it has accepted. Replicas reject any recovery
RPC whose generation is ≤ what they last accepted, returning
`StaleRecoveryGeneration`. This binds:
- **Multi-coordinator races (NEW-MF2)**: a losing coordinator's
  RPCs are rejected by replicas because the winning coordinator
  has already advanced `recovery_generation` in CRD.
- **Late RPCs from a crashed coordinator instance**: rejected
  because the new coordinator instance bumped the generation on
  resumption.
- **Out-of-order RPC delivery within a recovery**: each step's RPC
  carries the same generation; replicas use generation + step kind
  to detect protocol violations.

**Coordinator enforcement:** before issuing any recovery RPC, the
coordinator must have written its `recovery_generation` (and any
phase transition) to CRD. Optimistic concurrency on `resourceVersion`
ensures only one coordinator wins each phase transition; the loser
re-reads, observes the new generation, and either resumes or aborts.

#### The Fenced Quorum IS the Authority Quorum

A subtle hazard if not addressed: querying `received_lsn` from
**all reachable** replicas (rather than only fenced ones) can let
an unfenced replica with a stale-epoch tail poison `authority_lsn`.
Concrete trace (N=3, W=2): coordinator fences {R1, R2}; R3 was
unreachable during fence, reachable during query; old Writer's
LSN=101 reached R3 only (still at old epoch); R3 returns
`received_lsn=101` → `authority_lsn=101`. The cluster then wedges:
R1/R2 reject any new op at LSN=102 because they're missing
LSN=101 (replica order acceptance), and R3 carries an old-epoch
orphan tail.

**Fix:** Authority is computed only from the **fenced quorum** —
the set of replicas that ACKed `FreezeWrites` at the new
`recovery_generation`. The `FreezeWrites` response carries the
replica's `received_lsn` (so a separate query is unnecessary in
the common case); replicas that did not participate in the fenced
quorum are excluded from authority and rebuilt later (see Per-
Replica Reconciliation below).

#### The Protocol

```
On Writer poison/disconnect/takeover (coordinator detects via
WriterPoisoned signal, stream disconnect, or new Writer connect):

  1. Coordinator advances generation in CRD status (atomic via
     resourceVersion optimistic concurrency):
       status.recovery_generation = G + 1
       status.current_epoch       = (data_loss_number, writer_epoch + 1)
       status.recovery_phase      = "fencing"

  2. Coordinator pushes FreezeWrites{
       generation: G+1,
       new_epoch: ...,
     } to ALL replicas. Synchronous: must succeed on a quorum (≥W)
     for the protocol to advance.

     Each replica:
       - Validates: generation > my last accepted recovery_generation
       - If valid:
           - Records `pending_epoch := new_epoch` and
             `my_recovery_generation := generation` to durable storage
           - **IMPORTANT**: `current_epoch` is NOT updated here.
             FreezeWrites only sets a `pending_epoch` marker that
             enables the replica to start REJECTING ReplicationItems
             at the old `current_epoch`. The actual transition to the
             new epoch (and the corresponding op truncation) happens
             only on `UpdateEpoch` (step 4). Implementations MUST NOT
             conflate `pending_epoch` and `current_epoch` — doing so
             causes UpdateEpoch's idempotency check to short-circuit
             and silently skip the truncate.
           - Rejects all subsequent ReplicationItems whose
             `writer_epoch < pending_epoch.writer_epoch` (returns
             `StaleEpoch`)
           - Returns `FreezeWritesResponse{ received_lsn: X }`
       - If invalid (stale generation):
           - Returns `StaleRecoveryGeneration`

     Old Writer's in-flight ACKs naturally fail with StaleEpoch →
     old Writer's is_epoch_stale() returns true → old Writer poisons.

  3. After fenced quorum (≥W ACKs of FreezeWrites at generation G+1):
       fenced_quorum := { replicas that ACKed FreezeWrites at G+1 }
       status.recovery_phase = "applying"

       // Compute authority via the catch-up quorum rule:
       // sort fenced_quorum's received_lsn values descending,
       // take the (W-1)-th index (0-based) — i.e., the highest
       // value present on at least W replicas. This drops
       // single-replica tail values whose ops were not
       // quorum-confirmed at write time.
       sorted := sort_desc(received_lsn for r in fenced_quorum)
       status.last_authority_lsn = sorted[W - 1]

     **Why (W-1)-th highest, not max:**
     - If `Ok(lsn)` was returned to the user, by the In-Doubt
       contract ≥W replicas ACKed lsn at write time. After
       fence, at least W - F = (W replicas the writer ACKed) -
       (F replicas that died) of those replicas remain. With the
       fenced_quorum size of ≥W, the (W-1)-th highest value is
       the highest LSN guaranteed to exist on ≥W replicas in the
       fenced quorum — i.e., the highest LSN the new Writer can
       safely use as its starting authority without risk of
       being "single-replica pinned" (where the single tail
       holder dies before per-replica reconciliation can spread
       the data, leaving authority_lsn unrecoverable).
     - Ops above the (W-1)-th highest that exist on fewer than W
       fenced replicas are NOT quorum-confirmed (they may have
       been partially-sent by a poisoned Writer that returned
       InDoubt). Dropping them is consistent with the In-Doubt
       contract.
     - In the common case (no replica failures during recovery,
       all fenced replicas at the same received_lsn), the
       (W-1)-th highest equals the max — no loss.

     Note: replicas outside fenced_quorum are NOT used for
     authority computation. They are recorded for per-replica
     reconciliation (step 5 below).

  4. Coordinator pushes UpdateEpoch{
       generation: G+1,
       new_epoch: ...,
       authority_lsn: ...,
     } to all replicas.

     Each replica processes UpdateEpoch via the full 6-step sequence
     specified in [UpdateEpoch RPC](#rpc-contracts) (validate
     generation + idempotency, quiesce StateProvider apply pipeline,
     invoke `on_authority_truncate` callback, atomic durable
     mutation including epoch activation, resume apply pipeline,
     ACK).

     Replicas not at the current `recovery_generation` (e.g., they
     missed step 2 due to partition during fence) return
     `StaleRecoveryGeneration`; the coordinator records them as
     "needs reconciliation" (step 5).

     Quorum survival under partial UpdateEpoch failure: if a
     fenced replica crashes between FreezeWrites ACK and UpdateEpoch
     ACK, the UpdateEpoch quorum is allowed to use the same catch-up
     rule: ≥W - 1 UpdateEpoch ACKs from the fenced quorum suffice
     (because authority_lsn is by construction the highest LSN
     guaranteed to be on ≥W fenced replicas; one crashing replica
     still leaves W - 1 with the data, and the per-replica
     reconciliation step rebuilds the missing one). If fewer than
     W - 1 UpdateEpoch ACKs arrive, the coordinator advances
     recovery_generation to G+2 and restarts from step 1 with the
     remaining replicas.

  5. Per-replica reconciliation:
     For each replica NOT in the fenced_quorum (partitioned during
     fence) OR with received_lsn < authority_lsn (lagging):
       - Coordinator marks replica as "rebuilding" in CRD
       - On replica reconnect / availability, the coordinator
         **re-reads CRD's current `recovery_generation` and
         `authority_lsn`** (NOT the values cached at marking time —
         see "Reconciliation Generation Refresh" below):
            - Coordinator forces UpdateEpoch{generation: current,
              authority_lsn: current} before any other RPC is
              accepted
            - If post-update received_lsn < current authority_lsn,
              coordinator initiates a build (existing copy protocol,
              snapshot from a fenced-quorum replica with
              received_lsn ≥ authority_lsn)
       - Replica rejoins the cluster only after build completes

  6. After UpdateEpoch ACKed by fenced quorum:
       status.recovery_phase = "ready"
       Coordinator allows new Writer to connect.

  7. New Writer connects, receives ConfigUpdate with
     (recovery_generation, authority_lsn, current_epoch, ...).
     Sets next_lsn = authority_lsn + 1.
```

**Why this is correct:**
- **Step 2 (FreezeWrites)** prevents the old Writer from successfully
  ACKing any new ops on the fenced quorum. By the time we compute
  authority in step 3, no new `Ok` returns are possible.
- **Recovery generation** binds the coordinator's CRD decision to the
  RPC effects on replicas. A losing coordinator's stale-generation
  RPCs are rejected, preventing split-brain advancement of authority.
- **Fenced quorum = authority quorum** ensures unfenced (stale-epoch)
  replicas cannot poison `authority_lsn`. Such replicas go through
  per-replica reconciliation (step 5) before rejoining.
- **(W-1)-th highest authority** ensures that `authority_lsn` is
  always supported by ≥W replicas in the fenced quorum, preventing
  the "single tail replica dies before reconciliation spreads its
  data" wedge.
- **Step 4 atomic truncate** ensures replicas at the new epoch have
  no orphan ops > `authority_lsn`. New Writer's ops at
  `authority_lsn + 1` cannot collide with ghosts.
- **Per-replica reconciliation** handles two cases that would
  otherwise wedge the cluster: (a) replicas partitioned during the
  fence step that come back later with a stale-epoch tail, and
  (b) replicas in the fenced quorum but lagging behind
  `authority_lsn`. Both cases are forced through coordinator-
  mediated catch-up before serving traffic.

#### Reconciliation Generation Refresh

If a replica is marked for reconciliation at `recovery_generation =
G`, but several recoveries happen while the replica is partitioned
(advancing to G+2, G+3, ...), the coordinator MUST NOT use the
stale G value when the replica reconnects. On reconnect:

1. Coordinator re-reads the CURRENT `recovery_generation` and
   `authority_lsn` from CRD status (NOT the values cached when
   the replica was first marked).
2. Coordinator forces `UpdateEpoch{generation: current_generation,
   authority_lsn: current_authority_lsn}` on the replica.
3. Replica accepts because `current_generation >
   replica.my_recovery_generation`.
4. If the replica's now-up-to-date `received_lsn < current
   authority_lsn`, coordinator initiates a build from a fenced
   quorum member that has all ops ≤ `current authority_lsn`.

This "refresh-on-use" rule prevents two failure modes:
- **Frozen-stale**: applying `G+1, authority=100` after the cluster
  has advanced to `G+3, authority=200` would either be rejected
  (replica has `recovery_generation > G+1` from another path,
  unlikely but possible) or silently truncate to LSN=100 (data
  loss of 100..200).
- **Frozen-fresh-but-wrong**: applying `G+1, authority=100` when
  the actual current state is `G+3, authority=50` (data_loss bump
  reduced authority) would advance the replica past where the
  cluster currently is.

The rule applies to all per-replica catch-up paths: reconnect after
partition, late join, and rebuild completion.

**Idempotency under coordinator restart**: every step is encoded as
a `(recovery_generation, recovery_phase)` pair in CRD status. If the
coordinator crashes mid-recovery, the next coordinator instance
reads this state and resumes from the appropriate step. All RPCs
(FreezeWrites, UpdateEpoch) are idempotent on replicas: replicas
already at the current generation respond with their state without
re-mutating.

**Cost on the failover path: 2 RTT** (one for FreezeWrites,
one for UpdateEpoch). Per-replica reconciliation cost (step 5) is
amortized: a replica down during fence pays a build cost when it
rejoins, similar to today's leader-based system after a long
secondary outage.

The authority approach is correct under the in-doubt contract
because:

- If the user saw `Ok(lsn)`, the data must persist. Authority
  preserves it (any survivor with the LSN keeps it; FreezeWrites
  guarantees no new `Ok` after recovery starts). ✓
- If the user got `InDoubt`, the data may or may not persist
  depending on what reached replicas. Either outcome is
  response-consistent. ✓
- The user is **never** told "this write failed cleanly" while
  data persists, because the in-doubt contract makes that state
  unreachable.

**No per-commit `committed_lsn` tracking. No CRD writes per write.**
Coordinator state in CRD updates only on epoch bumps and
`recovery_phase` transitions (≤ a handful of writes per recovery
event).

### Edge Case Verification

| Scenario | Writer behavior | Result | Response-consistent? |
|---|---|---|---|
| W ACKs received | Return `Ok(lsn)` | Authority keeps op | ✓ User saw Ok |
| Writer crashes after `Ok` returned | (process died before user saw response) | Authority keeps op | ✓ User in-doubt via transport error |
| Partial ACKs (< W), retries exhausted | Return `InDoubt`, poison Writer; app restarts | Authority keeps op on whoever has it | ✓ App restart resolves ambiguity |
| Send fails to all replicas, retries exhausted | Return `InDoubt`, poison Writer; app restarts | Authority sees no new LSN | ✓ App restart resolves ambiguity |
| Epoch staleness detected mid-op | Return `InDoubt`, poison Writer; app restarts | New Writer's authority handles it | ✓ App restart resolves ambiguity |
| Old Writer ACKs LSN=N while coordinator queries | FreezeWrites blocks new ACKs first → old Writer's ACK rejected with stale-epoch | No race; old Writer poisons | ✓ Fence-first prevents the race |
| Coordinator crashes mid-recovery | Next coordinator resumes from CRD `recovery_phase` | Recovery completes idempotently | ✓ No partial-fence state |
| Caller invokes after poison | Return `WriterPoisoned` | No state change | ✓ Clean signal — app should be restarting |
| Pre-send validation fails | Return `PreSendFailure` | No replica saw the op | ✓ User told clean failure |
| All replicas dead | Return `InDoubt`, poison; quorum loss | Data loss recovery (historical protocol) | ✓ Same as the historical baseline |
| Old Writer reconnects with retired writer_id | Coordinator returns `WriterRetired` error | Old Writer must clear ID and restart | ✓ No cascading takeover |

### Caller Contract

The user code must handle the four outcomes correctly:

```rust
match writer.replicate(data).await {
    Ok(lsn) => {
        // Data is durably committed at this LSN.
    }
    Err(ReplicateError::PreSendFailure(e)) => {
        // Clean failure. No replica saw the op.
        // Safe to handle as a normal error or retry as new.
    }
    Err(ReplicateError::InDoubt { lsn, .. }) => {
        // Data MAY have committed; ambiguous outcome.
        //
        // RECOMMENDED HANDLING: treat InDoubt as a fatal application
        // error. Log and trigger application restart. On restart,
        // the application reads back its persisted state from the
        // cluster (via reads of known keys, recovery protocols, etc.)
        // to discover whether `lsn` was committed. The restart-and-
        // read-back loop resolves the ambiguity at the application
        // semantic layer.
        //
        // Why this is the recommended pattern:
        // - Avoids the trap of trying to "retry InDoubt with idempotency
        //   tokens" — application logic for that is subtle and mistakes
        //   silently double-write.
        // - Aligns with the former leader-based system's failover
        //   semantics: a primary crash also forces transport-error
        //   in-doubt at the client; clients must handle this anyway.
        // - The Writer is poisoned after this; no further ops will
        //   succeed without a new Writer instance, which restart
        //   provides naturally.
        //
        // Not recommended: caller-side idempotency tokens with retry.
        // Only viable for naturally-idempotent ops (e.g., kvstore SET
        // with full key+value); error-prone for general workloads.
        return Err(FatalReplicationError(lsn));
    }
    Err(ReplicateError::WriterPoisoned) => {
        // Writer was poisoned by an earlier InDoubt. The application
        // should already be restarting; this variant exists so
        // concurrent in-flight calls during the brief shutdown window
        // observe a clean error rather than hanging.
        return Err(FatalReplicationError(0));
    }
}
```

**Application restart pattern (S1):** The recommended user code is
to convert `InDoubt` to a process-fatal error (similar to a panic
or `unwrap` failure) and let Kubernetes restart the pod. On restart,
the application:
1. Reconnects to a new Writer instance via `Writer::connect(...)`.
   The coordinator runs authority recovery (fence-first-then-query)
   during this window if needed; the new Writer is configured
   with the post-recovery `authority_lsn` and starts at
   `authority_lsn + 1`.
2. Calls `let cursor = writer.current_authority().await;` to obtain
   an `AuthorityCursor { data_loss_number, recovery_generation,
   authority_lsn, current_epoch }`.
3. Reads back critical state via
   `replica.read_at(key, min_lsn = cursor.authority_lsn, timeout)`.
   This bounds reads to the post-recovery authoritative state
   without requiring the application to persist its own LSN
   cursor. If `read_at` returns `ReplicaInRecovery`, retry on a
   different replica; if it returns `LsnNotYetApplied`, retry
   based on the diagnostic fields.
4. Resumes operations from the recovered state.

This bounds the read-back precisely without requiring app-side
cursor persistence — the framework provides the necessary state
discovery via `current_authority()`. See [LSN Discovery on App Restart](#lsn-discovery-on-app-restart)
for the full pattern, including the `data_loss_number` check that
catches data-loss-bump races between successive `current_authority`
calls.

For applications that need stronger per-LSN restart consistency
(e.g., "I issued LSN=K before restart; did it commit?"), the
recommended pattern is to issue ops with idempotent natural keys
(client-provided UUIDs) and read back by key. The Writer does not
provide per-LSN durability tracking outside the `Ok(lsn)` return.

This pattern matches how stateful apps handle other fatal errors
(database connection loss, panic) and avoids subtle idempotency
bugs.

This caller contract is the standard distributed-systems pattern
for in-doubt RPCs (database commit timeouts, gRPC `UNAVAILABLE`
after submission, HTTP 504 after request body sent), elevated to
"app-level fail-stop" rather than "RPC-level retry-with-token."

### Why Halt-on-Poison Instead of Process Exit?

An earlier design considered `std::process::abort()` as the failure
mode at the Writer level. The poison-and-return-error approach is
strictly better:

| Aspect | Process exit (Writer level) | Poison + `InDoubt` error |
|---|---|---|
| Correctness | ✓ Forced | ✓ Enforced via type contract |
| Writer process stays up | ✗ Must restart | ✓ Stays alive briefly |
| User code can log / clean up | ✗ Killed mid-stack | ✓ Normal error path before app restart |
| Caller has LSN information | ✗ None | ✓ `lsn` and `partial_acks` for diagnostics |
| Coordinator notification | Via stream disconnect | Explicit `WriterPoisoned` notify |
| Fits Rust idiom | ✗ Unusual | ✓ Standard `Result` |

Note: under the S1-recommended caller contract, the **application**
still restarts on `InDoubt` (similar in spirit to the process-exit
approach, but at a higher layer). The difference is that the Writer
library returns a clean error first, allowing the application to
log diagnostics, flush metrics, and shutdown gracefully before
restarting.

### Cost: Availability Trade-Off

The in-doubt contract trades **Writer availability and throughput**
for **latency + correctness**:

| Aspect | Historical leader-based baseline | With in-doubt contract |
|---|---|---|
| Write latency (steady state) | 2 RTT (client→primary→secondary) | 1 RTT (Writer→replicas, in-cluster only) |
| Throughput per partition | High (pipelined) | ~1/RTT (~1000 ops/sec at 1ms RTT) |
| Failover latency | Durable multi-reconcile workflow | ~2 RTT (fence + query+update_epoch) |
| Correctness | Primary-as-replica authority | Authority approach + in-doubt contract |
| External store per commit | None | None |
| Writer-takeover frequency on quorum blip | N/A (operator-managed) | Configurable retry budget; PauseWrites for planned ops |
| User-visible failures | Connection-level errors | Three explicit variants (clean / in-doubt / poisoned) |
| App restart on InDoubt | N/A | Recommended pattern (see Caller Contract) |

The Writer does not crash on transient quorum miss — it returns
`InDoubt`, the app restarts, the coordinator runs authority recovery,
and a new Writer takes over.

### Mitigations to Reduce InDoubt Frequency

- **Configurable retry budget**: default 5-10 seconds, tunable up
  to 60+ seconds per workload. Workloads that prefer long-tail
  latency over restart frequency can extend the budget; latency-
  sensitive workloads can shorten it.
- **PauseWrites for planned operations**: the coordinator sends
  `PauseWrites` to the Writer before planned pod replacement
  (rolling updates, scale-down, switchover). The Writer holds new
  ops; the in-flight op completes before the pause takes effect.
  After replacement, `ResumeWrites` lets writes flow again. This
  prevents InDoubt storms during routine pod replacement, mirroring
  the former leader-based system's behavior during reconfiguration.
- **Pre-flight checks**: refuse to start a new op (return
  `PreSendFailure`) when the coordinator reports < W replicas
  reachable. Avoids sending ops predestined to be in-doubt.
- **Fast Writer takeover**: `WriterPoisoned` notification + fence-
  first-then-query recovery typically completes in 2 RTT (~2-10ms
  intra-cluster). The new Writer is ready to accept ops within this
  window plus app restart latency.

### Interaction with Coordinator and Epoch

The in-doubt Writer + fence-first-then-query recovery yields a clean
coordinator interaction:

- **No `WriterProgress` stream messages** for `committed_lsn`.
- **No CRD status writes per commit.** CRD writes happen only on
  epoch bumps and `recovery_phase` transitions.
- **`WriterPoisoned` notification** from Writer to coordinator
  speeds up takeover (fallback: stream disconnect detection).
- **Coordinator's role on poison/disconnect/takeover**: run the
  authority recovery protocol described in
  [Authority-Based Recovery (Fence-First-Then-Query)](#authority-based-recovery-fence-first-then-query).
  The protocol is fence-first (FreezeWrites carries
  `received_lsn` in its response — there is no separate query RPC),
  uses `recovery_generation` to bind CRD state to RPC effects,
  computes authority from the **fenced quorum only**, and finishes
  with per-replica reconciliation for any unfenced or lagging
  replicas before transitioning back to `recovery_phase = ready`.
- **`UpdateEpoch` RPC** carries `recovery_generation`, `new_epoch`,
  and `authority_lsn`, no `committed_lsn`.

#### Anti-Flap Admission Control (Stale-Writer Reconnect Rejection)

The coordinator MUST authenticate every `WriterSession` and gate
takeover-triggering reconnects via two complementary mechanisms:

**1. Authentication (always):** the `WriterConnect.auth_token` field
carries a Kubernetes ServiceAccount token (typically delivered as a
projected service-account volume). The coordinator validates the
token against the K8s API (TokenReview) before accepting the
session. Unauthenticated WriterConnects are rejected immediately —
without this, anyone with `partition_id` knowledge can spam
`WriterConnect{writer_id=""}` and force endless authority
recoveries.

**2. Epoch-comparison rejection (covers same-Writer reconnects):**
the Writer presents `(writer_id, stored_writer_epoch)` on reconnect.
The coordinator compares against
`status.current_epoch.writer_epoch`:

- `stored_writer_epoch == current_writer_epoch` AND `writer_id`
  matches: same Writer instance reconnecting; resume normally
  (or fence + recover if the previous shutdown was unclean).
- `stored_writer_epoch < current_writer_epoch`: this Writer's
  epoch was superseded; return
  `WriterRetired{current_writer_epoch}`. The old Writer MUST clear
  its stored ID/epoch (and the cluster relies on the underlying
  ServiceAccount being unique per Deployment to prevent abuse —
  see below).

This eliminates the unbounded `retired_writer_ids` list: each
retired Writer is implicitly identified by its stale epoch,
comparison is O(1), and CRD status size remains bounded regardless
of takeover frequency.

**3. Takeover rate-limit (covers takeover-via-empty-writer_id):**
a flapping old Writer that follows the "clear stored ID and
restart" instruction would otherwise reconnect with
`writer_id=""`, hit the takeover path, and trigger fresh recovery.
The coordinator therefore enforces:

- **Identity binding**: the ServiceAccount token is bound to the
  Writer's deployment. The coordinator records
  `(serviceaccount_subject, last_takeover_at)` in CRD `status`.
- **Rate-limit**: refuse new authority recoveries from the same
  ServiceAccount within a configurable cooldown (default 30s).
  Reconnects during cooldown observe
  `TakeoverInCooldown{retry_after_ms}` and back off.
- **Lease-style coordination (recommended for multi-instance
  deployments)**: when the user app is deployed with replicas > 1,
  the user MUST integrate Kubernetes lease election upstream of
  `Writer::connect()`. Only the lease holder calls
  `Writer::connect`; siblings wait. Without this, multiple alive
  user-app pods all attempt takeover simultaneously, each
  defeating the other. The Writer library SHOULD provide a
  `LeaseGuard` wrapper that integrates `kube::leases::Lease` for
  common cases.

These three layers together close the takeover-via-empty-writer_id
DoS, the multi-instance-Deployment thrash (NEW-SF6 from round-2),
and the takeover-rate circuit-breaker concern (NEW-SF7 from
round-2).
### Theoretical Foundation

This is the **fail-stop failure model** applied to the Writer's
output, expressed through the type system rather than process
termination. A fail-stop component never produces incorrect output;
the in-doubt contract achieves this by making "incorrect output"
(definitive failure with persistent data) **unrepresentable** in
the API.

By forcing all ambiguous-failure outcomes into the explicit
`InDoubt` variant, callers cannot accidentally mishandle them. The
Writer halts after `InDoubt` not because it has crashed, but
because there is no safe next action it can take without an
external authority decision.

The contract shifts the design space from
**correctness vs latency** to
**Writer takeover frequency vs latency**. Workloads that can
tolerate occasional Writer takeovers (with appropriate idempotency
in client code) get the latency benefit at full correctness.

### Historical Design Comparison

| Workload | Original proposal recommendation |
|---|---|
| Lowest write latency required, app handles InDoubt by restart | Writer-based with in-doubt contract |
| Writer must stay logically up across transient failures (no app-restart cost acceptable) | Then-existing leader-based system |
| App lifecycle includes graceful crash/restart already | Writer-based with in-doubt contract |
| Throughput-critical (>1000 ops/sec per partition) | Then-existing leader-based system (writer-based caps at ~1/RTT due to single-op-in-flight) |
| Cross-region or out-of-cluster Writer deployment | Then-existing leader-based system (writer-based is in-cluster only) |

This table records the proposal's original comparison; it is not a current
workload-selection guide.

---

## Writer Process Model

The Writer is a library, but where it runs determines the failure-
domain coupling. Three deployment topologies are supported, each with
different trade-offs:

### Topology 1: In-Process Embedded (default)

The Writer runs in the same process and address space as the user
application.

```
┌────────── User Pod ──────────┐
│  ┌─────── User Process ───┐  │      ┌──── Replica Pods ────┐
│  │ User App + Writer lib  │──┼─────►│ Replica₁  Replica₂   │
│  │                        │  │      │ Replica₃  ...        │
│  └────────────────────────┘  │      └──────────────────────┘
└──────────────────────────────┘                ▲
                                                │
                              ┌──── Coordinator (in operator)
```

| Property | Value |
|---|---|
| Latency | Best (no IPC) |
| Failure-domain coupling | High — GC pauses, debugger, OOM, hot-reload all couple to Writer |
| Operational complexity | Low (one binary) |
| Recommended for | Greenfield apps where Writer-takeover frequency is acceptable |

**Coupling implications:** Long GC pauses or debugger breakpoints
in the user app freeze the Writer too. If the freeze exceeds the
retry budget on an in-flight op, the op times out into `InDoubt`,
the Writer poisons, and the app must restart. CI/test environments
that pause threads frequently will see elevated InDoubt rates.

### Topology 2: Sidecar Process

The Writer runs as a separate process within the same Kubernetes
pod, communicating with the user app via local IPC (Unix socket or
loopback gRPC).

```
┌─────────────── User Pod ───────────────┐
│  ┌── User Process ──┐  ┌── Writer ──┐  │      ┌── Replicas ──┐
│  │  User App        │──│ Writer lib │──┼─────►│ R₁ R₂ R₃ ... │
│  │  (gRPC client)   │  │            │  │      └──────────────┘
│  └──────────────────┘  └────────────┘  │
└────────────────────────────────────────┘            ▲
                                                      │
                              ┌──── Coordinator ──────┘
```

| Property | Value |
|---|---|
| Latency | Near-best (~10-100μs IPC overhead) |
| Failure-domain coupling | Medium — pod-level only (kubelet kill, OOM-killed pod) |
| Operational complexity | Medium (sidecar container in deployment) |
| Recommended for | Apps in languages without efficient gRPC, apps with long GC pauses, apps that need to debug |

**Coupling implications:** User-process GC, debugger, hot-reload do
NOT freeze the Writer. The Writer keeps responding to coordinator
heartbeats during user-process pauses. However, kubelet evictions
(OOM, drain) take down both processes together, so pod-level
failures still trigger Writer takeover.

### Topology 3: Dedicated Pod

The Writer runs in its own pod, communicating with the user app via
in-cluster gRPC.

```
┌── User Pod ──┐    ┌── Writer Pod ──┐    ┌── Replicas ──┐
│  User App    │───►│  Writer        │───►│ R₁ R₂ R₃ ... │
└──────────────┘    └────────────────┘    └──────────────┘
                            ▲
                            │
                  ┌── Coordinator
```

| Property | Value |
|---|---|
| Latency | Worst of the three (~0.5-2ms intra-cluster gRPC adds back) |
| Failure-domain coupling | Lowest — independent pods |
| Operational complexity | High (Writer pod lifecycle, deployment, scaling) |
| Recommended for | Multi-tenant clusters, apps that should not embed the Writer |

**Coupling implications:** This topology effectively re-introduces
a network hop, undermining the latency benefit. It exists for
operational scenarios where the Writer cannot be embedded (e.g.,
the user app is a third-party binary, or the Writer must be
multi-tenant). For most cases, prefer in-process or sidecar.

### SLO and Selection

The expected Writer-takeover rate per pod per 24 hours under stable
network depends on:
- **App pause behavior** (GC, debugger, lock contention) — affects
  in-process topology disproportionately
- **Pod replacement frequency** (rolling updates) — affects all
  topologies via kubelet eviction
- **Configured retry budget** — longer budgets absorb more transient
  pauses at the cost of long-tail latency

Workloads should benchmark their expected takeover rate during
load testing. As a rough guide:
- In-process, GC-light app, stable cluster: < 1 takeover / pod / day
- In-process, GC-heavy app or noisy cluster: 5-20 takeovers / day
- Sidecar topology: ≈ pod-replacement rate (1-5 / day for typical
  rolling deployments)

The original proposal suggested escalating to the sidecar topology or returning
to the then-existing leader-based system if takeover rate was unacceptable.
That fallback implementation has since been removed.

---

## Read Path

The Writer-based system provides write semantics only. Reads are
served directly by replicas via separate gRPC services on the
replica pod (analogous to today's `kuberic-core/src/grpc/server.rs`).

### Read-After-Write Pattern

Because reads can hit any replica and replicas update their
`applied_lsn` asynchronously, an application that needs
read-after-write must explicitly request a minimum LSN:

```rust
// Application-side pattern (recommended):
let lsn = writer.replicate(my_op).await?;
let result = some_replica
    .read_at(my_key, min_lsn = lsn, timeout = Duration::from_secs(5))
    .await?;
// result reflects all ops up to and including `lsn`
```

The replica-side `read_at(key, min_lsn, timeout)` RPC contract:

```protobuf
message ReadAtRequest {
    bytes  key = 1;
    int64  min_lsn = 2;
    uint64 timeout_ms = 3;     // 0 = use replica default (5000)
}
message ReadAtResponse {
    oneof result {
        bytes value = 1;            // applied_lsn ≥ min_lsn at read time
        LsnNotYetApplied not_yet = 2;
        ReplicaInRecovery in_recovery = 3;
    }
}
message LsnNotYetApplied {
    int64 applied_lsn = 1;          // current applied state
    int64 received_lsn = 2;         // current received state
    int64 last_progress_at_ms = 3;  // wall-clock epoch ms; lets caller distinguish
                                    // "stuck" from "behind but progressing"
}
message ReplicaInRecovery {
    int64 my_recovery_generation = 1;
    string recovery_phase = 2;       // "fencing" | "applying" | etc.
}
```

Replica behavior:
1. If `applied_lsn ≥ min_lsn`: serve the read immediately.
2. If the replica is currently in `recovery_phase != ready` (e.g.,
   it received `FreezeWrites` but not yet `UpdateEpoch`, or is
   undergoing per-replica reconciliation/rebuild): return
   `ReplicaInRecovery`. The caller MUST retry on a different replica.
3. If `applied_lsn < min_lsn` and replica is healthy (`recovery_phase
   = ready`): wait for `applied_lsn` to advance to `min_lsn`, with
   the request's `timeout_ms` (default 5000ms if 0).
4. If timeout expires while still waiting: return `LsnNotYetApplied`
   with diagnostic fields. The caller can distinguish:
   - `applied_lsn == previous_applied_lsn` over multiple retries =
     replica is stuck → retry on a different replica.
   - `applied_lsn` advancing but slowly = replica is behind but
     progressing → retry with larger timeout, or wait.

This pattern is **NOT** equivalent to the former leader-based
"primary is always up-to-date" semantic; the application must thread
the LSN through. The trade-off is that reads can be load-balanced
across all N replicas instead of single-pointing through the primary.

**Replicas in recovery MUST refuse `read_at`** (option 2 above).
This prevents stale reads from a replica whose state is being
truncated by `UpdateEpoch`. The caller's normal retry-on-different-
replica path handles this transparently.

### Stale Reads (Eventually Consistent)

For workloads that tolerate stale reads (caches, dashboards,
analytics), `read(key)` can be served by any healthy replica
without an LSN parameter. The result reflects whatever the replica
has applied so far. Replicas in recovery still refuse stale reads
to avoid serving from a state about to be truncated.

### LSN Discovery on App Restart

After app restart (e.g., following an `InDoubt` poison), the app
has no in-memory record of its last-issued LSN. The Writer library
provides:

```rust
/// Snapshot of the cluster's authority at a moment in time.
///
/// Use the (data_loss_number, recovery_generation, authority_lsn)
/// tuple to detect changes between successive uses. If the
/// data_loss_number has advanced since the snapshot was taken, the
/// authority_lsn from the older snapshot is no longer comparable
/// (the cluster has been through a data-loss recovery and LSN
/// space has been re-anchored).
pub struct AuthorityCursor {
    pub data_loss_number:    i64,
    pub recovery_generation: i64,
    pub authority_lsn:       Lsn,
    pub current_epoch:       Epoch,
}

impl Writer {
    /// Returns the cluster's current AuthorityCursor at the moment
    /// of the call. Suitable for bounding read-after-write queries
    /// after app restart.
    pub async fn current_authority(&self) -> AuthorityCursor;
}
```

The recommended restart flow:
1. App restarts after `InDoubt`.
2. App reconnects to a fresh Writer instance (via `Writer::connect`).
3. App calls `let cursor = writer.current_authority().await;` to
   get the cluster's `AuthorityCursor`.
4. App reads back its critical state via
   `replica.read_at(key, min_lsn = cursor.authority_lsn, timeout)`.
   - If the read returns `LsnNotYetApplied` repeatedly across all
     replicas, the cluster is in distress; abort and surface to
     operator monitoring.
   - If the read returns `ReplicaInRecovery`, retry on a different
     replica.
5. If the app needs to do additional reads later, it should re-call
   `current_authority()` and check `data_loss_number` against the
   earlier value — if it advanced, the app's prior LSN values are
   no longer comparable and the app must restart its recovery
   logic from scratch (treat as another `InDoubt`).
6. App resumes operations.

This bounds the read-back to the post-recovery authoritative state,
without requiring the app to persist its own "last known committed
LSN" externally. Including `data_loss_number` in the cursor lets
callers detect data-loss bumps that would otherwise silently
re-anchor the LSN space between get and use.

For applications that DO want stronger consistency across restarts
(e.g., "I issued LSN=K before restart; did it commit?"), the
recommended pattern is to issue ops with idempotent natural keys
(client-provided UUIDs) and read back by key. The Writer does not
provide per-LSN durability tracking outside the `Ok(lsn)` return
value.

### Implementation Status

The replica-side `read_at` RPC and `Writer::current_authority()`
are part of the writer-based system's required surface area but are
not yet fully specified in this document. Tracked in Implementation
Plan Phase 5 (Example Application).

---

## Coordinator Failure Modes

The coordinator is on the critical path of authority recovery. This
section specifies its protocol with rigor matching the existing
failover protocol in `design-gaps.md`.

### HA Model: Stateless Coordinator, State in CRD

The coordinator is **stateless**. All persistent state lives in the
`KubericWriterPartition` CRD `status` field, which is replicated
through the Kubernetes API server (etcd-backed). Any coordinator
instance can serve any request by reading and updating the CRD.

```
Coordinator state (in CRD status):
┌──────────────────────────────────────────────────────┐
│  current_epoch: (data_loss_number, writer_epoch)     │
│  current_writer_id: UUID (or null if no writer)      │
│  recovery_generation: int64 (monotonically increasing)│
│  recovery_phase: "ready" | "fencing" | "applying"    │
│  last_authority_lsn: int64                           │
│  replica_set: [...]                                  │
│  config_version: int                                 │
│  in_progress_builds: [{build_id, replica_id, ...}]   │
└──────────────────────────────────────────────────────┘
```

Note: there is no `retired_writer_ids` list. Stale-writer reconnect
detection uses epoch comparison instead (see
[Interaction with Coordinator and Epoch](#interaction-with-coordinator-and-epoch)).
This keeps CRD status size bounded regardless of takeover frequency.

**Why stateless**: enables trivial coordinator HA via Kubernetes
deployment replicas; recovery is naturally idempotent (the next
coordinator instance reads the CRD and resumes).

**Trade-off**: every coordinator decision involves a CRD round-trip
(~1-5ms). Acceptable because coordinator decisions are rare
(epoch bumps, recovery transitions) — not on the per-write path.

### Recovery Idempotency

The authority recovery protocol is encoded as
`(recovery_generation, recovery_phase)` transitions:

| Phase | What's been done | What's next |
|---|---|---|
| `ready` | Normal operation | (No recovery in progress) |
| `fencing` | `recovery_generation` advanced + epoch bumped in CRD; FreezeWrites being pushed; `received_lsn` collected from each ACK | After ≥W ACKs: compute authority_lsn from fenced quorum, transition to `applying` |
| `applying` | `authority_lsn` computed and persisted; UpdateEpoch being pushed | After ≥W UpdateEpoch ACKs: transition to `ready`; per-replica reconciliation continues asynchronously |

If a coordinator crashes mid-recovery, the next coordinator reads
`(recovery_generation, recovery_phase)` and resumes:
- `fencing` → re-issue FreezeWrites at the same `recovery_generation`
  (idempotent: replicas at the current generation respond with state
  without re-mutating; replicas at older generation accept and bump)
- `applying` → re-issue UpdateEpoch at the same `recovery_generation`
  (idempotent for the same reasons)

If the next coordinator instance does not trust the in-flight phase
(e.g., suspects the previous coordinator made partial RPC progress),
it can advance `recovery_generation` to G+1 and restart from `fencing`.
Replicas reject any in-flight RPC at the old generation
(`StaleRecoveryGeneration`), preventing split-brain.

All RPCs (FreezeWrites, UpdateEpoch) carry `recovery_generation` and
MUST be idempotent under retry at the same generation.

### RPC Contracts

**FreezeWrites RPC:**
```protobuf
message FreezeWritesRequest {
    int64 recovery_generation = 1;
    Epoch new_epoch = 2;  // (data_loss_number, writer_epoch + 1)
}
message FreezeWritesResponse {
    oneof result {
        FreezeWritesOk ok = 1;
        StaleRecoveryGeneration stale = 2;
    }
}
message FreezeWritesOk {
    int64 received_lsn = 1;  // replica's received_lsn at fence time
}
message StaleRecoveryGeneration {
    int64 my_recovery_generation = 1;  // what the replica has
}
```

On receipt, the replica:
1. Compare `recovery_generation` to its own:
   - `recovery_generation > my_recovery_generation`: accept, record
     new generation and new epoch, reject all subsequent
     `ReplicationItem`s at the old epoch (return `StaleEpoch`),
     return `FreezeWritesOk{received_lsn}`.
   - `recovery_generation == my_recovery_generation`: idempotent
     re-fence; return `FreezeWritesOk{received_lsn}` without
     re-mutation.
   - `recovery_generation < my_recovery_generation`: return
     `StaleRecoveryGeneration{my_recovery_generation}`. Coordinator
     reads CRD, observes a higher generation than its own, aborts
     and restarts the recovery from CRD's current state.

- **Quorum requirement**: ≥ W replicas must return `FreezeWritesOk`
  for the coordinator to proceed to the `applying` phase. Replicas
  that return `StaleRecoveryGeneration` indicate the coordinator is
  losing a race — abort and restart.
- **Timeout**: configurable, default 2 seconds per attempt.
- **Retry**: idempotent; coordinator retries on transient transport
  failures while observing the generation comparison rule.
- **Authority-quorum identification**: `authority_lsn = max(received_lsn
  from replicas that returned FreezeWritesOk at this generation)`.
  Replicas that did not respond OR returned `StaleRecoveryGeneration`
  are NOT included in `authority_lsn` computation; they go through
  per-replica reconciliation later.

(There is no separate `GetReceivedLsn` RPC. The fenced quorum's
`FreezeWrites` responses provide all `received_lsn` values needed
for authority computation.)

**UpdateEpoch RPC** (with truncation semantics):
```protobuf
message UpdateEpochRequest {
    int64 recovery_generation = 1;
    Epoch new_epoch = 2;
    int64 authority_lsn = 3;
}
```

On receipt, replicas execute the following sequence:
1. **Validate generation and detect idempotent retry**:
   - If `recovery_generation < my_recovery_generation`: return
     `StaleRecoveryGeneration` (coordinator must restart from
     FreezeWrites).
   - If `recovery_generation == my_recovery_generation` AND
     `current_epoch == new_epoch` AND
     `update_completed_at_generation == my_recovery_generation`:
     idempotent retry of an already-applied UpdateEpoch — return
     ACK without re-mutating.
   - Otherwise: proceed to step 1.5.

   Note: the idempotency check uses a separate
   `update_completed_at_generation` field (set to
   `my_recovery_generation` AFTER step 3 completes successfully).
   This is distinct from `current_epoch` and `pending_epoch`, and
   ensures that a replica which received FreezeWrites (advancing
   `pending_epoch` and `my_recovery_generation`) but has NOT yet
   processed UpdateEpoch does NOT short-circuit the truncate.

1.5. **Quiesce the StateProvider apply pipeline**: signal the
     `StateProvider`'s apply loop to pause. Wait for any in-flight
     `accept_op` calls to complete and ACK. Drain the pending op
     channel. The apply loop must remain paused through step 3.
     This prevents the "apply loop races callback" failure mode
     where the loop applies ops above `authority_lsn` after the
     callback has rewound state.

2. **Invoke user callback** (now safe — apply pipeline is quiesced):
   `StateProvider::on_authority_truncate(authority_lsn)` —
   ordering chosen so that if the callback panics, no on-disk state
   has been corrupted; the replica restarts cleanly from the
   pre-`UpdateEpoch` durable state and the recovery is retried.

3. **Atomic durable mutation** (single fsync):
   a. Truncate in-memory `ops` storage to LSN ≤ `authority_lsn`
   b. Reset `received_lsn` cursor to `min(received_lsn, authority_lsn)`
   c. Reset `applied_lsn` cursor to `min(applied_lsn, authority_lsn)`
   d. Activate the new epoch: `current_epoch := new_epoch`,
      `pending_epoch := new_epoch`,
      `update_completed_at_generation := recovery_generation`
   e. Persist the above to local WAL/metadata, fsync, atomic rename

4. **Resume the StateProvider apply pipeline** at the new
   `received_lsn` watermark.

5. **ACK to coordinator** with the durable state.

6. **Crash recovery semantics**: a replica restarting after a crash
   between step 2 and step 3 re-reads its durable state (still at
   the pre-`UpdateEpoch` epoch and the previous
   `update_completed_at_generation`). On the coordinator's retry,
   the idempotency check in step 1 fails (because
   `update_completed_at_generation < recovery_generation`), so the
   replica re-runs steps 1.5 → 4. The recovery completes
   idempotently. If the coordinator advanced `recovery_generation`
   in the meantime, the replica returns `StaleRecoveryGeneration`
   and the coordinator restarts from FreezeWrites.

**`on_authority_truncate` contract:**

The `StateProvider::on_authority_truncate(lsn)` callback **MUST be
infallible**. The implementation is permitted only the following
behaviors:
- Return successfully after rewinding any applied state above `lsn`
  (in-memory only is acceptable; durability is the framework's
  job in step 3).
- **Panic** if the rewind is impossible (e.g., the application has
  externalized state past `lsn` to a system that cannot be reverted,
  such as having sent emails or charged a credit card). A panic
  causes the replica process to crash; on restart the replica
  re-reads its pre-`UpdateEpoch` durable state, the cluster
  treats the replica as failed (per existing fault-handling), and
  if too many replicas panic the cluster enters quorum loss /
  data-loss recovery (`data_loss_number` bump).

The callback **MUST NOT**:
- Perform durable writes (the framework owns durability).
- Block indefinitely (a slow callback prevents recovery from
  completing; this is a `must-fix` operational concern, not a
  correctness issue).
- Invoke any Writer or replica RPC (deadlock risk).

For most workloads — where `applied_lsn ≤ received_lsn ≤ authority_lsn`
already (because fence-first prevents new ACKs after recovery starts) —
the callback is a no-op. It exists for defense-in-depth and for
state machines with eager-apply semantics that may have advanced
beyond `received_lsn`.

**Defense against silent "lying success":** the contract above is
not enforceable by the framework alone — a buggy callback could
return successfully without rewinding state, producing silent ghost
reads via `read_at(min_lsn=authority_lsn)`. To detect this, the
`StateProvider` trait includes a companion query method:

```rust
trait StateProvider {
    fn on_authority_truncate(&mut self, lsn: Lsn);

    /// Returns the highest LSN whose op has been applied to the
    /// in-memory state. Used by the framework as a debug-mode
    /// post-callback assertion: after on_authority_truncate(L)
    /// returns, current_state_lsn() MUST be ≤ L.
    fn current_state_lsn(&self) -> Lsn;
}
```

In debug builds (or when the replica is run with
`RUST_LOG=debug` / similar instrumentation), the framework asserts
`current_state_lsn() <= authority_lsn` after the callback returns.
A failed assertion panics the replica, surfacing the "lying
success" bug deterministically before it corrupts read-after-write
semantics.

In release builds the assertion can be elided for performance, at
the cost of relying on the StateProvider's correctness. Operators
deploying writer-based partitions in production should run with
the assertion enabled at least during initial rollout and on a
canary subset.

### Coordinator Unreachability (from Writer's perspective)

If the Writer cannot reach the coordinator:
- Writer continues writing using its current epoch (no immediate
  poison).
- After the configured retry budget elapses without coordinator
  contact, Writer poisons (treats as suspicion of takeover).
- This handles transient coordinator restarts (single-instance
  coordinator can be down for seconds during pod restart) without
  triggering unnecessary takeovers.

### Multi-Coordinator Coordination

If multiple coordinator pods are deployed (HA via replication):
- All coordinator pods watch the same CRD.
- Mutating actions (`recovery_phase` transitions) use Kubernetes
  optimistic concurrency (CRD `resourceVersion`) to prevent
  conflicting updates.
- A coordinator that fails to update the CRD due to version
  conflict reads the latest version and resumes.
- This is naturally race-free without a distinguished leader.

---

## Trade-Offs

### Advantages

1. **~50-67% latency reduction** — eliminates the primary hop; writes
   go directly from the user app to replicas in parallel.
2. **No failover stall** — pod failure is just "one fewer replica in
   quorum." As long as W pods are reachable, writes continue
   uninterrupted. No election, no promotion delay.
3. **Dramatically simpler operator** — no primary election, no
   switchover, no write revocation, no rollback.
4. **Simpler pod code** — no role distinction, no ChangeRole
   transitions, no AccessStatus gating. All replicas run identical
   code.
5. **Simpler StateProvider for users** — no ChangeRole callback,
   no role-aware logic needed in user services.

### Disadvantages

1. **Fat client** — the user app must link the Writer library, maintain
   gRPC connections to all N replica pods and to the coordinator.
2. **More connections** — app opens N+1 connections (N replicas + 1
   coordinator) instead of 1 to a primary.
3. **Coordinator dependency** — the Writer depends on a reachable
   coordinator for initial bootstrap and ongoing config events. If
   the coordinator is unreachable, the Writer can continue writing
   with its last known config but cannot react to membership changes.
   The Writer should enforce a **max staleness timeout** — if no
   coordinator heartbeat is received within a configurable window,
   the Writer should pause writes to avoid operating on an
   arbitrarily stale configuration.
4. **Partial writes** — if the Writer crashes after sending to some
   replicas but before quorum, those replicas have ops that never
   committed. Needs GC (expiry or rollback of uncommitted ops by
   the operator on epoch bump).
5. **Read-after-write requires app-level coordination** — without a
   primary, reads go to any replica. Application code must coordinate
   read-after-write semantics by tracking the last `Ok(lsn)` returned
   from `replicate()` and passing it to read RPCs (replicas implement
   `read_at(min_lsn)` that waits until `applied_lsn ≥ min_lsn`). See
   [Read Path](#read-path) for the recommended pattern.
   This is a different distribution of responsibility than the
   leader-based system (where the primary is implicitly the
   read-after-write authority); it is NOT a contradiction of the
   "no `committed_lsn` in the replication path" claim — `committed_lsn`
   is no longer needed for replication-side correctness, but
   application-side LSN tracking remains required for read-after-write.
6. **Single writer only** — this system is designed for exactly one
   Writer. The original proposal referred multi-writer workloads to the
   then-existing leader-based system, which has since been removed.

---

## Implementation Plan

The proposal placed all work in **new crates and a new operator**. It assumed
the classic operator/runtime would remain unchanged except for extracting
`QuorumTracker` to a shared crate. Those implementation-plan assumptions are
historical.

### Phase 1: Shared Components

Extract reusable pieces from `kuberic-core` into a shared crate
(`kuberic-common` or similar):
- `QuorumTracker` — quorum logic
- Proto definitions (`ReplicationItem`, `ReplicationAck`, copy RPCs)
- `ReplicationQueue` — in-memory op buffer
- `Epoch`, `Lsn`, `ReplicaId` types

The existing `kuberic-core` re-exports from the shared crate — no
behavioral change.

### Phase 2: Roleless Replica Runtime (`kuberic-writer-runtime`)

New crate for roleless pods:
- `Write` RPC accepting `ReplicationItem` streams, returning
  `ReplicationAck` streams
- Epoch validation (reuse `SecondaryReceiver::accept_item()` logic)
- Local `ReplicationQueue` / WAL storage
- `StateProvider` trait (Open/Close/Abort — no ChangeRole)
- `OperationStream` for user-side op application

### Phase 3: Writer Library (`kuberic-writer`)

New crate:
- `CoordinatorClient` — bidirectional gRPC stream to coordinator
- gRPC connection pool to all replicas
- `QuorumTracker` (from shared crate)
- `ReplicationQueue` (from shared crate, for build replay)
- `next_lsn` atomic counter
- `Writer::replicate()` — user-facing API with `InDoubt` /
  `WriterPoisoned` error variants
- Bounded retry loop, epoch-staleness detection, poison transition
- React to `ConfigEvent`s: add/remove replicas, epoch changes,
  build coordination (replay from queue on `AddReplica`)

### Phase 4: New Operator with Coordinator (`kuberic-writer-op`)

New operator binary with new CRD (`KubericWriterPartition`):
- Replica set membership management
- Epoch and `recovery_generation` management
- Copy source selection (pick replica with highest `received_lsn`)
- **Authority recovery on Writer poison/disconnect** (full
  fence-first-then-query protocol):
  - Advance `recovery_generation` + epoch in CRD (atomic via
    `resourceVersion` optimistic concurrency)
  - Push `FreezeWrites{generation, new_epoch}` to all replicas;
    require ≥W ACKs (= fenced quorum)
  - Compute `authority_lsn = max(received_lsn from fenced quorum)`
  - Push `UpdateEpoch{generation, new_epoch, authority_lsn}` to
    all replicas (atomic truncate-above on receipt)
  - Per-replica reconciliation for unfenced/lagging replicas
    (force `UpdateEpoch` before serving; trigger build if lagging)
  - All steps idempotent under coordinator restart via
    `(recovery_generation, recovery_phase)` CRD state
- **Coordinator gRPC service** — `WriterSession` RPC with
  bidirectional streaming for Writer communication
- Writer liveness detection (stream disconnect or `WriterPoisoned`
  message → authority recovery + new Writer)
- Pod health monitoring and membership updates
- Build coordination with `build_id` and synchronous ACK protocol

### Phase 5: Example Application

Build a kvstore example (`examples/kvstore-writer/`) using the
Writer library end-to-end: embedded Writer, coordinator, roleless
replicas.

---

## Open Questions

1. ~~**Read consistency**~~ — **Resolved** in
   [Read Path](#read-path) section: Option (a), application threads
   `Ok(lsn)` through `replica.read_at(min_lsn)`. After-restart
   bound via `Writer::current_authority()`. Stale reads available
   via the basic `read()` for tolerant workloads.

2. **Writer takeover speed:** When the coordinator detects Writer
   poison or disconnect, how quickly can it run authority recovery
   and start a new Writer? This determines write-availability gap
   on transient failures. Target: < 1 second (2 RTT for fence +
   update_epoch, plus per-replica reconciliation if needed).

3. ~~**Coordinator availability**~~ — **Partially resolved** in
   [Coordinator Failure Modes](#coordinator-failure-modes) and
   [Coordinator Unreachability](#coordinator-unreachability-from-writers-perspective):
   Writer continues during transient coordinator outage, then
   poisons after retry budget. Open: tunable per workload, default
   value selection.

4. ~~**Write-ahead for crash safety**~~ — **Resolved** in
   [Replica Durability Contract](#replica-durability-contract):
   persisted mode is required for the In-Doubt Writer Contract;
   replicas MUST not ACK until durably persisted. Volatile mode is
   incompatible with the contract's correctness guarantees.

5. **Migration path:** Can a workload migrate from the existing
   leader-based system to the new Writer-based system? This would
   require a coordinated cutover: drain the old primary, start the
   new Writer with the current data state and LSN.

---

## Known Issues

Issues identified during design review (SoT premortem analysis) that
require further design work before implementation.

### KI-1: Dual-Writer Window During Takeover (inherited, not new)

When the old Writer dies and a new one starts, there is a window
where epoch propagation to replicas is not atomic. A partitioned-
but-alive "zombie" Writer could still send items to replicas that
haven't yet received the epoch bump.

**This was not a new problem relative to the historical baseline.** The former
leader-based system also needed
old-primary isolation while durable failover applies a new epoch, commits the
new authority, and converges retained secondaries. Unverifiable convergence
fails closed rather than becoming a successful stable snapshot.

The historical baseline accepted this because:
- Epoch propagation is **in-cluster** (operator → replicas), so it's
  fast (milliseconds)
- Once a replica receives the epoch bump, it truncates uncommitted
  ops from the old epoch (`secondary.rs:74-85`)
- The old primary's writes to not-yet-bumped replicas are
  **uncommitted** — they'll be truncated when the epoch arrives
- No committed data is corrupted — only uncommitted in-flight ops
  are at risk, and they're rolled back

The Writer-based system inherits the same model — the coordinator
pushes `update_epoch()` to replicas in-cluster (same path, same
speed). With the Writer also in-cluster (per scope), its stale
writes are still uncommitted and will still be truncated when the
epoch arrives at affected replicas.

**Mitigated by the Writer Lifecycle Protocol** (see section 2,
Coordinator): graceful shutdown sends `WriterShutdown` before
disconnect, so the coordinator skips fencing on clean restarts.
Crash restarts were designed to trigger an immediate `writer_epoch` bump,
matching the former system's failover.

**Lease considered and not adopted.** A lease would add crash
recovery latency (must wait for expiry) without meaningful safety
improvement — epoch fencing already truncates uncommitted ops.

**Severity: consider** (inherited from the historical baseline, not a new
risk). Improvements (lease, synchronous fence token) would benefit
both systems equally and can be designed as a shared enhancement.

### KI-2: Writer State Recovery During Build (inherited, simple fix)

The Writer's `ReplicationQueue` is in-memory. If the Writer crashes
during a replica build, the queue is lost and the zero-gap invariant
is broken for the in-progress build.

**This matched the historical baseline.** The former primary also had
an in-memory `ReplicationQueue` (`queue.rs`). If the primary crashed
during a build, the queue was lost. The former system handled this
by aborting the in-progress build: the operator detected primary
failure, triggers durable failover, and the new primary restarts the build
from scratch through durable add/rebuild. The partially-built secondary is
closed and rebuilt.

**The Writer system does the same thing:**
1. Writer crashes → coordinator detects stream disconnect
2. Coordinator bumps `writer_epoch`, aborts any in-progress build
3. New Writer connects, gets current config
4. Operator restarts the build from scratch (new copy from a replica)

**Why "abort and retry" is acceptable:**
- Builds are infrequent (scale-up, pod restart)
- A crash during a build is even rarer
- Copy is the expensive part, and it's idempotent — restarting just
  repeats the copy. No data corruption risk.
- The alternative (Writer-side WAL or replica-side query) adds
  significant complexity for a rare edge case

**Severity downgraded to consider.** The proposal treated the former system's
behavior as precedent. Its recommended approach was to abort builds on Writer
crash because that matched the historical baseline without new infrastructure.

The more complex solutions (Writer-side WAL, replica-side ops query)
remain available as future optimizations if build frequency or build
size makes retry unacceptable.

### KI-3: Read Path Design (resolved)

Resolved in [Read Path](#read-path) section. Recommended pattern:
application threads `Ok(lsn)` from `replicate()` into
`replica.read_at(min_lsn)`; after restart, use
`Writer::current_authority()` to bound reads. Stale reads available
via the basic `read()` for tolerant workloads.

### KI-4: Unbounded ReplicationQueue Growth (should-fix, simple mitigation)

During a replica build with GC deferred (`BuildInProgress`), or when
the coordinator is unreachable (no `BuildComplete` to resume GC), the
Writer's in-memory queue grows without bound. Since the Writer is
embedded in the user app, this can cause OOM in the user process.

**The historical baseline had this too** — the primary's in-memory
`ReplicationQueue` grew during builds. But the primary ran in a
dedicated pod with Kubernetes resource limits, so OOM only kills the
pod (which triggers failover). The Writer runs in the user's
process, where OOM is more damaging.

**Recommended mitigation: build timeout + queue size limit.**

1. **Build timeout:** The Writer starts a timer on `BuildInProgress`.
   If `BuildComplete` doesn't arrive within a configurable window
   (e.g., 5 minutes), the Writer notifies the coordinator that the
   build has timed out. The coordinator aborts the build and resumes
   GC. The build can be retried later. This matches the existing
   system's behavior — `build_replica` has a 30-second timeout
   (`copy.rs:23`), and the operator can retry.

2. **Queue size limit:** The Writer enforces a maximum queue size
   (configurable, e.g., 10,000 ops or 100MB). If the limit is
   reached during a build, `replicate()` returns an error
   (`QueueFull`) rather than OOMing. The user app can decide whether
   to retry, drop the write, or shut down cleanly. This is better
   than a silent OOM crash.

These are configuration options on the `Writer`, not protocol
changes. The coordinator doesn't need to know about them.

---

- Voldemort client-side routing:
  `PipelineRoutedStore.java`, `PerformParallelPutRequests`
  [source](https://github.com/voldemort/voldemort/blob/master/src/java/voldemort/store/routed/PipelineRoutedStore.java)
- Voldemort design doc (archived):
  [project-voldemort.com/voldemort/design.html](https://web.archive.org/web/2018/http://www.project-voldemort.com/voldemort/design.html)
- Current kuberic replication:
  - `kuberic-core/src/handles.rs:162-186` — `replicate()` API
  - `kuberic-core/src/replicator/actor.rs:312-336` — actor data path
  - `kuberic-core/src/replicator/primary.rs:171-193` — `send_to_all()`
  - `kuberic-core/src/replicator/quorum.rs` — `QuorumTracker`
  - `kuberic-core/proto/kuberic.proto:134-160` — data plane proto
- Leaderless quorum survey:
  [docs/background/leaderless-quorum-replication.md](../../../background/leaderless-quorum-replication.md)
