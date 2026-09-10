# Kuberic: User Application Model

Event-based API for building stateful services on Kuberic. The user
creates the state provider channel and replicator at Open time, keeps
`ServiceContext` handles, and interacts with the replicator via
`StateReplicatorHandle`.

> Part of the [Kuberic Design](../kuberic-replicator-design.md).
> Separation design: [Runtime–Replicator Separation](implemented/runtime-replicator-separation.md).
> State provider design: [State Provider Separation](implemented/state-provider-separation.md).
> Control design: [Pod-Local RA-Lite Boundary](implemented/pod-local-ra-lite-control-boundary.md).

---

## Overview

The runtime delivers `LifecycleEvent` on an mpsc channel. The critical
event is **Open**, where the user creates the state provider channel and
replicator, passes the state provider sender to the replicator, and returns
a `ReplicatorHandle` to the runtime. The user keeps `ServiceContext` with
the write handle, partition, and streams. State provider events arrive on
the user-created unbounded channel.

---

## Open Flow (User Creates Replicator)

```
Runtime ──── LifecycleEvent::Open { ctx: OpenContext } ────► User
                                                               │
                                                    1. Create unbounded state provider channel
                                                    2. WalReplicator::create(..., sp_tx)
                                                    3. Keep ServiceContext + sp_rx
                                                    4. Return ReplicatorHandle
                                                               │
Runtime ◄──── Ok(ReplicatorHandle) ────────────────────────────┘
```

The user chooses which replicator implementation to use. Currently
`WalReplicator` is the only implementation (WAL-based persisted quorum).

---

## Lifecycle Events

```rust
pub enum LifecycleEvent {
    Open {
        ctx: OpenContext,
        reply: oneshot::Sender<Result<ReplicatorHandle>>,
    },
    ChangeRole {
        new_role: Role,
        reply: oneshot::Sender<Result<String>>,
    },
    Close { reply: oneshot::Sender<Result<()>> },
    Abort,
}
```

| Event | When | What to do |
|-------|------|------------|
| `Open` | Pod startup | Create replicator, keep handles, return `ReplicatorHandle` |
| `ChangeRole` | Role change | Start/stop background work based on role |
| `Close` | Graceful shutdown | Drain in-flight work, checkpoint, reply |
| `Abort` | Emergency shutdown | Release resources, no reply |

---

## OpenContext and ServiceContext

```rust
/// Provided by the runtime at Open.
pub struct OpenContext {
    pub replica_id: ReplicaId,
    pub open_mode: OpenMode,
    pub data_bind: String,
    pub token: CancellationToken,
    pub fault_tx: mpsc::Sender<FaultType>,
}

/// Produced by WalReplicator::create(). User keeps these.
pub struct ServiceContext {
    pub replicator: StateReplicatorHandle,
    pub partition: Arc<PartitionHandle>,
    pub copy_stream: Option<OperationStream>,
    pub replication_stream: Option<OperationStream>,
}
```

> **Note:** `ServiceContext` does not contain `state_provider_rx` — the user
> creates the state provider channel before calling `WalReplicator::create()`
> and keeps the receiver directly. Operation streams remain in `ServiceContext`
> because they are replicator outputs (data flowing to the user).

---

## State Provider Callbacks

The user creates an unbounded state provider channel and passes the sender
to `WalReplicator::create()`. The replicator sends events when it needs
cooperation from the user's state. The user handles them in a
`tokio::select!` alongside lifecycle events, or in a dedicated handler
function.

```rust
// User creates the channel
let (sp_tx, sp_rx) = mpsc::unbounded_channel::<StateProviderEvent>();

// User passes sender to replicator
let (handle, svc_ctx) = WalReplicator::create(
    ctx.replica_id, &ctx.data_bind, ctx.fault_tx.clone(), sp_tx,
).await?;
// User keeps sp_rx, handles events in their event loop
```

`WalReplicator::create()` applies a 5-second deadline independently to each
replication operation and catch-up quorum wait. If quorum is not observed by
then, the operation returns `KubericError::NoWriteQuorum`. To configure the
bound, use:

```rust
let options = WalReplicatorOptions::new().quorum_timeout(Duration::from_secs(10));
let (handle, svc_ctx) = WalReplicator::create_with_options(
    ctx.replica_id, &ctx.data_bind, ctx.fault_tx.clone(), sp_tx, options,
).await?;
```

The runtime control reply timeout is separate and should exceed the quorum
deadline when remote catch-up callers need to observe `NoWriteQuorum`.

The state provider callback surface remains:

```rust
pub enum StateProviderEvent {
    UpdateEpoch { epoch, previous_epoch_last_lsn, reply },
    GetLastCommittedLsn { reply },
    GetCopyContext { reply },
    GetCopyState { up_to_lsn, copy_context, reply },
    OnDataLoss { reply },
}
```

| Event | Sender | When | Role |
|-------|--------|------|------|
| `UpdateEpoch` | Replicator | Reconfiguration | Secondary |
| `GetLastCommittedLsn` | Replicator | Build / catchup | Secondary |
| `GetCopyContext` | Replicator | build_replica | Idle (new) |
| `GetCopyState` | Replicator | build_replica | Primary |
| `OnDataLoss` | Replicator | Quorum loss | New Primary |

---

## User Event Loop

```rust
async fn run_service(
    mut lifecycle_rx: mpsc::Receiver<LifecycleEvent>,
    state: SharedState,
    client_bind: String,
) {
    let mut replicator = None;
    let mut state_provider_rx = None;

    loop {
        tokio::select! {
            biased; // Lifecycle takes priority

            Some(event) = lifecycle_rx.recv() => match event {
                LifecycleEvent::Open { ctx, reply } => {
                    // User creates state provider channel
                    let (sp_tx, sp_rx) = mpsc::unbounded_channel();
                    // User creates replicator, passes sp_tx
                    let (handle, svc_ctx) = WalReplicator::create(
                        ctx.replica_id, &ctx.data_bind, ctx.fault_tx.clone(),
                        sp_tx,
                    ).await.unwrap();
                    replicator = Some(svc_ctx.replicator);
                    state_provider_rx = Some(sp_rx);
                    // ... store partition, streams, token ...
                    let _ = reply.send(Ok(handle));
                }
                LifecycleEvent::ChangeRole { new_role, reply } => {
                    // start/stop work
                    let _ = reply.send(Ok(String::new()));
                }
                LifecycleEvent::Close { reply } => {
                    let _ = reply.send(Ok(()));
                    break;
                }
                LifecycleEvent::Abort => break,
            },

            Some(event) = async {
                match state_provider_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                handle_state_provider_event(event, &state).await;
            },

            else => break,
        }
    }
}

/// State provider handler — standalone function, easy to swap for
/// different state provider implementations.
async fn handle_state_provider_event(event: StateProviderEvent, state: &SharedState) {
    match event {
        StateProviderEvent::UpdateEpoch { previous_epoch_last_lsn, reply, .. } => {
            rollback_uncommitted(previous_epoch_last_lsn);
            let _ = reply.send(Ok(()));
        }
        // ... handle other state provider events ...
        _ => {}
    }
}
```

**Key patterns:**
- The user creates the state provider channel and passes the sender to the replicator
- State provider events are handled in a standalone function (`handle_state_provider_event`)
- The replicator forwards `UpdateEpoch`/`OnDataLoss` from the runtime through the state provider channel
- The copy protocol runs entirely inside the replicator (not the runtime)

---

## PodRuntimeBundle

```rust
pub struct PodRuntimeBundle {
    pub runtime: PodRuntime,
    pub lifecycle_rx: mpsc::Receiver<LifecycleEvent>,
    pub control_address: String,
}
```

`data_address` is no longer in the bundle — it comes from the replicator
created at Open time (`ReplicatorHandle::data_address()`).

---

## Pod-Local Control Boundary

`PodRuntimeBuilder::build()` starts both `ReplicaAgent` and `PodRuntime`
without changing `PodRuntimeBundle`. The control listener hosts
`ReplicatorControl` (`GetStatus` and `ExecuteCorrelatedControlAction`) plus the
internal `ReplicaLifecyclePeer` service. Only effect commands from the local
agent reach the runtime.

`ReplicaHandle::execute_correlated_control_action()` is the sole public
operator mutation method. Its request includes required protocol version 3,
target Pod UID, agent generation, agent control version, observed runtime
epoch, deterministic action ID/signature, and the coarse
`DurableReplicaAction`.
Individual mutation RPCs and `execute_durable_action()` are retired.

`AddReplicaIntent` and `RemoveReplicaIntent` are the compound actions. Each is
addressed to the exact current primary and freezes its own identities,
configuration semantics, safety constraints, and deadlines. Their primary
coordinators are transient; CRD status remains the durable authority.

The internal lifecycle-peer protocol is version 2. Add/build uses typed
Prepare/Activate/Cleanup stages; removal uses typed Retire. Sharing the peer
transport and fencing primitives does not merge the workflows or expose peer
control to user services. `ReplicaLifecyclePeer` is not an application API.

The agent serializes correlated actions and keeps status readable while an
effect runs. It does not change the lifecycle ordering below: `PodRuntime`
remains the sole caller of service and replicator callbacks.

---

## ChangeRole Ordering

| Direction | Transitions | Order |
|-----------|-------------|-------|
| **Promotion** | None→Primary, Idle→Active, *→Primary | 1. Replicator  2. Status  3. User |
| **Demotion** | P→S, Active→None, *→None | 1. Status  2. User  3. Replicator |

On promotion, reads may start after the role callback, but writes return
`ReconfigurationPending` until the runtime successfully installs the first
catch-up or current configuration. This prevents a newly promoted actor from
committing against an unconfigured zero quorum.
On demotion, writes are fenced before the user is notified.

---

## Secondary Data Delivery (Pull Model)

Aligned with the SF `IStateReplicator` pattern (verified against the
[kvmap sample](https://github.com/youyuanwu/service-fabric-apps-rs/blob/main/crates/samples/kvmap/src/kvmap.rs)).
The user pulls operations from streams and calls `acknowledge()` to release.

### Interfaces

```rust
pub struct Operation {
    pub lsn: Lsn,
    pub data: Bytes,
    ack_tx: Option<oneshot::Sender<()>>,
}

impl Operation {
    /// Mandatory for persisted replicators — blocks subsequent ops until called.
    pub fn acknowledge(mut self) {
        if let Some(tx) = self.ack_tx.take() { let _ = tx.send(()); }
    }
}

pub struct OperationStream {
    rx: mpsc::Receiver<Operation>,
}

impl OperationStream {
    /// Returns None when stream ends.
    pub async fn get_operation(&mut self) -> Option<Operation> {
        self.rx.recv().await
    }
}
```

### Data Flow

`Acknowledge()` semantics depend on the replicator mode (matching SF):

**Persisted mode** (`require_service_ack = true`, default):
```
Primary                    Secondary Replicator           Secondary User
   │                            │                              │
   │── ReplicationItem ────────►│                              │
   │                            │  1. Validate epoch           │
   │                            │  2. Queue in replicator      │
   │                            │  3. User pulls               │
   │                            │◄── get_operation() ─────────│
   │                            │──── Operation(lsn, data) ───►│
   │                            │                        4. apply + persist
   │                            │◄──── acknowledge() ─────────│
   │                            │  5. ACK to primary           │
   │◄── ACK ───────────────────│   (quorum gated on ack)      │
   │  replicate() completes     │  6. Release from queue       │
```

**Volatile mode** (`require_service_ack = false`):
```
Primary                    Secondary Replicator           Secondary User
   │                            │                              │
   │── ReplicationItem ────────►│                              │
   │                            │  1. Validate epoch           │
   │                            │  2. Queue + auto-ACK         │
   │◄── ACK ───────────────────│   (quorum on receive)        │
   │  replicate() completes     │                              │
   │                            │  3. User pulls (async)       │
   │                            │◄── get_operation() ─────────│
   │                            │──── Operation(lsn, data) ───►│
   │                            │                        4. apply
   │                            │◄──── acknowledge() ─────────│
   │                            │  5. Release from queue       │
```

**Our default is persisted mode** — `acknowledge()` gates quorum. This
gives the strongest durability guarantee: primary's `replicate()` returns
only after a quorum of secondaries have applied + acknowledged.

### Stream Usage by Role

**`ChangeRole(IdleSecondary)`** — drain copy stream:

```rust
if let Some(copy) = copy_stream.take() {
    tokio::spawn(drain_stream(state.clone(), copy, token.clone(), "copy"));
}
```

**`ChangeRole(ActiveSecondary)`** — drain replication stream:

```rust
if let Some(repl) = replication_stream.take() {
    tokio::spawn(drain_stream(state.clone(), repl, token.clone(), "replication"));
}
```

**`ChangeRole(Primary)`** — user writes via `replicate()`:

```rust
let data = serde_json::to_vec(&("key", "value")).unwrap();
let lsn = replicator.replicate(Bytes::from(data), token).await?;
```

### Copy Protocol (StateProvider Callbacks)

The copy stream is produced by a bidirectional exchange between the
primary's and secondary's state provider callbacks:

```
Secondary (idle)              System                 Primary
      │                         │                      │
      │◄── GetCopyContext ─────│                      │
      │──── stream(my_lsn) ───►│                      │
      │                         │── GetCopyState ─────►│
      │                         │   (up_to_lsn,        │
      │                         │    context_stream)    │
      │                         │◄── stream(state) ────│
      │◄── copy_stream ───────│                      │
      │    (apply + ack)        │                      │
```

**`GetCopyContext`** (secondary): returns a stream with the secondary's
current LSN. Primary uses this to know what the secondary already has.

**`GetCopyState`** (primary): receives `up_to_lsn` + copy context stream.
Reads the secondary's LSN from context, produces only the missing state.

Example from the KV app:

```rust
// GetCopyContext (secondary) — send current LSN
StateProviderEvent::GetCopyContext { reply } => {
    let lsn = state.read().await.last_applied_lsn;
    let (tx, stream) = OperationStream::channel(1);
    let _ = tx.send(Operation::new(0, Bytes::from(lsn.to_string()), None)).await;
    drop(tx);
    let _ = reply.send(Ok(stream));
}

// GetCopyState (primary) — read context, produce state
StateProviderEvent::GetCopyState { up_to_lsn, mut copy_context, reply } => {
    let peer_lsn = if let Some(op) = copy_context.get_operation().await {
        String::from_utf8_lossy(&op.data).parse::<i64>().unwrap_or(0)
    } else { 0 };

    // Snapshot under read lock, then send without lock
    let snapshot = { /* collect state */ };
    let (tx, stream) = OperationStream::channel(64);
    let _ = reply.send(Ok(stream));
    for (k, v) in snapshot { /* send ops */ }
    drop(tx);
}
```

### Boundary Between Copy and Replication

- `up_to_lsn` = primary's committed LSN at build start
- Copy: `[0, up_to_lsn]` — full state
- Replication: `(up_to_lsn, ∞)` — incremental ops
- Replicator buffers ops > `up_to_lsn` during copy, delivers after

### Backpressure

**Persisted mode:** Slow `acknowledge()` directly slows primary writes
(quorum waits for ack). This is by design — it prevents the primary from
outrunning the secondaries. If a secondary is too slow, the operator should
replace it.

**Volatile mode:** Slow `acknowledge()` → ops queue in replicator → no
quorum impact → memory grows → configurable limit → health warning or
rebuild.
