# ADR: State Provider Separation from Replicator

> **Status:** Archived — implemented in the removed classic v1 stack
> **Date:** 2026-04-11

Separated the state provider from the replicator by moving channel
ownership to the user. The user creates the state provider channel and
passes the sender to `WalReplicator::create()`. The replicator holds
the sender and forwards events. The copy protocol moved from the runtime
into the replicator.

---

## Problem

`WalReplicator::create()` internally created `state_provider_tx/rx` and
distributed them via `ReplicatorHandle` (tx) and `ServiceContext` (rx).
This coupled the runtime to the state provider (runtime called
`handle.state_provider_tx()` for the copy protocol) and prevented
user-controlled state provider lifecycle.

---

## Decision: User-Owned Channel

### Why the User Creates the Channel

Matches SF's `CreateReplicator(IStateProvider*)` — the user creates
the state provider and passes it in. The user decides buffer size,
lifecycle, and can wrap/proxy the channel.

### Why Channels (Not Traits)

Consistent with all other kuberic boundaries:

| Boundary | Channel | Direction |
|----------|---------|-----------|
| Runtime → User | `LifecycleEvent` | runtime sends, user receives |
| Runtime → Replicator | `ReplicatorControlEvent` | runtime sends, replicator receives |
| User → Replicator | `ReplicateRequest` | user sends, replicator receives |
| Replicator → User | `StateProviderEvent` | replicator sends, user receives |

A trait would break this consistency. Channels also allow the state
provider to have its own event loop (SQLite, RocksDB) without forcing
`Arc<RwLock<>>` on user state.

### Unbounded Channel

`mpsc::unbounded_channel()` for state provider events, matching the
`PrimarySender` pattern. Event rate is extremely low (a few per
reconfiguration). The actor uses synchronous `send()` — never blocks.

### Copy Protocol Moved Into Replicator

`run_build_replica_copy()` moved from `pod.rs` into `replicator/copy.rs`.
The actor's `BuildReplica` handler spawns a copy task with a clone of
`state_provider_tx`. The runtime just sends `BuildReplica` and waits.

### Actor Forwarding

The actor forwards `UpdateEpoch` and `OnDataLoss` to the user via
`state_provider_tx`, inline in the actor loop (blocks the pump — matches
SF). Copy runs as a spawned task (non-blocking).

| Control Event | Actor Behavior | Why |
|--------------|----------------|-----|
| `UpdateEpoch` | Forward inline, await reply | Correctness: must complete before next event |
| `OnDataLoss` | Forward inline, await reply (`bool→DataLossAction`) | Runtime needs the answer |
| `BuildReplica` | Spawn copy task | Long-running, must not block ChangeRole/Close |

### Error Handling

Actor replies `Err` on the control event's oneshot — no redundant
`fault_tx` reports. The runtime receives errors directly and decides
the response.

### Sender Ownership

| Holder | Events Sent | Lifecycle |
|--------|-------------|-----------|
| WalReplicatorActor | `UpdateEpoch`, `OnDataLoss` | Replica lifetime |
| SecondaryReceiver | `GetCopyContext`, `GetLastCommittedLsn` | Replica lifetime |
| Copy task (spawned) | `GetCopyState` | Single copy operation |

---

## Key Changes

| Component | Change |
|-----------|--------|
| `WalReplicator::create()` | Takes `UnboundedSender<StateProviderEvent>` parameter |
| `ReplicatorHandle` | Removed `state_provider_tx` field |
| `ServiceContext` | Removed `state_provider_rx` field |
| `replicator/copy.rs` | New — copy protocol moved from `pod.rs` |
| `pod.rs` | `BuildReplica` sends to replicator and waits |
| Actor | Forwards `UpdateEpoch`/`OnDataLoss` via `state_provider_tx` |
| KV service | Creates `unbounded_channel()`, passes tx, handles events via `handle_state_provider_event()` |

---

## Alternatives Considered

- **Trait-based** (`Arc<dyn StateProvider>`) — rejected for consistency
  with channel-based APIs. Would require `Send + Sync + 'static` and
  `Arc<RwLock<>>` on user state.
- **Replicator-owned channel** (status quo) — rejected because runtime
  should not touch the state provider, and user should control lifecycle.
