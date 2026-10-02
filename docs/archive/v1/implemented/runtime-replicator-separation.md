# ADR: Runtime–Replicator Separation

> **Status:** Archived — implemented in the removed classic v1 stack
> **Date:** 2026-04-11

Separated the replicator data plane from the runtime, aligning with
SF's architecture where the replicator is an independent component
created by the user service and driven by the runtime via channels.

---

## Problem

`PodRuntimeBuilder::build()` created the replicator actor, data plane
gRPC server, and runtime command loop as one monolithic bundle. This
coupled the runtime to replicator internals (streams, PartitionState,
state_provider_tx) and prevented pluggable replicator implementations.

---

## Decision: Channel-Based Separation

### Why Channels, Not Traits

The runtime doesn't own the replicator — the user creates it at Open
and returns a channel handle. A trait object (`Box<dyn StateReplicator>`)
implies ownership and creates mutability questions. Channels are:

1. **Symmetric** — same pattern as runtime → user (both use mpsc + oneshot)
2. **Ownership-free** — `mpsc::Sender` is Clone + Send, no `&self`/`&mut self`
3. **Already proven** — `ReplicatorControlEvent` + `control_tx` was the existing internal pattern
4. **Pluggable** — different replicators = different event loops behind the same channel

### Why User Creates Replicator

SF's pattern: user service creates and returns the replicator from Open.
This enables pluggable replicator implementations (WAL-based, volatile,
log-shipping) without the runtime knowing which one is used.

### PartitionState Split-Writer Invariant

`PartitionState` is shared via `Arc` between runtime and replicator:

| Atomic field | Writer | Reader |
|-------------|--------|--------|
| `read_status`, `write_status` | Runtime (`set_status_for_role`) | User (`PartitionHandle`) |
| `current_progress`, `catch_up_capability` | Replicator actor | Runtime (`GetStatus`) |
| `committed_lsn` | Replicator actor + PrimarySender + SecondaryReceiver | Runtime (`GetStatus`) |

`set_status_for_role` must be synchronous (atomic write, not channel
round-trip) — it's a fencing operation in the ChangeRole ordering.

### StateProviderEvent Routing

All state provider interactions route through the replicator, matching
SF's `ComReplicator` → `IStateProvider` relationship:

```
Runtime ──► Replicator ──► StateProvider (user)
```

The runtime never holds `state_provider_tx`.

### ChangeRole Ordering

| Direction | Order |
|-----------|-------|
| Promotion (→Primary, Idle→Active) | 1. Replicator  2. Status  3. User |
| Demotion (P→S, *→None) | 1. Status  2. User  3. Replicator |

---

## Key Types

| Type | Owner | Purpose |
|------|-------|---------|
| `ReplicatorHandle` | Runtime | Channel handle: `control_tx`, `state`, `shutdown`, `data_address` |
| `ServiceContext` | User | Write handle, partition, streams, `state_provider_rx` |
| `OpenContext` | Passed at Open | `replica_id`, `open_mode`, `data_bind`, `token`, `fault_tx` |

---

## Alternatives Considered

- **Trait-based** (`Box<dyn StateReplicator>`) — rejected due to
  ownership/mutability issues and inconsistency with existing channel patterns
- **Runtime-created replicator** (user receives handles passively) —
  rejected to enable pluggable replicator implementations

---

## Future Replicator Implementations

| Replicator | Use Case | Difference from WalReplicator |
|------------|----------|-------------------------------|
| `VolatileReplicator` | In-memory state | No WAL, ACK on receive |
| `LogShippingReplicator` | SQLite WAL frames | Ships WAL frames, not app ops |
| `ExternalReplicator` | Custom replication | Delegates to external system |
