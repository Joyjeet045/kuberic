# kuberic-runtime

Application and replication runtime for the level-triggered Kuberic stack.

## Service Fabric V1 interfaces and ownership

The implemented public traits preserve the **V1 COM divisions**, rather than
adding engine or agent operations to the application callbacks. This is an
implemented subset of the complete V1 partition contract: Begin/End pairs
become async Rust methods and Abort remains synchronous. The authoritative
definitions are
`FabricRuntime.idl:495–539,577–633,687–758`.

| Rust interface | Methods |
|---|---|
| `StatefulServiceReplica` | `open`, `change_role`, `close`, `abort` |
| `Replicator` | `open` → replication address, `change_role(epoch, role)`, `update_epoch`, `close`, `abort`, `current_progress`, `catch_up_capability` |
| `PrimaryReplicator: Replicator` | `on_data_loss`, `update_catch_up_replica_set_configuration`, `wait_for_catch_up_quorum`, `update_current_replica_set_configuration`, `build_replica`, `remove_replica` |
| `StateReplicator` | `replicate(operation_data)` → committed LSN, `get_replication_stream`, `get_copy_stream`, `update_replicator_settings` |
| `StateProvider` | `update_epoch(epoch, previous_epoch_last_lsn)`, `last_committed_lsn`, `get_copy_context`, `get_copy_state`, `on_data_loss` |

`StatefulServicePartition` provides partition information, independent read
and write status, `CreateReplicator`, load reporting, and fault reporting.
The agent owns the access and report source of truth rather than inferring it
from the replica role.

Service Open receives a `StatefulServicePartition` in its `OpenContext`.
The service selects a `ReplicatorFactory` with `partition.with_factory(...)`,
then calls `create_replicator(state_provider, settings)`. The result contains
the control and state-replicator interfaces; the optional primary interface is
the explicit Rust counterpart of querying `IFabricPrimaryReplicator`.

```text
kuberic-agent::PodRuntime ── Open(partition) ──► StatefulServiceReplica
                                                │
                                                ├─ CreateReplicator(StateProvider, settings)
                                                ├─ retains StateReplicator
                                                ├─ consumes copy/replication streams
agent hosting ◄──────── returns Replicator ─────┘
     │
     └─ opens and drives exactly the returned control interface
```

Choose the built-in implementation with
`DefaultReplicatorFactory::new(storage)`, where `storage` implements the
non-COM `engine::DurableState` persistence adapter. The state provider may be a
separate object. Custom factories use the same partition boundary and need not
use the default engine or implement `DurableState` on the service.
`CreateReplicator` is the construction boundary: the default factory creates
one shared `DefaultReplicatorInner`, and the control, primary, state, and
agent-managed façades all reference that inner. The managed façade is
transferred directly to agent registration and is never returned in the
application-visible interface bundle. `PodRuntime` owns the
hosting registrar, application lifetime, Open registration, effect ordering,
and exact returned-interface identity; it does not preconstruct unused default
replication state for a custom factory. `PodRuntime` and that hosting registrar
now live in `kuberic-agent`, not this application-facing crate.
There is no `PodRuntime::new_with_replicator` ownership shortcut.
Services can observe `partition.get_write_status()`; custom factories receive
the same access gate through `ReplicatorFactoryContext::write_status`.
Custom implementations must honor that gate rather than infer write access
from the Primary role.
The built-in engine fences pending writes whenever access changes away from
`Granted`, including `NoWriteQuorum`, while preserving the admitted epoch and
configuration so returning quorum can restore access non-destructively.
During failover it records only the controller-selected election-safe prefix
under the new authority fence; a replica cannot reuse an arbitrary
previous-epoch suffix as verified progress.

`ReplicatorFactoryContext` exposes stable identity and partition-access
capabilities, not a concrete runtime or default-engine pointer. Application
and custom-factory code constructs only the SF-shaped interface bundle through
`ReplicatorInterfaces::secondary` or `ReplicatorInterfaces::primary`. The
primary constructor derives the control and primary views from the same
allocation, matching SF's coherent interface-query invariant. The default
implementation's managed data-plane bridge is transferred through an
unforgeable unpublished agent/runtime registration boundary; custom
replicators own their transport independently.

Service Fabric custom implementations return a custom control object from
Open. Kuberic deliberately uses a Rust factory wrapper so creation can reserve
and register one coherent interface bundle before Open completes.

## Durable streams and engine integration

`StateProvider` copy callbacks exchange `OperationDataStream`s (opaque byte
buffers), not durable delivery acknowledgements. `StateReplicator` returns
service-owned `OperationStream`s. Each delivered `StreamOperation` carries
metadata, data, and a one-shot acknowledgement:

- Persist the operation or copy boundary before calling `acknowledge(progress)`.
- `reject(error)` reports a failed application operation.
- Dropping an operation is **not** success; the waiting delivery fails.
- The engine validates durable progress and commits authority/build metadata
  before returning an applied peer ACK. Close/Abort and runtime drop terminate
  outstanding deliveries.

Inbound replication exposes two acknowledgements. `PendingReplication::received`
is available after ordered receiver admission and may advance transport resend
state without granting quorum credit. `PendingReplication::applied()` completes
only after durable service acceptance; only its applied progress is eligible
for quorum accounting.

`OperationStream::channel` supplies the producer/consumer boundary for custom
engines. Streams can be taken only once from the default state interface.

Reservations, exact-authority admission and ACK validation, queue retention,
quorum finalization, copy/build bookkeeping, and durable retry IDs live in the
non-COM replication engine, not the SF traits. The default state interface
retains write identity across failures and cancellation. Agent transport uses
a separate `RuntimeDataPlane` handle; `PodRuntime` remains the hosting and
lifecycle owner.

The runtime emits implementation-neutral domain messages. `kuberic-agent`
owns protobuf conversion, separate control and replication listeners, process
session fencing, reliable resend windows, reconnect, cancellation, and
full-copy fallback signaling. The runtime has no dependency on
`kuberic-wire` and does not start a network server.

Role changes drive the replicator before the service callback. Primary
promotion additionally invokes replicator/state-provider `UpdateEpoch`
between those callbacks. The completed role is published only after every
required stage succeeds; an in-process `RoleTransition` exposes partial
completion after failure. Close fences writes,
closes the replicator, then closes the service, with abort cleanup on callback
failure. Abort also stops the returned control before application teardown.
Failed or cancelled Open aborts created interfaces;
lifecycle/epoch failures never reopen writes.

Hosting, effect, authority-store, and snapshot types are absent from the
runtime root and generated user documentation. Process hosting and durable
effect execution are owned by `kuberic-agent`; shared persistence and
postcondition data live in unpublished `kuberic-runtime-internal`.
Runtime role and write access remain separate: startup is write-closed,
becoming Primary does not grant writes, and direct client writes require an
explicit granted `WriteStatus`.

The runtime does not create an operator, replica agent, or control-plane
server. It consumes narrow `ReplicaAuthorityStore`,
`ReplicationProgressStore`, `LocalWriteJournal`, `BuildAuthorityStore`, and
`BuildProgressStore` capabilities from the unpublished contract crate.
`kuberic-agent::SqliteStore` implements all capabilities while callers receive
only the mutation authority they require.

Replica builds use separate exact-target authority outside quorum membership.
The agent admits immutable build authority before source copy execution; the
replication engine consumes but does not create that permission.
Copy context remains a multi-item operation-data stream. `prepare_copy` returns
a bounded stream that incrementally carries snapshot chunks, the captured copy
boundary, and subsequent live replication without holding the global runtime
effect lock across provider enumeration. Durable duplicate snapshot chunks are
verified and acknowledged without redelivery to the application.
Dropping the returned copy stream cancels provider iteration and removes the
generation-scoped build.

## Public API boundary

The intended application surface is the documented service, state-provider,
partition, replicator-factory, replicator, and operation-stream API. Some
cross-crate host signatures are `pub` and `#[doc(hidden)]` because
`kuberic-agent` is a separate crate; hidden documentation is not treated as
access control.

`scripts/check_runtime_public_api.sh` reviews both generated rustdoc and an
exhaustive source-level inventory of public signatures. Compile-fail fixtures
prove that safe external application code cannot obtain the managed
replicator, construct a host partition, inject authority stores, register a
managed runtime directly, or access the private authority module.

Applications must persist copy and replication operations before
acknowledging them. A received transport item is not quorum evidence; only the
applied acknowledgement may contribute to commit.

## Remaining Service Fabric completion contracts

The independent controller and agent provide fixed-cardinality bootstrap,
replacement, ordinary failover, and quorum-loss ownership. The remaining
deferred contracts are:

- persistent resend payloads across process sessions where incremental
  reconnect is required instead of full-copy fallback;
- operation-specific mappings for removal, cancellation, backpressure, and
  transient reconfiguration outcomes beyond the current tonic status mapping;
- destructive data-loss recovery and its external fencing provider.

See the
[level-triggered operator guide](../docs/features/kuberic/level-triggered-operator.md)
for the supported operational contract and fail-closed limitations.
