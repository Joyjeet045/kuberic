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
Before regranting access, it reconciles interrupted durable local writes while
remaining closed: original operation/data identity must verify, and durable
application commit evidence or selected-authority quorum completion must resolve
the journal. Preparation includes reserved operations in its durable applied
handoff prefix. Failed client completions do not erase these operations.
During failover it records only the controller-selected election-safe prefix
under the new authority fence; a replica cannot reuse an arbitrary
previous-epoch suffix as verified progress.

The controller enables SF-inspired secondary scale-down using PC/CC quorum
principles, with Kuberic-specific target/minimum coupling, deterministic
selection, write closure, sequential cleanup, and Kubernetes resource deletion.
`spec.replicas` target=min is Kuberic policy; SF target and minimum are
independently configurable. The evaluator, not the runtime, checks retained
read-quorum availability under stable accepted current-only authority and fresh
exact sessions before freezing intent, removing routing, or closing writes.
`ScaleDownRetainedReadQuorumUnavailable` preserves existing service with bounded
re-observation, without preparing the primary or selecting another target.

Managed secondary-removal preparation serializes with write admission and ACK
completion, closes writes, reconciles journaled operation identities, and
persists an authority-verified durable prefix. PC and reduced CC retain their
independent policies. Removal catch-up needs session-bound, exact reduced-CC
write-quorum witnesses, including the unchanged primary; ordinary client
commits still require both PC and CC write quorums. Removal grants no PC/CC
client writes. A separate accepted current-only certificate and verified
catch-up gate the write regrant, including singleton recovery.
Frozen quorum certificates remain immutable authorization evidence after a
retained peer restarts. They cannot restore obsolete-session credit: acceptance
uses freshly verified current-session progress for the exact reduced authority
and prepared boundary. Old-session reports, ACKs, and registration still reject.
Post-commit live progress is validated separately from transition witnesses and
binds the exact immutable commit certificate. A current-only primary may report
Granted access with its completed availability command after restarting and
resuming writes. This does not relax pre-commit write closure or alter certificates.

Historical local acceptance is a separate managed effect for an exact retained
secondary whose installed current-only removal authority covers the frozen verified
boundary. The agent durably binds its pending/completed effect to the certificate.
Unlike live commit acceptance, it neither loads witnesses into the quorum tracker
nor persists a live runtime commit; restart replays only that exact local effect.
It grants no access or configuration authority, and rejects primary/target misuse,
conflicting installed authority, mutated receipts and insufficient verified progress.

Exact peer eviction after PC removal cancels retained windows and prevents a
delayed session from reconnecting the excluded incarnation. Local retirement
validates and durably records the exact retirement-started authority before
revoking access, fencing traffic, driving role None and hosting Close. Finalization
atomically writes the terminal tombstone, removes active authority, and clears
the started record; either lifecycle record prevents active authority admission.
Hosting checks the tombstone and then the started record before application Open.
After process termination, a started record is finalized without Open: termination
already closed the prior host. The pending agent effect then completes its exact
durable receipt normally. Failed finalization keeps reconstruction closed.
Preparation, acceptance, and retirement postconditions are unpublished managed
contracts, not additions to the SF-shaped application traits. Agent schema-4
storage persists preparation, accepted-current-only, and retirement evidence.
Recovery revalidates accepted evidence before restoring previously granted
access; preparation and current-only coordination by themselves stay closed.
The controller enables secondary scale-down; the application still receives no
managed authority setters. See the
[scale-down guide](../docs/features/kuberic/level-triggered-operator.md#secondary-scale-down)
for target=min policy and availability limits. Exact original PVC provenance
must be reconstructable before admission; pre-admission Pod/PVC disappearance
without that provenance waits/fails closed, never treating list omission as
absence. Unavailable-target support requires frozen or reconstructable exact
cleanup identity. PVC object deletion has no retention or import path, not a
physical storage erasure guarantee. Frozen-primary loss during removal/cleanup
can cause indefinite outage. Sequential cleanup and each retained member's
original completed current-only witness or fresh completed local acceptance
gate replacement of the bounded receipt. Sequential scale-up uses the same
authority-validated runtime boundary without moving desired policy or cleanup
authority into the runtime. It freezes a separate post-enumeration catch-up
boundary, admits the candidate through independently validated previous and
expanded policies, and may preserve same-primary writes only while both
configurations remain writable. Candidate readiness/copy completion alone never
grants membership or quorum credit. Protocol 8/schema 4 require a fresh
coordinated v2 deployment; classic v1 remains unchanged.
The [deferred follow-ups](../docs/proposal/v1-retirement-plan.md#deferred-scale-down-follow-ups)
include separating replication proof from Kubernetes cleanup obligations; neither
desired policy nor Kubernetes deletion authority belongs in the runtime.

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

`get_copy_state(up_to_lsn, ...)` retains its single-LSN signature. The agent
freezes that snapshot boundary at durable committed progress; application-applied
operations above it travel as retained catch-up with their original watermarks.
The immutable boundary survives source restart. Copy final markers require both
progress values to equal it, including duplicate completion replay.

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
between those callbacks, then settles application commitment through the durable
authority-fenced verified prefix before invoking the Primary application callback.
Primary-local unresolved reservations still require exact quorum recovery;
arbitrary applied suffixes are not committed merely by changing role.
The completed role is published only after every
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

The independent controller and agent provide full-set bootstrap,
replacement, ordinary failover, planned switchover, secondary scale-down, and
quorum-loss ownership. The remaining
deferred contracts are:

- persistent resend payloads across process sessions where incremental
  reconnect is required instead of full-copy fallback;
- operation-specific mappings for removal, cancellation, backpressure, and
  transient reconfiguration outcomes beyond the current tonic status mapping;
- destructive data-loss recovery and its external fencing provider.

See the
[level-triggered operator guide](../docs/features/kuberic/level-triggered-operator.md)
for the supported operational contract and fail-closed limitations.
