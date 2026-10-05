# kuberic-runtime

Application and replication runtime for the level-triggered Kuberic stack.

Together with `kuberic-controller`, this is one of exactly two public
production crates. DEX, all examples and level tests
are separate unpublished workspace packages. Applications need only this crate:

```toml
kuberic-runtime = "0.0.1"
```

The default `host` feature enables `host::ReplicaHost`, process recovery,
durable metadata and authenticated listeners. Shared identities, commands and
observations live in `protocol`; protobuf contracts and conversion live in
`control`. Enable `testing` for isolated fixture hosts/stores and deterministic
transport. Application-specific SQLite WAL commit barriers live privately in
the unpublished SQLite example, not in runtime.
Application and replication callbacks return `RuntimeError`; process hosting,
durable metadata, RPC and transport orchestration return `host::HostError`.
Controller-only consumers use `default-features = false` for the contracts-only
`protocol` and `control` modules; application and runtime implementation APIs
require `host`. Pure evaluation and plans belong to `kuberic-controller`.

Repository commands enable cross-crate policy proofs through
`KUBERIC_WORKSPACE_TESTS` in the workspace's `.cargo/config.toml`. That
configuration is not packaged: default tests of the published runtime run
without the workspace-only controller dev dependency.

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

Successful public `PrimaryReplicator` catch-up, build, and ordinary removal
completion is also the built-in engine's durable completion contract. The
host-private capability does not expose duplicate receipts for those
operations; it is limited to local write fencing/access preparation,
pending-write recovery, committed-prefix reconciliation, exact topology
durability, and narrow reporting/recovery observation.

Service Open receives a `StatefulServicePartition` in its `OpenContext`.
The service selects a `ReplicatorFactory` with `partition.with_factory(...)`,
then calls `create_replicator(state_provider, settings)`. The result contains
the control interface and optional state-replicator capability; the optional primary interface is
the explicit Rust counterpart of querying `IFabricPrimaryReplicator`.

```text
runtime host (private PodRuntime) ── Open(partition) ──► StatefulServiceReplica
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
agent-managed capabilities all reference that inner. `ReplicatorInterfaces`
owns one identity-bound creation containing the public handles, an armed
abandonment guard, and optional host-only lifecycle/data-plane capabilities.
Attachment consumes that coherent creation; cancellation, attachment failure,
or an Open identity mismatch aborts all of its capabilities together. A wrapper
that retains the built-in creation consumes the original bundle through
`wrap_primary`; reconstructing with `primary` intentionally creates an
independent public bundle without the original native provenance.
`PodRuntime` owns the
hosting registrar, application lifetime, Open registration, effect ordering,
and exact returned-interface identity; it does not preconstruct unused default
replication state for a custom factory. `PodRuntime` and that hosting registrar
are private runtime host implementation details; applications use `host::ReplicaHost`.
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
contracts, not additions to the SF-shaped application traits. Agent schema-5
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
grants membership or quorum credit. Protocol 9/schema 5 require a fresh
coordinated v2 deployment; the removed classic v1 stack has no conversion path.
The [deferred follow-ups](../docs/proposal/v1-retirement-plan.md#deferred-scale-down-follow-ups)
include separating replication proof from Kubernetes cleanup obligations; neither
desired policy nor Kubernetes deletion authority belongs in the runtime.

`ReplicatorFactoryContext` exposes stable identity and partition-access
capabilities, not a concrete runtime or default-engine pointer. Application
and custom-factory code constructs only the SF-shaped interface bundle through
`ReplicatorInterfaces::secondary` or `ReplicatorInterfaces::primary`. The
primary constructor derives the control and primary views from the same
allocation, matching SF's coherent interface-query invariant. The default implementation's lifecycle proof capability and managed data-plane
bridge travel with that coherent bundle and are consumed through an
unforgeable unpublished agent/runtime attachment boundary. Every primary implementation uses the common agent
lifecycle owner; only the default engine supplies replication/copy operations.
Custom replicators own their transport independently.

`StateReplicator` and the factory's `StateProvider` argument are optional.
The default factory requires `Some(provider)` and returns `Some(state_replicator)`;
a custom replicator such as PostgreSQL supplies neither operation/copy interface.
It still returns the same `Replicator` from service Open and implements
`PrimaryReplicator`, not a second application/driver hierarchy.

`ReplicaSetConfiguration` carries the voting configuration and exact
`ReplicaInformation` descriptions (incarnation, process session, endpoint, role,
and progress/catch-up boundary), including authorized idle replicas outside the
voting set. Agent hosting installs these through configuration callbacks before
dispatching `build_replica`. Configuration/epoch/session changes revoke old work;
`remove_replica` retires idle build work without removing an admitted secondary.
The private agent lifecycle host retains all durable authority/effect/store
capabilities, fences delayed callback completion by exact authority and session,
and publishes access only after implementation-specific proof. Standard
catch-up, build, and remove requests cross the returned public
`PrimaryReplicator`; the private capability exposes only native proof,
recovery, reconciliation, canonical topology receipts, and narrow observation.
Topology receipts bind engine identity/generation, authority, durable boundary,
and switchover/removal/retirement evidence. Public build completion is fenced by
the host's exact target, process sessions, configuration, and attempt admission
rather than a second native receipt. Independent custom replicators are not
required to construct topology receipts.
An unmanaged custom factory without that hosting support remains rejected for
managed admission.

The common agent lifecycle host durably selects one build per logical target
slot for custom implementations.
Descriptions for superseded builds are withdrawn, and callback receipts bind
the full authority/generation to both process sessions and the local attempt.
An idle custom replicator must withhold build-ready progress until its durable
copy certificate matches that exact installed description; unrelated existing
data is not completion of a newly selected build. The platform never fans one
scalar out as completion of every described build. The default operation engine
continues to use its own per-build copy acknowledgements.

Custom services retain the partition handle and reconcile direct-client access
against its read/write statuses, never role notifications alone. Progress
observation must finish that reconciliation before returning; hosting awaits it
before accepting the access transaction's effect result. Application-specific lineage and
recovery evidence stay in the application. See the repository's
[SF interface mapping](../docs/background/service-fabric/references.md) and
[service-created replicator design](../docs/archive/v1/implemented/runtime-replicator-separation.md).

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

The replication engine emits implementation-neutral domain messages. Runtime
`control` owns protobuf conversion; the `host` feature supplies separate control
and replication listeners, process
session fencing, reliable resend windows, reconnect, cancellation, and
full-copy fallback signaling. Disabling `host` leaves the shared contracts and
application interfaces available without the replica process implementation.

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

Internal effect, authority-store, and snapshot types remain crate-private.
Process hosting and durable effect execution are owned by runtime's private
host modules; only the supported `host::ReplicaHost` process API is public.
Runtime role and write access remain separate: startup is write-closed,
becoming Primary does not grant writes, and direct client writes require an
explicit granted `WriteStatus`.

The runtime does not create a Kubernetes operator. Its host consumes private
`ReplicaAuthorityStore`,
`ReplicationProgressStore`, `LocalWriteJournal`, `BuildAuthorityStore`, and
`BuildProgressStore` capabilities. The private host `SqliteStore` implements
them while internal callers receive only the mutation authority they require.

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
partition, replicator-factory, replicator, operation-stream and process-host
API. Host registration, attachment, durable authority/effect and native
capability constructors remain private even with all features enabled; hidden
documentation is not treated as access control.

The opt-in `testing` API provides opaque, independently constructed fixture
hosts/stores and detached serializable records. It cannot extract a host from
`RunningReplica`, turn fixture records into production authority capabilities,
or obtain attachment/registration/native lifecycle capabilities. Application
tests can drive real durable effects and transport without widening production
admission APIs.

Private lifecycle mutation, attachment introspection, acceptance pause gates,
and control-plane drivers are compiled only for runtime's own unit tests.
Enabling the downstream `testing` feature does not enable those private hooks.

Repository-only unit tests also exercise the controller's actual evaluator
against private host admission. Their unversioned controller dev-dependency is
omitted by Cargo when packaging runtime, preserving the one-way production
dependency and allowing runtime to be published before controller.

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
