# Service Fabric Stateful API Semantics and Kuberic Conformance

## Status and Scope

This document defines the Service Fabric V1 stateful-service and Replicator
contract as an executable semantic model, then evaluates the current Kuberic
runtime against that model.

It covers:

- stateful replica lifecycle ordering;
- Replicator role and epoch transitions;
- current/previous replica-set configuration;
- quorum catch-up and planned primary swap;
- replica build and removal;
- data-loss processing;
- read/write access publication;
- progress meanings;
- cancellation, close, abort and fault reporting;
- the distinction between Kuberic's legacy/default managed path and its
  public/custom path.

It does not define the Service Fabric V2 Transactional Replicator. Kuberic's
separate V2 direction is documented in
[SQLite on a V2 Transactional Replicator](../sqlite/v2-transactional-replicator.md).

The public Kuberic `Replicator` and `PrimaryReplicator` interfaces are protected
and are not changed by this analysis. The conformance work described here is
about runtime sequencing, public value semantics, task ownership and evidence,
not adding private escape hatches or changing those method sets.

## Source Authority and Revisions

The contract is reconstructed from three source levels:

1. **Native Service Fabric implementation and tests** are authoritative for
   actual runtime sequencing and completion semantics.
2. **Public Service Fabric COM interfaces** define the externally visible API
   shape.
3. **`service-fabric-rs` traits and comments** provide the Rust projection, but
   their Replicator documentation is explicitly marked TODO or unofficial.

The analysis is pinned to:

| Source | Revision |
|---|---|
| Kuberic | `d54a84a3a76f14b2dc4ba0a91c042a5d345bb85e` |
| Azure Service Fabric | `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d` |
| `Azure/service-fabric-rs` | `39fbf9124c0e5a3934c453343f7f3ed36ebac627` |

Service Fabric paths below are relative to the local checkout at
`/data/code/reference/service-fabric`. Kuberic paths are relative to this
repository. Rust facade line references refer to
`crates/libs/core/src/runtime/stateful_traits.rs` at the pinned
`service-fabric-rs` revision.

When a Rust comment conflicts with native Service Fabric behavior, this
document follows the native implementation. Two known examples are:

- the Rust comment that describes `update_epoch` as secondary-only, while
  native promotion action lists can include an explicit epoch update;
- broad commentary around catch-up modes, while native specific-quorum support
  maps both swap catch-up phases to write quorum.

The words **MUST**, **MUST NOT**, **SHOULD** and **MAY** below describe the
contract derived from those sources. They are not quotations from one formal
Service Fabric specification.

## Contract Model

### Ownership

Service Fabric separates three durable or executable owners:

| Owner | Responsibility |
|---|---|
| Reconfiguration Agent and Failover Manager | Replica identity, role intent, epoch, current/previous configuration, reconfiguration phase and access decisions |
| Stateful service and state provider | Durable application state, applied progress, copy representation and application-specific data-loss correction |
| V1 Replicator | Live role engine, transport, queues, peer sessions, ACK aggregation, quorum completion, copy and replication mechanics |

The V1 Replicator is restart-reconstructable. Its process-local queues,
sessions and role engine are recreated from RA-owned lifecycle/configuration
calls and provider-owned durable state. It does not recover a separate V1
Replicator metadata database.

This division is visible in:

- `src/prod/src/Reliability/Failover/ra/FailoverUnit.h:566-577`;
- `src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.h:548-588`;
- `src/prod/src/Reliability/Replication/Replicator.ChangeRoleAsyncOperation.cpp:37-188`;
- `src/prod/src/Reliability/Replication/ReplicaManager.h:293-339`.

### Public Completion Is Semantic Evidence

A successful public operation is not merely a notification. Its completion
certifies the postcondition assigned to that operation:

| Operation | Completion means |
|---|---|
| `open` | The object is open and its owned endpoint/resources are available |
| `change_role` | Role-specific state and fencing for the supplied epoch/role are established |
| `update_epoch` | The replica has accepted the newer epoch barrier and rejected obsolete work |
| configuration update | The primary has installed the exact current/previous configuration description |
| catch-up wait | The requested quorum predicate is true against the current committed boundary |
| `build_replica` | Copy and replication through the build boundary have completed |
| `remove_replica` | Resources for the non-configured idle replica are released |
| `on_data_loss` | Provider-specific data-loss processing has completed; the boolean reports whether state changed |
| `close` | Graceful resource release completed or the host contained failure through abort |
| `abort` | The object synchronously fences and releases owned resources as far as possible |

The runtime may durably remember that an exact call completed. It MUST NOT
invent stronger facts such as an internal committed LSN, ACK set, copy
boundary or quorum position unless the public contract exposes that fact or
the runtime owns an independent proof.

### Ordering Is Part of the Contract

The same methods in a different order do not implement the same protocol.
Important ordering constraints include:

1. The stateful service replica opens before the returned Replicator is opened.
2. Replicator role change completes before application replica role change.
3. Primary-only configuration methods run only after the Replicator is primary.
4. Epoch barriers precede acceptance of work from the newer epoch.
5. Data-loss processing completes before catch-up/access can expose a promoted
   primary when possible data loss exists.
6. During a planned swap, write access is revoked between the two catch-up
   phases.
7. A build target remains outside active configuration until build succeeds.
8. A running build is cancelled and settled before `remove_replica`.
9. Graceful close and abort process the Replicator before the application
   replica.

Native action recipes are concentrated in
`src/prod/src/Reliability/Failover/ra/ProxyActionsList.cpp:31-176`.

### Cancellation and Supersession

Every asynchronous Rust facade lifecycle method receives a cancellation token.
Native Service Fabric operation managers additionally own and cancel exact
outstanding operations during close, abort, replacement or supersession.

Cancellation has two distinct requirements:

1. **Publication fencing:** a late result MUST NOT mutate current host state or
   grant current access.
2. **Work containment:** the obsolete future and its external side effects
   SHOULD be cancelled and drained; merely ignoring the late return value is
   insufficient when the callback owns sessions, copy work or durable
   application mutation.

Native close/abort cancellation is visible in
`src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.cpp:1483-1534,1578-1607`.

### Access Is an RA Decision

Read and write status are runtime/RA projections. Replicator operations provide
evidence used by the runtime, but the Replicator does not directly grant
application access.

Access MUST remain closed until the required role, epoch, data-loss,
configuration and catch-up operations have completed. Revocation MUST precede
the final swap catch-up so no new writes can advance the committed boundary
while the successor is being finalized.

## Interface Semantics

### `IStatefulServiceFactory`

`create_replica` creates one application replica for the supplied service type,
service name, initialization data, partition ID and replica ID
(`stateful_traits.rs:13-26`; public
`src/prod/src/idl/public/FabricRuntime.idl:482-491`).

Creation does not open the service or assign a role. It establishes the object
identity later driven through `IStatefulServiceReplica`.

### `IStatefulServiceReplica`

#### `open`

`open` initializes the service replica and returns its primary Replicator
interface. The runtime then opens the Replicator. Native initial-open action
lists order `OpenReplica` before `OpenReplicator`
(`ProxyActionsList.cpp:34-53`; `stateful_traits.rs:34-45`).

The application MUST return one coherent Replicator identity. Registration,
primary operations, state replication and lifecycle must refer to the same
created object bundle.

#### `change_role`

The runtime first changes the Replicator role, then changes the application
replica role. This gives the Replicator an opportunity to establish epoch and
data-plane fencing before application code serves the new role
(`ProxyActionsList.cpp:107-128`; `stateful_traits.rs:47-58,80-89`).

The returned service address is semantically observable. The runtime stores it
as the service location and publishes a new primary endpoint
(`src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.ChangeReplicaRoleAsyncOperation.cpp:89-145`).

#### `close`

Graceful close processes the Replicator before the application replica. Native
FUP converts a failed child close into abort cleanup and continues composite
teardown
(`src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.CloseAsyncOperation.cpp:29-105,205-275`).

The important postcondition is containment: access is revoked and all owned
resources are released even if one child close fails.

#### `abort`

Abort is synchronous and best-effort. It is used for ungraceful shutdown,
permanent faults and failed graceful operations. The implementation MUST
immediately fence new work, drop references and release owned resources.
Replicator abort precedes application abort
(`ProxyActionsList.cpp:63-80`; `stateful_traits.rs:63-67`).

### `IReplicator`

#### `open`

The Replicator opens without an assigned role. It creates and registers its
replication transport/message processor and returns the published Replicator
endpoint
(`src/prod/src/Reliability/Replication/Replicator.OpenAsyncOperation.cpp:29-83`;
`stateful_traits.rs:70-78`).

The endpoint lifetime belongs to the Replicator lifecycle:

- no replication traffic is accepted before successful `open`;
- failed `open` does not leave a live listener;
- `close` and `abort` stop accepting traffic;
- the address returned by `open` is the address exposed in replica
  information.

#### `change_role`

`change_role(epoch, role)` creates, replaces or closes the role-specific
primary/secondary engine. It is not a label update. Successful completion
means the Replicator is prepared to execute that role under the supplied epoch
(`Replicator.ChangeRoleAsyncOperation.cpp:37-188`).

The callback precedes application `change_role`.

#### `update_epoch`

An epoch is a fencing barrier. Accepting a newer epoch means operations from an
older primary/configuration period can no longer be accepted as current.

Native secondary update advances the minimum allowed epoch and discards old
work
(`src/prod/src/Reliability/Replication/SecondaryReplicator.UpdateEpochAsyncOperation.cpp:41-82`).
RA action lists include epoch updates during standalone/reconfiguration paths
(`ProxyActionsList.cpp:107-128,163-176`).

A replica that remains secondary while the epoch changes still requires the
new barrier; unchanged role is not evidence that epoch work is unnecessary.

#### `get_current_progress`

Current progress is role-sensitive:

- on a primary, native V1 reports the replication queue's last committed
  sequence number, not an arbitrary local uncommitted tail
  (`src/prod/src/Reliability/Replication/ReplicationQueueManager.cpp:206-225`);
- on a secondary, it represents the end of the contiguous locally accepted
  history used for election and catch-up decisions.

The runtime MUST NOT reinterpret this single public value as several stronger
independent facts such as committed, verified and quorum progress.

#### `get_catch_up_capability`

Catch-up capability is the beginning of retained history from which another
replica can catch up. If a candidate's progress is below this boundary, a copy
or rebuild is required (`stateful_traits.rs:110-116`).

Current progress and catch-up capability are a public first/last pair. They are
not a public ACK set.

### `IPrimaryReplicator`

All `IPrimaryReplicator` operations are primary-only. Native implementation
guards both configuration methods with `VerifyIsPrimaryCallerHoldsLock`
(`src/prod/src/Reliability/Replication/Replicator.cpp:397-423,441-463`).

#### `on_data_loss`

When RA determines that possible data loss exists, it invokes data-loss
processing only after Replicator and application primary role transitions.
Callback failure fails promotion. A `true` result means application state
changed; the runtime rereads progress and treats old secondary state as
incompatible
(`src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.ReplicatorOnDataLossAsyncOperation.cpp:21-64,83-161`).

The required sequence is:

1. establish Replicator primary role;
2. establish application primary role;
3. invoke `on_data_loss`;
4. if state changed, reread provider/Replicator progress and recreate or
   rebuild incompatible secondaries;
5. install configuration and satisfy catch-up;
6. grant access.

Returning `false` means the provider made no correction. It does not allow the
runtime to skip invoking the callback when a data-loss transition requires it.

#### Current and Previous Configurations

At any instant the primary has:

- one current configuration; and
- optionally one previous configuration.

While both exist, a replicated write completes only after both configurations
have independently reached their write quorum.

Configuration descriptions represent participating remote replicas, not the
local primary and not idle replicas still being built. Native projection:

- includes up, ready replicas presented as secondaries;
- supplies meaningful first/last progress or invalid progress when the
  Replicator already owns the information;
- identifies the designated successor using `must_catchup`
  (`src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.ConfigurationUtility.cpp:192-219,262-319`;
  `stateful_traits.rs:145-179`).

`update_catch_up_replica_set_configuration` installs current plus previous
configuration during reconfiguration. `update_current_replica_set_configuration`
removes the previous configuration after transition.

#### `wait_for_catch_up_quorum`

Catch-up compares replica progress with the current committed boundary. It is
not satisfied merely because enough replicas are connected.

- `All` requires every relevant current-configuration replica to reach the
  boundary.
- `Write` requires a write quorum and, when designated, the `must_catchup`
  successor.

Native predicate evaluation is in
`src/prod/src/Reliability/Replication/PrimaryReplicator.CatchupAsyncOperation.cpp:62-105`.

With catch-up-specific quorum support, planned primary swap performs two
write-quorum waits:

1. catch up while writes are still granted;
2. revoke write access;
3. catch up again to the final committed boundary;
4. continue demotion/promotion.

Both swap phases map to write quorum
(`src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.ReplicatorCatchupReplicaSetAsyncOperation.cpp:107-119`;
`ProxyActionsList.cpp:130-142`).

The designated successor MUST participate in the completing quorum. An
unrelated slow replica MUST NOT block a specific-quorum swap.

#### `build_replica`

Build transfers state to an idle replica outside the current and previous
configurations. The public descriptor has:

- role `IdleSecondary`;
- `must_catchup = false`;
- invalid/unknown progress and catch-up capability;
- no quorum membership.

FUP constructs invalid target progress in
`src/prod/src/Reliability/Failover/ra/ComProxyReplicator.BuildIdleReplicaAsyncOperation.cpp:23-36`
and converts disabled progress to `FABRIC_INVALID_SEQUENCE_NUMBER` in
`FailoverUnitProxy.ConfigurationUtility.cpp:296-317`.

Successful build completion means:

1. the provider copy stream completed; and
2. replication caught the target through the boundary captured when copy
   enumeration ended.

This prevents a target from missing writes concurrent with copy
(`src/prod/src/Reliability/Replication/PrimaryReplicator.BuildIdleAsyncOperation.cpp:42-166`).

The operation is cancellable. A target MUST NOT simultaneously be in build and
in an active configuration.

#### `remove_replica`

`remove_replica` releases resources for an idle/non-configured target that has
gone down. Removal of a configured replica is expressed by a configuration
update that excludes it.

Service Fabric does not call `remove_replica` while the same target's build
future is still running (`stateful_traits.rs:237-244`). A superseding build or
retirement cancels and settles the old build before removal
(`src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.ReplicatorBuildIdleReplicaAsyncOperation.cpp:66-108,216-299`).

### `IFabricStateReplicator`

`IFabricStatefulServicePartition::CreateReplicator` binds one state provider
and Replicator settings to one returned lifecycle Replicator and one returned
state Replicator
(`src/prod/src/idl/public/FabricRuntime.idl:529-539,576-633`).
The two returned interfaces are views of the same replication engine, not
independent implementations.

Kuberic projects the state-replication surface as `StateReplicator`
(`kuberic-runtime/src/replicator/mod.rs:73-80`).

#### `replicate`

Primary replication assigns an LSN, queues the operation and completes only
after the operation satisfies the active replication commitment rule. During
joint previous/current configuration, that means a write quorum in both
configurations.

The completion does not require every replica unless the configuration's write
quorum does. It also does not mean the provider on every secondary has applied
the operation; persisted secondaries acknowledge according to their operation
stream/provider durability contract.

Kuberic's managed implementation explicitly promises durable local acceptance
plus admitted previous/current write quorums
(`kuberic-runtime/src/replicator/mod.rs:73-75,1084-1154`). That completion
shape is aligned and must be preserved when the private managed boundary is
removed.

#### Replication and copy streams

`get_replication_stream` and `get_copy_stream` are secondary operations. Native
Replicator verifies secondary role before returning either stream
(`src/prod/src/Reliability/Replication/Replicator.cpp:537-569`).

- the copy stream initializes or replaces provider state for an idle/new
  secondary;
- the replication stream carries ordered operations after the copy boundary;
- provider application and acknowledgement determine the secondary's durable
  accepted progress;
- stream lifetime is tied to the current role engine and is invalidated by
  role/epoch/close/abort transitions.

The runtime MUST NOT treat obtaining a stream as proof that its operations were
applied or persisted.

#### `update_replicator_settings`

Settings update changes supported live Replicator configuration. It does not
transfer endpoint ownership away from the Replicator or establish a new role,
epoch or replica-set configuration
(`FabricRuntime.idl:576-598`).

Kuberic currently replaces its in-memory settings value
(`kuberic-runtime/src/replicator/mod.rs:1164-1168`). Whether each setting takes
effect is implementation-specific and must not be inferred from storage alone.

### `IFabricStateProvider`

The state provider is the durable application-side counterpart of the V1
Replicator
(`FabricRuntime.idl:600-633`;
`kuberic-runtime/src/application.rs:74-95`).

#### `update_epoch`

Provider epoch update receives both the new epoch and the last sequence number
from the previous epoch
(`src/prod/src/Reliability/Replication/ComProxyStateProvider.cpp:76-149`).
This lets the provider durably record the exact history boundary associated
with the epoch transition.

Successful Replicator epoch completion must include successful provider epoch
work when the role-specific engine requires it.

#### `last_committed_lsn`

The provider reports its durable committed application boundary. Native V1
uses it when creating an initial primary and after state-changing data-loss
processing. Values below the invalid LSN are normalized to invalid
(`ComProxyStateProvider.cpp:63-74`).

Provider committed progress and Replicator current progress are related but not
interchangeable. The runtime must not silently choose the larger value and call
it committed without proving the role-specific invariant.

#### Copy context and copy state

`get_copy_context` lets the target provider describe state relevant to copy.
`get_copy_state(up_to_lsn, context)` returns source state bounded by the
supplied sequence number
(`ComProxyStateProvider.cpp:151-207`; `FabricRuntime.idl:624-632`).

The source Replicator combines provider copy completion with retained
replication through the captured copy-end boundary before completing
`build_replica`.

Kuberic strengthens repeatability by requiring copy bytes frozen at the
authorized committed boundary to be reproducible across an authorized retry
(`kuberic-runtime/src/application.rs:83-91`). That is a valid deterministic
extension as long as public build input and completion retain SF semantics.

#### Provider `on_data_loss`

The primary Replicator's public `on_data_loss` normally delegates
application-specific repair to the state provider
(`ComProxyStateProvider.cpp:209-270`). The provider boolean is then returned
through the primary Replicator to the runtime.

These are two layers of one operation:

1. the runtime invokes `IPrimaryReplicator::on_data_loss`;
2. the Replicator invokes provider data-loss handling;
3. the provider returns whether durable application state changed;
4. the Replicator resets/reconciles its queues as required;
5. the runtime consumes the final boolean and refreshes progress/topology.

Directly testing the provider callback does not prove the production runtime
routes the primary Replicator operation.

### `IStatefulServicePartition`

#### `create_replicator`

Native `CreateReplicator` accepts one state provider and settings object, then
returns both lifecycle/primary and state-replication interfaces from one engine
(`FabricRuntime.idl:529-539`).

Kuberic expresses construction through `ReplicatorFactory`,
`ReplicatorFactoryContext` and `ReplicatorInterfaces` rather than an
SF-shaped partition method. Conformance therefore depends on preserving one
coherent creation identity and binding the same provider/settings/lifecycle/
state interfaces together.

#### Partition information and access

Partition information is immutable identity/range metadata. Read and write
status are runtime projections and SHOULD be checked by application code before
serving operations (`stateful_traits.rs:255-268`).

The status values are not advisory:

- loss of write status fences new primary writes;
- loss of read status fences reads that require serving authority;
- access changes only after the runtime's ordered lifecycle proof changes.

#### `report_fault`

`report_fault` means the replica cannot recover without runtime action
(`stateful_traits.rs:277-280`).

Native RA behavior distinguishes:

- transient fault: restart a persisted replica or drop a volatile replica;
- permanent fault: terminally remove/drop the current incarnation.

Both fault kinds are delivered to RA
(`src/prod/src/Reliability/Failover/ra/ComStatefulServicePartition.cpp:213-232`;
`src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp:3742-3780`).

A fault report MUST close access. It is not merely a health annotation.

#### Load, move cost and health

Load and move cost affect placement. Partition and replica health reports feed
the health subsystem. They do not replace lifecycle faults and do not directly
change Replicator state (`stateful_traits.rs:269-307`).

## Canonical Stateful Sequences

### Initial Secondary

1. Create application replica.
2. Open application replica and obtain Replicator.
3. Open Replicator and publish its Replicator endpoint.
4. Establish secondary role and epoch in the Replicator.
5. Establish secondary role in the application.
6. If idle and requiring state, remain access-closed while build/copy runs.
7. Join active configuration only after build completion.
8. Grant read status only after required synchronization.

### Initial Primary

1. Open application and Replicator if necessary.
2. Change Replicator to primary with the new epoch.
3. Change application replica to primary and retain/publish its service address.
4. If the transition carries possible data loss, invoke `on_data_loss`.
5. If state changed, reread provider progress and invalidate incompatible
   secondary state.
6. Complete initial-primary reconfiguration and publish access.

Primary-only configuration callbacks MUST NOT occur before step 2.

Native initial-primary open does not unconditionally perform the catch-up
configuration/wait recipe used by failover promotion
(`ProxyActionsList.cpp:44-53`). Initial primary role creation initializes
provider epoch/progress through the role engine
(`src/prod/src/Reliability/Replication/Replicator.ChangeRoleAsyncOperation.cpp:128-197`).

### Failover Promotion

1. Change Replicator to primary with the new epoch.
2. Explicitly update the promoted Replicator/provider epoch barrier.
3. Change the application replica to primary and retain/publish its service
   address.
4. If possible data loss exists, invoke `on_data_loss`.
5. If state changed, reread progress and invalidate incompatible secondary
   state.
6. Install catch-up current/previous configuration.
7. Wait for the required catch-up quorum.
8. Grant access only after promotion evidence is complete.

Native ordering is
`ChangeReplicatorRole -> UpdateEpoch -> ChangeReplicaRole ->
ReportAnyDataLoss -> UpdateCatchUpConfiguration -> Catchup`
(`ProxyActionsList.cpp:107-128`).

### Same-Role Secondary Epoch Advance

1. Keep access fail-closed as required by the reconfiguration.
2. Invoke `update_epoch` even though the role remains secondary.
3. Reject old-epoch traffic and discard obsolete work.
4. Apply new configuration/catch-up decisions.
5. Regrant access only after the new barrier is effective.

### Planned Primary Swap

1. Mark the intended successor `must_catchup`.
2. Install current/previous configuration.
3. Wait for catch-up while writes remain open.
4. Revoke write access.
5. Apply the swap epoch barrier.
6. Refresh catch-up current/previous configuration.
7. Wait again at the final committed boundary.
8. Demote the old primary and promote the successor.

When the Replicator supports catch-up-specific quorum, both waits use write
quorum and MUST include the designated successor. An unrelated slow replica
does not block those waits. Without that capability, native SF falls back to
`All`
(`ProxyActionsList.cpp:130-142`;
`FailoverUnitProxy.ReplicatorCatchupReplicaSetAsyncOperation.cpp:81-119`).

### Add a Replica

1. Start target as idle and outside current/previous configuration.
2. Call cancellable `build_replica` with invalid target progress.
3. Copy provider state.
4. Replicate through the copy-end boundary.
5. Complete build.
6. Change target to active secondary.
7. Add it through a configuration callback.
8. Catch up the required quorum.
9. Grant target read access.

### Remove or Replace a Replica

For a configured replica, first install a configuration that excludes it. For
an idle build target:

1. cancel the exact build;
2. wait for build termination;
3. call `remove_replica`;
4. reject any late completion from the retired attempt.

### Graceful Close and Abort

Graceful close:

1. close access;
2. cancel/drain outstanding work;
3. close Replicator;
4. close application replica;
5. abort a child whose close failed;
6. finish teardown with no live endpoint or operation.

Abort invokes Replicator abort before application abort and returns
synchronously.

## Kuberic Runtime Paths

Kuberic currently has two semantically different hosting paths.

### Legacy/Default Managed Path

`DefaultReplicatorFactory` returns public interfaces plus unpublished managed
lifecycle/data-plane capabilities. The host detects those capabilities and
uses private authority, topology, access, build, observation and receipt
operations.

This path has stronger internal proof than the public interface exposes, but
that proof means the public methods are not the complete correctness boundary.

### Public/Custom Path

An application-provided Replicator exposes only the public interfaces. The host
adapts durable authority, configuration, build and access work into those
calls.

Several current values and orderings require Kuberic-specific behavior from a
custom implementation and are not compatible with a strict SF V1 Replicator.

## Conformance Verdict

**Overall: materially misaligned.**

| Area | Current verdict |
|---|---|
| Application/Replicator open order | Aligned |
| Replicator-before-application role order | Aligned |
| Close/abort object order | Aligned |
| Managed previous/current write quorum | Aligned |
| Managed durable build boundary | Aligned |
| Public primary configuration ordering | Not aligned |
| Data-loss orchestration | Not implemented in production lifecycle |
| Same-role secondary epoch delivery | Not aligned for public/custom |
| Public configuration value semantics | Not aligned |
| Public build/remove semantics | Not aligned |
| Planned swap catch-up | Not aligned for public/custom; unresolved end-to-end for managed |
| Public progress semantics | Not aligned |
| Public-only correctness boundary | Not aligned |
| Replicator endpoint ownership | Not aligned |
| Callback cancellation and draining | Partially aligned |
| Transient fault handling | Not aligned |
| Access publication | Structurally strong but based on incomplete/overstated evidence |
| Test protection | Partial |

## Kuberic Misalignment Catalog

### KSF-01: Primary Configuration Runs Before Primary Role

**Severity:** High
**Affected path:** Public/custom

**SF contract:** Primary configuration methods are valid only after the
Replicator has entered primary role. Native implementation rejects non-primary
callers (`Replicator.cpp:397-423,441-463`).

**Kuberic behavior:** `AdmitAuthority` runs before the `ReplicatorRole` stage
(`kuberic-runtime/src/host/coordinator.rs:407-425,529-545`). Public/custom
authority admission immediately invokes current or catch-up configuration
(`kuberic-runtime/src/host/custom/authority.rs:154-181`;
`kuberic-runtime/src/host/custom.rs:2110-2140`).

**Consequence:** A strict Replicator can correctly return `NotPrimary`, causing
initial primary open or failover to fail. A permissive implementation mutates
primary-only state before role/epoch fencing.

**Required conformance property:** Complete Replicator primary role before any
primary configuration callback.

### KSF-02: Production Promotion Omits `on_data_loss`

**Severity:** Critical
**Affected path:** Both

**SF contract:** When possible data loss is reported, promotion invokes
`on_data_loss` after primary roles and before catch-up/access. Failure blocks
promotion. A `true` result causes progress reread and secondary
recreation/rebuild.

**Kuberic behavior:** The method exists
(`kuberic-runtime/src/replicator/mod.rs:55-71,1026-1041`), but there is no
data-loss runtime effect (`kuberic-runtime/src/effects.rs:44-102`), no durable
coordinator stage (`kuberic-runtime/src/host/state.rs:76-91`), and promotion
advances from role through catch-up and access without it
(`kuberic-runtime/src/host/coordinator.rs:529-603`).

**Consequence:** Provider repair, truncation, restore or rejection is skipped.
Kuberic can publish a primary history that native SF would first reconcile or
reject.

**Required conformance property:** Add an ordered, durable public data-loss
operation and consume `false`, `true` and error distinctly before access.

### KSF-03: Same-Role Secondaries Skip the New Epoch Barrier

**Severity:** High
**Affected path:** Public/custom

**SF contract:** A secondary that remains secondary across a newer epoch still
receives `update_epoch` and fences old-primary traffic.

**Kuberic behavior:** Non-primary targets initialize epoch completion as
already true, same-role transitions skip role work, and the coordinator's
separate epoch stage is primary-only
(`kuberic-runtime/src/host/hosting.rs:3529-3563`;
`kuberic-runtime/src/host/coordinator.rs:529-545`).

**Consequence:** A custom secondary can retain the predecessor epoch and fail
to reject obsolete operations.

**Required conformance property:** Drive one exact public `update_epoch` for
every accepted newer secondary epoch, independent of role change.

### KSF-04: Public Configuration Descriptions Have Non-SF Semantics

**Severity:** High
**Affected path:** Public/custom

**SF contract:** Configuration descriptions contain participating remote
secondaries, meaningful/invalid progress, and a `must_catchup` successor when
required. Idle build targets are excluded.

**Kuberic behavior:**

- `ReplicaInformation` has no `must_catchup`;
- constructors default both progress values to valid zero
  (`kuberic-runtime/src/replicator/mod.rs:369-395`);
- projection includes all members, appends selected build targets and ensures
  the local replica is present
  (`kuberic-runtime/src/host/custom.rs:2023-2076`);
- that list is passed directly to public configuration callbacks
  (`kuberic-runtime/src/host/custom.rs:2110-2140`).

**Consequence:** A portable Replicator can count local/idle entries as peers,
treat zero as real history, omit the mandatory successor from catch-up, or
open a session to a target still being built.

**Required conformance property:** Project exact SF-shaped remote active
membership and successor/progress semantics without changing the protected
method sets.

### KSF-05: `build_replica` Receives Source Progress as Target Progress

**Severity:** High
**Affected path:** Public/custom

**SF contract:** An idle build descriptor carries invalid/unknown target
progress because the target is outside configuration and may be empty.

**Kuberic behavior:** `prepare_build_description` writes the source's
authorized replication boundary into both target progress fields before
calling `build_replica`
(`kuberic-runtime/src/host/custom.rs:3232-3289`).

**Consequence:** An SF-portable Replicator may interpret the target as already
having that history, skip copy and report false success.

**Required conformance property:** Pass unknown target progress; keep the
source-selected copy boundary internal to the Replicator/build implementation.

### KSF-06: Removal Can Overlap a Running Public Build

**Severity:** High
**Affected path:** Public/custom

**SF contract:** Cancel and settle the exact build before `remove_replica`.

**Kuberic behavior:** The public build future is awaited independently
(`kuberic-runtime/src/host/custom.rs:3278-3289`). Retirement advances local
generation state and later calls `remove_replica`, but no owner cancels and
joins the exact outstanding callback
(`kuberic-runtime/src/host/custom.rs:3708-3746,4260-4286,4616-4646`;
`kuberic-runtime/src/host/coordinator.rs:639-661`).

**Consequence:** Custom code can continue copy or recreate sessions/resources
after removal. Host generation checks reject publication but do not contain
external side effects.

**Required conformance property:** Give the host an exact task handle, cancel
and drain it, then invoke removal.

### KSF-07: Planned Swap Uses the Wrong Catch-Up Predicate

**Severity:** High
**Affected path:** Public/custom; managed proof incomplete

**SF contract:** With specific-quorum support, both swap waits use write quorum
and include the `must_catchup` successor.

**Kuberic behavior:** Public/custom switchover waits for `All`, revokes access,
then waits for `All` again
(`kuberic-runtime/src/host/custom.rs:4022-4058`). Managed preparation fences
writes and republishes retained writes but does not itself show the exact
successor/quorum wait
(`kuberic-runtime/src/runtime.rs:2453-2543`).

**Consequence:** A valid public/custom swap can stall on an unrelated slow
replica. Managed completion can be consumed without an established
successor-specific postcondition unless another exact stage supplies it.

**Required conformance property:** When specific quorum is supported, use two
write-quorum waits around write revocation and require the designated
successor. Use `All` only as the explicit no-specific-quorum fallback.

### KSF-08: Public Progress Is Promoted Into Stronger Evidence

**Severity:** High
**Affected path:** Both, differently

**SF contract:** Primary current progress is committed progress. The public
first/last pair does not expose independent ACK, verified or quorum positions.

**Kuberic behavior:**

- default `control_progress` takes the maximum of provider progress and the
  replication log's highest local LSN
  (`kuberic-runtime/src/runtime.rs:602-610`;
  `kuberic-runtime/src/replicator/log.rs:320-326`);
- local registration/apply can advance that highest LSN before commitment
  (`kuberic-runtime/src/replicator/quorum.rs:379-396`);
- custom refresh copies one public value into current, committed,
  current-configuration quorum and verified replication fields
  (`kuberic-runtime/src/host/custom.rs:3038-3112`);
- controller election persists reported current progress as election progress
  (`kuberic-controller/src/evaluator.rs:2420-2480,4235-4273`).

**Consequence:** Election, access and failover can treat an uncommitted or
merely local tail as a safe committed/quorum prefix.

**Required conformance property:** Define role-correct public progress and stop
manufacturing stronger evidence from that one value.

### KSF-09: The Default Path Bypasses the Public Correctness Boundary

**Severity:** High
**Affected path:** Legacy/default managed

**SF contract:** Runtime/Replicator semantics are expressed through the public
lifecycle, primary and progress interfaces.

**Kuberic behavior:** Private managed interfaces expose authority, topology,
access, recovery, build proof and observation
(`kuberic-runtime/src/replicator/mod.rs:82-176`). The default factory attaches
them (`kuberic-runtime/src/replicator/mod.rs:927-979`), registration detects
them (`kuberic-runtime/src/host/hosting.rs:2527-2625`), and host correctness
branches onto private prepare/commit/publish operations
(`kuberic-runtime/src/host/custom.rs:492-565,1433-1484`).

**Consequence:** Success of the built-in engine does not demonstrate that an
ordinary public Replicator can reproduce the behavior. Managed and custom
Replicators have different semantic contracts.

**Required conformance property:** The built-in replacement must run through
the same public bundle and evidence model as custom implementations.

### KSF-10: The Agent Owns the Replication Listener

**Severity:** Medium
**Affected path:** Primarily legacy/default; constrains all hosted transport

**SF contract:** Replicator `open` creates/registers the endpoint and
`close`/`abort` tears it down.

**Kuberic behavior:** `AgentService::serve` binds control and replication
sockets before runtime reconstruction and directly serves replication/copy RPCs
(`kuberic-runtime/src/host/service.rs:512-568,1203-1315`). Process startup
creates the dispatcher/listener
(`kuberic-runtime/src/host/process.rs:307-357`). Default `open` opens engine
state and returns a configured string
(`kuberic-runtime/src/replicator/mod.rs:991-997`).

**Consequence:** Endpoint choice and lifetime are not atomic with public
Replicator open/close/abort. A custom endpoint return cannot become
authoritative.

**Required conformance property:** Select one endpoint owner before binding;
the public Replicator lifecycle must control its replication transport.

### KSF-11: Application Role-Change Addresses Are Discarded

**Severity:** Medium
**Affected path:** Both

**SF contract:** Application `change_role` returns the service endpoint that
the runtime stores and publishes.

**Kuberic behavior:** `RoleChange` carries `service_address`
(`kuberic-runtime/src/application.rs:30-33`), but role execution ignores the
returned value (`kuberic-runtime/src/host/hosting.rs:3601-3636`).

**Consequence:** An application cannot publish a role-specific client endpoint;
the return value is observationally dead.

**Required conformance property:** Retain, clear and publish the returned
address as part of exact role completion.

### KSF-12: Supersession Does Not Contain Arbitrary Public Callbacks

**Severity:** High
**Affected path:** Public/custom and shared lifecycle callbacks

**SF contract:** Close, abort or replacement cancels and drains outstanding
operations. Cancellation prevents both current publication and obsolete
external work.

**Kuberic behavior:** `prepare_effect` holds lifecycle serialization while
awaiting public open/role/epoch/application/close work
(`kuberic-runtime/src/host/hosting.rs:2863-2960,3396-3688`). Newer authority can
invalidate generations, but arbitrary callbacks have no exact host-owned task
handle and can retain the lock until they return. Command tasks are aborted as
a set only during service shutdown
(`kuberic-runtime/src/host/service.rs:410-456`).

**Consequence:** Supersession or shutdown can stall behind a callback forever;
external mutation can continue after host state has rejected its result.

**Required conformance property:** Run every cancellable public callback under
exact host task ownership with bounded cancellation and drain behavior.

### KSF-13: Transient Fault Is Diagnostic Instead of Lifecycle Control

**Severity:** High
**Affected path:** Both

**SF contract:** Transient fault causes restart for persisted state or drop for
volatile state. Fault closes access.

**Kuberic behavior:** The host records the fault
(`kuberic-runtime/src/host/hosting.rs:2447-2468`), but reporting and controller
health exclude only permanent faults
(`kuberic-runtime/src/host/report.rs:140-161`;
`kuberic-controller/src/evaluator.rs:4089-4127`).

**Consequence:** A replica that reports it cannot recover can remain ready and
continue serving indefinitely.

**Required conformance property:** Treat transient fault as an exact
restart/drop request with immediate access revocation.

### KSF-14: Graceful-Close Failure Has a Different Outer Result

**Severity:** Medium
**Affected path:** Both

**SF contract:** Native FUP aborts a child whose graceful close fails,
continues teardown and completes the composite close after containment.

**Kuberic behavior:** Replicator or application close failure triggers abort
cleanup and then returns the child error
(`kuberic-runtime/src/host/hosting.rs:3639-3688`).

**Consequence:** A fully contained terminal replica can retain/retry a durable
failed close and trigger redundant recovery work.

**Required conformance property:** Explicitly choose and document the outer
close result after successful containment; native parity means normalizing the
contained child failure.

### KSF-15: Durable Replay Assumes Callback Idempotence

**Severity:** Medium
**Affected path:** Both, especially public/custom

**SF contract:** Native operations reconcile known duplicate cases, but the
public interface does not provide a universal transaction joining application
mutation with RA persistence.

**Kuberic behavior:** Effect intent is durable before callback execution;
applied/completed markers are persisted afterward
(`kuberic-runtime/src/host/runtime_adapter.rs:202-370`). A crash or store error
after callback success can therefore replay the same public callback. The
current boundary documentation acknowledges that custom configuration
callbacks can mutate durable state without rollback
([Replicator Boundary and Native Proof](replicator-boundary.md#custom-authority-admission)).

**Consequence:** Correctness relies on undocumented callback idempotence or
operation-specific duplicate reconciliation.

**Required conformance property:** Define replay semantics per public
operation and verify crash-after-success behavior. Exact-once publication does
not imply exact-once external mutation.

### KSF-16: The Legacy Engine Restores Private Continuation State

**Severity:** Low architectural incompatibility
**Affected path:** Legacy/default managed

**SF contract:** V1 role engines, queues, sessions and build cursors are
reconstructed from RA configuration and provider state after restart.

**Kuberic behavior:** `DefaultReplicatorInner` owns durable authority,
replication progress, local-write and build stores
(`kuberic-runtime/src/runtime.rs:205-256`). Open restores local writes
(`kuberic-runtime/src/runtime.rs:2331-2350`) and build admission restores build
progress (`kuberic-runtime/src/runtime.rs:2399-2448`).

**Consequence:** The Replicator is a second durable control owner and restart
continues private engine workflow instead of reconstructing a V1 engine. This
is stronger continuation, but it is not SF V1 ownership and blocks a
public-only correctness boundary.

**Required conformance property:** Treat this as a legacy extension and remove
it during the fresh-state stateless-default cutover rather than presenting it
as V1 parity.

## Unimplemented or Deliberately Different Surface

These gaps are distinct from incorrect implementation of an existing method:

| SF surface | Current Kuberic status |
|---|---|
| `IStatefulServiceFactory::create_replica` | Kuberic supplies an application object to `PodRuntime`; there is no equivalent runtime factory callback |
| partition `CreateReplicator` | No SF-shaped partition method; construction is split across `ReplicatorFactory`, its context and interface-bundle registration |
| `report_move_cost` | No public equivalent |
| partition health report | No public equivalent |
| replica health report | No public equivalent |
| COM interop access | Not applicable to the native Rust/Kubernetes runtime |

These methods need not be added merely for naming parity. If Kuberic claims
behavioral compatibility for the corresponding placement or health feature,
it must provide an explicit public equivalent and document the mapping.

## Confirmed Current Alignments

The audit also confirmed behavior that should be preserved:

1. **Application open before Replicator open**
   `kuberic-runtime/src/host/hosting.rs:3396-3427` matches
   `ProxyActionsList.cpp:34-53`.

2. **Replicator role before application role**
   `kuberic-runtime/src/host/hosting.rs:3450-3455,3487-3636` matches native
   promotion ordering.

3. **Replicator close/abort before application close/abort**
   `kuberic-runtime/src/host/hosting.rs:2782-2794,3639-3688` preserves object
   order even though composite close result differs.

4. **Managed previous/current write quorum**
   `kuberic-runtime/src/replicator/quorum.rs:666-688` requires both quorums.

5. **Managed build reaches a durable copy/catch-up boundary**
   `kuberic-runtime/src/runtime.rs:768-832` preserves the core copy-plus-
   replication completion invariant.

6. **Access publication is staged and fail-closed**
   `kuberic-runtime/src/host/coordinator.rs:571-603` and
   `kuberic-runtime/src/host/custom.rs:1433-1484` preserve ordered publication.
   The remaining problem is the strength and completeness of preceding proof.

7. **Stale host completion and predecessor process sessions are strongly fenced**
   `kuberic-runtime/src/host/runtime_adapter.rs:320-370` and
   `kuberic-runtime/src/host/service.rs:327-397` reject obsolete publication
   and traffic.

8. **The default `on_data_loss(true)` implementation has the right local reset shape**
   `kuberic-runtime/src/replicator/mod.rs:1026-1041` and
   `kuberic-runtime/src/runtime.rs:619-655` reread provider progress and reset
   pending state. The orchestration defect is that production never routes the
   callback.

## Method-Level Conformance Matrix

| Public operation | Managed/default | Public/custom |
|---|---|---|
| partition `create_replicator` | Factory returns one public bundle plus private managed capabilities | Factory/context registration returns a public bundle; coherence depends on creation identity |
| application `open` | Partial: correct order; host owns endpoint | Partial: correct order; cancellation/endpoint caveats |
| application `change_role` | Partial: correct order; address discarded | Partial: same |
| application `close` | Partial: correct order; outer error differs | Partial: same |
| application `abort` | Aligned object order | Aligned object order |
| Replicator `open` | Not aligned for endpoint ownership | Partial; returned endpoint is not authoritative |
| Replicator `change_role` | Partial; private managed work supplements public call | Partial; configuration can precede role |
| `update_epoch` | Partial through private repair | Not aligned for same-role secondary |
| `get_current_progress` | Not aligned for primary committed semantics | Not aligned because value is upgraded into stronger fields |
| `get_catch_up_capability` | Partial; private observations supplement it | Implementation-dependent |
| `on_data_loss` | Method exists, production route absent | Method exists, production route absent |
| catch-up configuration | Public description discarded in favor of private authority | Not aligned ordering and payload |
| current configuration | Public description reduced to private identity | Not aligned payload |
| catch-up wait | General engine support; swap proof unresolved | General support; swap `All`/`All` not aligned |
| build | Strong managed completion | Non-SF descriptor |
| remove | Strong generation fencing | Can overlap running build |
| state `replicate` | Aligned completion shape for local durability plus admitted PC/CC quorums | Application-defined; host cannot independently prove its replication algorithm |
| replication/copy streams | Public streams exist; agent owns transport and build orchestration | Secondary-role semantics are implementation-owned |
| update Replicator settings | Replaces in-memory settings; endpoint ownership remains outside Replicator | Implementation-defined |
| provider `update_epoch` | Default engine invokes provider epoch work | Custom Replicator owns provider delivery; host cannot prove it |
| provider committed progress | Used on default open and data-loss reset, but default progress can overclaim local tail | Custom provider relationship is implementation-owned |
| provider copy context/state | Managed build preserves copy-plus-retained-replication completion | Custom build semantics are implementation-owned |
| provider `on_data_loss` | Local implementation exists; production primary callback route absent | Same production routing gap |
| partition read/write status | Strong fail-closed projection, conditional proof | Same |
| `report_fault` | Permanent partially effective; transient not effective | Same |

## Required Conformance Tests

Existing tests strongly cover host ordering, generation fencing, restart
identity and managed quorum internals. The following public observable tests
are still required:

1. **Data loss:** end-to-end promotion for `false`, `true` and error results;
   verify progress reread and secondary retirement/rebuild after `true`.
2. **Strict primary ordering:** a custom Replicator that returns `NotPrimary`
   if configuration precedes primary role.
3. **Same-role secondary epoch:** require one epoch callback and rejection of
   old-epoch traffic.
4. **Public joint quorum:** hold a real public replication completion pending
   after only previous or current quorum, then complete after both.
5. **`must_catchup`:** satisfy an ordinary write quorum without the successor
   and prove the public catch-up future remains pending.
6. **Swap trace:** with specific-quorum support, verify first write-quorum
   wait, write revocation, epoch/configuration refresh, second write-quorum
   wait, successor inclusion and non-blocking unrelated replica; separately
   verify the `All` fallback when specific quorum is unavailable.
7. **Build descriptor:** reject non-invalid target progress and any in-build
   configuration membership.
8. **Cancel-before-remove:** observe exact build future cancellation and
   settlement before removal.
9. **Callback supersession:** block each public async callback, submit newer
   authority/close/abort, and require bounded cancellation and progress.
10. **Transient fault:** verify immediate access closure and exact
    restart/drop of the current incarnation.
11. **Service endpoint:** return a different address per role and verify
    publication/clearing.
12. **Replay cuts:** crash after callback success but before applied/completed
    persistence and prove operation-specific convergence.

Tests must assert public outcomes and ordering rather than only internal tracker
or snapshot fields.

## Conformance Priority

The semantic repair order is constrained by dependencies:

1. route data-loss handling before access;
2. establish role-before-primary-configuration ordering;
3. deliver exact epoch barriers to same-role secondaries;
4. correct public replica/configuration/build values;
5. own and cancel exact public callback tasks;
6. implement successor-specific double catch-up;
7. correct progress meanings and remove inferred private evidence;
8. move the built-in Replicator to the public-only boundary;
9. transfer endpoint lifetime to the selected Replicator owner;
10. align fault, address publication and close-result policy.

This sequence preserves the protected public interfaces. It changes how the
runtime owns, orders, cancels and records their operations.

## Relationship to the Roadmap

[Service Fabric Alignment and Runtime Simplification](service-fabric-alignment.md)
defines the phased migration plan.
[Stateless Default Replicator](stateless-default-replicator.md) defines the
target public-only V1 engine.
[Replicator Boundary and Native Proof](replicator-boundary.md) documents the
current private managed capability boundary.

Those documents describe direction and target ownership. This document is the
normative semantic/conformance reference for evaluating current and future
runtime behavior. Roadmap intent is not evidence that a current mismatch has
already been resolved.
