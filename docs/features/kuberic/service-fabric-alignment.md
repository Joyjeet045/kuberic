# Service Fabric Alignment and Runtime Simplification

This document compares Kuberic's runtime and local reconfiguration agent with
the Service Fabric Reconfiguration Agent (RA) and Replicator. It identifies
which Service Fabric architecture boundaries would simplify Kuberic, which
Kuberic guarantees must remain, and a phased plan for reaching the target
design.

The Service Fabric findings come from a read-only analysis of the local
`service-fabric` checkout at commit
`3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`. Service Fabric citations are
relative to that checkout. Kuberic citations are relative to this repository.

This is an architecture direction and implementation plan, not a description
of already completed changes.

## Executive Summary

Kuberic already follows several important Service Fabric principles:

- the controller evaluates observed cluster state separately from executing a
  plan;
- the local agent durably records reconfiguration stages and effect intent;
- lifecycle work independently completes role, epoch, application role,
  access and cleanup;
- process sessions, resource identities, generations and receipts reject stale
  work;
- copy completion and replication catch-up are distinct from membership
  admission.

The main difference is state and operation ownership. Service Fabric separates
the durable RA partition aggregate, transient operation envelope, runtime
proxy, role-specific replication engines, membership manager and per-peer
session state. Kuberic has corresponding responsibilities, but authority,
access, builds, topology and progress are represented across the durable
agent, hosting layer, custom-replicator host and default replication engine.
Broad runtime actions and snapshots cross these layers.

The primary simplification should therefore be a **typed replica-runtime
boundary**, followed by state-owner separation and then decomposition of
`host/custom.rs`. Splitting the file before correcting the boundary would
distribute the existing coupling without reducing it.

## Service Fabric Architecture

### Reconfiguration Agent

Service Fabric gives the RA three visibly different kinds of object:

| Object | Responsibility |
|---|---|
| `FailoverUnit` | Durable partition aggregate and intended replica/configuration state |
| `EntityJobItem` | Transient message or operation envelope with checks and pending actions |
| `FailoverUnitProxy` | Actual service and replicator objects, observed roles and asynchronous runtime operations |

The serialized `FailoverUnit` contains recovery-relevant partition,
configuration, replica and lifecycle facts, but not proxy handles or the live
reconfiguration phase object
(`src/prod/src/Reliability/Failover/ra/FailoverUnit.h:566-577`).
`EntityJobItem` carries execution context and actions rather than becoming
durable entity state
(`src/prod/src/Reliability/Failover/ra/Infrastructure.EntityJobItem.h:119-141`).
`FailoverUnitProxy` separately owns service and replicator roles, configuration
and catch-up stages, operation managers and runtime handles
(`src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.h:548-588`).

RA work is serialized per partition instead of under one node-wide execution
lock. A scheduled list runs against one locked entity and can commit several
jobs together
(`src/prod/src/Reliability/Failover/ra/Infrastructure.EntityScheduler.h:14-25`,
`src/prod/src/Reliability/Failover/ra/Infrastructure.EntityJobItemListExecutorAsyncOperation.h:49-72`).
Entity publication follows a successful store commit, and queued actions are
released only on success
(`src/prod/src/Reliability/Failover/ra/Infrastructure.EntityMap.h:227-285,338-382`,
`src/prod/src/Reliability/Failover/ra/Infrastructure.EntityJobItem.h:228-279`).

Reconfiguration uses named phases and guarded transitions rather than one
general lifecycle callback
(`src/prod/src/Reliability/Failover/ra/ReconfigurationState.cpp:174-273`).
The RA translates those decisions into explicit proxy recipes for
configuration, epoch, catch-up, role, access and close operations
(`src/prod/src/Reliability/Failover/ra/ProxyActionsList.cpp:75-81,106-186`).
Replication algorithms remain behind that proxy boundary, although failover
selection policy remains an RA responsibility
(`src/prod/src/Reliability/Failover/ra/FailoverUnit.h:716-725`).

Retry and reporting are separate subsystems rather than branches inside every
transition handler. FM reporting separates pending-entity selection, sending,
post-send updates and retry scheduling
(`src/prod/src/Reliability/Failover/ra/MessageRetry.FMMessageRetryComponent.cpp:15-32,71-92`).

### Replicator

Service Fabric's Replicator is also divided by ownership:

| Object | Responsibility |
|---|---|
| `Replicator` | Public lifecycle coordination and role-engine replacement |
| `PrimaryReplicator` | Primary-specific build, catch-up and state-provider coordination |
| `SecondaryReplicator` | Copy/replication receive and application coordination |
| `ReplicaManager` | Membership, replication admission, quorum progress and client completion |
| `RemoteSession` | Per-peer sending and ACK processing |

Role change is an explicit transition/action process that closes or replaces
the role-specific engine
(`src/prod/src/Reliability/Replication/Replicator.ChangeRoleAsyncOperation.cpp:37-121`).
`ReplicaManager` owns current, previous and idle membership, quorum parameters,
the replication queue and pending client operations
(`src/prod/src/Reliability/Replication/ReplicaManager.h:293-339`).
Per-peer ACK processing belongs to `RemoteSession`, while aggregate quorum and
client completion remain in `ReplicaManager`
(`src/prod/src/Reliability/Replication/RemoteSession.cpp:500-610`,
`src/prod/src/Reliability/Replication/ReplicaManager.cpp:2234-2274`).

Replication admission linearizes while the operation is queued and registered
under the manager lock. Network fan-out and client completion execute after
that critical section
(`src/prod/src/Reliability/Replication/ReplicaManager.cpp:2089-2181,2234-2274`).
ACK processing uses a coalescing per-peer runner: arrivals update cached
progress while one processor drains until stable
(`src/prod/src/Reliability/Replication/RemoteSession.cpp:500-568,613-638`).

Service Fabric distinguishes retained, completed and committed queue positions
instead of treating progress as one LSN
(`src/prod/src/Reliability/Replication/OperationQueue.h:287-306`).
A replica build is complete only after both copy completion and replication
through the boundary captured when copy enumeration ended
(`src/prod/src/Reliability/Replication/PrimaryReplicator.BuildIdleAsyncOperation.cpp:42-75,83-135`).
Catch-up predicates explicitly account for current and previous configurations
and required replicas
(`src/prod/src/Reliability/Replication/PrimaryReplicator.CatchupAsyncOperation.cpp:43-103`).

## Current Kuberic Architecture

The intended Kuberic boundary is already visible:

| Component | Current responsibility |
|---|---|
| Controller evaluator/executor | Pure cluster planning followed by Kubernetes or agent-command execution |
| Durable agent state | Authority, reconfiguration stage, journaled effects, retained results and transition evidence |
| Hosting layer | Application and replicator registration, staged role projection and effect execution |
| `CustomReplicatorHost` | Configuration, access, build, peer, topology and outbound adaptation |
| Default runtime replicator | Replication progress, copy/build streams, repair targets, local-write fencing and log mechanics |

Evaluation and execution are separate
(`kuberic-controller/src/evaluator.rs:56-165`,
`kuberic-controller/src/executor.rs:26-89`).
The durable agent state owns the facts required to resume reconfiguration
(`kuberic-runtime/src/host/state.rs:119-175`).
The coordinator persists and advances explicit transition stages
(`kuberic-runtime/src/host/coordinator.rs:318-614`).

The complexity appears below that boundary:

- hosting carries a fallback runtime snapshot and process-local lifecycle
  projection (`kuberic-runtime/src/host/hosting.rs:124-163`);
- `CustomReplicatorHost` owns sessions, addresses, retirements, build state,
  receipts, configurations, access generations, locks and another runtime
  snapshot (`kuberic-runtime/src/host/custom.rs:1440-1472`);
- the default replication engine also stores role, access, admitted authority,
  removal, retirement, copy and replication progress
  (`kuberic-runtime/src/runtime.rs:49-82,168-200`);
- reporting composes durable and runtime observations and applies precedence
  and reconciliation rules (`kuberic-runtime/src/host/report.rs:31-95,102-284`).

Some repetition is intentional: durable truth, execution fencing and public
projection are different concepts. The problem is that the distinctions are
encoded through merge rules and conventions rather than narrow types owned by
specific components.

## Differences That Create Complexity

### Capability Views Without Capability Owners

`LifecycleWiring` presents separate process, authority, access, build,
topology, observation and outbound interfaces, but one backend still
implements the complete set
(`kuberic-runtime/src/host/lifecycle.rs:24-183`).
This narrows consumers without dividing implementation state.

Service Fabric uses distinct durable aggregate, proxy and replication owners.
Kuberic should retain its capability-facing interfaces but give those
capabilities explicit state owners.

### RA Vocabulary Crosses the Replicator Boundary

`ManagedReplicatorLifecycle` includes authority admission, access publication,
peer registration, topology effects, builds, recovery and full observations
(`kuberic-runtime/src/replicator/mod.rs:83-119`).
The native boundary can receive the general `RuntimeEffectAction` vocabulary.

Service Fabric sends typed runtime instructions through its proxy. The
replicator handles configuration, role, epoch, catch-up, copy, replication and
progress; it does not execute the RA's complete transition vocabulary.

### Broad Snapshots Act as Completion Contracts

`RuntimePostcondition` carries most of `RuntimeSnapshot`: authority, role,
access, topology, progress, removal, retirement and all builds
(`kuberic-runtime/src/effects.rs:104-181`).
Consequently, an operation that changes one fact can depend on unrelated
fields, and every layer must understand the same broad representation.

Service Fabric uses operation-specific completion and progress concepts.
Kuberic should preserve exact replay and receipt validation while narrowing the
result required by each effect.

### Multiple Overlapping Serialization Domains

Kuberic uses command serialization, effect sequencing, native delivery
fencing, the custom-host gate, configuration and access generations, access
commit locking, session registration and per-build cleanup locks. Each protects
a real race, but no single abstraction explains the complete cross-layer
commit point.

Service Fabric's useful pattern is not its exact locking implementation. It is
the visible transaction shape: mutate one partition aggregate, commit it, then
release success-gated runtime actions while preserving partition ordering.

### Reporting Performs Reconciliation

Kuberic reporting compares durable and runtime state, applies precedence rules
and can participate in deferred restoration. This makes observation part of
state repair and expands the consistency fence.

Service Fabric separates transition execution, retry scheduling and FM
reporting. Kuberic should move repair into an explicit recovery/control task
and make reporting a composition of owned, read-only observations.

## Target Architecture

### Explicit State Owners

Kuberic should converge on three principal owners:

#### `PartitionAgentState`

Durable RA-equivalent state:

- admitted authority and epoch;
- current and previous configurations;
- reconfiguration stage and command ownership;
- effect intent, completion and retained results;
- transition-specific controller evidence;
- recovery and retry obligations.

This type must not contain runtime handles, active tasks, channels, locks or
replicator-private progress structures.

#### `ReplicaHostProxy`

Transient runtime-execution state:

- application and replicator instances;
- actual application and replicator roles;
- pending role, epoch, access, build and close operations;
- externally visible access publication;
- process-session and runtime-incarnation fencing;
- translation of typed agent instructions into public lifecycle calls.

This corresponds to the useful part of Service Fabric's
`FailoverUnitProxy`. It should not own durable transition authority or
replication algorithms.

#### `ReplicationEngine`

Replication and copy state:

- current, previous and idle membership;
- local epoch and role;
- write admission and fencing;
- copy and replication streams;
- per-peer sessions and ACK progress;
- retained, applied, committed and reclaimable progress;
- quorum and catch-up predicates;
- build and removal mechanics.

This owner should not understand controller transition evidence or the general
agent effect journal.

### Typed Runtime Instructions

The agent-to-proxy boundary should use explicit operations such as:

- install replication configuration;
- update epoch;
- change replicator role;
- change application role;
- prepare or revoke access;
- wait for catch-up;
- build or remove a replica;
- fence writes;
- close or abort.

The proxy can compose public service and replicator calls into the required
order. The replication engine receives only replication-specific operations.
General `RuntimeEffectAction` values must not cross into
`ManagedReplicatorLifecycle`.

### Typed Outcomes and Receipts

Each operation should return only the proof needed to complete that operation:

- role-change outcome;
- epoch-update receipt;
- access-preparation receipt;
- catch-up receipt;
- build-completion receipt;
- certified-prefix receipt;
- secondary-removal or retirement receipt.

Full runtime snapshots remain useful for diagnostics, testing and reporting,
but should not be the normal effect-completion type.

### One Local Commit Protocol

Every agent-owned transition should follow one documented protocol:

1. validate the command against durable identity and revision;
2. compute the durable transition and ordered runtime instructions;
3. persist intent and the new durable state;
4. execute typed proxy instructions;
5. validate operation-specific fences and record narrow receipts;
6. publish access or externally visible completion;
7. recover or retry from persisted intent after interruption.

External calls must not hold durable-state mutexes. A durable revision or
operation token should revalidate ownership when execution resumes. Existing
generation checks remain until this protocol demonstrably replaces their
individual safety responsibilities.

## Simplification Plan

### Phase 1: Establish the Typed Replica-Runtime Boundary

**Goal:** stop exposing the complete RA effect vocabulary to the replication
engine.

Planned work:

- define typed proxy instructions and operation-specific outcomes;
- separate application-role, replicator-role and access operations;
- replace `ManagedReplicatorLifecycle::apply_topology(RuntimeEffectAction)`
  with explicit replication operations;
- separate durable agent authority from executable replication
  configuration;
- retain adapters temporarily so behavior and recovery remain unchanged.

Exit criteria:

- no general `RuntimeEffectAction` crosses the managed replicator boundary;
- the engine does not consume controller transition evidence;
- current ordinary, crash-boundary and live test identities remain intact;
- stale session, generation and receipt rejection remains covered.

### Phase 2: Separate State Ownership and Observations

**Goal:** make durable, host-runtime and replication-engine facts impossible to
confuse.

Planned work:

- introduce explicit durable agent, host proxy and engine observation types;
- remove full `RuntimeSnapshot` ownership from layers that need only a
  projection;
- define one reporting composition rule for each field;
- replace implicit precedence with constructors that require the owning view;
- make reporting read-only and move restoration/reconciliation into an
  explicit recovery task.

Exit criteria:

- each authority, role, access and progress field has one documented owner;
- reporting does not mutate or repair lifecycle state;
- adding an engine-only progress field does not change durable agent
  serialization;
- persisted data remains backward-readable or receives an explicit migration.

### Phase 3: Narrow Effect Completion

**Goal:** stop using a nearly complete runtime snapshot as every effect's
postcondition.

Planned work:

- define operation-specific outcome and receipt types;
- update effect persistence and replay to compare the relevant outcome only;
- keep a diagnostic snapshot outside the correctness contract;
- consolidate duplicated snapshot-to-postcondition conversion and retained
  result handling.

Exit criteria:

- role, epoch, access, catch-up and build effects have independent completion
  contracts;
- unrelated runtime fields cannot invalidate an effect replay;
- exact replay still rejects a changed action or changed canonical result;
- topology receipts remain authority- and operation-bound.

### Phase 4: Consolidate Partition Execution

**Goal:** make one local transaction model explain command admission, durable
commit and runtime effects.

Planned work:

- introduce a partition execution context containing durable revision,
  transition result and ordered proxy instructions;
- centralize command admission and supersession checks;
- execute external calls outside state locks and revalidate revision on
  completion;
- identify which configuration, access and build generations remain necessary;
- preserve separate per-peer and replication-engine synchronization.

Exit criteria:

- one documented linearization point exists for durable authority changes;
- commit failure cannot publish runtime success;
- supersession and cancellation have one ownership rule;
- redundant generation or lock domains are removed only after adversarial
  tests demonstrate equivalent fencing.

### Phase 5: Decompose `host/custom.rs`

**Goal:** make the source layout reflect the new ownership boundaries.

Planned modules:

| Module | Responsibility |
|---|---|
| `proxy/access.rs` | Access preparation, publication, acceptance and rollback |
| `proxy/configuration.rs` | Configuration projection and peer-session registration |
| `proxy/build.rs` | Build admission, generation, cancellation and completion |
| `proxy/topology.rs` | Catch-up, failover, switchover, removal and retirement instructions |
| `proxy/managed.rs` | Managed/native proof adaptation |
| `proxy/mod.rs` | Host-proxy composition and lifecycle registration |

State such as `AccessState`, `BuildState`, `ConfigurationState` and
`PeerState` should own their locks and invariants. A file-only move without
state ownership is not sufficient.

Exit criteria:

- no replacement file becomes another universal lifecycle implementation;
- capability modules access only their owned state plus explicit shared
  context;
- lock ordering is documented at the owning type;
- the current source-analysis guards become smaller rather than learning the
  structure of additional monolithic files.

### Phase 6: Strengthen Progress and Retry Types

**Goal:** adopt the useful Replicator distinctions without copying its legacy
queue machinery.

Planned work:

- introduce typed received, applied, committed, verified and catch-up
  positions;
- represent build completion as copy completion plus a replication fence;
- isolate per-peer progress updates with a coalescing one-runner mailbox where
  useful;
- separate transition retry policy and reporting retry policy from transition
  handlers;
- test catch-up, quorum and queue predicates independently of transport.

Exit criteria:

- progress values with different meanings cannot be accidentally compared;
- slow peers cannot serialize unrelated peer progress;
- retry state has explicit sequence/ownership fencing;
- joint current/previous configuration guarantees remain unchanged.

## Guarantees to Preserve

Alignment must not weaken:

- durable transition stages and deterministic replay;
- exact Pod, PVC, replica and process-session fencing;
- epoch and configuration monotonicity;
- separate replicator role, application role and access completion;
- intent-before-effect persistence;
- durable completion before public access publication;
- copy plus replication-gap closure;
- current and previous configuration quorum requirements;
- cancellation-safe build and access cleanup;
- native receipt and generation validation;
- independent custom replicators using only public SF-shaped interfaces.

## Service Fabric Patterns Not to Copy

Kuberic should preserve Service Fabric's safety boundaries without importing
its implementation history:

- COM begin/end callback and synchronous-completion machinery;
- shared-pointer lifetime scaffolding;
- giant partition methods and scattered lifecycle booleans;
- flag-parameterized universal queues;
- action ordering that depends on implicit lock scope;
- process termination as a substitute for an explicit retry contract;
- non-durable reconfiguration phases;
- long proxy recipes with special cases hidden in imperative action lists.

Kuberic's persisted stage machine, typed Rust ownership and exact effect replay
are stronger foundations. The objective is to make their boundaries narrower
and more visible, not to reproduce Service Fabric internally.

## Recommended Starting Point

Begin with **Phase 1: Establish the Typed Replica-Runtime Boundary**. It creates
the architectural seam needed by every later simplification and prevents the
`host/custom.rs` decomposition from becoming a cosmetic file split.

The first implementation should be considered complete only when the managed
replicator no longer accepts general runtime effects, access publication has a
clear host-proxy owner, operation-specific receipts replace broad completion
where introduced, and all existing fencing and recovery behavior remains
observable through the current validation suites.
