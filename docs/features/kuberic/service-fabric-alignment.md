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

The typed boundary, state-owner separation and narrow effect contracts are now
complete foundations. The remaining simplification should move the runtime and
controller to a public-only contract, build a new stateless default Replicator
beside the legacy engine, then delete the managed path. Decomposing
`host/custom.rs` before that deletion would organize substantial code that the
replacement removes, so source decomposition is deferred until the
post-cutover shape is known.

The default-replicator durability target is specified separately in
[Stateless Default Replicator](stateless-default-replicator.md). That design
makes the managed engine restart-reconstructable like the Service Fabric V1
Replicator while retaining agent-owned topology durability and
state-provider-owned application durability.

The current default replicator is not restart-stateless. Its durable progress,
local-write and build stores give it some of the continuation characteristics
of the Service Fabric V2 Transactional Replicator. However, those records are
spread across the agent store, replication engine and application
`DurableState`; they do not form V2's coherent transaction log, provider
checkpoint and replay model.

Alignment therefore has two distinct targets:

1. the ordinary default replicator becomes an SF V1-style transient transport
   and quorum engine;
2. an optional future V2 layer may deliberately add a transactional log,
   provider checkpoints, recovery replay, copy history and backup above that
   V1 engine.

Existing managed-engine persistence must not be retained merely by calling it
V2. Agent workflow records remain agent-owned, application durability remains
provider-owned for V1 services, and any future V2 storage must have an
explicit transactional-replicator contract.

These targets make the roadmap branch after the shared agent/runtime
foundations. Partition execution and progress semantics remain alignment work.
Source decomposition is then an independent maintainability track, while the
default-replicator track removes engine metadata persistence before an optional
transactional-replicator track can become authoritative for any application.

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
| Hosting layer | Application and replicator registration, actual host-proxy role/access projection, effect execution and service-owned recovery tasks |
| Custom-authority operation state | Attempt ownership, invalidation, pending exact recovery and restoration serialization |
| `CustomReplicatorHost` | Configuration, access, build, peer, topology and outbound adaptation |
| Default runtime replicator | Replication progress, copy/build streams, repair targets, local-write fencing and log mechanics |

Evaluation and execution are separate
(`kuberic-controller/src/evaluator.rs:56-165`,
`kuberic-controller/src/executor.rs:26-89`).
The durable agent state owns the facts required to resume reconfiguration
(`kuberic-runtime/src/host/state.rs:119-175`).
The coordinator persists and advances explicit transition stages
(`kuberic-runtime/src/host/coordinator.rs:318-614`).

The Phase 2 ownership boundary is now explicit below that layer:

- hosting stores process-local adapter state as `ReplicaRuntimeState`, emits
  `HostProxyObservation`, and holds one host-lifetime custom-authority
  containment owner plus service-owned recovery task ownership
  (`kuberic-runtime/src/host/observation.rs:50-169`,
  `kuberic-runtime/src/host/hosting.rs:259-300,1590-1692`);
- `CustomAuthorityContainment` owns independent custom-authority attempt
  invalidation, pending exact recovery, the authorization latch, and
  callback/restoration serialization
  (`kuberic-runtime/src/host/custom/authority.rs:17-150`);
- `CustomReplicatorHost` owns sessions, addresses, retirements, build state,
  receipts, configurations, access generations and non-durable host/proxy
  state, and produces exact pending-restoration observations
  (`kuberic-runtime/src/host/custom.rs:1600-1643,3940-4024`);
- the default replication engine stores role, access, executable replication
  configuration, removal, retirement, copy and replication progress; durable
  admitted authority remains host-owned
  (`kuberic-runtime/src/runtime.rs:49-86,174-206`,
  `kuberic-runtime/src/replicator/configuration.rs:9-23`);
- reporting combines `DurableAgentObservation`, `HostProxyObservation` and
  `ReplicationEngineObservation` under an explicit linked fence. It does not
  refresh, reconcile, restore, retry or persist lifecycle state
  (`kuberic-runtime/src/host/observation.rs:24-244`,
  `kuberic-runtime/src/host/report.rs:14-303`);
- reporting validates the host authority projection against the complete
  durable `ReplicaAuthorityStore` value instead of reconstructing transition
  evidence from the broader agent aggregate.

Some repetition is intentional: durable truth, execution fencing and public
projection are different concepts. The distinctions are now represented by
owner-specific types and constructors. The broad `RuntimeSnapshot` remains
only in effect-evidence/postcondition and opt-in testing paths; projection-only
report, build, peer, outbound and restart consumers no longer transport it.

The agent service owns its control and replication listeners. Address-based
startup binds them internally, while the opt-in testing facade can transfer
already-bound listeners into the same serving lifecycle
(`kuberic-runtime/src/host/service.rs:456-490`,
`kuberic-runtime/src/testing/service.rs:76-85`). This closes a fixture
release/rebind race but does not change replication, authority or runtime state
ownership and does not require another simplification phase.

### Current Durability Is V2-Like but Not a V2 Replicator

The current default replicator is closer to SF V2 than SF V1 in one narrow
respect: it receives durable stores and restores engine work after process
restart. The host supplies durable authority, replication progress,
local-write, build-authority and build-progress capabilities. The engine uses
them to continue pending writes, replication boundaries and builds rather than
starting empty.

That resemblance must not obscure the ownership difference:

| Concern | Current Kuberic default replicator | SF V1 | SF V2 |
|---|---|---|---|
| Replicator-layer durable storage | Shared agent/engine stores | None | Dedicated transactional and physical log |
| Pending writes | Engine workflow journal | Process-local | Structured commit/abort transaction records |
| Application records | Opaque operations retained by `DurableState` | State provider-owned | V2 logical redo/undo records |
| Restart recovery | Agent records plus engine stores plus application progress | RA replay plus state-provider progress | Provider checkpoint plus V2 log replay |
| Builds and copy | Durable engine authority/progress plus application staging | Restarted from RA/provider state | Progress-vector selection, provider copy and log suffix |
| Checkpoints and backup | Application-specific | Application-specific | Framework-coordinated |

The current design therefore contains pieces of both models:

- V1-shaped public replication and state-provider interfaces;
- V2-like durable continuation inside the managed engine;
- application-owned retained history and copy state;
- agent-owned topology and effect journals.

This is overlapping persistence, not an intentional V2 architecture. A real
V2 layer would own a complete application transaction and recovery protocol:

```text
provider checkpoint + V2 durable log -> recovered application state
```

It would remain layered over the V1 transport and RA-facing lifecycle. It
would not own replica topology, reconfiguration effects or agent authority.
See
[SF V2 Transactional Replicator](../../background/service-fabric/v2-transactional-replicator.md)
for the reference model and
[SQLite on a V2 Transactional Replicator](../sqlite/v2-transactional-replicator.md)
for the proposed first Kuberic consumer.

## Differences That Create Complexity

### Capability Views Have Explicit Observation Owners

`LifecycleWiring` still projects process, authority, access, build, topology,
observation and outbound capabilities from a shared backend, but report,
recovery, build, peer and outbound consumers now receive owner-specific views.
`ReportObservationRuntime` contains only lifecycle observation. Durable effect
access publication remains in `AccessRuntime`; restoration/reconciliation and
progress mutation are reachable through `RecoveryRuntime` and
`RecoveryOwnerRuntime`
(`kuberic-runtime/src/host/lifecycle.rs:137-225,574-763`;
`kuberic-runtime/src/host/hosting.rs:466-555`).

### Transient Custom-Authority Ownership Is Explicit

Custom-authority admission now has an explicit host-lifetime
`CustomAuthorityContainment` owner and transient `CustomAuthorityAttempt`
guard, directionally aligned with Service Fabric's `EntityJobItem`. The owner
contains preflight, access closure, callback execution, authority publication,
exact recovery, invalidation and restoration serialization without changing a
replicator interface
(`kuberic-runtime/src/host/custom/authority.rs:17-190,264-399`,
`kuberic-runtime/src/host/custom.rs:3491-3493,3644-3646`).

This containment is necessary because independent custom configuration is a
stateful callback rather than a dry-run validator. It establishes the
operation-ownership seam needed before the private managed boundary changes,
without absorbing managed peer recovery, reporting or general partition
execution.

### RA Vocabulary Is Contained by a Typed Replicator Boundary

The current implementation uses a private managed boundary as an intermediate
containment step. `ManagedReplicatorLifecycle` includes explicit
executable-configuration, access, peer, topology, build, recovery and
narrow-observation operations
(`kuberic-runtime/src/replicator/mod.rs:83-166`). General
`RuntimeEffectAction`, durable `AdmittedAuthority`, transition kind and broad
runtime snapshots do not cross into the engine. The host proxy translates
controller-owned transitions into operation-specific engine instructions and
binds transient outcomes back to durable receipts.

This is not the final runtime/replicator contract. Runtime-to-replicator
interaction must use only the public `Replicator`, `PrimaryReplicator`,
`StateReplicator`, `StateProvider` and `DurableState` interfaces. The built-in
default may be product-provided, but it must not receive a privileged private
lifecycle, data-plane, observation or receipt channel from the runtime.

The target Service Fabric ownership pattern is that the replicator handles
replication configuration, fencing, copy, replication and progress behind its
public implementation. Application role remains outside the engine, while
replicator role, epoch, configuration, catch-up, build and removal cross only
their existing public methods. Agent receipts are runtime-owned records of
durable intent, exact public-call completion and current agent/session fences;
they are not returned through an unpublished engine contract.

### Effects Have Operation-Specific Completion Contracts

`RuntimeEffectOutcome` is a closed, action-compatible result model. Role,
epoch, access, catch-up and build completion retain only their canonical facts
and required authority or receipt proof. The durable completion transaction
matches the exact action/outcome pair and applies only that operation's
declared fields (`kuberic-runtime/src/effects.rs:250-619`;
`kuberic-runtime/src/host/sqlite_store.rs:417-991`).

`RuntimeSnapshot` remains a diagnostic/testing observation, not persisted
effect identity. Intent-only recovery establishes the first canonical result;
the applied marker persists that exact result; retained replay returns the
historical action/result record. Unrelated runtime observations cannot change
completion equality.

### Multiple Overlapping Serialization Domains

Kuberic uses command serialization, effect sequencing, native delivery
fencing, the custom-host gate, configuration and access generations, access
commit locking, session registration and per-build cleanup locks. Each protects
a real race, but no single abstraction explains the complete cross-layer
commit point.

Service Fabric's useful pattern is not its exact locking implementation. It is
the visible transaction shape: mutate one partition aggregate, commit it, then
release success-gated runtime actions while preserving partition ordering.

### Reporting Is Read-Only

Kuberic reporting compares durable, host and engine owner views under a linked
consistency fence, but it performs no lifecycle work. `RecoveryOwner` advances
eligible progress/access work at a bounded cadence without report polling.
`PartitionReportOwner` separately persists load/fault revisions. Managed peer
discovery hands exact deferred restoration to that owner rather than
publishing from the discovery observer. Both background owners and their
registered descendants are terminated before final persistence and runtime
abort
(`kuberic-runtime/src/host/recovery.rs:14-192`;
`kuberic-runtime/src/host/service.rs:536-621`).

## Target Architecture

### Explicit State Owners

Kuberic should converge on three long-lived principal owners plus a transient
operation owner:

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

Transient V1 replication and copy state:

- current, previous and idle membership;
- local epoch and role;
- write admission and fencing;
- copy and replication streams;
- per-peer sessions and ACK progress;
- retained, applied, committed and reclaimable progress;
- quorum and catch-up predicates;
- build and removal mechanics.

This owner should not understand controller transition evidence or the general
agent effect journal. For the ordinary default replicator, its queues, peer
sessions, ACK aggregation, build cursors and pending client completions are
process-local and reconstructed from agent authority plus state-provider
progress after restart.

A future V2 Transactional Replicator is a separate owner layered above this
engine. It may durably own transaction records, stable LSNs, provider
checkpoint coordination, recovery replay and copy history. Those concerns must
not be added back to the V1 engine as isolated continuation stores.

#### `PartitionOperation`

Transient operation state:

- exact command, effect and durable revision ownership;
- operation cancellation, invalidation and supersession;
- ordered runtime instruction progress;
- externally visible publication readiness;
- handoff to durable recovery obligations after interruption.

This corresponds to the useful part of Service Fabric's `EntityJobItem`. It
must not become another durable partition aggregate or universal runtime
facade. The first concrete extraction should distinguish a host-lifetime
`CustomAuthorityContainment` owner from a short-lived
`CustomAuthorityAttempt` guard. Containment owns the authorization latch,
pending recovery and callback/restoration serialization; the guard owns entry,
invalidation and completion of one attempt. `PartitionOperation` generalizes
the attempt model later rather than absorbing all host-lifetime policy.

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
General `RuntimeEffectAction` values must not cross a public replicator
interface, and the proxy must not bypass those interfaces through a managed
capability.

### SF-Style Public Evidence

The target runtime observes the same narrow surface as the SF RA proxy:

- first/last replication progress through public progress methods;
- success or failure of exact public role, epoch, configuration, catch-up,
  build, removal, data-loss, close and abort operations;
- service access computed and published by the runtime after those operations.

SF does not expose a build's internal copy/catch-up boundary to the RA.
Successful `BuildReplica` completion marks the remote proxy ready, while the
Replicator owns the boundary and transport session internally
(`src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.ReplicatorBuildIdleReplicaAsyncOperation.cpp:311-350`,
`src/prod/src/Reliability/Replication/PrimaryReplicator.BuildIdleAsyncOperation.cpp:42-135`).
Likewise, SF status reporting copies only first/last acknowledged LSNs from the
public Replicator
(`src/prod/src/Reliability/Failover/ra/ReconfigurationAgentProxy.ActionListExecutorAsyncOperation.cpp:801-823`).

Kuberic's runtime receipts therefore contain the exact public call, durable
operation/revision, authority and process-session fences. They do not contain
engine-private ACK sets, quorum positions, copy boundaries or native
observations. Full runtime snapshots remain useful for local diagnostics and
testing but are not a correctness or reporting contract.

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

### Roadmap Topology

The completed phases established narrow ownership and effect contracts. The
remaining roadmap avoids deep refactoring of the managed engine that Phase 7
will delete:

| Track | Purpose | Ordering |
|---|---|---|
| Public operation ownership | Give durable command/public-call sequencing one minimal owner | Phase 4 |
| Public V1 contract | Remove controller/runtime dependence on private evidence | Phase 5 |
| V1 replacement | Build the stateless engine beside the legacy engine, then cut over and delete the legacy path | Phases 6-7 |
| Deferred maintainability | Decompose the remaining custom host only after the managed path is gone | After Phase 7 |
| Optional V2 storage | Add a deliberate transactional log and provider protocol above V1 | Phase 8 |

The critical replication path is therefore:

```text
Phase 4 minimal public operation ownership
    -> Phase 5 public V1 evidence contract
    -> Phase 6 parallel stateless default replicator
    -> Phase 7 cutover and legacy deletion
    -> Phase 8 optional V2 transactional replicator and SQLite migration
```

Lock cleanup, legacy managed-state decomposition and per-peer mailbox
refactoring are not prerequisites. They belong in the replacement engine or
post-cutover cleanup, not in the engine that will be removed.

### Interface and Persistence Constraints

- `Replicator`, `PrimaryReplicator` and `StateProvider` are protected
  interfaces and must not change in any phase.
- Runtime-to-replicator interaction must use only public interfaces. The
  private managed lifecycle, data-plane, observation and receipt attachments
  are transitional implementation details and must be removed.
- Implementation-private types may organize the default replicator internally,
  but the runtime must not call or receive them.
- Public construction and settings types may carry immutable endpoint and
  security configuration, but must not carry callbacks, agent-store handles or
  host-only capabilities.
- Backward compatibility for persisted runtime data is not required. A phase
  that changes durable formats may bump the schema and reject old stores
  explicitly rather than adding migration code.
- Removing V1 engine persistence and introducing a V2 transactional log are
  separate changes. The latter requires new V2 transaction and provider
  contracts rather than extending the protected V1 `StateProvider`.

### Phase 0: Extract Custom-Authority Operation Ownership

**Goal:** contain the complexity added by fail-closed custom-authority
admission and exact recovery before changing the private managed lifecycle
boundary.

Planned work:

- introduce a host-lifetime `CustomAuthorityContainment` owner with a
  short-lived `CustomAuthorityAttempt` guard;
- move the authorization latch, pending recovery and callback/restoration
  serialization out of `RuntimeHost`;
- move attempt entry, invalidation and completion into the attempt guard;
- move custom-authority preflight, access closure, callback execution,
  authority publication and exact recovery orchestration out of
  `CustomReplicatorHost`;
- keep managed durable-first admission separate from independent custom
  callback-first containment;
- leave managed peer-recovery behavior and reporting redesign outside this
  extraction;
- preserve the existing `Replicator`, `PrimaryReplicator`,
  `ManagedReplicatorLifecycle`, `ManagedReplicatorDataPlane` and
  `ReplicatorInterfaces` contracts during this extraction.

Exit criteria:

- one host-lifetime owner contains custom-authority policy, and one attempt
  guard owns the lifetime and cancellation rules of each attempt;
- failed, dropped or ambiguous attempts cannot reopen access or record effect
  success;
- callback serialization and restoration restrictions cannot end merely
  because one waiting caller or attempt guard is dropped;
- restart reconciles only the exact pending candidate before obsolete
  configuration can be applied;
- report and peer-discovery restoration cannot race or outlive newer authority
  ownership;
- all custom-authority rejection, persistence-error, cancellation, restart and
  peer-session restoration regressions remain intact.

Implementation status: complete. The private extraction is implemented in
`kuberic-runtime/src/host/custom/authority.rs:17-399`; the managed lifecycle
and data-plane boundaries and all public replicator interfaces remain
unchanged.

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

Implementation status: complete. The managed lifecycle and data-plane
capabilities now expose explicit configuration, access, peer, build, progress,
acknowledgement, certified-prefix, switchover, secondary-removal and retirement
operations.
Application role, replicator role and epoch remain separate application/public
paths. The host projects durable `AdmittedAuthority` into engine-owned
`ManagedReplicaConfiguration`; runtime, log and quorum code consume neither
`RuntimeEffectAction`, transition kind nor full authority. Managed authority
admission uses prepare/fence, host persistence, exact prepared commit, common
publication and generation synchronization. Transient engine outcomes are
bound by the host to the unchanged durable receipt formats. Broad engine
snapshots were replaced by a narrow observation, temporary action adapters were
removed, secondary-removal and retirement stages have distinct private outcome
types, and source guards reject broad lifecycle/data-plane/store leakage and
wrong-stage outcome substitution. Managed restart validates full authority in
the host and reuses the prepared admission path.

This phase is an intermediate containment result, not the final public
boundary. Phase 7 removes the private managed attachment after its durable
responsibilities have moved to the agent or state provider and its
process-local responsibilities have moved behind the default replicator's
public implementation.

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
  explicit recovery task;
- define recovery triggers, retry ownership, shutdown behavior and
  supersession fencing independently of report polling.

Exit criteria:

- each authority, role, access, topology, build, peer and progress field has
  one documented owner and composition rule;
- reporting does not mutate or repair lifecycle state;
- eligible deferred recovery progresses without requiring a status request,
  while superseded or unauthorized restoration remains closed;
- adding an engine-only progress field does not change durable agent
  serialization;
- durable-format changes follow the explicit fail-closed schema-change policy.

Implementation status: complete. `DurableAgentObservation`,
`ReplicaRuntimeState` / `HostProxyObservation`,
`ReplicationEngineObservation` and consumer-specific
projections define the owner boundary. Reporting is read-only.
`RecoveryOwner`, `PartitionReportOwner` and `RecoveryTaskOwner` own
caller-independent recovery, observation persistence and descendant shutdown.
No durable schema or protected public replicator interface changed
(`kuberic-runtime/src/host/observation.rs:1-244`;
`kuberic-runtime/src/host/recovery.rs:14-192`;
`kuberic-runtime/src/host/report.rs:14-303`;
`kuberic-runtime/tests/lifecycle_capability_boundaries.rs:1266-1460`).
Evidence includes the complete 924-test ordinary tier, the six-test PostgreSQL
smoke tier, focused runtime/store/coordinator/recovery/crash coverage, strict
workspace Clippy and API/privacy/source guards, plus owned fresh-cluster
`replacement`, `quorum-loss` and `adversarial` KinD identities.

### Phase 3: Narrow Effect Completion

**Goal:** stop using a nearly complete runtime snapshot as every effect's
postcondition.

Planned work:

- define operation-specific outcome and receipt types;
- update effect persistence and replay to compare the relevant outcome only;
- define which durable fields each outcome is allowed to update;
- keep a diagnostic snapshot outside the correctness contract;
- consolidate duplicated snapshot-to-postcondition conversion and retained
  result handling;
- replace persisted result formats directly and reject old schemas explicitly
  when necessary.

Exit criteria:

- role, epoch, access, catch-up and build effects have independent completion
  contracts;
- each outcome updates only its declared durable fields;
- unrelated runtime fields cannot invalidate an effect replay;
- exact replay still rejects a changed action or changed canonical result;
- topology receipts remain authority- and operation-bound;
- incompatible old stores fail explicitly rather than being silently
  misinterpreted.

Implementation status: complete. Schema 6 persists tagged narrow outcomes and
rejects schema 5 without migration. The applied marker stores the exact
canonical result, and one shared recorded-effect shape is used for process-local
and durable retention. Store completion is exhaustive and action-owned; exact
duplicates are idempotent, changed actions/results fail closed, topology
receipts retain their variant-specific authority and operation binding, and
late catch-up/build completion revalidates current durable authority. Full
runtime snapshots remain available for diagnostics and opt-in testing only.
The public `Replicator` and `PrimaryReplicator` interfaces remain unchanged.
Evidence includes 946 ordinary tests, seven PostgreSQL smoke tests, strict
workspace Clippy, doctests, public API/privacy checks and lifecycle source
guards.

### Phase 4: Establish Public Partition Operation Ownership

**Goal:** give durable command ownership and ordered public calls one minimal
execution model without refactoring the legacy managed engine.

Planned work:

- introduce a `PartitionOperation` context containing operation ID, durable
  revision, cancellation/supersession state and ordered public instructions;
- centralize command admission and supersession checks;
- execute public application/replicator calls outside durable-state locks;
- revalidate revision and operation ownership before persisting completion or
  publishing access;
- retain existing legacy engine generations, locks and private adapters until
  cutover unless a change is required for correctness;
- use the same operation owner for custom replicators and the future built-in
  replacement.

Exit criteria:

- one owner explains the lifetime, cancellation and supersession of every
  external public call;
- commit failure cannot publish runtime success;
- late public-call completion cannot complete a newer operation or reopen
  access;
- no broad lock, generation or source-layout cleanup is required to finish the
  phase.

### Phase 5: Establish the Public V1 Evidence Contract

**Goal:** make the runtime and controller depend only on public progress and
exact public-call completion before the replacement engine exists.

Planned work:

- define role-specific public current progress, catch-up capability and
  election-safe recovery progress;
- reduce reporting to public first/last progress plus agent-owned
  role/access/configuration and durable operation completion;
- remove controller decisions based on native verified/quorum/committed
  progress or reported internal build boundaries;
- replace native topology/build fields in durable outcomes with exact public
  call, authority, revision, operation and process-session fences;
- define public completion recipes for catch-up, switchover, build, removal,
  data loss, retirement and rebuild-required failure;
- bump the report/protocol version and reject mixed evidence formats;
- introduce one new agent-owned public build store containing only build
  authorization, exact source/target/session identity and terminal public-call
  completion; do not carry engine copy cursors or native progress;
- update PostgreSQL and every custom replicator to satisfy the new public
  progress, build and error semantics.

Exit criteria:

- the runtime and controller require no evidence unavailable through public
  interfaces;
- successful exact `build_replica` completion is sufficient build proof;
- failover and scale-down use only public first/last progress, PC/CC state and
  durable public operation completion;
- recovery progress can elect a provisional primary without implying serving
  readiness;
- public errors retain typed rebuild-required identity across transport,
  durable operation failure and controller handling;
- public-only test implementations and new-protocol controller/runtime tests
  pass;
- none of the Phase 5 protocol, report, durable-outcome or store formats is
  activated against the legacy production engine.

### Phase 6: Build the Stateless Default Replicator

**Goal:** implement a new SF V1-style built-in Replicator beside the legacy
engine, using only the public runtime/replicator contract.

The complete ownership, recovery, migration and validation design is in
[Stateless Default Replicator](stateless-default-replicator.md).

Planned work:

- create a separate stateless default implementation rather than reshaping
  `DefaultReplicatorInner` in place;
- add an immutable process launch selection for `LegacyManaged` or
  `StatelessPreview`, decided before any replication listener is bound;
- expose that selection through public construction/settings so the host and
  selected factory agree without inspecting private capabilities;
- in legacy mode, retain the agent-owned replication listener; in preview
  mode, do not bind or register that service and let the replacement own the
  endpoint;
- require exactly one selected engine and provider writer; replacement Open
  failure aborts startup and never falls back to legacy;
- return only the standard public `ReplicatorInterfaces` bundle;
- own listener, authentication, peer sessions, ACK aggregation, copy,
  replication queues, retry and cancellation internally;
- initialize applied, committed and retained progress from
  `StateProvider`/`DurableState`;
- implement public role, epoch, PC/CC, catch-up, build, removal and data-loss
  behavior against an empty engine session;
- implement role-specific progress types, per-peer mailboxes and retry
  mechanics inside the replacement rather than the legacy engine;
- validate exact history after `on_data_loss` and return
  `ReplicaRebuildRequired` for incompatible providers;
- permanently retire divergent incarnations and build empty replacements;
- run the replacement through public conformance, crash-boundary and live
  tests while the legacy engine remains the production default.

Exit criteria:

- the replacement opens no agent/engine metadata store;
- it exposes no private lifecycle, data-plane, observation, fencing or receipt
  capability;
- agent authority plus provider state reconstruct a fresh engine;
- all-survivor restart can elect, settle and serve without private evidence;
- build, removal, switchover, failover and ambiguous-write contracts match the
  approved public plan;
- SQLite and KVStore2 pass the fresh-storage and corrective-replacement suites;
- production still selects the legacy engine until the complete replacement
  acceptance suite passes.
- hosted preview tests prove endpoint ownership, failed-Open cleanup, immutable
  selection and unchanged production manifests.

### Phase 7: Cut Over and Remove the Legacy Managed Path

**Goal:** atomically activate the replacement, then remove the implementation
structures that existed only for the managed engine.

Planned work:

- stop every application and require fresh agent/application storage for the
  breaking activation; persisted compatibility is intentionally unsupported;
- switch `DefaultReplicatorFactory` to the Phase 6 implementation;
- remove `ManagedReplicatorLifecycle`, `ManagedReplicatorDataPlane`, managed
  attachments, native observations and private receipts;
- remove the agent-owned default replication RPC dispatch path;
- delete the legacy `DefaultReplicatorInner` persistence/recovery path and its
  managed authority, replication-progress, local-write and engine-build
  continuation dependencies;
- migrate all public/custom build consumers to the Phase 5 agent-owned public
  build store, then delete `BuildAuthorityStore`, `BuildProgressStore` and
  their old tables;
- replace the agent schema outright; old default and custom agent stores are
  rejected rather than migrated;
- start PostgreSQL and other custom replicators from fresh agent/application
  state under the new public contract;
- update source guards and documentation to enforce the public-only boundary.

Exit criteria:

- the replacement is the only built-in default engine;
- no runtime code can attach or discover a managed/private replicator
  capability;
- every deployment activates only from fresh storage;
- custom replicators satisfy the new public contract without retained legacy
  state or effect compatibility;
- the legacy engine, adapters and data-plane server are unreachable or
  deleted;
- ordinary, PostgreSQL smoke and fresh SQLite/KVStore2 live suites pass.

### Deferred Maintenance: Decompose `host/custom.rs`

Full source decomposition is intentionally deferred until after Phase 7.
Removing the managed backend first prevents a file split around code that will
be deleted. Any later decomposition should contain only the remaining
public/custom path:

| Module | Responsibility |
|---|---|
| `proxy/authority.rs` | Independent custom-authority admission and recovery |
| `proxy/access.rs` | Public access sequencing and publication |
| `proxy/configuration.rs` | Public configuration and peer descriptions |
| `proxy/build.rs` | Public build admission, cancellation and completion |
| `proxy/topology.rs` | Public catch-up, removal and retirement sequencing |
| `proxy/mod.rs` | Public host-proxy composition |

This maintenance work is not a prerequisite for Phase 8.

### Phase 8: Add an Optional V2 Transactional Replicator

**Goal:** add deliberate transactional durability above the stateless V1
engine without reintroducing isolated continuation stores into that engine.

The Service Fabric reference model is documented in
[SF V2 Transactional Replicator](../../background/service-fabric/v2-transactional-replicator.md).
The first proposed Kuberic consumer and migration plan are documented in
[SQLite on a V2 Transactional Replicator](../sqlite/v2-transactional-replicator.md).

Planned work:

- define separate transactional-replicator and transactional-provider
  contracts rather than extending the protected V1 provider interface;
- implement one coherent durable log, stable-prefix, checkpoint, replay, copy
  and backup owner above V1;
- prove the contracts with a test provider before migrating SQLite;
- move SQLite generic transaction logging and recovery into V2 while retaining
  SQLite-specific WAL interpretation and checkpoint images in the provider;
- use an atomic storage-generation switch so the old application frame log and
  V2 log are never simultaneous authorities for new writes.

Exit criteria:

- V2 is layered over the Phase 6 replacement after the Phase 7 cutover and
  owns no agent topology or
  reconfiguration state;
- provider checkpoint plus V2 log recover the selected committed application
  state;
- SQLite serving never mixes old-stack logging, recovery or copy with V2-backed
  writes;
- V2 activation is optional and ordinary V1 state providers remain supported;
- no V2 serving path activates before Phase 7 has removed the legacy managed
  engine.

## Phase Dependencies and Validation

- Phase 0 is a behavior-preserving private extraction; it must not absorb
  managed peer recovery, reporting redesign or general partition execution.
- Phase 1 may use temporary private adapters, which remain until all managed
  callers use typed operations.
- Phase 2 must establish the independent recovery task before reporting becomes
  read-only.
- Phase 3 changes persistence and durable-field updates together; compatibility
  with existing stores is intentionally out of scope.
- Phase 4 extends the Phase 0 attempt model only far enough to own public call
  ordering, cancellation and revision revalidation. It must not spend time
  simplifying legacy managed-engine locks or generations.
- Phase 5 depends on Phase 4 operation ownership and defines a new, incompatible
  protocol/store contract. It is tested independently but never activated
  against the legacy production engine.
- Phase 6 builds a separate replacement while the legacy engine remains the
  production default. Its immutable pre-bind launch selection is a Phase 6
  prerequisite; internal progress types, per-peer mailboxes and retry mechanics
  belong in the replacement, not the legacy engine.
- Phase 7 requires the complete Phase 6 acceptance suite. It performs one
  atomic switch, removes the private managed attachment and deletes the legacy
  persistence/recovery path.
- Phase 7 activates through one breaking protocol boundary after old
  runtimes are stopped. SQLite, KVStore2, PostgreSQL and every other
  application restart from fresh agent/application storage; no intermediate
  protocol or persistence phase activates independently. Persisted
  compatibility, enrollment, backup restore and rolling migration are out of
  scope.
- Full `host/custom.rs` decomposition is post-cutover maintenance and does not
  gate the replacement or V2.
- Phase 8 may prototype contracts after the V1 layering boundary is stable,
  but no application may activate V2 storage before the Phase 7 exit criteria
  hold.
- Every phase must preserve ordinary, cancellation, dropped-caller,
  late-completion, restart and live-cluster safety tests relevant to its
  boundary.
- Update `replicator-boundary.md` in the same phase whenever ownership,
  persistence, reporting or recovery behavior changes.

## Guarantees to Preserve

Alignment must not weaken:

- the `Replicator`, `PrimaryReplicator` and `StateProvider` interface
  contracts;
- durable transition stages and deterministic replay;
- exact Pod, PVC, replica and process-session fencing;
- epoch and configuration monotonicity;
- separate replicator role, application role and access completion;
- intent-before-effect persistence;
- durable completion before public access publication;
- copy plus replication-gap closure;
- current and previous configuration quorum requirements;
- cancellation-safe build and access cleanup;
- equivalent authority, operation, process-session and generation validation
  around public replicator calls;
- failed or ambiguous custom-authority admission remaining fail-closed;
- exact pending custom authority being reconciled before obsolete stored
  configuration;
- access restoration requiring matching authority, role, session, durable
  revision and exact public-call completion;
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

Phases 0-3 are complete. Continue with **Phase 4: Establish Public Partition
Operation Ownership**. Keep it deliberately small: command ownership, ordered
public calls, cancellation/supersession and revision revalidation only.

Then complete **Phase 5: Establish the Public V1 Evidence Contract** before
building the replacement in **Phase 6**. Phase 6 should add a new engine beside
the legacy implementation rather than refactoring `DefaultReplicatorInner`.
After its full acceptance suite passes, **Phase 7** performs the fresh-storage
cutover and deletes the legacy managed path. Decompose the remaining custom
host only if its post-cutover shape still warrants it. Phase 8 builds optional
V2 durability on the completed public-only V1 replacement.
