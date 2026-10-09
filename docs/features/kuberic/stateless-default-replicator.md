# Stateless Default Replicator

## Status

Proposed architectural alignment with the Service Fabric V1 Fabric Replicator.
This document defines the target ownership and recovery model; it does not
describe behavior that is already implemented.

## Overview

Kuberic's default replicator currently participates directly in durable
metadata management. The host injects SQLite-backed authority, replication
progress, local-write, build, removal and retirement stores into the managed
engine. This allows exact continuation across many process crash boundaries,
but it also makes the replicator a second durable control-plane owner alongside
the replica agent.

The Service Fabric V1 Replicator uses a simpler boundary:

- the Replicator owns only live replication mechanics;
- the state provider owns durable application state and progress;
- the Reconfiguration Agent (RA) owns topology, role and reconfiguration
  intent;
- a restarted Replicator is reconstructed from those two durable owners.

Kuberic should adopt the same boundary. The default replicator should not open,
write or recover a metadata database. It should be an executable,
restart-reconstructable data-plane component. The local replica agent remains
durable, but its SQLite state must be clearly agent-owned rather than exposed
as replicator persistence.

The default remains a built-in implementation, but the runtime must host it
through the same public interface bundle as an application-provided custom
replicator. No private managed lifecycle, data-plane, observation, fencing or
receipt capability may cross that boundary. This change does not require
changing the public `Replicator` or `PrimaryReplicator` interfaces.

## Goals

1. Make every default-replicator instance disposable and reconstructable after
   process restart.
2. Make the state provider the sole durable authority for application bytes,
   applied progress, committed progress and retained application history.
3. Make the local replica agent the sole durable authority for identity,
   topology, epochs, PC/CC configuration, role intent and reconfiguration
   workflow.
4. Remove default-replicator access to SQLite-backed authority, write-journal
   and build-progress stores.
5. Reconstruct the replication engine by replaying agent-owned state and
   querying state-provider progress.
6. Keep writes closed until reconstructed agent authority and state-provider
   progress have been validated together.
7. Match Service Fabric V1 restart behavior for in-flight writes, replica
   builds and peer sessions.
8. Remove every privileged runtime-to-default-replicator capability and use
   only the public replicator and state-provider interfaces.

## Non-Goals

- Removing durable state from the local replica agent.
- Making persisted applications volatile.
- Replacing the PC/CC, epoch or quorum protocols.
- Changing custom replicator ownership.
- Changing `Replicator` or `PrimaryReplicator`.
- Adopting the Service Fabric V2 Transactional Replicator or a
  replicator-owned write-ahead log.
- Guaranteeing transparent completion of an in-flight client request after the
  hosting process has crashed.
- Preserving or restoring any pre-cutover agent or application data, including
  SQLite, KVStore2 and PostgreSQL.
- Rolling or mixed-version migration.

## Service Fabric Reference Model

The Service Fabric V1 Fabric Replicator has in-memory operation queues,
role-specific engines, peer sessions and acknowledgement state. It does not
recover those objects from a replicator-owned log.

After restart:

1. The RA recreates the service and Replicator.
2. The state provider reports its last committed LSN.
3. The RA drives role, epoch and replica-set configuration again.
4. The Replicator recreates queues and peer sessions from those calls.
5. A replica that cannot catch up from retained application history is rebuilt
   through the state provider's copy stream.

`hasPersistedState` describes the service's durability; it does not enable
Replicator metadata persistence. See
[Service Fabric State Management and Persistence](../../background/service-fabric/state-management.md#v1-restart-and-recovery-ownership).

## Current Kuberic Ownership

The current default engine receives these durable dependencies:

- `ManagedReplicaStore`;
- `ReplicationProgressStore`;
- `LocalWriteJournal`;
- `BuildAuthorityStore`;
- `BuildProgressStore`.

They are backed by the same host SQLite database that stores agent state. The
engine consequently restores admitted authority, authority-fenced replication
progress, incomplete local writes, build authority and build progress into its
own runtime state.

This creates three problems:

1. **Split ownership:** agent and engine both interpret durable topology and
   workflow records.
2. **Recovery coupling:** changing an agent transition often requires matching
   engine persistence and restoration behavior.
3. **Stronger-than-SF continuation:** the engine attempts to resume internal
   work that SF V1 discards and reconstructs.

The SQLite database is still necessary for the agent. The problem is not the
existence of `.kuberic/agent.sqlite3`; it is allowing the default replication
engine to treat parts of that database as its own durable state.

## Target Ownership

| State | Target owner | Restart behavior |
|---|---|---|
| Replica/PVC identity and incarnation fencing | Agent SQLite | Reload and validate before opening the service |
| Previous/current configuration, epoch and role intent | Agent SQLite | Replay into a new replicator instance |
| Reconfiguration effect intent and completion | Agent SQLite | Reissue or observe through the normal effect protocol |
| Build requirement, exact source/target/session authorization and terminal public completion | Agent public build store | Reload and reissue the public build operation |
| Application data, applied tail and committed LSN | State provider / `DurableState` | Query during replicator reconstruction |
| Retained replication operations | `DurableState` | Enumerate when peer catch-up requires them |
| Copy snapshot bytes | `StateProvider` | Regenerate from a frozen committed boundary |
| Replication queues and quorum ACK aggregation | Default replicator memory | Recreate empty |
| Peer sessions, addresses and send windows | Default replicator memory | Recreate from public configuration calls and reconnect |
| Pending client completion | Caller memory | Complete with process failure; caller resolves ambiguity |
| In-progress build stream and sequence cursor | Replicator memory | Cancel and restart from a new authorized attempt |

The agent may persist the fact that a topology operation is required or that a
target replica must be built. It must not persist an engine cursor merely to
continue a particular in-memory copy stream.

## Public V1 Contract

The public boundary includes both construction and runtime interfaces:

- `ReplicatorFactory`, `ReplicatorFactoryContext`, `ReplicatorSettings` and
  `ReplicatorInterfaces`;
- `Replicator`, `PrimaryReplicator` and `StateReplicator`;
- `StateProvider` and `DurableState`.

The protected method sets of `Replicator`, `PrimaryReplicator`,
`StateReplicator`, `StateProvider` and `DurableState` remain unchanged.
Public construction and settings types may gain immutable transport or
security configuration, but they must not carry runtime callbacks, agent-store
handles or host-only lifecycle capabilities. `ReplicatorInterfaces` returns
only the standard public handles.

This follows SF V1:

- the RA proxy obtains only first/last progress through
  `GetCatchUpCapability` and `GetCurrentProgress`
  (`src/prod/src/Reliability/Failover/ra/ComProxyReplicator.cpp:72-84`,
  `src/prod/src/Reliability/Failover/ra/ReconfigurationAgentProxy.ActionListExecutorAsyncOperation.cpp:801-823`);
- successful public `BuildReplica` completion marks the remote proxy ready;
  the copy-end replication boundary remains internal to the Replicator
  (`src/prod/src/Reliability/Failover/ra/FailoverUnitProxy.ReplicatorBuildIdleReplicaAsyncOperation.cpp:311-350`,
  `src/prod/src/Reliability/Replication/PrimaryReplicator.BuildIdleAsyncOperation.cpp:42-135`);
- the Replicator binds, authenticates and closes its own replication transport
  and returns its published endpoint from Open
  (`src/prod/src/Reliability/Replication/ReplicationTransport.cpp:36-165`);
- the RA sequences public replicator calls and updates service read/write
  status separately rather than using a private access transaction
  (`src/prod/src/Reliability/Failover/ra/ProxyActionsList.cpp:106-147`,
  `src/prod/src/Reliability/Failover/ra/ReconfigurationAgentProxy.ActionListExecutorAsyncOperation.cpp:1136-1144`).

### Public completion evidence

Kuberic should use the same evidence shape:

| Agent operation | Public completion evidence |
|---|---|
| Open, role and epoch | Successful exact `Replicator` call, fenced by durable operation, process session and current revision |
| PC/CC configuration | Successful exact `PrimaryReplicator` configuration call |
| Catch-up or switchover readiness | Successful `wait_for_catch_up_quorum` for the exact current/previous configuration and requested mode |
| Build | Successful `build_replica` for the exact build ID, target identity, process session and address |
| Removal | Durable PC/CC transition using public progress/catch-up evidence, followed by successful `remove_replica` for the exact target |
| Data loss | Successful `on_data_loss`, followed by fresh public progress |
| Retirement | Agent-owned identity fence plus successful public close or synchronous abort |
| Access publication | Agent-owned ordering after required public calls complete; no replicator receipt |

The agent persists these exact call completions as its own receipts. A receipt
does not claim an internal copy boundary, ACK set, queue position or quorum
value that the public call does not expose.

### Public reporting model

The runtime reports only:

- identity, desired and actual role, access, configuration and retained
  operation state owned by the agent/runtime;
- `current_progress` from `Replicator::current_progress`;
- `catch_up_capability` from `Replicator::catch_up_capability`;
- exact durable completion of public build, catch-up, removal and retirement
  operations.

Controller safety decisions must stop depending on
`verified_replication_lsn`, `current_configuration_quorum_progress`, an
engine-observed `committed_lsn`, or a controller-visible copy/catch-up boundary.
Those are Replicator/provider internals. The new protocol removes them
atomically; mixed old/new report formats are rejected.

For failover, scale-down and reconfiguration decisions, the controller uses
the SF-shaped first/last progress pair, current/previous configuration and
durable completion of the relevant public catch-up/configuration operation.
It does not reconstruct an internal ACK set from reports.

### Public progress semantics

The first/last pair has role-specific SF V1 semantics:

| Lifecycle state | `current_progress` | `catch_up_capability` |
|---|---|---|
| Primary | Highest operation quorum-committed by the current replication queue and durably accepted locally | Earliest exact retained operation available for peer catch-up |
| Idle or active secondary | Highest contiguous in-order operation durably accepted locally and safe to acknowledge | Earliest exact retained/completed operation available from local history |
| Opened before role replay, recovery or unresolved suffix | Provider durable applied tail, usable only for election and history selection | Earliest exact provider-retained operation |
| Closed or aborted | Returns the corresponding terminal error |

Provider commitment remains internal and satisfies
`committed_lsn <= applied_lsn`. A secondary's public current progress may be
above its provider-committed boundary. Promotion must drain and settle that
received/applied suffix through public role, catch-up and data-loss processing
before primary access opens. A primary's public current progress is not an
assigned queue tail or a merely local apply.

Reports distinguish **election-safe** from **serving-safe** state. An
agent-owned recovery status marks the replica as access-closed and recovery
pending while still publishing provider-derived first/last progress. The
controller may use that progress to rank candidates and choose the provisional
primary, but it cannot treat the replica as caught up, healthy for promotion,
or serving-ready until public recovery and catch-up operations complete.

This deliberately differs from SF's persisted-secondary optimization, which
may report the highest in-order received operation before service
acknowledgement. Kuberic reports only the highest contiguous operation already
durably accepted by `DurableState`; process-local receive progress is never
election evidence.

The controller compares public progress only between reports produced in
compatible role/history contexts. It uses public operation completion—not a
numeric equality alone—to prove quorum catch-up, switchover or build safety.

## Recovery Model

### Startup

A default-replicator restart follows this order:

1. Open and validate the agent store and storage identity.
2. Load the agent-owned admitted authority and any pending topology effect.
3. Open the application and obtain its `StateProvider` and `DurableState`.
4. Create a new default replicator with no restored engine state.
5. Read the durable applied and committed application boundaries.
6. Replay role and epoch through the public replicator interface.
7. Replay the exact agent-owned PC/CC configuration.
8. Recreate peer sessions from the publicly supplied current configuration and
   its current replica addresses and identities.
9. Reconcile application progress with peer progress.
10. Grant read or write access only after the reconstructed configuration,
    epoch, quorum and application progress satisfy the normal activation
    invariants.

The engine session and generation remain process-local fences. A restarted
engine always has a new session and cannot complete work prepared by its
predecessor.

### Progress Reconstruction

The state provider supplies two local durable boundaries:

- **applied tail:** the highest durably accepted operation for which exact
  retained bytes remain available;
- **committed boundary:** the highest contiguous operation made authoritative
  by the provider's commit protocol.

The reconstructed engine initializes its next LSN above the applied tail, not
merely above the committed boundary. It initializes externally visible
committed progress from the committed boundary. This matches SF V1's
separation between operations retained by the service and the last committed
sequence number reported during recovery.

Peer progress may prove that additional catch-up is required, but it cannot
silently reinterpret or reduce the provider's durable progress. If the local
state is ahead of the agent-authorized configuration, access remains closed
until public catch-up or data-loss handling settles the suffix.

An applied suffix above the committed boundary is unresolved, not committed
and not disposable in place. During reconstruction:

1. the runtime keeps access closed and replays public role, epoch and PC/CC
   configuration;
2. the Replicator compares peer progress and exact retained operations behind
   its public implementation;
3. an identical suffix may be re-replicated and committed through the normal
   quorum path;
4. a data-loss decision invokes `PrimaryReplicator::on_data_loss`, which lets
   the `StateProvider` change its state through its existing contract;
5. the Replicator re-reads provider progress, retained bytes and peer history
   and validates that the callback result is compatible with the selected
   history, including when the provider returns `Ok(false)`;
6. a compatible result resets volatile queues, matching SF V1
   (`src/prod/src/Reliability/Replication/Replicator.OnDatalossAsyncOperation.cpp:30-129`);
7. an incompatible result returns an explicit public
   `ReplicaRebuildRequired` error and remains access-closed;
8. the runtime grants access only after the required public catch-up/data-loss
   calls and fresh public progress complete.

Different bytes at the same LSN are divergent history and can never be merged.
The generic V1 contract does not add an in-place rollback method. If
`StateProvider::on_data_loss` cannot safely replace false progress, that
replica enters the rebuild-required path. Callback success or `Ok(false)` is
never interpreted as history compatibility by itself.

Corrective rebuild uses the existing public build/copy interfaces:

1. the Replicator returns `ReplicaRebuildRequired` through the public operation
   that discovered incompatible history;
2. the runtime records the failed exact operation and keeps role/access closed;
   a target that detects the conflict also reports a permanent fault through
   its public `ReplicatorFactoryContext`;
3. the permanently faulted host, process session, replica incarnation and
   application storage/PVC are never readmitted;
4. the controller provisions a replacement replica/PVC incarnation and
   authorizes a new build ID, source and target process session;
5. the replacement opens with empty provider storage in repair-only
   `IdleSecondary` state;
6. the target accepts one authenticated public build stream only when its
   currently installed public configuration exactly names the source, target,
   build ID and both process sessions, its epoch matches, it is empty, and no
   newer build has superseded the attempt; withdrawing that configuration
   immediately revokes ingress;
7. `DurableState::finish_copy` promotes the copy in the replacement storage;
8. retained catch-up and public build completion settle the new history before
   the replacement can join PC/CC.

Permanent fault is terminal for the old replica incarnation. There is no
in-place clearing operation and no attempt to make that host healthy again.
The replacement receives a new process session and storage identity, so its
ordinary healthy report is not constrained by the old host's latched fault.
If no compatible source exists, the partition remains unavailable pending
explicit data-loss authorization. A corrected branch is admitted under a newer
epoch/history; an LSN is never assigned to different bytes within the same
admitted history.

`ReplicaRebuildRequired` is an error classification carried by existing public
method results, not a new lifecycle method. For a remote copy conflict, the
source `build_replica` result identifies the exact target/build failure and the
target reports its permanent fault through the public partition context. The
agent/controller can therefore replace the target storage without receiving a
private Replicator callback.

A single application LSN is not enough to authorize writes by itself. Write
access still requires:

- exact agent-owned authority;
- a non-regressing epoch;
- valid PC/CC quorum configuration;
- application progress consistent with the admitted authority;
- current peer process sessions;
- completion of any required catch-up or build.

The difference is that these facts are combined during reconstruction rather
than loaded from an engine-owned metadata record.

### Epoch Recovery

The agent replays the authoritative epoch through `Replicator::update_epoch`.
The default replicator forwards it to `StateProvider::update_epoch` together
with the previous-epoch LSN, as it does today.

Successful completion is the state-provider durability boundary for
application-specific epoch handling. The agent records completion of its
effect; the replicator does not separately persist the epoch.

### Pending Writes

An in-flight replication request belongs to the process that accepted it. A
process crash ends that request without reconstructing its completion future.
The client may not know whether the operation became durable or quorum
committed and must resolve the ambiguity with an application operation ID,
read-back, or retry.

The default engine must therefore stop persisting `DurableLocalWrite` records.
After restart:

- committed application operations are represented by the provider's committed
  boundary and retained history;
- durably applied operations above that boundary remain an unresolved retained
  suffix until recovery either recommits the identical bytes or corrects false
  progress;
- uncommitted in-memory reservations are discarded;
- the next LSN is greater than the durable applied tail for the admitted
  history;
- exact application retries remain the application's idempotency
  responsibility.

Kuberic must not silently report an interrupted write as successful. Removing
the journal removes client-future continuation, not operation-settlement
recovery. It does not change the requirement for explicit errors,
durable-before-acknowledgement application semantics, or reconciliation of an
applied-but-uncommitted suffix before serving.

### Replica Builds

Build necessity and target identity remain agent-owned. Copy enumeration,
sequence counters, delivery sessions and partial engine progress become
volatile.

If the source or target process restarts:

1. the old build attempt is fenced by its process session and build identity;
2. any partial stream is abandoned;
3. the agent evaluates whether the target still requires a build;
4. a new build identity and frozen replication boundary are authorized;
5. the state provider regenerates deterministic copy bytes for that boundary;
6. retained operations catch the target up after copy.

The frozen copy/catch-up boundary remains internal to the source Replicator.
`PrimaryReplicator::build_replica` completes only after the target has
acknowledged both copy completion and replication through the boundary captured
when enumeration ended. The agent records success for the exact build ID and
target session; the controller no longer requires matching source/target
reports of that internal boundary.

Scale-up admission therefore uses:

1. durable agent authorization of the exact source, target, build ID and
   target process session;
2. successful public `build_replica` completion on the source;
3. target liveness and identity/configuration reporting;
4. the ordinary public PC/CC configuration transition that promotes the
   target.

The source call is the copy-plus-replication proof, as in SF. Target public
progress remains useful for diagnostics and failover ranking, but it is not a
second independent build-completion receipt.

The target state provider remains responsible for making an exact copy retry
safe. A partially installed copy must either be replaceable by the new attempt
or rejected explicitly; the default replicator does not recover a copy cursor
from SQLite.

### Reconfiguration and Removal

PC/CC authority, switchover intent, secondary-removal preparation, accepted
cleanup and retirement remain durable because they are RA facts, not
replicator facts.

The agent replays the resulting configuration into a fresh engine. The engine
proves catch-up, build and removal only by completing the corresponding public
method. Switchover uses public catch-up configuration plus full catch-up
completion before role changes. Retirement uses agent identity fencing plus
public close/abort. No engine-private topology, removal or retirement receipt
survives restart.

This preserves permanent incarnation fencing while aligning the Replicator
itself with SF V1.

## Access Sequencing

Access is runtime-owned and deliberately separate from Replicator internals.
The runtime follows the SF proxy ordering model:

- promotion replays role and epoch, installs PC/CC configuration, completes
  required catch-up and only then grants application access;
- demotion or supersession first denies externally visible application access,
  then performs the required public configuration/catch-up/role calls;
- close and retirement deny access before public close or synchronous abort.

The built-in Replicator checks the public partition write status from
`ReplicatorFactoryContext` when admitting each new write and independently
checks its own role, epoch and PC/CC configuration. Revocation prevents new
admission; an operation already admitted may settle under the authority that
accepted it. Its caller may observe success, failure or ambiguity, but a stale
completion cannot reopen access or complete a newer agent operation.

This is also part of the public `StateReplicator` contract: an implementation
created for a partition must reject new replication while the public partition
write status is not `Granted`. Custom and built-in implementations receive the
same public context and are held to the same behavior.

The runtime persists intent before the sequence, revalidates its operation and
durable revision before granting access, and remains closed on any failed or
ambiguous public call. There is no cross-component prepare/publish transaction
and no private native access proof.

## Transport Ownership

The default Replicator owns its replication listener, inbound authentication,
peer-session replacement, send windows, ACK processing and copy delivery.

- immutable process launch configuration selects `LegacyManaged` or
  `StatelessPreview` before `AgentService` binds any replication listener;
- the same selection is available through public factory construction, and a
  mismatch fails startup;
- `ReplicatorSettings` supplies public bind/publish and security configuration.
- `ReplicatorFactoryContext` supplies public replica and partition identity.
- `ReplicaInformation` supplies peer address, replica identity, build identity
  and process session through public PC/CC configuration calls.
- `Replicator::open` binds the listener and returns the published address.
- `Replicator::close` and `abort` stop the listener and all descendant work.

The agent retains its control and peer-discovery endpoints, but no longer
serves or dispatches replication/copy RPCs for the default Replicator. Public
configuration is the only source of peer membership; exact process-session
checks reject predecessor connections and ACKs.

## Write and Acknowledgement Contract

The stateless design depends on a strict application durability boundary:

1. A secondary receives an operation.
2. `DurableState::apply` durably applies it.
3. Only then may the secondary acknowledge it.
4. The primary completes the request only after the configured PC/CC write
   quorums acknowledge it.

The primary's own state provider must expose progress consistent with any
operation considered locally durable. Restart recovery trusts the provider's
durable progress and retained bytes, not a lost in-memory queue.

`DurableState::apply` establishes durable acceptance and may advance
`applied_lsn`. `DurableState::commit` establishes provider commitment and may
advance `committed_lsn`. The following relationship must always hold:

```text
committed_lsn <= applied_lsn
```

The range `(committed_lsn, applied_lsn]` must remain exactly enumerable until
it is committed, removed through an authorized false-progress correction, or
replaced by a build. Merely observing that range on one replica does not make
it committed.

Implementations must reject:

- progress regression;
- different bytes for the same durable operation identity;
- an epoch older than the provider's accepted epoch;
- retained history that conflicts with already applied state;
- copy retries whose bytes differ at the same authorized boundary.

## API and Internal Boundary

The runtime/replicator boundary consists only of these public interfaces:

- `ReplicatorFactory`;
- `ReplicatorFactoryContext`;
- `ReplicatorSettings`;
- `ReplicatorInterfaces`;
- `Replicator`;
- `PrimaryReplicator`;
- `StateReplicator`;
- `StateProvider`;
- `DurableState`.

The built-in factory returns the same public bundle shape as a custom
replicator. The replacement has a separate construction boundary:

- leave the legacy `DefaultReplicatorInner::new` and its stores untouched until
  cutover;
- add a separate store-free replacement constructor and factory;
- replay role, epoch, PC/CC configuration, catch-up, build and removal only
  through the public `Replicator` and `PrimaryReplicator` methods;
- initialize progress from `StateProvider`/`DurableState` during open;
- remove engine calls that write agent SQLite tables;
- move peer transport, acknowledgements, copy delivery and process-local
  fencing behind the default replicator implementation;
- remove `ManagedReplicatorLifecycle`, `ManagedReplicatorDataPlane`, native
  observation and private receipt attachment from hosting;
- keep process-local engine session and generation fencing.

If existing `StateProvider` and `DurableState` methods cannot express a
required correction, `StateProvider::on_data_loss` must either perform it or
leave history incompatible so the Replicator returns
`ReplicaRebuildRequired`. Corrective copy is installed into a new empty replica
storage/PVC incarnation through the existing `DurableState::apply_copy_chunk`
and `finish_copy` methods. Do not add another durability method, create a
host-only backchannel, or add topology, PC/CC or RA workflow state to the
application provider.

## Safety Invariants

The implementation is acceptable only if all of these remain true:

1. A fresh engine cannot grant writes before exact authority replay.
2. Restart never permits epoch, configuration or committed-progress
   regression.
3. A predecessor engine session cannot complete work in its successor.
4. Application acknowledgement occurs only after durable application.
5. PC and CC quorum rules remain unchanged.
6. A stale peer process session cannot contribute an acknowledgement or build
   result.
7. Restarted builds cannot reuse an unauthorized boundary or target.
8. Retirement permanently prevents the retired incarnation from reopening.
9. Ambiguous client writes are surfaced as ambiguous failures, not reconstructed
   successes.
10. Reporting composes agent authority with public progress queries and
    runtime-owned completion records, and does not treat missing live state as
    proof of completion.
11. Restart allocates above the durable applied tail and never overwrites an
    unresolved operation in the same admitted history.
12. Provider-visible committed progress advances only after recovery selects
    and durably commits an authoritative contiguous prefix.

## Migration Plan

### Offline activation boundary

The migration is intentionally breaking and is not a rolling upgrade.
Intermediate implementation phases remain inactive until one atomic
protocol cutover.

For every deployment, including SQLite, KVStore2 and PostgreSQL:

1. disable application and replica access;
2. stop all old runtime/replicator processes;
3. delete or replace all old agent and application storage/PVCs;
4. deploy the controller, runtime, default Replicator and application provider
   that implement the complete public-only contract;
5. initialize fresh agent and provider storage;
6. bootstrap new authority from the empty deployment;
7. build the remaining replicas through the normal stateless public path;
8. enable application traffic only after all required public recovery
   operations complete.

No old process may run after the new protocol version is selected. Phase 7
does not preserve, restore, enroll or reinterpret any pre-cutover agent or
application data. Operators must export any data they need through an
application-level mechanism before cutover and reimport it as new application
writes afterward. Backup restoration into the new architecture,
identity-preserving enrollment and in-place provider conversion are deferred
features.

PostgreSQL remains an application-owned custom replicator, but it must
implement the new public progress/build/error contract and starts with fresh
agent and application storage at the cutover. No persisted PostgreSQL identity
or effect compatibility is required.

### Deferred migration features

The following are explicitly outside the alignment roadmap's Phase 7 cutover:

- preserving any agent store across the cutover;
- binding established SQLite or KVStore2 storage to a new agent identity;
- restoring a pre-cutover backup directly into the stateless protocol;
- preserving or rebinding PostgreSQL durable metadata;
- rolling or mixed-version activation;
- physical deletion of obsolete default-engine tables from an existing agent
  database.

These require a separately reviewed enrollment/restore protocol with
authoritative source selection, epoch floors, storage identity and old-
incarnation fencing.

### Phase 1: Migrate the Public Contract

This phase is the implementation breakdown for alignment Phase 5. It changes
the runtime/controller contract under a new protocol version that is not
activated against the legacy production engine.

- map every controller decision and report field to public first/last progress
  or exact durable public-call completion;
- remove controller decisions that require engine-private verified, quorum,
  committed or build-boundary evidence;
- replace native topology/build receipt fields in durable outcomes with exact
  public-call completion and agent-owned fences;
- define role-specific election-safe and serving-safe progress;
- define `ReplicaRebuildRequired` propagation through public errors, durable
  operation failure and public partition fault reporting;
- add a new agent-owned public build store containing exact authorization and
  terminal public completion but no engine continuation cursor;
- update PostgreSQL and other custom replicators to satisfy the new public
  progress, build and error semantics;
- bump the report/protocol/store versions and reject every old persisted
  format;
- validate the new contract with public-only test implementations while
  production remains entirely on the old protocol and legacy engine.

### Phase 2: Build a Parallel Stateless Engine

This phase begins alignment Phase 6. It creates a separate replacement rather
than removing persistence from `DefaultReplicatorInner` incrementally.

- implement a new stateless default Replicator behind a test-only or explicitly
  selected factory;
- add an immutable `LegacyManaged`/`StatelessPreview` process launch selection
  resolved before the host binds a replication listener;
- expose the same selection through public construction/settings, with exactly
  one selected engine and provider writer;
- keep the agent-owned replication server only in legacy mode; in preview mode
  the replacement binds and owns the endpoint;
- abort startup without fallback when replacement Open fails;
- return only the public `ReplicatorInterfaces` bundle;
- create the engine empty on every open and derive progress from
  `StateProvider`/`DurableState`;
- own listener, authentication, peer sessions, ACK aggregation, copy,
  replication queues and cancellation internally;
- replay role, epoch and PC/CC configuration through public methods;
- define role-specific internal progress types, per-peer processing and retry
  ownership in the replacement;
- keep the legacy engine as the production default until the replacement
  acceptance suite passes;
- prove hosted preview endpoint ownership and unchanged production selection.

### Phase 3: Complete Recovery and Provider Conformance

- implement durable-before-acknowledgement replication and conservative
  ambiguous client failure without local-write recovery;
- settle applied-above-committed suffixes before serving while publishing
  provider-derived election progress;
- validate exact history after every `on_data_loss` result, including
  `Ok(false)`;
- return `ReplicaRebuildRequired` for incompatible history and permanently
  retire that replica/storage incarnation;
- build empty replacement SQLite and KVStore2 replicas through authorized
  public copy;
- complete public build only after internal copy plus retained replication;
- implement SF-style access sequencing around public role, configuration and
  catch-up calls;
- pass ordinary, crash-boundary, all-survivor restart, divergence, transport,
  stale-session and live-cluster conformance tests without selecting the
  replacement for production.

### Phase 4: Perform the Atomic Fresh-Storage Cutover

This phase is the activation portion of alignment Phase 7.

- disable every application and stop all old runtime processes;
- delete or replace all old agent and application storage/PVCs;
- recreate the controller-owned resource or explicitly reset its accepted
  status so the deployment has no retained topology, epoch or initialized
  authority;
- deploy the controller, runtime, replacement Replicator and provider versions
  that implement the complete public contract;
- switch `DefaultReplicatorFactory` to the replacement;
- initialize fresh authority/provider state and build the remaining replicas;
- start custom-replicator applications from fresh identity and storage under
  the new public contract;
- reject mixed-version participation and enable application traffic only after
  public recovery completes.

No part of Phases 2-4 is activated independently in a production deployment.

### Phase 5: Delete the Legacy Managed Path

This phase is the cleanup portion of alignment Phase 7.

- remove `ManagedReplicatorLifecycle`, `ManagedReplicatorDataPlane`, managed
  attachments, native observations and private receipts;
- remove the agent-owned default replication RPC dispatch path;
- delete the legacy `DefaultReplicatorInner` persistence/recovery
  implementation;
- delete runtime dependencies on `ManagedReplicaStore`,
  `ReplicationProgressStore`, `LocalWriteJournal` and legacy engine build
  continuation stores;
- move all custom/public build consumers to the Phase 1 agent-owned public
  build store, then delete `BuildAuthorityStore`, `BuildProgressStore` and
  their old tables;
- replace the agent schema and reject all old default/custom stores;
- retain agent-owned authority, effects, removal, switchover and retirement
  state;
- update source guards and documentation so the private managed boundary cannot
  return.

## Testing Strategy

The implementation must include process-boundary tests, not only in-process
unit tests.

### Restart Reconstruction

- crash before and after replicator creation;
- crash after role replay but before epoch completion;
- crash during PC/CC configuration replay;
- change provider state through `on_data_loss`, re-read progress and rebuild
  volatile queues;
- treat `Ok(false)` as unproven until exact retained history validates;
- require rebuild when post-callback provider history is incompatible;
- verify access remains closed until reconstruction completes;
- verify next LSN starts after state-provider durable progress.

### Writes

- crash before local durable apply;
- crash after local durable apply but before quorum;
- crash after quorum but before caller notification;
- restart with `applied_lsn > committed_lsn` on one and several replicas;
- lose the primary, restart every surviving replica with
  `applied_lsn > committed_lsn`, elect from provider-derived progress and
  settle the suffix before granting access;
- recommit an identical retained suffix after fresh quorum reconstruction;
- reject different bytes at the same LSN and rebuild the divergent replica;
- verify next LSN starts above the applied tail rather than the committed
  boundary;
- verify the caller observes failure or disconnection rather than a fabricated
  success;
- retry by application operation ID and verify no duplicate mutation.

### Builds

- restart source and target at every copy boundary;
- verify the old attempt is fenced;
- verify the new attempt uses a new identity and deterministic snapshot;
- verify public build completion occurs only after internal copy plus retained
  catch-up;
- verify the controller does not require a reported internal build boundary.
- create divergent SQLite history, observe a no-op `on_data_loss`, return
  `ReplicaRebuildRequired`, permanently retire the old replica/PVC, build an
  empty replacement and admit only its fresh healthy session;
- create divergent KVStore2 history, permanently retire the old replica/PVC,
  build an empty replacement and admit only its fresh healthy session;
- verify the old permanent fault cannot be cleared or readmitted after the
  replacement succeeds;
- reject stale, revoked, wrong-epoch and wrong-target build streams without
  changing replacement provider state;
- select a source below the discarded SQLite false tail and verify the empty
  replacement does not retain the old required-progress floor.

### Reconfiguration

- restart during failover, switchover, scale-up, scale-down and retirement;
- verify agent effects recover while the engine starts empty;
- verify stale engine-session outcomes are rejected;
- verify promotion grants access only after public role/epoch/configuration and
  catch-up completion;
- verify revocation blocks new writes before demotion/removal public calls;
- verify PC/CC quorum safety is unchanged.

### Public boundary and reporting

- create the built-in and an application-provided replicator through the same
  public registration path;
- reject any `ManagedReplicatorLifecycle`, `ManagedReplicatorDataPlane`, native
  observation or private receipt attachment in source guards;
- report only public current progress, catch-up capability and durable
  public-call completion;
- verify controller transitions do not read native verified/quorum/committed
  progress or a reported build boundary;
- verify stale successful public calls cannot complete a newer durable
  operation or reopen access.
- verify built-in and custom `StateReplicator` implementations reject new
  writes whenever the public partition write status is not granted.
- verify target copy rejection preserves the typed
  `ReplicaRebuildRequired` classification across wire delivery, source
  `build_replica`, durable operation failure and controller replacement;
- cover disconnect and restart between target fault detection and source
  failure observation.

### Transport

- verify Open binds and returns the Replicator-owned published endpoint;
- reject a peer with the wrong replica identity or predecessor process
  session;
- replace an old peer session after public configuration changes;
- verify Close and Abort terminate the listener, peer sessions, copy streams
  and acknowledgement work;
- verify the agent control/peer-discovery server has no default-replication
  dispatch path after cutover.

### Compatibility

- reject attempts to attach any pre-cutover agent or application storage;
- initialize fresh agent and provider storage for SQLite, KVStore2 and
  PostgreSQL;
- keep every application disabled throughout the atomic cutover;
- recreate or explicitly reset stale initialized controller status before
  bootstrapping fresh authority;
- verify PostgreSQL and other custom replicators satisfy the new public
  progress, build and typed-error contracts from fresh state;
- verify no mixed-version rollout is accepted accidentally.

## Operational Consequences

- Replicator process restart may restart copy work from the beginning.
- Clients must tolerate ambiguous write completion and use idempotent operation
  identities where exactly-once user behavior is required.
- The agent database remains required and durable.
- Application providers must accurately report durable progress and preserve
  deterministic copy/retained-history behavior.
- Recovery becomes easier to reason about because there are two durable
  owners: agent topology and application state.

## Limitations and Future Work

Strict SF V1 alignment intentionally gives up transparent continuation of
engine-internal work. If Kuberic later requires resumable builds or durable
request completion, those features should be implemented by an explicit
agent-owned workflow or application-owned transaction protocol, not by making
the default Replicator a hidden metadata store again.

The V2 Transactional Replicator model remains a separate possible feature. It
would add a deliberate replicator-level durable log and transaction protocol;
it must not emerge accidentally through incremental exceptions to this
stateless boundary.
