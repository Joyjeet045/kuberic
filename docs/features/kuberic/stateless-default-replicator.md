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

This change does not require changing the public `Replicator` or
`PrimaryReplicator` interfaces.

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
| Application data, applied tail and committed LSN | State provider / `DurableState` | Query during replicator reconstruction |
| Retained replication operations | `DurableState` | Enumerate when peer catch-up requires them |
| Copy snapshot bytes | `StateProvider` | Regenerate from a frozen committed boundary |
| Replication queues and quorum ACK aggregation | Default replicator memory | Recreate empty |
| Peer sessions, addresses and send windows | Host and replicator memory | Rediscover and reconnect |
| Pending client completion | Caller memory | Complete with process failure; caller resolves ambiguity |
| In-progress build stream and sequence cursor | Replicator memory | Cancel and restart from a new authorized attempt |

The agent may persist the fact that a topology operation is required or that a
target replica must be built. It must not persist an engine cursor merely to
continue a particular in-memory copy stream.

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
8. Recreate peer sessions from current process-session observations.
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

Peer observations may prove that additional catch-up is required, but they
cannot silently reinterpret or reduce the provider's durable progress. If the
local state is ahead of the agent-authorized configuration, access remains
closed until the existing data-loss or authority protocol either certifies the
suffix or authorizes explicit false-progress correction.

An applied suffix above the committed boundary is unresolved, not committed
and not disposable in place. During reconstruction the runtime:

1. fences reads, writes and ordinary new LSN allocation while the unresolved
   history is being selected;
2. obtains exact retained operations and durable progress from the candidate
   replicas under freshly replayed authority;
3. compares epoch/history identity and operation bytes at every shared LSN;
4. selects the authoritative contiguous prefix through the normal PC/CC
   recovery and data-loss protocol;
5. either re-replicates an identical retained suffix and durably commits it,
   or performs false-progress correction by rebuilding/restoring the provider
   to the selected prefix;
6. opens access only after `DurableState::commit` has durably advanced the
   selected boundary and all required peers report a compatible prefix.

Different bytes at the same LSN are divergent history and can never be merged.
If the provider cannot remove or replace false progress safely, that replica
must be rebuilt. A corrected branch is admitted under a newer epoch/history;
an LSN is never assigned to different bytes within the same admitted history.

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

The target state provider remains responsible for making an exact copy retry
safe. A partially installed copy must either be replaceable by the new attempt
or rejected explicitly; the default replicator does not recover a copy cursor
from SQLite.

### Reconfiguration and Removal

PC/CC authority, switchover intent, secondary-removal preparation, accepted
cleanup and retirement remain durable because they are RA facts, not
replicator facts.

The agent replays the resulting configuration into a fresh engine. The engine
may return runtime proof that catch-up or quorum conditions are currently
satisfied, but the proof is scoped to the new engine session. No
engine-private removal or retirement record survives restart.

This preserves permanent incarnation fencing while aligning the Replicator
itself with SF V1.

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

The public interfaces remain unchanged:

- `Replicator`;
- `PrimaryReplicator`;
- `StateReplicator`;
- `StateProvider`;
- `DurableState`.

The internal construction boundary changes:

- remove durable-store parameters from `DefaultReplicatorInner::new`;
- pass agent-owned authority to the engine only through lifecycle preparation
  and commit calls;
- initialize progress from `StateProvider`/`DurableState` during open;
- remove engine calls that write agent SQLite tables;
- keep process-local engine session and generation fencing.

If existing `StateProvider` and `DurableState` methods cannot express a
required consistency check, prefer an internal adapter or a narrowly scoped
application durability method. Do not add topology, PC/CC or RA workflow state
to the application provider.

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
10. Reporting composes agent authority with live engine observation and does
    not treat missing live state as proof of completion.
11. Restart allocates above the durable applied tail and never overwrites an
    unresolved operation in the same admitted history.
12. Provider-visible committed progress advances only after recovery selects
    and durably commits an authoritative contiguous prefix.

## Migration Plan

### Phase 1: Separate Ownership

- Rename or reshape internal store capabilities so agent-owned persistence is
  not presented as replicator storage.
- Identify every engine write to `ReplicaAuthorityStore`,
  `ReplicationProgressStore`, `LocalWriteJournal`, `BuildAuthorityStore` and
  `BuildProgressStore`.
- Classify each record as agent authority, application durability, or
  removable process-local continuation.

### Phase 2: Reconstruct Authority

- Create the engine empty on every open.
- Replay agent-owned authority and topology through the existing lifecycle
  protocol.
- Derive initial progress from the state provider.
- Keep access closed through the entire reconstruction.

### Phase 3: Remove Local-Write Recovery

- Stop creating new durable local-write journal entries.
- Define process-crash completion as an ambiguous client failure.
- Reconstruct applied and committed boundaries from `DurableState`.
- Settle or correct every applied-but-uncommitted suffix before serving.
- Validate application idempotency, exact retained bytes and false-progress
  behavior.
- Remove journal recovery after compatibility with existing stores is no
  longer required.

### Phase 4: Restart Builds

- Stop restoring engine build cursors and partial stream progress.
- Fence old build sessions and authorize a new build after restart.
- Retain only agent-owned build necessity, target and topology intent.

### Phase 5: Remove Engine Metadata Persistence

- Remove the durable dependencies from the default engine.
- Delete obsolete SQLite tables only after old installations can be opened and
  migrated safely.
- Retain agent authority, effects, removal, switchover and retirement records.

## Testing Strategy

The implementation must include process-boundary tests, not only in-process
unit tests.

### Restart Reconstruction

- crash before and after replicator creation;
- crash after role replay but before epoch completion;
- crash during PC/CC configuration replay;
- verify access remains closed until reconstruction completes;
- verify next LSN starts after state-provider durable progress.

### Writes

- crash before local durable apply;
- crash after local durable apply but before quorum;
- crash after quorum but before caller notification;
- restart with `applied_lsn > committed_lsn` on one and several replicas;
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
- verify retained catch-up reaches the authorized boundary.

### Reconfiguration

- restart during failover, switchover, scale-up, scale-down and retirement;
- verify agent effects recover while the engine starts empty;
- verify stale engine-session outcomes are rejected;
- verify PC/CC quorum safety is unchanged.

### Compatibility

- open an existing SQLite store containing engine metadata;
- ignore or migrate obsolete records without weakening fences;
- verify mixed-version rollout behavior before removing old schema fields or
  tables.

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
