# SQLite on a V2 Transactional Replicator

## Status

Proposed architecture. This document describes how the existing replicated
SQLite application could use a future Kuberic transactional replicator modeled
after the Service Fabric V2 Transactional Replicator. It does not describe
behavior that is currently implemented.

The term **transactional replicator** in this document is distinct from the
existing SQLite application's migration to Kuberic's level-triggered **v2
runtime stack**. The current application still uses the default V1-shaped
replicator and implements its own durable frame log, recovery and copy
protocol.

## Overview

The current SQLite application already implements many responsibilities that
an SF-style V2 Transactional Replicator would provide:

- an application-owned durable log of WAL-frame operations;
- applied and committed LSN tracking;
- retained redo history;
- a base image and post-base recovery suffix;
- durable copy staging and retry evidence;
- reconciliation after ambiguous commit boundaries;
- committed-image materialization before SQLite opens.

These mechanisms are necessary with the current V1-shaped default replicator,
because that replicator deliberately does not own a durable transaction log.
They should not remain duplicated if Kuberic adds an explicit V2 layer.

SQLite should be the first application migrated to that layer. Each SQLite
transaction naturally produces one atomic redo payload, while a SQLite
database image naturally forms a provider checkpoint and full-copy image. The
transactional replicator can own ordering, durable logging, quorum commitment,
recovery replay, retained history and checkpoint coordination. The SQLite
provider can then focus on translating between transactional-replicator
operations and SQLite database state.

The intended boundary is:

```text
SQL transaction
    -> capture complete WAL-frame transaction
    -> submit one V2 atomic redo operation
    -> durably log and replicate through V1
    -> reach required quorum and stable commit
    -> apply/publish frames to the SQLite provider
    -> return SQL success
```

The V2 log becomes the replicated recovery authority. SQLite files become
provider checkpoint and materialized serving state. The design must not add a
V2 log underneath the existing application-owned durable frame log.

## Goals

1. Use SQLite as the first production-quality consumer of a Kuberic V2
   Transactional Replicator.
2. Map one committing SQLite transaction to one atomic redo-only V2
   transaction.
3. Move generic durable logging, LSN assignment, commit tracking, retained
   history, replay and copy-suffix handling out of the SQLite application.
4. Retain SQLite-specific WAL capture, frame validation, application,
   checkpoint creation and checkpoint installation in the SQLite provider.
5. Recover by opening a provider checkpoint and replaying committed V2 log
   records before serving SQL.
6. Coordinate SQLite checkpoints with the V2 stable LSN and log-truncation
   protocol.
7. Support full and incremental backup through V2 checkpoint and log
   boundaries.
8. Preserve agent ownership of topology, epochs, roles, builds, removal and
   retirement.
9. Preserve the existing `Replicator`, `PrimaryReplicator` and `StateProvider`
   interfaces.

## Non-Goals

- Turning SQLite into a multi-primary database.
- Replicating SQL text or re-executing SQL on secondaries.
- Making arbitrary SQLite extensions, attached databases or virtual tables
  safe for replication.
- Retaining both the current application frame log and a new V2 log as
  independent recovery authorities.
- Replacing the V1 replication transport, quorum protocol or RA-facing
  lifecycle.
- Moving topology or reconfiguration durability into the V2 log.
- Changing the existing protected `StateProvider` interface into a V2 provider
  interface.
- Providing cross-partition transactions.
- Treating a V2 commit as client success before local SQLite publication has
  completed.

## Current SQLite Ownership

The current implementation divides state between the agent and
`SqlitePersistence`.

| Current component | Responsibility |
|---|---|
| Agent SQLite | Identity, authority, role and configuration intent, effects, local-write recovery and builds |
| Commit-barrier VFS | Capture a complete WAL transaction and prevent its local publication before replication |
| `DurableFrameLog` | Base image, retained operations, applied/committed progress, copy staging and recovery state |
| `SqlitePersistence` | `DurableState` implementation, snapshot generation, apply, commit and materialization |
| Default replicator | Transport, operation LSNs, quorum acknowledgement, copy and peer sessions |
| Live SQLite database | Primary SQL execution and materialized committed view |

The application manifest and history log are not merely a SQLite checkpoint.
Together they form a specialized transactional-replicator log:

```text
application/
  state-v2.json
  base-<generation>.sqlite
  history-<generation>.log
  db.sqlite
  db.sqlite-wal
  db.sqlite-shm
```

`state-v2.json`, the immutable base and retained history establish durable
application progress independently of the live database. That is appropriate
for the current V1-shaped replicator, but it would duplicate the central
responsibility of a future V2 Logging Replicator.

## Target Architecture

### Layering

```text
Replica agent
  owns topology, epoch, role, access, build intent and lifecycle effects

V1 default replicator
  owns transport, replica sessions, quorum ACKs and RA-facing configuration

V2 transactional replicator
  owns transactions, durable logical/physical log, LSN stability,
  checkpoint coordination, replay, retained suffixes, copy and backup

SQLite V2 state provider
  owns WAL-frame interpretation, database pages, provider checkpoints,
  checkpoint installation, redo application and SQLite lifecycle
```

V2 is layered over V1 rather than replacing it. The replica agent continues to
drive V1 roles, epochs and configurations. The service receives a separate
transactional handle for submitting operations and managing V2 providers.

### Durable ownership

| State | Target owner |
|---|---|
| Replica identity, epoch, PC/CC configuration and role intent | Agent |
| V2 transaction records, stable-prefix evidence and abort outcomes | Transactional replicator log |
| Stable LSN, progress vector and physical checkpoint records | Transactional replicator log |
| SQLite database checkpoint image | SQLite V2 state provider |
| Post-checkpoint committed redo | Transactional replicator log |
| Incomplete transaction records | Transactional replicator; ignored or undone during recovery |
| SQL connection, live WAL and VFS buffers | SQLite process memory and materialized files |
| Client request completion | Hosting process; not reconstructed after process failure |

There must be one authoritative path for every durable fact. In particular,
SQLite must not report an operation durable from its old frame log while V2
reports a different transaction boundary.

## SQLite Provider Contract

Kuberic should introduce a new internal or public V2 provider contract modeled
after SF `IStateProvider2`. It should be separate from the protected V1
`StateProvider` interface.

The SQLite implementation requires these capability groups:

### Lifecycle

- initialize with provider identity, work directory and transactional handle;
- open provider checkpoint state;
- change role without selecting its own authority;
- close or abort all SQL and provider work;
- remove local provider state only after agent-authorized lifecycle removal.

### Apply and unlock

- apply redo at the transaction's commit LSN;
- distinguish primary, secondary and recovery application contexts;
- reject invalid WAL-frame geometry or conflicting bytes;
- return an operation context if resources must remain held until transaction
  completion;
- release those resources through an explicit unlock callback.

SQLite initially needs only atomic redo operations. Undo payloads are
unnecessary because uncommitted SQL is still controlled by SQLite and
uncommitted V2 redo must not become provider-visible.

### Checkpoint

- prepare a checkpoint at a captured stable LSN;
- produce an immutable SQLite image for that exact version;
- atomically promote the completed image;
- recover the current image on restart;
- delete obsolete images only after V2 makes them unreachable.

### Copy

- enumerate a provider checkpoint image from the source;
- begin target-side installation into staging;
- accept numbered and retryable copy records;
- atomically complete installation;
- abandon incomplete staging when copy does not complete.

### Backup and restore

- copy the current provider checkpoint into a backup directory;
- restore a validated checkpoint into staging;
- atomically promote restored state only as part of V2 restore;
- leave log-suffix selection and replay to the transactional replicator.

## Transaction and Commit Path

### Primary operation

The commit-barrier VFS continues to capture the exact WAL-frame set before its
commit frame becomes locally recoverable. Instead of calling
`StateReplicator::replicate` directly, it submits one atomic redo-only
operation to the V2 transaction manager.

```text
SQLite/VFS       V2 transaction       V1 replicator      V2 logging adapter       SQLite provider
    |                  |                    |                     |                      |
    |-- frame set ---->|                    |                     |                      |
    |                  |-- transaction --->|                     |                      |
    |                  |                    |-- assigned op ----->|                      |
    |                  |                    |                     |-- flush intent       |
    |                  |                    |<-- durable accept --|                      |
    |                  |                    |-- replicate/ACKs -->|                      |
    |                  |                    |-- commit boundary ->|                      |
    |                  |                    |                     |-- apply committed -->|
    |                  |<-- committed LSN --|                     |<-- apply complete ---|
    |<-- publish ------|                    |                     |                      |
```

The redo payload remains the current deterministic `WalFrameSet`, not SQL
text. It contains page images, database geometry and final database size.
Non-deterministic SQL therefore still executes only on the primary.

### V1 logging bridge

The existing `StateReplicator::replicate` returns an LSN only after durable
local acceptance and the admitted PC/CC quorums. V2 must not assume that it can
receive that LSN first and flush its log afterward.

Kuberic V2 instead follows SF's layering pattern: the V2 Logging Replicator
wraps the inner V1 transport and supplies a V2 logging adapter at V1's
application-durability boundary. The submitted V1 payload contains a stable V2
transaction identity and redo bytes, but not a caller-chosen LSN.

- V1 assigns the operation LSN.
- The adapter's durable-apply path appends and flushes the V2 logical record
  with that assigned LSN before returning local acceptance.
- Secondary adapters append and flush the same record before acknowledging
  replication.
- V1 gathers PC/CC acknowledgements using its unchanged quorum protocol.
- V1's durable-commit callback advances the adapter's candidate stable
  boundary and permits committed provider processing.
- `StateReplicator::replicate` returns the committed LSN to the V2 transaction
  manager only after those steps complete.

This bridge is private composition between V1 and V2. It does not change
`Replicator`, `PrimaryReplicator`, `StateProvider`, or the public completion
contract of `StateReplicator`. Cancellation before V1 acceptance leaves no
record; cancellation after durable acceptance leaves an unresolved V2 record
that recovery must settle rather than deleting it.

### Success boundary

A successful V2 transaction means that V1 has completed the required
replication quorum, V2 has durably advanced its stable committed prefix through
the transaction, and the operation has been applied according to the V2
operation-processing contract. The SQL request must additionally wait for
successful local SQLite publication.

A locally durable transaction record is only an accepted intent. Its presence
does not prove quorum commitment. V2 separately tracks:

- the local accepted tail;
- the locally recorded stable committed boundary;
- provider-applied progress;
- the history/progress vector used to validate those boundaries after role
  change.

The stable record is a durable lower bound produced after V1 quorum completion,
not an infallible answer to every crash race. If quorum completes but the
primary crashes before recording the stable advance, recovery may recertify
the identical record from current replica histories. If only local acceptance
occurred, recovery must not expose the record as committed.

If V2 commits but local WAL publication fails:

1. the replica is behind its own committed V2 state;
2. the service closes write and read access for that process;
3. the client receives an ambiguous failure, never a rollback claim;
4. recovery selects or recertifies the committed prefix, reopens the provider
   checkpoint and replays the record if it belongs to that prefix;
5. SQL serving resumes only after materialized state reaches the V2 committed
   LSN.

This preserves the current conservative failure contract without an
application-owned reconciliation journal.

### Secondary and recovery apply

Secondaries do not need a continuously open client-facing SQLite connection.
They apply committed redo into provider state under V2 ordering. An
implementation may maintain either:

- an incrementally updated database image; or
- checkpoint state plus replayable log records, materializing SQLite when role
  change or checkpoint policy requires it.

The initial implementation should prefer correctness and simple checkpoint
semantics over minimizing secondary writes. Provider state must be equivalent
at a given committed LSN regardless of whether redo was applied during normal
replication or recovery replay.

## Checkpoints and Log Truncation

The current base image becomes a provider checkpoint managed under V2's
three-stage protocol:

1. **Prepare:** capture the stable LSN and freeze the logical database version.
2. **Perform:** create and fsync an immutable SQLite image for that version.
3. **Complete:** atomically promote the image and make older provider files
   eligible for deletion.

V2 must not truncate log records required by:

- the current provider checkpoint;
- a checkpoint still being performed or completed;
- an active replica copy;
- an incremental backup;
- a replica whose retained-history requirements are still admitted.

SQLite automatic WAL checkpoints remain disabled for client-visible storage
unless they are proven compatible with the provider checkpoint protocol.
SQLite WAL maintenance is an implementation detail; the V2 stable LSN and
provider checkpoint define recovery retention.

## Restart Recovery

A persisted replica recovers in this order:

1. The agent validates replica identity and opens topology authority.
2. V2 opens and validates its durable physical/logical log.
3. V2 reconstructs the accepted tail, local stable boundary, progress vector
   and any unresolved suffix.
4. The agent replays role, epoch and configuration through V1 while access
   remains closed.
5. V1/V2 compare current replica histories and select or recertify the
   authoritative stable prefix through normal quorum and data-loss rules.
6. V2 performs false-progress truncation or provider replacement for records
   outside the selected history.
7. The SQLite provider opens its latest completed checkpoint at or below the
   selected stable prefix.
8. V2 replays committed redo after the checkpoint LSN.
9. Accepted-but-unresolved, aborted and false-progress records remain
   provider-invisible.
10. The provider materializes and validates the recovered SQLite database.
11. Read or write access remains closed until V1 authority and V2/provider
   recovery are both complete.

Recovery must produce the same database bytes and logical contents as applying
the same committed redo during normal operation. Corrupt provider checkpoints
or missing acknowledged log records fail closed and require an authorized
rebuild or data-loss workflow.

The current application-owned `RecoveryState::ReconciliationRequired` state
should disappear because V2 owns reconciliation. Reconciliation itself does
not disappear: a local V2 log cannot determine every distributed commit race
without comparing admitted replica histories. A rebuild-required fence remains
necessary when neither a valid provider checkpoint nor sufficient V2 log
history can recover the selected prefix.

## Replica Copy and Divergent History

V2 selects copy mode from the source and target progress histories:

- no copy when the target already has the selected history;
- log-only copy when the target checkpoint and retained suffix are compatible;
- full SQLite checkpoint plus log suffix when they are not;
- false-progress correction when the target contains a divergent suffix.

For full copy:

1. the source enumerates an immutable SQLite checkpoint;
2. the target installs it through numbered, retry-safe copy records;
3. V2 transfers the required committed log suffix;
4. the target replays that suffix;
5. the provider validates and materializes the resulting database;
6. V1 admits the built replica only after it reaches the authorized boundary.

Provider copy staging remains application-specific, but build identity,
progress-vector comparison and suffix selection belong to V2. Exact retry
evidence does not need to remain indefinitely in the promoted SQLite
checkpoint after V2 has durably completed or abandoned the copy session.

False-progress correction must never mutate a live primary SQLite connection.
The replica is fenced, divergent provider state is restored or undone under
V2 control, and SQLite is reopened only at a valid selected history.

## Backup and Restore

A full backup contains:

- the V2 metadata needed to identify its stable recovery boundary;
- the SQLite provider checkpoint at that boundary;
- any V2 log suffix required to reach the backup LSN;
- checksums and provider identity metadata.

An incremental backup contains only V2/provider artifacts after an accepted
parent backup boundary. Backup completion is reported only after all artifacts
are durable in the user-selected destination.

Restore is a lifecycle operation, not a live SQL file replacement:

1. close SQL access and provider activity;
2. validate backup identity, chain and checksums;
3. stage the provider checkpoint and V2 log;
4. atomically select the restored generation;
5. run normal V2 recovery and redo replay;
6. require agent-authorized data-loss/restore policy before granting access.

Copy and backup may share provider checkpoint encoding, but they have different
retention, authorization and destination semantics.

## State-Provider Registry

The first implementation should register exactly one SQLite provider for one
partition. V2's replicated provider registry is still useful because it makes
provider identity and reconstruction explicit, but dynamic creation and
removal need not be exposed through the SQL API initially.

Future services could use V2 transactions across multiple providers within the
same partition. SQLite should not claim atomicity with another provider until
the primary apply/publication sequence and recovery behavior are defined for
all participants.

## API Boundary

The existing interfaces remain unchanged:

- `Replicator`;
- `PrimaryReplicator`;
- `StateProvider`.

The V2 feature should add separate interfaces, tentatively:

- `TransactionalReplicator`;
- `Transaction` and `AtomicRedoOperation`;
- `TransactionalStateProvider`;
- `TransactionalStateProviderFactory`;
- provider checkpoint, copy, backup and apply contexts.

Names are illustrative rather than final API commitments. The important
constraint is that V2 provider concerns must not be added incrementally to the
V1 `StateProvider` or `DurableState` contracts.

The SQLite service should receive:

- the V1-compatible primary-replicator handle used by the host and agent; and
- a V2 transactional handle used by the SQL commit barrier.

## Migration Plan

### Phase 1: Define V2 contracts

- Add the transactional-replicator and V2 provider abstractions.
- Layer V2 over the existing V1 transport and lifecycle.
- Implement durable transaction records, atomic redo operations, recovery and
  checkpoint coordination with a test provider.

### Phase 2: Add a SQLite V2 provider

- Reuse `WalFrameSet` as the provider redo format.
- Implement lifecycle, redo apply, checkpoint and copy callbacks.
- Keep the current application durable frame log available as the active path
  while validating equivalent provider state in isolated tests.

### Phase 3: Complete the inactive V2 storage path

- Change the commit barrier to submit one atomic redo-only V2 operation.
- Define the V2 commit plus local publication success boundary.
- Preserve ambiguous client outcomes after dispatch.
- Recover from V2 log plus provider checkpoint.
- Move retained suffix selection and replay into V2.
- Replace `SqlitePersistence` copy-progress metadata with V2-managed copy.
- Add a durable storage-generation selector that chooses either the complete
  old stack or the complete V2 stack, never individual mechanisms from both.
- Keep this path non-serving or shadow-only until crash-boundary equivalence is
  proven.

### Phase 4: Atomically activate V2

- Fence SQL and replica access.
- Finish or reject any old-stack in-flight operation.
- Convert a validated old checkpoint and boundary, or require fresh storage.
- Durably select the V2 storage generation only after V2 logging, recovery,
  copy, checkpoint and false-progress correction are all ready.
- Reopen through normal V2 recovery before accepting the first V2-backed
  write.
- Stop creating application-frame-log operations only after the generation
  switch commits.
- Never fall back to old recovery or copy after a V2-backed write is accepted.

### Phase 5: Remove obsolete storage

- Remove `state-v2.json` and `history-<generation>.log` as transactional
  authorities.
- Retain only provider checkpoint/materialized files required by the new
  design.
- Provide an explicit one-time migration or require fresh storage; never infer
  V2 progress from partially converted files.

## Safety Invariants

1. SQL success is impossible before V2 quorum commitment and successful local
   SQLite publication.
2. A committed V2 transaction is recovered even if the primary crashes before
   replying to the client.
3. An incomplete or aborted V2 transaction cannot become visible in SQLite.
4. Normal apply and recovery replay produce equivalent SQLite state.
5. The provider checkpoint LSN never exceeds V2's stable committed boundary.
6. V2 never truncates the only log suffix required by a checkpoint, copy or
   backup.
7. A copied or restored database is not served before its required log suffix
   has been replayed.
8. Provider corruption or missing acknowledged log data fails closed.
9. Agent authority remains necessary for role and access; possession of a
   recoverable V2 log cannot self-promote a replica.
10. The old application frame log and the V2 log are never simultaneous,
    conflicting authorities for new writes.
11. A locally accepted V2 record is not provider-visible until it belongs to
    the selected stable committed prefix.
12. V2-backed writes cannot activate before V2 recovery, copy, checkpoint and
    storage-generation selection activate together.

## Testing Strategy

### Transaction boundaries

- crash before V2 log append;
- crash after append but before quorum commitment;
- crash after quorum commitment but before provider apply;
- crash after quorum commitment but before local stable-prefix persistence and
  recover the record through fresh history recertification;
- retain but do not apply a locally accepted record that cannot be
  recertified;
- crash after provider apply but before local WAL publication;
- crash after publication but before client response;
- verify exact database contents and conservative client outcomes.

### Recovery

- recover from a checkpoint with no suffix;
- replay one and many committed transactions;
- ignore incomplete and aborted transactions;
- reconcile an accepted tail above the local stable boundary;
- correct divergent false progress before opening the provider;
- reject a corrupted checkpoint or acknowledged log record;
- verify repeated recovery is idempotent.

### Checkpoints

- crash at prepare, perform and complete boundaries;
- checkpoint while writes continue;
- retain the suffix required by an older active copy or backup;
- verify promotion and obsolete-file cleanup are atomic.

### Copy and reconfiguration

- log-only and full copy;
- source and target restart during every copy stage;
- divergent target history and false-progress correction;
- failover immediately before and after checkpoint completion;
- planned switchover with an ambiguous old-primary response.

### Backup and restore

- full backup followed by restore;
- incremental backup chains;
- interrupted or corrupt backup;
- restore authorization and identity mismatch;
- recovery from checkpoint plus backup log suffix.

### Compatibility

- ensure the existing V1 SQLite application remains usable while V2 is
  experimental;
- prove no serving phase routes writes to V2 while recovery or copy still uses
  the old frame log;
- reject mixed storage generations without an explicit migration marker;
- verify old application-frame-log files are never mistaken for V2 log state.

## Operational Consequences

- The V2 log adds deliberate replicated-log storage and checkpoint management
  to each SQLite replica.
- The SQLite provider becomes smaller conceptually, but V2 itself is a
  substantial new storage engine requiring independent corruption, retention
  and recovery testing.
- Checkpoint and backup policies affect disk usage and replica-build cost.
- Client write outcomes remain ambiguous across process loss after dispatch;
  applications still need idempotency or result verification.
- A singleton still has no redundancy even though its local V2 log is durable.

## Limitations and Future Work

The initial SQLite integration should support one provider, one database and
atomic redo-only transactions. Multi-provider transactions, online restore,
provider hierarchy, dynamic provider creation, group commit and incremental
provider checkpoints can follow after the single-provider recovery model is
proven.

The current SQLite implementation is valuable evidence for the V2 design, but
it is not itself a reusable transactional replicator. Generic behavior should
move into V2 only when its contracts also work for a second state-provider
implementation; SQLite-specific WAL and image rules must remain in the
provider.

## References

- [Current SQLite design](design.md)
- [SF V2 Transactional Replicator](../../background/service-fabric/v2-transactional-replicator.md)
- [SF state management and persistence](../../background/service-fabric/state-management.md)
- [Kuberic stateless default replicator](../kuberic/stateless-default-replicator.md)
- [`SqliteService`](../../../examples/sqlite/src/service.rs)
- [`SqlitePersistence`](../../../examples/sqlite/src/state.rs)
- [`DurableFrameLog`](../../../examples/sqlite/src/framelog/durable.rs)
- [SQLite commit-barrier VFS](../../../examples/sqlite/src/commit_barrier/mod.rs)
