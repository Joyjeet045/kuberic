# Service Fabric V2 Transactional Replicator

The V2 **Transactional Replicator** is Service Fabric's durable transaction,
logging, checkpoint, recovery, and state-provider framework. It powers Reliable
Collections, but it can also host custom `IStateProvider2` implementations.

V2 is not a replacement for the V1 Fabric Replicator's transport and
reconfiguration protocol. It is a storage and transaction layer built on top
of V1:

```text
Reliable Collection or custom state provider
                    |
             IStateProvider2
                    |
       State Manager + Transaction Manager
                    |
             Logging Replicator
       logical log, recovery, checkpoints
                    |
       V1 IFabricStateReplicator transport
                    |
         primary and secondary replicas
```

> Part of the [SF Architecture Reference](README.md).

---

## Purpose and Design Boundary

The V1 Fabric Replicator assigns LSNs, sends opaque operations, tracks replica
ACKs, and implements the primary-replicator contract used by the
Reconfiguration Agent (RA). The service must provide its own transaction,
logging, recovery, and checkpoint behavior.

V2 supplies those missing storage-engine services:

- transactions spanning multiple named state providers;
- logical transaction records with redo, undo, and metadata payloads;
- a durable physical log for persisted replicas;
- log replay after restart;
- coordinated state-provider checkpoints;
- log truncation and version retention;
- full, partial, and false-progress-aware replica copy;
- backup and restore;
- transactional creation and removal of state providers.

The boundary is important: the RA still drives role, epoch, quorum, build, and
replica-set changes through the V1 `IFabricPrimaryReplicator` surface. V2
returns an `IFabricPrimaryReplicator` for that control path and separately
exposes its transactional API to the service.

`ComTransactionalReplicator::Create` constructs this stack explicitly:

1. create a V2 logging state-provider adapter;
2. create the real V1 replicator around that adapter;
3. wrap V1's `IFabricStateReplicator`;
4. create the Transactional Replicator, State Manager, and Logging Replicator;
5. return the V1-compatible primary-replicator interface and the V2
   transactional handle.

Source:
`src/prod/src/data/txnreplicator/ComTransactionalReplicator.cpp:123-285`.

---

## Major Components

| Component | Responsibility |
|---|---|
| `TransactionalReplicator` | Top-level composition, lifecycle, role changes, public API forwarding |
| `LoggingReplicatorImpl` | Durable log, transaction protocol, recovery, copy, backup/restore, checkpoint orchestration |
| `StateManager` | Named state-provider registry, provider lifecycle, apply dispatch, copy and checkpoint fan-out |
| `TransactionManager` | Creates transaction log records and coordinates begin, operation, commit, and abort |
| `ReplicatedLogManager` | Assigns V1 replication LSNs, appends logical records, tracks the log tail and progress vector |
| `OperationProcessor` | Applies committed redo, recovery redo/undo, and false-progress corrections to state providers |
| `CheckpointManager` | Coordinates barriers, stable LSNs, provider checkpoints, and log-head truncation |
| `RecoveryManager` | Opens the physical log, finds the recovery checkpoint, and replays recoverable records |
| `CopyStream` / `SecondaryDrainManager` | Select and transfer state plus log records, then install them on a secondary |
| V1 Fabric Replicator | Network replication, operation streams, ACKs, quorum, builds, and RA-facing reconfiguration |

The `TransactionalReplicator` itself is mostly a composition and API boundary.
Transaction operations are forwarded to the Logging Replicator; state-provider
map operations are forwarded to the State Manager. This keeps transaction-log
mechanics separate from provider discovery and lifecycle.

---

## Write and Commit Path

### Transaction model

The transaction API supports:

- multi-operation `Transaction`;
- single-operation `AtomicOperation`, with undo and redo;
- single-operation `AtomicRedoOperation`, with redo only.

Each operation identifies a target state-provider ID and may carry:

- provider-defined metadata;
- undo data;
- redo data;
- an `OperationContext` returned by the provider for later unlock.

The logical log represents a multi-operation transaction as begin, zero or
more operation, and end records. The end record records commit or abort.
Single-operation transactions use a specialized begin record.

### Primary flow

```text
State provider       Transaction Manager      Logging Replicator       V1
      |                       |                       |                  |
      |-- add redo/undo ----->|                       |                  |
      |                       |-- create log record ->|                  |
      |                       |                       |-- replicate ---->|
      |                       |                       |<- assigned LSN --|
      |                       |                       |-- append/flush    |
      |                       |                       |<- quorum ACK -----|
      |<------ ApplyAsync(commit LSN, redo) ----------|                  |
      |------ OperationContext ---------------------->|                  |
      |<---------------- commit completes -----------|                  |
```

`ReplicatedLogManager::BeginReplicate` serializes a logical record and submits
it to V1. V1 assigns the replication sequence number, which becomes the
logical record's LSN
(`src/prod/src/data/txnreplicator/loggingreplicator/ReplicatedLogManager.cpp:918-974`).

For a multi-operation transaction, V2 does not expose partial state as
committed merely because individual operation records exist. When the commit
end record is processed, `OperationProcessor` walks the transaction's linked
records and calls the State Manager's `ApplyAsync` for each redo operation
using the transaction's commit sequence number. Aborted transactions are not
redo-applied as committed state
(`src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp:757-916`).

`CommitTransactionAsync` returns the end record's LSN as the commit LSN. A
successful return therefore identifies the replicated transaction boundary,
not an application-selected sequence number.

### Apply and unlock

The State Manager routes each apply call to the target `IStateProvider2`.
`ApplyAsync` receives:

- the operation LSN;
- transaction identity and commit sequence number;
- an `ApplyContext`;
- provider metadata;
- redo or undo data.

`ApplyContext` distinguishes both the execution role and the action:

- primary, secondary, or recovery;
- redo, undo, unlock, or false progress.

A provider can return an `OperationContext`, commonly representing locks or
other transaction-scoped resources. V2 later calls `Unlock` after the
transaction no longer needs those resources.

---

## Durable Log, Stability, and Checkpoints

### Logical and physical records

The log contains two broad record classes:

- **logical records** describing transactions, operations, barriers, epochs,
  backup boundaries, and other replicated state;
- **physical records** describing checkpoints, indexing, information records,
  and log truncation structure.

The physical log makes V2 restart-stateful. Unlike V1, V2 reopens its own log
and reconstructs transaction and replication state from it.

Persisted replicas use the KTL-backed logical log. Volatile replicas use an
in-memory log and do not support backup/restore. Platform-specific shared-log
and dedicated-log behavior is summarized in
[State Management and Persistence](state-management.md#shared-log-vs-dedicated-log-windows-vs-linux).

### Stable LSN and barriers

V2 distinguishes the log tail from the **last stable LSN**. The tail is the
highest locally known committed sequence number; stability represents a
quorum-safe boundary used by checkpoints, backup, and truncation.

Barrier records establish ordered stability points. They ensure earlier
transactions have reached the required processing boundary before checkpoint
or backup work advances.

### Three-phase provider checkpoint

V2 coordinates checkpoints across all state providers:

1. `PrepareCheckpoint(checkpointLsn)` synchronously captures the logical
   version that the provider must checkpoint.
2. `PerformCheckpointAsync()` writes the captured version without blocking
   newer application work for the entire I/O duration.
3. `CompleteCheckpointAsync()` atomically promotes the prepared checkpoint and
   removes obsolete checkpoint artifacts.

The Logging Replicator records checkpoint progress in its own physical log and
coordinates provider completion with log flush and, after full copy, atomic
copy-log rename. Only after both sides are consistent can older log ranges be
truncated.

Source:
`src/prod/src/data/txnreplicator/loggingreplicator/CheckpointManager.cpp:372-430`;
`src/prod/src/data/txnreplicator/loggingreplicator/CheckpointManager.cpp:1393-1540`.

---

## Restart Recovery

V2 recovery combines two durable sources:

| Durable source | Contains |
|---|---|
| State-provider checkpoints | Materialized provider state at a checkpoint LSN |
| Transactional Replicator log | Transactions and physical metadata needed after that checkpoint |

The open sequence is deliberately ordered:

1. initialize and open the V2 log;
2. inspect the tail and linked physical records to locate the last valid
   checkpoint and log head;
3. open the State Manager and recover every provider checkpoint;
4. replay the recoverable log suffix through the normal operation processor;
5. reconstruct transaction maps, progress vectors, stable LSN, and log-tail
   state;
6. complete any checkpoint work required to leave the recovered files in a
   canonical state;
7. allow role transition and normal traffic.

`TransactionalReplicator::OpenAsync` first asks the Logging Replicator for
recovery information, then opens the State Manager, and only then performs log
replay. State providers can call back into the Transactional Replicator during
checkpoint recovery, so the top-level async service is marked open before
provider recovery begins
(`src/prod/src/data/txnreplicator/TransactionalReplicator.cpp:205-291`).

Recovery applies committed redo after the recovered checkpoint. It can also
process undo and false-progress corrections when the durable tail is not valid
for the replica's current history.

This is the central difference from V1:

- V1 reconstructs its process-local queues from the state provider and RA;
- V2 recovers a framework-owned log and replays it into checkpointed
  `IStateProvider2` state.

---

## Replica Copy and Divergent History

V2 uses V1's copy stream as a transport for a richer copy protocol. The
secondary sends a copy context containing its progress vector and log
boundaries. The primary compares that history with its own and selects a copy
mode.

| Copy mode | Meaning |
|---|---|
| None | Target already has the required state |
| Log copy | Histories share a usable point; send missing log records |
| Full copy | No retained common point; send provider state, then a log suffix |
| False progress | Target contains records not present in the selected primary history; truncate or undo them before continuing |

The **progress vector** records epoch-to-LSN history. It allows V2 to find the
last shared history across failover and reconfiguration rather than comparing
only one tail LSN.

For full copy:

1. the State Manager enumerates provider metadata and each provider's
   `GetCurrentState()` stream;
2. the primary sends provider state records;
3. the primary sends enough log records to establish the copied checkpoint
   and a stable continuation point;
4. the secondary calls `BeginSettingCurrentStateAsync`;
5. it dispatches each record through `SetCurrentStateAsync`;
6. it calls `EndSettingCurrentStateAsync`;
7. it checkpoints the installed state and atomically promotes the copy log;
8. normal replication drain continues.

If copy is interrupted, `EndSettingCurrentStateAsync` is not called. Providers
must treat begin/set/end as an installation protocol and keep incomplete copy
state separate from their current state.

Source:
`src/prod/src/data/txnreplicator/loggingreplicator/CopyStream.cpp:220-340`;
`src/prod/src/data/txnreplicator/loggingreplicator/SecondaryDrainManager.cpp:970-1070`.

---

## Role and Reconfiguration Integration

V2 participates in replica lifecycle without replacing RA or V1:

- **Primary:** the Logging Replicator establishes primary log state before the
  State Manager changes providers to primary.
- **Idle secondary:** V2 prepares to receive copy before providers enter the
  idle-secondary role.
- **Active secondary:** V2 coordinates replication drain and the provider role
  transition so read consistency is not granted prematurely.
- **None:** V2 changes providers to none and deletes the local log.

The COM wrapper passes RA-facing methods that V2 does not need to reinterpret
directly to the inner V1 primary replicator, including epoch updates, progress,
catch-up capability, replica-set configuration, quorum waiting, build, and
remove-replica operations.

V2 still records epoch transitions in its own durable log and progress vector.
The distinction is:

- V1/RA own distributed role and membership orchestration;
- V2 owns the durable storage-engine consequences of those transitions.

---

## API Model

### Creation API

The native creation API is on the internal partition interface:

```text
IFabricInternalStatefulServicePartition::CreateTransactionalReplicator(
    stateProviderFactory,
    dataLossHandler,
    v1ReplicatorSettings,
    transactionalReplicatorSettings,
    ktlLoggerSharedLogSettings
) -> (IFabricPrimaryReplicator, transactionalReplicator)
```

The API accepts separate settings because V1 transport and V2 storage are
different layers. It returns:

- `IFabricPrimaryReplicator`, consumed by SF reconfiguration;
- a Transactional Replicator handle, consumed by the service or Reliable
  Collections runtime.

The native COM creation and `IFabricStateProvider2Factory` contracts are
internal rather than part of the ordinary public V1 SDK surface
(`src/prod/src/idl/internal/FabricRuntime_.idl:292-341`).

### `ITransactionalReplicator`

The native C++ interface composes three main capabilities:

| Base interface | API purpose |
|---|---|
| `ITransactionManager` | Create transactions and add, commit, or abort operations |
| `IStateProviderMap` | Get, add, remove, enumerate, or get-or-add named providers |
| `IVersionManager` | Coordinate safe removal of checkpoints and provider versions |

It also exposes:

- `IsReadable` and `HasPersistedState`;
- last stable and last committed LSN;
- current epoch;
- transaction and state-manager change notifications;
- full and incremental backup;
- restore.

#### Transaction operations

| Operation | Meaning |
|---|---|
| `CreateTransaction` | Create a multi-operation transaction |
| `BeginTransaction` / `AddOperation` | Add provider-specific metadata, undo, and redo |
| `CommitTransactionAsync` | Replicate and commit the transaction; return commit LSN |
| `AbortTransactionAsync` | Log and complete an abort |
| `CreateAtomicOperation` | One operation with undo and redo |
| `CreateAtomicRedoOperation` | One redo-only operation |

#### State-provider map operations

Provider names are URIs. Add and remove operations participate in a
transaction, so the set of registered providers is itself replicated state.

| Operation | Meaning |
|---|---|
| `Get` | Resolve a provider by name |
| `AddAsync` | Transactionally add a provider by type name |
| `RemoveAsync` | Transactionally remove a provider |
| `GetOrAddAsync` | Repeatable get-or-create within a transaction |
| `CreateEnumerator` | Enumerate registered providers |

### `IStateProvider2Factory`

The factory creates a provider from `FactoryArguments`, which identify its
name, type, ID, parent, initialization data, and replica context. This permits
the State Manager to reconstruct provider instances during recovery and copy
without application code manually rebuilding the provider registry.

### `IStateProvider2`

`IStateProvider2` is a storage-engine plugin contract, not a raw replication
stream contract.

#### Identity and lifecycle

| API | Responsibility |
|---|---|
| `GetName` / `GetChildren` | Expose provider identity and hierarchy |
| `Initialize` | Receive the Transactional Replicator, work folder, and child providers |
| `OpenAsync` | Open local provider resources |
| `ChangeRoleAsync` | Enter the role selected by the replica lifecycle |
| `CloseAsync` / `Abort` | Graceful or immediate shutdown |
| `RemoveStateAsync` | Delete local durable provider state |
| `PrepareForRemoveAsync` | Fence or validate transactional provider removal |

#### Apply contract

| API | Responsibility |
|---|---|
| `ApplyAsync` | Apply redo, undo, recovery, or false-progress work at an LSN |
| `Unlock` | Release the provider-owned context retained for the transaction |

The provider does not choose global LSNs or independently decide commit order.
It implements its data structure under the ordering and transaction boundary
supplied by V2.

#### Checkpoint and backup contract

| API | Responsibility |
|---|---|
| `PrepareCheckpoint` | Capture the version associated with a checkpoint LSN |
| `PerformCheckpointAsync` | Persist the captured checkpoint |
| `CompleteCheckpointAsync` | Promote it as the current checkpoint |
| `RecoverCheckpointAsync` | Reopen current checkpoint state |
| `BackupCheckpointAsync` | Copy checkpoint artifacts into a backup |
| `RestoreCheckpointAsync` | Install checkpoint artifacts from a backup |

#### Copy contract

| API | Responsibility |
|---|---|
| `GetCurrentState` | Produce provider snapshot records on the source |
| `BeginSettingCurrentStateAsync` | Start target-side installation |
| `SetCurrentStateAsync` | Consume one numbered provider record |
| `EndSettingCurrentStateAsync` | Atomically finish target-side installation |

Unlike V1 `IStateProvider`, `IStateProvider2` does not expose
`GetLastCommittedSequenceNumber`, `UpdateEpoch`, or raw replication operation
streams. The Logging Replicator owns those concerns and calls the provider at
structured transaction, checkpoint, and copy boundaries.

---

## Backup, Restore, and Data Loss

V2 backup combines provider checkpoint files with the log range required to
recover from the backup point. Full and incremental backup are coordinated
against stable log boundaries. The user-supplied backup callback decides where
the completed backup is stored.

Restore replaces local provider checkpoint and log state, then uses the normal
open and recovery machinery to reconstruct a consistent replica. Persisted
restore is guarded by restore policy and timeout/cancellation APIs.

Data loss remains a service-level decision through `IFabricDataLossHandler`.
After SF determines that quorum recovery cannot preserve the old history, V2
invokes the handler and records the resulting epoch/history transition rather
than silently inventing application state.

---

## V1 and V2 Comparison

| Concern | V1 Fabric Replicator | V2 Transactional Replicator |
|---|---|---|
| Replication transport | Built in | Uses V1 internally |
| RA primary-replicator API | Native implementation | Delegates/wraps V1 |
| Replicator-owned durable log | No | Yes |
| Restart recovery | State provider + RA reconstruct V1 | Checkpoint recovery + V2 log replay |
| Service operation format | Opaque bytes | Structured transaction records |
| Transactions | Service-defined | Framework-managed |
| State-provider registry | Service-defined | Replicated State Manager |
| Checkpoint coordination | Service-defined | Framework-managed three-phase protocol |
| Divergent history | Service copy policy | Progress vector + false-progress correction |
| Provider API | `IStateProvider` | `IStateProvider2` |
| Typical consumer | Custom stateful service/storage engine | Reliable Collections or custom transactional provider |

V2 therefore should not be used as evidence that the V1 replicator persists
metadata. They are intentionally different designs: V2 adds a database-style
log and recovery layer while retaining V1 as its distributed replication
transport.

The corresponding Kuberic application proposal is
[SQLite on a V2 Transactional Replicator](../../features/sqlite/v2-transactional-replicator.md).

---

## Key Source References

Paths are relative to the Service Fabric repository.

| Area | Source |
|---|---|
| Top-level composition and API | `src/prod/src/data/txnreplicator/TransactionalReplicator.h/.cpp` |
| V1/V2 construction and COM bridge | `src/prod/src/data/txnreplicator/ComTransactionalReplicator.cpp` |
| Transactional interface | `src/prod/src/data/txnreplicator/common/ITransactionalReplicator.h` |
| Transaction API | `src/prod/src/data/txnreplicator/common/ITransactionManager.h` |
| Provider map API | `src/prod/src/data/txnreplicator/common/IStateProviderMap.h` |
| Provider API | `src/prod/src/data/txnreplicator/common/IStateProvider2.h` |
| Provider factory | `src/prod/src/data/txnreplicator/statemanager/IStateProvider2Factory.h` |
| Logging Replicator | `src/prod/src/data/txnreplicator/loggingreplicator/LoggingReplicatorImpl.h/.cpp` |
| Transaction record creation | `src/prod/src/data/txnreplicator/loggingreplicator/TransactionManager.cpp` |
| Apply semantics | `src/prod/src/data/txnreplicator/loggingreplicator/OperationProcessor.cpp` |
| Recovery | `src/prod/src/data/txnreplicator/loggingreplicator/RecoveryManager.cpp` |
| Checkpointing | `src/prod/src/data/txnreplicator/loggingreplicator/CheckpointManager.cpp` |
| Copy-mode selection | `src/prod/src/data/txnreplicator/loggingreplicator/CopyStream.cpp` |
| Secondary copy installation | `src/prod/src/data/txnreplicator/loggingreplicator/SecondaryDrainManager.cpp` |
| Internal native creation API | `src/prod/src/idl/internal/FabricRuntime_.idl` |
