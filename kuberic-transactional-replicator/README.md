# Transactional Replicator

`TransactionalReplicator<State>` adds transaction semantics above Kuberic's
opaque `StateReplicatorHandle`. It does not replace the low-level replicator or
modify the operator. `State` implements `TransactionalStateProvider`, declaring
a stable format ID, observation validation, deterministic apply, and snapshot
validation. The Reliable State Manager implements this contract for a registry
of named dictionaries; other providers can use the generic coordinator.

## Commit and Recovery

1. Begin captures a state snapshot and a replica-generation-bound context.
2. Commit acquires a shared gate, checks access, expiration, identity and
   observations, and prepares the complete next state privately.
3. One versioned binary envelope is replicated with one Kuberic commit LSN.
4. The primary synchronously persists the complete record, then publishes the
   whole next state. Secondaries likewise persist before applying/acknowledging.
5. A failed validation has no replicated effect. An uncertain quorum or local
   persistence outcome fences writes until recovery and reports a transient
   replica fault. A caller disconnecting does not abandon an accepted commit.

State is cloned and applied privately, then swapped while holding one lock;
multi-provider apply cannot leak partial state. Provider callbacks must be
deterministic and free of external side effects. Their apply/unlock boundary
is the atomic state publication, not per-collection visible mutations.

Terminology:
- **Accepted:** received by the low-level replication pipeline, not yet an
  application durability guarantee.
- **Durable:** the complete transaction record has been synced locally.
- **Quorum-committed:** low-level replication returned its quorum result.
- **Applied:** the complete state and retained result were published together.
- **Checkpointed:** a synced snapshot generation was atomically selected.

The coordinator tracks local quorum confirmation separately from applied LSN.
Only confirmed primary state can be checkpointed or backed up. A promoted
replica can retain an in-doubt accepted suffix according to Kuberic's election
authority; clients must query/retry the original identity, not assume that a
lost reply means abort. A subsequent successful commit confirms that prefix.
Epoch callbacks reconstruct state and retained outcomes to the supplied
authority boundary before truncating the log. A boundary older than the
checkpoint fails explicitly and requires authoritative full copy.

## Checkpoint, Copy and Backup

The snapshot includes all provider state, registry metadata, the LSN and bounded
duplicate-request results. Checkpoint publication precedes log truncation.
Copy reconstructs the requested retained LSN, sends bounded chunks, validates
the entire received snapshot and completion boundary, and atomically installs
it before acknowledging application copy completion. A failed copy cannot
publish progress or promote to a writable role.

Backup is a separate atomic snapshot file. Restore is allowed only before
opening the replica's lifecycle host. The administrative caller must restore
into a fresh replica and establish an appropriate new data-loss epoch through
the existing Kuberic control protocol; this API does not silently replace a
live primary's history or coordinate cluster-wide disaster recovery.

## Limits and Retries

- 16 outstanding transaction contexts per coordinator, 60-second maximum TTL.
  Contexts are caller-owned: drop/abort releases admission immediately. Expired
  contexts cannot commit; applications should not retain expired objects.
- 1 MiB transaction envelopes and 8 MiB aggregate checkpoint snapshots.
- The latest 1,024 request results survive recovery, checkpoints and copy.
  Reusing a retained request or transaction ID with different data is an error.
  After eviction, an old ID has no exactly-once guarantee and may execute again.
- The log's 64 MiB retained-history limit is checked before replication.
  Applications invoke checkpoint at confirmed boundaries to reclaim it.

This MVP uses one record per complete transaction, optimistic serializability,
and synchronous durability. It does not implement separate prepare/commit
records, distributed transactions, configurable lock modes, or group commit.