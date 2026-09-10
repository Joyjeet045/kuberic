# kuberic-core

Core replication framework for [Kuberic](../README.md). Provides the building blocks for stateful replicated services on Kubernetes.

## Key Components

| Module | Purpose |
|--------|---------|
| `ReplicaAgent` | Pod-local correlated control admission, fencing, replay, and serialization |
| `add_replica` | Coarse add intent, coordinator progress/results, and tracked-build contracts |
| `remove_replica` | Coarse removal intent, coordinator progress/results, deadlines, and commit semantics |
| `replica_lifecycle` | Internal typed add/build and removal peer-stage contracts |
| `PodRuntime` | Per-pod async runtime — executes ordered service and replicator effects |
| `WalReplicator` | Quorum-based replicator — manages primary→secondary data flow, copy protocol, epoch tracking |
| `PartitionDriver` | Read-only reconstruction of a durably committed stable topology |
| `ReplicaHandle` | Status access plus the single correlated control-action mutation method |
| `ReplicaLifecyclePeer` | Internal primary-agent→target-agent Prepare/Activate/Cleanup/Retire protocol |

## Usage

Your service receives lifecycle events and implements a state provider:

```rust
// In your Open handler:
let (handle, handles) = WalReplicator::create(id, &data_bind, fault_tx, sp_tx).await?;

// On ChangeRole(Primary):
// Start your client-facing gRPC server

// On replication stream (secondary):
// Persist data, then acknowledge
op.acknowledge();
```

Replication operations and catch-up quorum waits use a 5-second deadline by
default and return `KubericError::NoWriteQuorum` when it expires. Services that
need a different bound can use `WalReplicator::create_with_options` with
`WalReplicatorOptions::new().quorum_timeout(duration)`.

Copy streams require an explicit application completion acknowledgement.
After `get_operation()` returns `None`, verify `copy_lsn()`, finish snapshot
validation/installation and required checkpoints, then call
`acknowledge_completion()`. Only that acknowledgement permits the copy RPC to
succeed and the secondary's `current_progress` and `committed_lsn` to advance.
Dropping the stream on any failure rejects the copy without publishing progress.
An interrupted network stream has no completion boundary and cannot be acknowledged.

See [examples/kvstore](../examples/kvstore/) and [examples/sqlite](../examples/sqlite/) for complete implementations.

## Replication Model

- **Primary** serializes operations, calls `replicator.replicate()`, blocks until write quorum ACKs
- **Bounded quorum** fails a write or catch-up wait with `NoWriteQuorum` when its deadline expires
- **Secondary** receives operations via gRPC data plane, persists, then acknowledges
- **Copy protocol** builds new replicas from full snapshot + incremental replay
- **Epoch fencing** rejects stale operations from old primaries after failover
