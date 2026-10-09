# Service Fabric Stateful Service Architecture

A detailed study of how Azure Service Fabric achieves high availability and
failover for stateful services — covering the system architecture, the
replicator protocol, reconfiguration phases, and the Rust API surface from
[service-fabric-rs](https://github.com/Azure/service-fabric-rs).

Source code reference: `build/service-fabric/` (depth-1 clone).

---

## Documents

| Document | Contents |
|----------|----------|
| [Architecture and Concepts](architecture.md) | System architecture, key concepts (partitions, replicas, epochs), replica roles and lifecycle |
| [Replication Protocol](replication-protocol.md) | Replicator protocol (copy/replication streams, LSN tracking), quorum mechanics, building new replicas, scale-down |
| [Failover and Recovery](failover.md) | Failover reconfiguration, switchover (swap primary), epoch-based fencing, failure detection, quorum loss and data loss |
| [State Management and Persistence](state-management.md) | Reliable Collections, V1 vs V2 replicator architecture, IStateProvider vs IStateProvider2, shared/dedicated log (Windows vs Linux), ReadStatus/WriteStatus |
| [V2 Transactional Replicator](v2-transactional-replicator.md) | V2 layering over V1, transaction and commit path, durable log, checkpoints, recovery, copy modes, and ITransactionalReplicator/IStateProvider2 APIs |
| [API Surface and References](references.md) | Rust API bindings (service-fabric-rs), comparison with CloudNativePG, key source code references |

## Related Kuberic Documents

- [Service Fabric Alignment and Runtime Simplification](../../features/kuberic/service-fabric-alignment.md)
- [Stateless Default Replicator](../../features/kuberic/stateless-default-replicator.md)
- [SQLite on a V2 Transactional Replicator](../../features/sqlite/v2-transactional-replicator.md)
- [Replicator Boundary and Native Proof](../../features/kuberic/replicator-boundary.md)
- [Archived Kuberic Replication Protocols](../../archive/v1/protocols.md)
- [Archived Design Gaps](../../archive/v1/design-gaps.md)
- [Archived WAL Persistence Design](../../archive/v1/future/wal-persistence.md)
- [CloudNativePG Architecture](../cloudnative-pg-architecture.md)
