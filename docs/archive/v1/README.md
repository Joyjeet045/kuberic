# Kuberic: SF-Style Stateful Replication on Kubernetes

> **Archived classic v1 design.** The implementation described here was
> removed. This page is retained only as architecture history and is not a
> supported API, deployment or roadmap.

High-level design for a Service Fabric–inspired stateful replication system
running on Kubernetes. The operator acts as the Failover Manager; user
application pods run an in-process replicator that reacts to operator signals.

**Historical implementation:** `kuberic-core`, `kuberic-operator`, `kvstore`.
See [Status & Roadmap](status.md) for historical source maps and the current
[Testing strategy](../../features/kuberic/testing.md) for surviving coverage.

---

## Documents

| Document | Contents |
|----------|----------|
| **This file** | Architecture overview, key design decisions |
| [Protocols](protocols.md) | Failover, switchover, create, scale-up/down, restart, access status, epoch fencing, quorum model |
| [Operator](operator.md) | CRD spec/status, reconciler phases, healthy phase, gRPC tracking, scale-up/down, restart recovery |
| [Status & Roadmap](status.md) | MVP simplifications, known gaps (SF + CNPG), degenerate configs, open questions, source code map |
| [Current Testing](../../features/kuberic/testing.md) | Surviving v2, database, SQL Server and DEX validation |
| [Design Gaps](design-gaps.md) | Historical protocol, resilience and implementation review |
| [Failure Scenarios](operator-failure-scenarios.md) | 9 failure scenarios with detection + recovery design |

### Background References

| Document | Contents |
|----------|----------|
| [SF Architecture](../../background/service-fabric/README.md) | SF replicator, failover, switchover, epochs, quorum, data loss, Rust API |
| [CNPG Architecture](../../background/cloudnative-pg-architecture.md) | CNPG operator, failover, fencing, failure scenarios, instance manager |

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                      Kubernetes Cluster                          │
│                                                                  │
│  ┌──────────────────────┐                                        │
│  │ Kuberic Operator     │   Watches: KubericSet CRD             │
│  │                      │   Uses: durable workflows +           │
│  │                      │   GrpcReplicaHandle for remote pods   │
│  └──────────┬───────────┘                                        │
│             │ gRPC (ReplicatorControl)                           │
│             ▼                                                    │
│  ┌───────────────────────────────────────────────────────┐       │
│  │               Replica Set (3 Pods)                    │       │
│  │                                                       │       │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │       │
│  │  │ Pod 1       │  │ Pod 2       │  │ Pod 3       │   │       │
│  │  │ PRIMARY     │  │ ACTIVE SEC  │  │ ACTIVE SEC  │   │       │
│  │  │             │  │             │  │             │   │       │
│  │  │ ReplicaAgent│  │ ReplicaAgent│  │ ReplicaAgent│   │       │
│  │  │ PodRuntime  │  │ PodRuntime  │  │ PodRuntime  │   │       │
│  │  │ ┌─────────┐ │  │ ┌─────────┐ │  │ ┌─────────┐ │   │       │
│  │  │ │Replicatr│◄├──├─┤Replicatr│ ├──├─┤Replicatr│ │   │       │
│  │  │ │Actor    │ │  │ │Actor    │ │  │ │Actor    │ │   │       │
│  │  │ └────┬────┘ │  │ └─────────┘ │  │ └─────────┘ │   │       │
│  │  │      │      │  │             │  │             │   │       │
│  │  │ User App    │  │ User App    │  │ User App    │   │       │
│  │  │ (events)    │  │ (standby)   │  │ (standby)   │   │       │
│  │  └─────────────┘  └─────────────┘  └─────────────┘   │       │
│  └───────────────────────────────────────────────────────┘       │
│          ReplicaLifecyclePeer: primary agent → target agent      │
│          (typed add/build stages and exact removal Retire)       │
│                                                                  │
│  Services:                                                       │
│    {name}-rw  ──► Primary       (label: role=primary)            │
│    {name}-ro  ──► Secondaries   (label: role=secondary)          │
└──────────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| **No StatefulSets** | Operator manages Pods directly | LSN-based primary selection, per-instance control |
| **Replicator is in-process** | Library linked into app, not sidecar | No coordination overhead, shared memory |
| **Durable global membership authority** | Operator/CRD freezes topology intent and publishes committed snapshots | Keeps Kubernetes placement and global commit authority outside volatile agents |
| **Pod-local RA-lite boundary** | `ControlServer → ReplicaAgent → PodRuntime` | Agent owns local admission/fencing/correlation; runtime owns ordered effects |
| **Agent-owned add/remove coordination** | Current primary `ReplicaAgent` sequences transient local/peer work from one coarse intent | Removes the operator as a runtime step sequencer without creating a second durable store |
| **Narrow lifecycle peer** | `ReplicaLifecyclePeer` v2 supports add/build stages and removal `Retire` | Reuses exact fencing and replay without exposing arbitrary RA-to-RA runtime actions |
| **Event-based APIs** | Both replicator and user use mpsc channels | Owned `&mut` state, no `Arc<Mutex<...>>` |
| **Dual-channel replicator** | Separate control (low-freq) and data (high-freq) channels | Control events don't block write path |
| **Atomic status reads** | PartitionState uses AtomicU8/AtomicI64 | Zero-cost read_status()/write_status() polling |
| **Failover epoch ordering** | Confirm candidate, apply epoch to it, promote, then converge retained secondaries | Matches durable Phase-1/Phase-4 ordering while fencing the chosen write authority |
| **State provider as durability layer** | Persisted mode: `acknowledge()` gates quorum (SF default) | Strongest guarantee: replicate() returns only after quorum applied |
| **Primary self-fencing** | Liveness probe isolation check (CNPG-style) | Defense-in-depth for asymmetric partitions |
| **Failover delay** | Optional `spec.failoverDelay` (default 0 = immediate) | K8s adaptation — SF failovers immediately, K8s pod probes can flap |
| **Data loss protocol** | Wait on unresolved safety evidence; after conclusive loss, advance epoch then invoke correlated `OnDataLoss` | Prevents premature promotion and makes callback ambiguity recoverable |
| **gRPC failure tracking** | Per-replica failure counter in CRD status | K8s adaptation — replaces SF federation heartbeats |
| **Operator restart recovery** | Reconstruct read-only stable topology from CRD status + pod list | SF FM pattern — stateless operator, durable state in API |
| **mTLS deferred** | Post-MVP; MVP assumes trusted cluster | Reduces initial complexity |
| **ReplicaHandle trait** | Status plus one correlated mutation method | Operator and high-fidelity tests use the gRPC client. |
