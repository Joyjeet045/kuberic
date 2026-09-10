# Kuberic: Status and Roadmap

Implementation status, known gaps, and open questions.

> Part of the [Kuberic Design](../kuberic-replicator-design.md).

---

## MVP Simplifications

| Full Design | Current Status |
|---|---|
| Replicator WAL on disk | No WAL — replicator queues in memory, user `acknowledge()` releases |
| Incremental catchup via WAL replay | Full rebuild via copy stream (no WAL to replay) |
| build_replica copies state + replays WAL | Full state copy via data-plane CopyStream RPC |
| Copy stream delivers full state | ✅ Implemented — GetCopyContext + GetCopyState + CopyStream on data plane |
| `acknowledge()` gates quorum (persisted mode) | ✅ Implemented — SecondaryReceiver defers ACK until user acknowledges |
| `must_catch_up` enforced in catchup | ✅ Implemented — QuorumTracker tracks per-replica ACK progress |
| Build completes on copy+repl ACKed | ✅ Implemented — PrimarySender buffers ops during copy, replays on connect |
| mTLS on all gRPC | Deferred — assumes trusted cluster |
| ReportFault rate limiting | Operator-side, basic |
| Failover delay | ✅ Implemented — non-negative continuous-failure delay; recovery resets it |
| Durable Phase-1 failover | ✅ Implemented — live evidence, previous/current read quorum, deterministic best-primary confirmation |
| Data loss failover | ✅ Implemented — write-ahead data-loss epoch + correlated epoch-fenced `OnDataLoss` |
| Quorum/best-candidate wait | ✅ Implemented — explicit durable wait with rotating unavailable probes; no premature promotion |
| Secondary health detection | ✅ Implemented — durable rebuild or force-removal |
| Missing pod detection | ✅ Implemented — durable eviction/rebuild path |
| gRPC failure tracking | ❌ Not implemented — K8s adaptation of SF federation heartbeats |
| Stable Healthy operator restart recovery | ✅ Implemented — authoritative status snapshot + read-only `PartitionDriver::recover()` |
| Durable initial partition creation | ✅ Implemented — explicit no-previous-topology checkpoint, partial committed bootstrap snapshots, gated routing |
| Durable switchover restart recovery | ✅ Implemented — compact CRD operation checkpoint, correlated activities, compensation |
| Durable scale-up and stale-secondary rejoin | ✅ Implemented — one coarse primary-agent intent, target peer stages, tracked copy/quorum, commit-aware compensation |
| Durable scale-down and stale/dead-secondary eviction | ✅ Implemented — one coarse primary-agent intent, config-first commit, lifecycle-peer retirement, exact connection cleanup, UID-fenced deletion |
| Pod-local RA-lite boundary | ✅ Implemented — control v3, one versioned correlated mutation path with generation/version fencing and bounded replay |
| Lifecycle peer protocol | ✅ Implemented — v2 typed AddBuild Prepare/Activate/Cleanup and Remove Retire on the existing control listener |
| Primary self-fencing (liveness probe) | ❌ Not implemented — K8s defense-in-depth (from CNPG) |
| Node drain handling | ❌ Not implemented — K8s adaptation (analogous to SF PLB) |
| CRD conditions (Ready, Degraded, Quorum) | ❌ Not implemented — K8s addition |
| force_remove_secondary | ✅ Implemented through the coarse agent-owned durable remove protocol |

---

## Pod-Local Control Status

`GetStatus` requires control protocol version 3 and lifecycle-peer protocol
version 2, `AgentGeneration`,
`AgentControlVersion`, the current generation-qualified action, 16 retained
terminal observations, and up to 16 successfully received local fault records.
The current action and retained terminals are the only local correlation
ledger. Terminal failures carry a stable error class; error text is limited to
1,024 UTF-8 bytes.

Pod UID and agent generation have different meanings. The Pod UID is the
replica incarnation; the generation changes on every process start, including
a container restart in the same Pod. Agent observations are process-local and
are never merged across generations. CRD `pendingAction` remains the durable
global source of intent and stores only optional dispatch fences, not the
agent's action/fault history.

Fault records are best effort because user fault reporting is non-blocking.
Terminal replay is bounded and volatile. An absent or evicted record is not
evidence that an action did not execute.

During add/rebuild, the primary action includes coarse coordinator phase and
typed terminal result. Status also exposes one current/latest tracked build
observation with exact target generation and acknowledged copy LSN. Target
process generation change invalidates that copy proof.

CRD `operation.addIntent` is the durable semantic input. `committedSnapshot`
records irreversible current configuration before serving publication.
`CommittedDegraded` means membership committed but the target is deliberately
not serving until Healthy reconciliation re-attests and repairs its label.

During removal, the primary action exposes one bounded phase from
Validating through Attesting or Compensating, the attempt ID,
`current_install_dispatched`, exact commit time, connection absence, target
retirement observation, signed expiries, and a typed terminal result.
`operation.removeIntent` is the durable semantic input. The operator persists
the primary's commit timestamp and reduced-configuration signature in
`removeCommitEvidence`; the reduced `committedSnapshot` is scoped to the
active workflow and does not replace `stableSnapshot` until exact-UID cleanup
and final publication complete.

Removal terminal coordinator results are `CommittedClean`,
`CommittedDegraded`, `Compensated`, and `CompensationIncomplete`. Separate
operator dispositions pin the operation in `Poisoned`:
`FailedPreCommitIncomplete`, `InvalidRemovalState`, and
`AmbiguousPrimaryRestart`. The first records known pre-commit exhaustion; the
second records structurally impossible or post-dispatch ambiguous state; the
third records evidence erased by a complete same-Pod primary process restart.
They are durable global decisions, unlike the volatile coordinator ledger.

---

## Known Gaps vs SF

1. **WAL persistence.** The replicator has no write-ahead log. State survives
   only in the user's state provider. This means incremental catchup (replay
   from WAL) is not possible — every restart requires a full copy rebuild.

2. **mTLS.** All gRPC channels are unencrypted. Production deployments need
   mTLS or equivalent pod-to-pod encryption.

---

## Known Gaps vs CNPG (deliberate divergences)

CNPG patterns we **adopted** (K8s-specific value-adds):
1. Pod anti-affinity generation from CRD spec
2. Primary self-fencing via liveness probe isolation check
3. gRPC failure tracking (replaces CNPG's HTTP failure tracking)
4. Node drain detection with proactive switchover
5. CRD conditions (Ready, Degraded, QuorumAvailable)
6. Stable Healthy operator restart recovery from `stableSnapshot`, current
   pod identities, and runtime `GetStatus`
7. Optional failover delay for environments with flappy probes

CNPG patterns we **rejected** (conflict with SF model):
1. **`R + W > N` quorum formula.** CNPG uses Dynamo-style quorum for PostgreSQL
   sync standbys. We use SF's `WriteQuorum = ⌊N/2⌋+1` which is simpler and
   already handled by `QuorumTracker`.
2. **Timeout-based quorum-loss override.** Kuberic does not let elapsed time
   discard an unavailable quorum-restoring or possibly better replica. It
   waits while evidence could change the result, and invokes `OnDataLoss`
   only after the remaining observations conclusively establish data loss.
3. **Annotation-based fencing.** CNPG fences instances via a JSON annotation.
   We use SF's epoch-based fencing (replicator-level, stronger).
4. **pg_rewind for replica rejoin.** CNPG rewinds the old primary's WAL to
   rejoin as replica. We use full copy rebuild (no WAL on disk).

---

## Degenerate Configurations

**replicas=1:** No replication, no failover. Pod restart = resume as primary.
Development/testing only.

**replicas=2:** Write quorum=2 (both must ACK). Zero write fault tolerance.
Single failure → NoWriteQuorum. Failover is safe (survivor has all data).

**Minimum recommended:** `replicas >= 3`. Pod anti-affinity across nodes
(ensures failure independence for quorum model).

---

## Open Questions

1. **Multi-partition support** — multiple independent replica sets per
   KubericSet. SF supports this via partitioning schemes. Current
   design: one partition per CRD.
2. **Pod identity and PVC binding** — how to bind recreated pods to correct
   PVCs. Relevant for WAL persistence.
3. **Mid-reconfiguration failover handoff** — the pure election model and
   status schema support an explicit previous configuration, but production
   failover currently starts from stable `Healthy` topology where previous
   configuration is absent.
4. **Old-primary physical cleanup** — failover fences the serving label and
   excludes the old incarnation from stable topology; deletion/rebuild remains
   subsequent topology reconciliation.
5. **Liveness probe HTTP endpoint** — PodRuntime currently exposes only gRPC.
   The self-fencing liveness probe (K8s-specific addition) needs an HTTP
   health endpoint. This is not an SF pattern — it compensates for K8s
   lacking SF's federation-level failure detection.
6. **Agent-owned switchover** — add/build and removal now delegate their local
   reconfiguration sequence through one coarse primary intent. Switchover is
   the next candidate; it still persists and dispatches its revoke, catch-up,
   demotion, promotion, epoch, configuration, and compensation activities from
   the operator checkpoint.

---

## Source Code Map

```
kuberic-core/
├── proto/kuberic.proto           # gRPC: ReplicatorControl, ReplicaLifecyclePeer, ReplicatorData
├── src/
│   ├── add_replica.rs               # Coarse add intent and tracked-build contracts
│   ├── remove_replica.rs            # Coarse removal intent, progress, results, deadlines
│   ├── replica_lifecycle.rs          # Shared typed lifecycle-peer contracts
│   ├── types.rs                     # Epoch, Role, AccessStatus, ReplicaInfo, Operation, OperationStream
│   ├── error.rs                     # KubericError enum (NotPrimary, NoWriteQuorum, etc.)
│   ├── events.rs                    # LifecycleEvent, StateProviderEvent, ReplicatorControlEvent
│   ├── handles.rs                   # PartitionState (atomics), PartitionHandle, StateReplicatorHandle
│   ├── noop.rs                      # NoopReplicator actor (testing)
│   ├── runtime.rs                   # KubericRuntime (lower-level harness)
│   ├── replica_agent.rs             # Pod-local admission, correlation, replay, status and faults
│   ├── pod.rs                       # PodRuntime ordered service/replicator effects
│   ├── grpc/peer_client.rs           # Strict primary-to-target lifecycle peer client
│   ├── grpc/peer_server.rs           # Target peer protocol endpoint
│   ├── driver.rs                    # Read-only PartitionDriver + ReplicaHandle trait
│   ├── replicator/
│   │   ├── quorum.rs                # QuorumTracker (single + dual-config + must_catch_up + baseline)
│   │   ├── queue.rs                 # ReplicationQueue (in-memory op retention for replay to new replicas)
│   │   ├── actor.rs                 # WalReplicatorActor (control + data event loop + ReplicationQueue)
│   │   ├── primary.rs               # PrimarySender (unbounded per-secondary channels + replay)
│   │   └── secondary.rs             # SecondaryReceiver (replication + copy gRPC server)
│   └── grpc/
│       ├── convert.rs               # Proto ↔ domain type conversions
│       ├── server.rs                # ControlServer (routes through ReplicaAgent)
│       └── handle.rs                # GrpcReplicaHandle (remote ReplicaHandle for operator)

kuberic-operator/
├── src/
│   ├── lib.rs                       # Public API (cluster_api, crd, reconciler)
│   ├── main.rs                      # Binary entry point (kube controller)
│   ├── crd.rs                       # KubericSet CRD with spec/status/enums
│   ├── cluster_api.rs               # ClusterApi trait + KubeClusterApi impl
│   ├── durable/remove_replica.rs     # Coarse intent freeze/redrive, commit, cleanup, dispositions
│   ├── reconciler.rs                # Reconcile loop and Kubernetes-owned cleanup/publication
│   └── tests.rs                     # Mock reconciler tests

examples/kvstore/
├── src/
│   ├── lib.rs                       # Module declarations
│   ├── main.rs                      # Binary entry point
│   ├── state.rs                     # KvOp, KvState, drain_stream
│   ├── server.rs                    # Client-facing KV gRPC server
│   ├── service.rs                   # Lifecycle + StateProvider event loop
│   ├── demo.rs                      # Operator/client simulators
│   ├── tests.rs                     # Operator-driven integration tests (6)
│   └── reconciler_tests.rs          # Reconciler-driven E2E tests (2)
└── proto/kvstore.proto              # Client KV API (Get/Put/Delete)
```

**Tests:** See [Testing](testing.md) for full test strategy, layers, and listing.

---

## Documentation Index

- `docs/background/service-fabric/README.md` — SF architecture study
- `docs/background/cloudnative-pg-architecture.md` — CNPG architecture study
- `docs/features/kuberic-replicator-design.md` — Main design (this doc's parent)
- `docs/features/kuberic/protocols.md` — Replication protocols
- `docs/features/kuberic/operator.md` — Operator design
- `docs/features/kuberic/user-api.md` — User application model
- `docs/features/kuberic/status.md` — Status and roadmap (this doc)
- `docs/features/kvstore-design.md` — KV store example design
- `docs/features/operator-failure-scenarios.md` — Failure scenarios + recovery
