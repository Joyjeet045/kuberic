# Durable Execution Framework Roadmap

This document tracks deferred work for the
`kuberic-durable-execution` crate. The crate is a replay and persistence safety
kernel, not an end-user orchestration runtime. Production remove-replica and
switchover consume it through an in-process operator runner. Items
below are ordered possibilities, not commitments.

The ordering is informed by the broader user and provider surfaces in
[Azure Durable Task Framework](https://github.com/Azure/durabletask) and
[Duroxide](https://github.com/microsoft/duroxide). It does not claim API
compatibility with either project.

## Current Boundary

The implemented kernel provides:

- deterministic linear replay;
- typed, versioned, bounded activity declarations and ordinary async calls
  over the compatible exact-byte API;
- exact activity matching and stable logical identity;
- asynchronous load and compare-and-swap persistence;
- opaque provider revision tokens and conservative unknown outcomes;
- conservative separately persisted schedule/dispatch exposure for low-level
  callers, plus opt-in atomic schedule/exposure fusion;
- bounded activity count plus independent active and terminal encoded
  checkpoint sizes;
- maximum-result capacity reservation before dispatch;
- immutable execution-level terminal payload and admitted capacity;
- completion-only active-to-terminal checkpoint compaction;
- direct terminal outcome reload without workflow polling;
- ambiguity quarantine and authoritative observation recovery;
- opt-in atomic observation/replay/next-exposure or terminal progression;
- a feature-gated ConfigMap checkpoint provider using opaque Kubernetes
  `resourceVersion` compare-and-swap; standalone kernel builds may omit it,
  while the production operator enables it unconditionally for
  framework-native remove-replica;
- independently retained checkpoints by default, with validated optional
  non-controlling owner references and separately authorized orphan cleanup;
- a configurable conservative ConfigMap data budget with documented headroom;
- namespace-scoped writer and cleanup RBAC examples with separate identities;
- feature-gated real-API coverage through the existing all-features workspace
  test command after the one-control-plane KinD CI job is provisioned;
- real-API spike measurements for checkpoint/object size, accepted writes,
  canonical typed watch-event bytes, and unknown-outcome recovery;
- a shared bounded operator runner used by production framework-native
  remove-replica and switchover workflows while preserving the
  `ReplicaAgent` mutation boundary;
- direct kube-controller integration through Send workflow/store futures,
  without another executor or scheduler;
- same-namespace owner-bound checkpoints, owner garbage-collection validation,
  execution-keyed write/outcome and active/terminal size telemetry, and
  separately authorized orphan cleanup.

The bounds prevent unlimited growth and ensure a declared-valid result remains
persistable after dispatch. Active history is never compacted: every completed
activity input and result remains in the checkpoint until the workflow
terminalizes or reaches a configured limit.

## History Lifecycle

### Implemented: Completion-Only Compaction

The as-built lifecycle compacts only completed workflows:

1. Keep complete bounded history while a workflow is active.
2. When replay reaches terminal completion, compare-and-swap the active
   checkpoint to a bounded terminal record.
3. Retain only the immutable execution contract, exact success or failure
   outcome, and completed activity count. The terminal shape has no activity
   history or digest.
4. Report durable completion only after the terminal transition is accepted or
   subsequently observed.
5. Return the terminal result directly on later loads without replaying the
   discarded history.

The execution contract declares one immutable terminal payload bound and
persists the active and terminal encoded-checkpoint capacities under which it
was admitted. Before workflow evaluation can approach its first external
effect, the kernel proves that both terminal variants at the declared maximum
fit that capacity. Any later configured-capacity change is rejected.
Exact-bound outcomes succeed; oversized outcomes violate the predeclared
contract.

### No Generic Mid-Operation Compaction

Mid-operation compaction is not planned for the current Kuberic workflows.
Deterministic replay may depend on any previous activity result, so deleting an
active prefix requires a new durable continuation state and changes the
workflow authoring contract. The framework must not silently discard history
or infer a continuation point from checkpoint pressure.

When an active workflow reaches an activity-count or encoded-size limit, it
fails before scheduling another effect. It does not automatically compact or
start a replacement execution.

### Deferred: Continue-as-New

If a concrete long-running workflow cannot fit within the bounded active
history, evaluate an explicit Continue-as-New operation:

1. The workflow requests continuation with new bounded input.
2. Storage atomically terminates the current execution and creates a fresh
   execution checkpoint.
3. The new execution starts with empty history and a distinct execution
   identity.
4. The terminal record may retain a link between the old and new executions.
5. Recovery never starts a continuation merely because a size limit is near.

Continue-as-New should be added only after a real workflow demonstrates the
need. Short topology workflows should complete and use terminal compaction.

## Deferred Usability Work

### Kernel Ergonomics

1. **Implemented:** typed serde activity declarations and calls retaining exact
   canonical encoded-byte matching, immutable version identity, declared
   bounds, and portable deterministic codec/call failures.
2. Keep domain rejection/failure in each typed bounded activity output; add a
   generic activity-failure lifecycle only if a broader workflow demonstrates
   that need.
3. Add an activity registry only if a non-operator host needs runtime lookup.
4. Generalize passive convergence resolution only after another workflow
   demonstrates reusable policy beyond the in-process operator adapters.
5. Add replay-aware tracing and checkpoint inspection.

### Workflow Primitives

Add durable timers and retry policy only when a concrete Kuberic workflow
requires them. Retry behavior must not turn an uncertain exposed activity into
an automatic duplicate dispatch.

External events, parallel scheduling, join/select, child workflows, and
cancellation remain deferred until demonstrated by a specific workflow.

### Runtime and Operations

Generic instance lifecycle and query APIs, workers, queues, leases, routing,
and distributed ownership are not required for the operator-hosted workflows.
The Kubernetes operator already supplies reconciliation wakeups and effect
ownership.

If the framework later serves applications outside the operator, reassess
those runtime facilities rather than growing the kernel speculatively.

### Kubernetes Integration

The provider began as an isolated feasibility spike and its readiness
prerequisites are implemented. It is now production-required by
framework-native remove-replica; the crate feature remains optional only for
standalone kernel consumers that do not host that workflow:

1. **Implemented:** Kubernetes ConfigMap checkpoint storage using opaque
   `resourceVersion` create/replace compare-and-swap, portable errors, and
   conservative unknown mutation outcomes.
2. **Implemented:** Deterministic and real-API validation with measurements for
   active/terminal checkpoint and returned-object size, confirmed accepted
   writes, canonical typed watch-event JSON bytes, and both reload branches
   after an unknown outcome.
3. **Implemented:** A retention contract that keeps terminal checkpoints,
   defaults to independent retention, accepts only validated non-controlling
   owner references, and assigns orphan deletion to a separately authorized
   lifecycle actor.
4. **Implemented:** A configurable 786,432-byte default ConfigMap data budget,
   standalone least-privilege writer and cleanup RBAC examples, and
   feature-gated real-API coverage through the existing all-features workspace
   test command after the one-control-plane KinD CI job is provisioned.

### Operator Adoption

The shared in-process runner owns load/reload, terminal short-circuit,
bounded-fuel requeue, one-use dispatch permits, fused progression, quarantine,
conflict and unknown-write reload, persistence failures, and nondeterminism.
Operation adapters retain observation collection, authority and exact-command
preparation, effect/quarantine handling, deadlines, terminal validation, and
publication. The Kubernetes reconciler remains the scheduler; no worker,
queue, lease, watcher, distributed owner, or retry scheduler was added.

Switchover is now a production framework-native consumer with no public
selection model or optional build feature. Its representative no-redelivery
path remains nine external effects, three passive observations, 12 boundaries,
and 13 accepted writes. Its byte measurements and lifecycle limits remain
operation-specific.

### Graduated: Framework-Native Switchover

`status.switchoverExecution` owns immutable admission and checkpoint identity.
A resource in the `Switchover` phase without a current native reference fails
closed; removed historical formats are neither migrated nor converted.

The operation reuses the shared runner and ConfigMap provider while retaining
the existing deterministic switchover reducer and individually correlated
ReplicaAgent and exact-UID label commands. A coarse agent-owned switchover
intent was not introduced: unlike add and remove, the sequence spans multiple
replicas and Kubernetes routing objects, and the existing per-command fences
already supply authoritative ambiguity recovery.

The product supports 1–9 replicas. The upper bound is enforced by the CRD and
reconciler because the nine-member maximum-fault rollback consumes all 33
admitted activity records while remaining within the ConfigMap budget.
The status schema has one native execution-reference shape with required
immutable input; no switchover incompatibility variant remains. Missing fields
fail schema admission, while strict Kubernetes field validation rejects
removed or unknown fields before persistence.

The independent limits are 33 activity records, 4,096 workflow-input bytes,
8,192 activity-input bytes, 4,096 result bytes, 770,048 active bytes, 16,384
terminal bytes, 4,096 terminal-payload bytes, 512 error bytes, 64 workflow
transitions, and 32 runner outcomes per reconcile. Maximum fixtures measure
736,181 active bytes and 15,093 terminal bytes. The measured no-fault sample
was 31,785 active bytes, 4,169 terminal bytes, and a 1,041-byte terminal
payload.

### Graduated: Framework-Native Remove Replica

Remove-replica is a production framework-native consumer. It has one
default execution path and no build or resource mode selector. Legacy pilot
and explicit remove records are converted to durable incompatibility markers;
they are never resumed, migrated, cleared as absent, or used to admit a new
execution. The `ReplicaAgent`, correlated control v3,
`RemoveReplicaIntent` v1, and lifecycle-peer v2 protocol are unchanged.

`status.removeReplicaExecution` owns immutable admission, checkpoint identity,
and incompatibility state. Its referenced same-namespace ConfigMap owns compact
boundary history, exact prepared commands, and terminal evidence; the other
explicit operator workflows retain their operation-specific CRD status
checkpoints.

The version-3 compact contract stores immutable admission once and records only
tagged observations, exact prepared commands, compact effect results, or
bounded proven-no-admission evidence at durable boundaries. The version bump
stores the already encoded protobuf action as binary exact bytes rather than
hexadecimal text. A public reducer and mid-operation compaction remain
unnecessary.

The canonical three-member no-fault `ScaleDown` path is exactly three external
effects, two passive observations, five completed durable boundaries, and six
accepted writes. Across the final three samples, the active-record lifecycle
range was 3,373–18,693 bytes; the per-run maxima were 18,685, 18,685, and
18,693 bytes. The terminal record was 4,245 bytes and its payload was 683
bytes. These are run-specific measurements, not compatibility constants. Every
sample remains below the 49,152-byte (48 KiB) acceptance gate.

The immutable lifecycle limits are 16 history records, 4,096 decoded input
bytes, 2,048 decoded result bytes, 262,144 active encoded bytes, 12,288
terminal encoded bytes, and 4,096 terminal-payload bytes. The maximum-fault
projection measured 182,589 active bytes, 11,453 terminal bytes, and a
4,096-byte terminal payload. Exact one-byte-over tests reject every bound
independently.

External effects, passive observations, completed boundaries, accepted writes,
active record, terminal record, and terminal payload are deliberately separate
measurements. A write is accepted only when persistence returns an
authoritative revision.

### Next Operation Extension

Add-replica was not migrated. A future framework-native add adapter can reuse
the runner outcomes and supply its own observation, authority/preparation,
exact effect and quarantine, deadline, terminal-validation, and publication
rules. It must declare an independent compact versioned contract and limits.
No additional service is required by that extension point.

## Explicitly Deferred

The roadmap does not currently commit to:

- mixed-version checkpoint migration;
- exactly-once activity execution;
- generic automatic compensation;
- worker queues, leases, or a distributed scheduler;
- a public orchestration platform;
- additional Kuberic workflow ports, including add-replica.
