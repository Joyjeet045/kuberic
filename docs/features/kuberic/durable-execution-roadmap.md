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
- named, typed, versioned ordinary activities with independent bounded inputs
  and outputs, direct async calls, and immutable registry validation;
- bounded at-least-once physical attempts under one stable logical identity,
  with persisted retry policy, backoff, deadlines, and timeouts;
- an optional narrow strict-effect facility with separate bounded logical
  requests, exact prepared commands, one-use permits, observation,
  proof-authorized redelivery, and observation-only quarantine;
- exact activity matching and stable logical identity;
- asynchronous load and compare-and-swap persistence;
- opaque provider revision tokens and conservative unknown outcomes;
- conservative separately persisted schedule/dispatch exposure for low-level
  callers, plus opt-in atomic schedule/exposure fusion;
- bounded activity count plus independent active and terminal encoded
  checkpoint sizes;
- maximum-result capacity reservation before dispatch;
- immutable execution-level terminal payload and admitted capacity;
- completion-only active-to-terminal checkpoint compaction with authenticated
  completed, external-effect, and passive-observation counts;
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

### Duroxide Semantics

| Semantic area | Status in Kuberic |
|---|---|
| Direct-style deterministic workflow, named typed activities, exact replay, ordinary `Result` handling, bounded at-least-once retries | Implemented |
| Scheduling, retry wakeups, and deadlines | Adapted to existing Kubernetes watches and reconciliation requeues |
| Durable storage | Adapted to one owner-bound ConfigMap per execution with API-server `resourceVersion` CAS |
| Operations unsafe under ordinary duplicate ambiguity | Adapted through the optional four-operation strict prepared-command/quarantine facility |
| Embedded Duroxide runtime, workers, queues, leases, heartbeat objects, auxiliary watchers/services | Intentionally omitted |
| Parallel orchestration, child workflows, external events, cancellation, and generic Continue-as-New | Intentionally omitted until a concrete Kuberic workflow requires them |

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

1. **Implemented:** ordinary typed activity declarations and calls retaining
   exact canonical input matching, immutable version identity, independent
   input/result bounds, persisted options, and portable deterministic
   codec/call failures.
2. **Implemented:** immutable activity registration and typed invocation for
   operator-hosted handlers that borrow reconciliation state. Dynamic runtime
   discovery remains deferred.
3. **Implemented:** narrow strict-effect integration for operations whose
   safety proof requires separately persisted prepared commands.
4. Generalize passive convergence resolution only after another workflow
   demonstrates reusable policy beyond the in-process operator adapters.
5. Add replay-aware tracing and checkpoint inspection.

### Workflow Primitives

Ordinary bounded retry policy and reconciler wakeups are implemented. Add
durable timers only when a concrete Kuberic workflow requires them. Strict
retry behavior must never turn an uncertain exposed write-authority operation
into an automatic duplicate dispatch.

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
Operation adapters retain observation collection, authority, ordinary typed
handlers, the optional strict exact-command/quarantine path, deadlines,
terminal validation, and publication. The Kubernetes reconciler remains the scheduler; no worker,
queue, lease, watcher, distributed owner, or retry scheduler was added.

Switchover is now a production framework-native consumer with no public
selection model or optional build feature. Its representative no-redelivery
path has 12 logical boundaries and 25 accepted writes: each completed activity
uses exposure/result persistence, followed by terminal compaction. It is also the reference
direct-style authoring slice:
the workflow source names each protocol boundary and owns normal and
compensating control flow, while the adapter owns observations, exact command
preparation/dispatch, quarantine, and terminal validation. Its byte
measurements and lifecycle limits remain operation-specific.

### Graduated: Framework-Native Switchover

`status.switchoverExecution` owns immutable admission and checkpoint identity.
A resource in the `Switchover` phase without a current native reference fails
closed; removed historical formats are neither migrated nor converted.

The operation reuses the shared runner and ConfigMap provider through 20
operation-specific version-1 typed activities in one immutable registry.
Four passive/read-only, four naturally idempotent, and eight identity-fenced
idempotent handlers use ordinary at-least-once semantics. Only four
write-authority handlers retain exact prepared-command strict handling.
The direct async workflow visibly spells out ordered normal, pre-promotion
restore, and post-promotion compensation paths. The scoped registry owns all
16 ordinary async handlers and lends each one the current reconciliation
state. The adapter prepares individually correlated `ReplicaAgent` commands
only for the four strict write-authority operations; ordinary ReplicaAgent and
exact-UID label handlers use normal typed results, retryable failures, and
authoritative observation. A coarse agent-owned switchover intent was not
introduced: unlike add and remove, the sequence spans multiple replicas and
Kubernetes routing objects, and the existing per-command fences already
supply authoritative ambiguity recovery.

The activity requests are operation-local rather than aliases of a shared
replica/label request and contain no host redelivery or prepared-command
fields. Prepared exposed commands use a stricter recovery mode than undispatched
requests: strict replica quarantine accepts only matching terminal ledger
evidence, an exact postcondition, or generation-change non-admission.
Ordinary recovery invokes the same registered handler observation-only; a
missing result consumes a bounded persisted retry before redispatch. Compact
terminals record the completed branch; the kernel authenticates exact
completion and classification totals while the operator validates the legal
branch and topology before publication.

The product supports 1–9 replicas. A one-member set has no distinct switchover
target; direct switchover accepts valid stable topologies with 2–9 members.
The upper bound is enforced by the CRD and reconciler. The nine-member
maximum-fault rollback uses 19 logical activity records; redelivery attempts
remain inside those records. The status schema has one
contract-version-4 execution-reference shape with required immutable input;
there is no compatibility variant or migration. Missing fields fail schema
admission, while strict Kubernetes field validation rejects removed or unknown
fields before persistence.

The independent limits are 19 logical activity records, 4,096 workflow-input bytes,
8,192 maximum activity-input and activity-result bytes, 524,288 active bytes,
16,384 terminal bytes, 4,096 terminal-payload bytes, 512 error bytes, 64
workflow transitions, and 32 runner outcomes per reconcile. Declared-maximum
fixtures remain below the 524,288-byte active and 16,384-byte terminal limits.
Run-specific tests report exact active and terminal measurements. The
nine-member maximum-fault production case completes 19 logical activities
with a 39-write base, ten one-retry ordinary activities, and four
proof-authorized strict redeliveries: `39 + (10 × 2) + (4 × 2) = 67`
accepted writes.

The 64-transition limit is one workflow-wide budget consumed by normal,
compensation, attestation, and redelivery calls. Activity and terminal error
strings are enforced at 512 UTF-8 bytes on both write and reload paths.

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
hexadecimal text. Mid-operation compaction remains unnecessary.

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

### Future Direct-Style Operation Migrations

Direct-style switchover does not migrate or reinterpret the other production
operations. Remove-replica remains on its existing framework-native compact
workflow with one coarse primary-agent intent. Add/build/rejoin, failover, and
creation retain their current operation-specific CRD-status checkpoints and
behavior.

A future direct-style remove- or add-replica migration may reuse the runner
outcomes while supplying its own named activity catalog, observations,
authority/preparation, exact effects, quarantine, deadlines, terminal
validation, publication rules, versioned contract, and independent limits.
That work requires separate design and compatibility review; it is not implied
by the switchover reference slice. No additional service is required by the
extension point.

## Explicitly Deferred

The roadmap does not currently commit to:

- mixed-version checkpoint migration;
- exactly-once activity execution;
- generic automatic compensation;
- worker queues, leases, or a distributed scheduler;
- a public orchestration platform;
- direct-style ports of remove-replica, add/build/rejoin, failover, or
  creation.
