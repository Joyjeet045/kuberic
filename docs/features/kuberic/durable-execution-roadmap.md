# Durable Execution Framework Roadmap

This document tracks deferred work for the experimental
`kuberic-durable-execution` crate. The current crate is a replay and persistence
safety kernel, not an end-user orchestration runtime. Items below are ordered
possibilities, not commitments.

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
- bounded activity count and encoded checkpoint size;
- maximum-result capacity reservation before dispatch;
- immutable execution-level terminal payload and admitted capacity;
- completion-only active-to-terminal checkpoint compaction;
- direct terminal outcome reload without workflow polling;
- ambiguity quarantine and authoritative observation recovery;
- opt-in atomic observation/replay/next-exposure or terminal progression;
- an optional, isolated ConfigMap checkpoint-provider spike using opaque
  Kubernetes `resourceVersion` compare-and-swap;
- independently retained checkpoints by default, with validated optional
  non-controlling owner references and separately authorized orphan cleanup;
- a configurable conservative ConfigMap data budget with documented headroom;
- namespace-scoped writer and cleanup RBAC examples with separate identities;
- feature-gated real-API coverage through the existing all-features workspace
  test command after the one-control-plane KinD CI job is provisioned;
- real-API spike measurements for checkpoint/object size, accepted writes,
  canonical typed watch-event bytes, and unknown-outcome recovery.
- feature-gated operator workflow pilots for switchover and remove-replica on
  sets with at most three members, each retaining its explicit implementation
  as the default and preserving the `ReplicaAgent` mutation boundary;
- direct kube-controller integration through Send workflow/store futures,
  without another executor or scheduler;
- same-namespace owner-bound pilot checkpoints, owner garbage-collection
  validation, execution-keyed write/outcome and active/terminal size telemetry,
  and a reproducible explicit-versus-pilot source-complexity command.

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
persists the encoded-checkpoint capacity under which it was admitted. Before
workflow evaluation can approach its first external effect, the kernel proves
that both terminal variants at the declared maximum fit that capacity. A later
smaller configured capacity is rejected. Exact-bound outcomes succeed;
oversized outcomes violate the predeclared contract.

### No Generic Mid-Operation Compaction

Mid-operation compaction is not planned for the initial Kuberic pilot.
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
and distributed ownership are not required for the first operator-hosted
pilot. The Kubernetes operator already supplies reconciliation wakeups and
effect ownership.

If the framework later serves applications outside the operator, reassess
those runtime facilities rather than growing the kernel speculatively.

### Kubernetes Integration

The provider and its readiness prerequisites are implemented as an isolated
spike:

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

The operator integrations remain behind explicit default-off Cargo and runtime
selection gates and do not authorize a workflow-ownership change or broader
migration. Both use compact mutable state, combine deterministic transitions in
memory, and use fused host progression while preserving exact durable commands,
authoritative observations, and conservative quarantine.

A representative successful three-member execution now records exactly nine
external effects and three passive observations: 12 durable boundaries and 13
accepted checkpoint writes, including terminal persistence. Seven
ReplicaAgent preparation-only records were removed by preparing the exact
command inside the same accepted exposure that authorizes dispatch; no effect
or required observation was removed. In the authoritative happy-path
measurement, checkpoint write attempts and accepted writes were both 13, with
no conflict, unknown, or definite-failure outcomes. Six repeated remediation
runs observed maximum active checkpoints from 31,597 to 31,605 bytes and compact
terminal checkpoints from 4,077 to 4,081 bytes. These values are run-specific
snapshots, not exact byte contracts; runtime-generated serialized values can
change their lengths. The stable admission contracts remain the configured
770,048-byte (752 KiB) encoded-checkpoint ceiling and 4,096-byte terminal
payload ceiling. The executable measurement checks authoritative
self-consistency and those limits.

The terminal carries authoritative external-effect and passive-observation
accounting, making the classification recoverable after a process restart.
Successful terminal semantics require exactly three passive observations and
the base nine external effects plus only the reachable per-effect bounded
redeliveries. Only the seven ReplicaAgent effects have such retry slots; the
two UID-fenced label effects do not. The no-redelivery path therefore remains
exactly 9/3, while fault paths may use additional boundaries and writes. Compensation uses the
exact reachable accounting pairs for the terminal operation's restore or
failed-promotion transcript rather than a broad rollback upper bound. Active
size passes both the 64 KiB baseline and 32 KiB stretch gates, and terminal
size passes its baseline gate.

These fields have deliberately different meanings. External effects are the
seven ReplicaAgent mutations and two UID-fenced label patches. Passive
observations are the three durable evidence-only steps. Their sum is the
completed durable-boundary count. A write attempt is any checkpoint CAS call,
whereas an accepted write is confirmed only by an authoritative accepted
revision. Active and terminal sizes are canonical encoded checkpoint sizes for
different lifecycle states and are reported separately.

The lexical report distinguishes a 201-line/29-decision workflow body and the
comparable 930/108 legacy-marker scope. Shared typed, fused, and effect-adapter
infrastructure is 1,208/110; operator integration is 1,047/55; the honestly
charged non-overlapping total is 4,161/326. The explicit implementation remains
measured at 1,449/172. Charging it as well yields a combined 5,610/498. The
measurement script rejects overlapping charged scopes. Shared code may
amortize across workflows; the remove-replica port below tests that claim.

A public compact reducer remains deferred. The achieved write gate comes from
atomic prepared exposure, not history compaction, and no reducer prototype or
API is introduced.

The second kernel-hosted workflow did **not** demonstrate source-cost
amortization. The explicit remove-replica baseline is 1,611 executable lines /
213 decision points. The complete kernel remove workflow is 1,632/162,
remove-specific operator integration is 1,114/81, and shared reusable
infrastructure grew by 374/3 from the frozen 1,208/110 baseline. The resulting
marginal cost is 3,120/246: 1.9367 times the explicit baseline in executable
lines and 1.1549 times in decision points. Shared growth is 30.96% in lines and
2.73% in decisions. Both dimensions therefore classify as negative under the
fixed measurement thresholds.

The isolated async remove workflow body is small at 138/18, but it is not
representative of the cost of safely hosting the workflow. Workflow-specific
state, admission, terminal and replay support plus operator effect, recovery,
routing, and publication integration eliminate that local advantage. This
answers the roadmap's amortization question: it was tested and was not
demonstrated.

Three representative successful no-fault three-member ScaleDown executions
recorded three external effects and two passive observations: five completed
durable boundaries and six accepted checkpoint writes. The original unfused
production reconcile loop recorded 11 accepted writes—one exposure and one
observation write per activity plus terminal persistence. That amplification
was an integration artifact, not an intrinsic workflow requirement. Fused
observation/progression now records one initial exposure plus five fused writes
without changing the three effects, two observations, or five boundaries.

Each run reached an active-checkpoint maximum of approximately 91.6 KiB. The
reported 5,005–93,837-byte interval is the aggregate lifecycle
minimum-to-maximum range
within those executions, not a range of per-run maxima. Repeated local
validation observed terminal checkpoints from 8,117 to 8,129 bytes, and the
terminal payload was 2,188 bytes.
These are run-specific snapshots, not exact byte contracts.

The approximately 91.6 KiB active maximum passes the stable 770,048-byte
encoded-checkpoint admission ceiling, but exceeds both switchover comparison
gates: the 65,536-byte (64 KiB) baseline and 32,768-byte (32 KiB) stretch gate.
This does not violate a formal remove-replica acceptance gate because those
smaller gates were defined for switchover and were not assigned to this
workflow. Active checkpoint size grows approximately with the number of durable
activities multiplied by serialized workflow-state size. The dominant
contributors are complete retained activity history and repeated full-state
projections in activity inputs and results. The stable terminal-payload ceiling
remains 4,096 bytes.

No other workflow, generic worker, queue, lease, scheduler, retry framework, or
compact-envelope migration is authorized by this result.

## Explicitly Deferred

The roadmap does not currently commit to:

- production migration or mixed-version checkpoint support;
- exactly-once activity execution;
- generic automatic compensation;
- worker queues, leases, or a distributed scheduler;
- a public orchestration platform;
- additional Kuberic workflow ports.
