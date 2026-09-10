# Kuberic DEX Roadmap

This document tracks deferred work for the
Kuberic DEX (`kuberic-dex`) durable execution crate. The crate is a replay and
persistence safety kernel, not an end-user orchestration runtime.
It is not currently integrated into the Kuberic operator. Items below are
ordered possibilities, not commitments.

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

1. **Implemented:** ordinary typed activity declarations and calls retaining
   exact canonical input matching, immutable version identity, independent
   input/result bounds, persisted options, and portable deterministic
   codec/call failures.
2. **Implemented:** immutable activity registration and typed invocation,
   including scoped handlers that borrow embedding state. Dynamic runtime
   discovery remains deferred.
3. **Implemented:** narrow strict-effect integration for operations whose
   safety proof requires separately persisted prepared commands.
4. Generalize passive convergence resolution only after a concrete consumer
   demonstrates reusable policy.
5. Add replay-aware tracing and checkpoint inspection.

### Workflow Primitives

Ordinary bounded retry policy is implemented. Add durable timers and a runtime
wakeup mechanism only when a concrete consumer requires them. Strict retry
behavior must never turn an uncertain exposed write-authority operation into
an automatic duplicate dispatch.

External events, parallel scheduling, join/select, child workflows, and
cancellation remain deferred until demonstrated by a specific workflow.

### Runtime and Operations

Generic instance lifecycle and query APIs, workers, queues, leases, routing,
and distributed ownership remain deferred. A future embedding must supply
wakeups and effect ownership without weakening the framework's persistence
boundaries.

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

The remaining steps stay ordered and deferred:

5. Design a new operator integration only if it can meet the framework's
   authoring and ownership goals without increasing API-server cost.
6. Evaluate whether that authoring model is materially simpler than the
   existing explicit state machine.
7. Adopt it only if the integration preserves safety and reduces complexity.

The provider-readiness work does not add an operator dependency, alter
reconciliation or deployment RBAC, or authorize a workflow migration.
Switchover, remove-replica, deployment rollout, and any workflow-ownership
change remain future gates.

## Explicitly Deferred

The roadmap does not currently commit to:

- production migration or mixed-version checkpoint support;
- exactly-once activity execution;
- generic automatic compensation;
- worker queues, leases, or a distributed scheduler;
- a public orchestration platform;
- adoption by existing Kuberic workflows.
