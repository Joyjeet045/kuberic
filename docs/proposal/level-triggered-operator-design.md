# Level-Triggered Kuberic Operator

> **Status:** Proposal
>
> **Scope:** A future Kuberic operator and replica-agent contract. This proposal
> does not change the existing `kuberic.io/v1` operator or CRD.

## Summary

Build the next Kuberic operator around Kubernetes-style level-triggered
reconciliation:

1. Observe the desired resource, Kubernetes objects, and every available
   replica.
2. Reconstruct the strongest safely provable cluster state.
3. Evaluate the complete state again on every reconcile.
4. Issue at most one fenced, idempotent command.
5. Persist only domain authority, compact transition intent, and user-visible
   conditions.

The operator must not persist a phase cursor for every protocol step. A
reconcile may skip directly to the next action when durable observations prove
that earlier actions already completed. Missing or contradictory evidence must
produce `Wait` or `Unsafe`, never an inferred success.

This model follows the broad CloudNativePG pattern: the controller continually
re-evaluates PostgreSQL and Kubernetes state, while PostgreSQL and the instance
manager retain the durable protocol evidence. CloudNativePG still stores small
coordination markers such as current primary, target primary, and failover
timestamps; it does not store a complete replay history for every transition.

DEX remains available for bounded operations that are demonstrably simpler as
workflows, but it is not the default execution model for the permanent
Kubernetes reconciliation loop.

## Motivation

The classic operator already provides restart-safe creation, failover,
switchover, replica addition, replica rebuild, and replica removal. It achieves
this by storing an explicit workflow implementation in `KubericSet.status`:

- source, target, committed, and stable topology snapshots;
- operation and execution identities;
- a large protocol phase enum;
- pending action identity and dispatch evidence;
- retry counters, deadlines, and errors;
- frozen add/remove intents;
- failover observations and election progress;
- compensation and cleanup progress.

The reconciler then reconstructs handles and observations, evaluates the
persisted phase, patches the next checkpoint, dispatches one action, and
requeues. This is safe, but the CRD has become both the public resource model
and the operator's private workflow journal.

Moving the same phase machine unchanged into a durable workflow engine would
relocate the complexity rather than remove it. The larger simplification is to
make replica operations declarative, fenced, idempotent, and durably
observable. The operator can then derive progress from postconditions instead
of recording every step it attempted.

## Goals

1. Keep the CRD focused on desired state, stable domain authority, compact
   transition intent, and user-visible status.
2. Make each reconcile independent of process-local operator state.
3. Recover after an operator or replica-agent restart by observing durable
   state rather than replaying an operator phase cursor.
4. Permit the evaluator to skip completed steps when their postconditions are
   already proven.
5. Preserve Kuberic's epoch, quorum, incarnation, and write-authority safety
   invariants.
6. Make every external command safe under retries, lost responses, and
   concurrent reconciliation.
7. Separate pure state classification and planning from Kubernetes and gRPC
   effects.
8. Allow DEX only where an operation remains intrinsically procedural after
   the replica contract is improved.

## Non-Goals

- Changing the existing `kuberic.io/v1` CRD in place.
- Reusing the classic `DurableOperationStatus` in a new API.
- Deriving safety from Pod readiness or labels alone.
- Treating unavailable replicas as evidence that an action did not execute.
- Providing exactly-once command delivery.
- Moving quorum, election, fencing, or epoch rules into a generic workflow
  framework.
- Running one never-ending durable workflow per `KubericSet`.
- Removing all persisted coordination state.

## Reference Model: CloudNativePG

CloudNativePG's cluster controller is level-triggered:

- it lists managed Kubernetes resources;
- updates observed resource status;
- queries instance-manager status from the Pods;
- orders failover candidates using PostgreSQL state such as received and
  replayed WAL positions;
- checks readiness, fencing, WAL receivers, and optional failover quorum;
- updates a compact target-primary marker;
- returns and re-evaluates the cluster on the next reconcile.

The instance manager participates in the protocol. A target instance observes
that it is the requested primary, performs the local PostgreSQL transition,
and updates the current-primary status. Old primaries detect that the
transition completed and demote or rewind themselves.

This works because the durable protocol state is not owned solely by the
operator:

- PostgreSQL data directories retain WAL, timelines, control state, and role
  evidence.
- The instance manager owns local promotion and demotion.
- The Cluster status retains current and target primary markers and timing
  information.
- A separate failover-quorum object can retain synchronous replication
  authority.

The relevant implementation is under:

- `internal/controller/cluster_controller.go`
- `internal/controller/replicas.go`
- `internal/controller/replicas_quorum.go`
- `internal/management/controller/instance_startup.go`

Kuberic should adopt the reconciliation shape, not copy PostgreSQL-specific
election rules.

## Design Principles

### Observations, Not Attempt History

The planner consumes current durable facts. It does not ask which line of the
previous reconcile ran.

For example, if the desired target configuration is already durably installed
and attested, reconciliation proceeds to publication even if the operator
never recorded that it dispatched the installation command.

### Postconditions Define Progress

Every command has a durable postcondition. The next reconcile first checks the
postcondition and dispatches only when it is absent and the preconditions still
hold.

Examples:

- role is Primary at the expected epoch;
- exact configuration ID is Current;
- replica incarnation is retired;
- build for an exact source and target configuration completed;
- routing points only to the attested primary.

### One Safe Action Per Reconcile

The planner returns at most one mutation:

```text
Stable
Execute(command)
Wait(reason, wakeup)
Unsafe(reason)
```

Read-only observations may be collected together. A mutation is followed by a
new observation cycle rather than by assuming its result.

### Durable Intent Is Smaller Than a Workflow Journal

Some transitions require a persisted intent marker to prevent a later
reconcile from choosing an incompatible target. This marker identifies what is
being attempted, but not the current implementation step.

Examples include:

- source configuration ID;
- operation ID;
- operation kind;
- target replica or target configuration;
- start time or failover-delay timestamp.

### Fail Closed

The planner may advance only from evidence that satisfies the protocol's
quorum and fencing rules. Missing, stale, contradictory, or unsupported
evidence results in `Wait` or `Unsafe`.

## Sources of Authority

| State | Authority |
|---|---|
| Desired replica count, image, policy | `KubericSet.spec` |
| Kubernetes object existence and identity | Kubernetes API |
| Replica incarnation | Pod UID plus durable agent generation |
| Replica-local role, epoch, and configuration | Persisted replica-agent state |
| Committed topology | Quorum-attested configuration, optionally cached in status |
| Current transition target | Compact `status.transition` intent |
| User-visible health and progress | `status.conditions` and status projection |
| Command completion | Durable agent action ledger and observable postcondition |
| Workflow retry history, when DEX is used | DEX checkpoint |

Status may cache the last accepted stable topology for availability,
diagnostics, and conservative recovery. That cache is domain state, not a
record of individual operator actions.

## Proposed CRD Shape

The exact schema is deferred, but the API should resemble:

```yaml
apiVersion: dex.kuberic.io/v1alpha1
kind: KubericSet
metadata:
  name: example
spec:
  replicas: 3
  image: example:v1
  minReplicas: 2
  failoverDelay: 10
status:
  observedGeneration: 4
  topology:
    configurationId: "cfg-..."
    epoch:
      dataLossNumber: 1
      configurationNumber: 8
    primaryId: 2
    members:
      - id: 1
        instanceId: "..."
      - id: 2
        instanceId: "..."
      - id: 3
        instanceId: "..."
    writeQuorum: 2
  transition:
    id: "..."
    kind: Failover
    sourceConfigurationId: "cfg-..."
    targetReplicaId: 2
    startedAt: "..."
  conditions:
    - type: Ready
      status: "True"
      reason: Stable
```

The following do not belong in the public status:

- per-RPC workflow phases;
- retry counters for ordinary transient failures;
- a pending activity payload;
- dispatch-attempt IDs;
- loop indexes over members;
- serialized intermediate observations;
- generic workflow history.

`status.transition` is optional. Steady state has no transition. Scaling may
derive its intent entirely from `spec.replicas`; failover and switchover
usually require an explicit target marker.

## Replica-Agent Contract

The level-triggered design depends on strengthening the replica agent.

### Durable Local State

The agent must persist enough state across container and process restarts to
report:

- replica ID and incarnation;
- agent generation;
- role and access state;
- epoch;
- exact committed configuration ID and descriptor;
- current and catch-up configuration when applicable;
- progress and election metadata;
- active action identity;
- bounded terminal action results;
- build or retirement state.

The current classic design treats loss of process-local role, epoch, or action
correlation under the same Pod UID as a stale replica requiring removal and
rebuild. Operator2 should not depend on volatile correlation state for normal
recovery.

### Declarative Fenced Commands

Commands should describe a desired postcondition and include exact authority:

```text
EnsureConfiguration {
    operation_id,
    target_replica_id,
    expected_instance_id,
    expected_agent_generation,
    expected_source_configuration_id,
    expected_source_epoch,
    target_configuration
}
```

The agent must:

1. reject stale incarnation, generation, epoch, or configuration authority;
2. return the retained result for an already completed operation ID;
3. continue or report an in-progress matching operation;
4. reject reuse of an operation ID with different input;
5. persist terminal evidence before acknowledging completion;
6. expose the resulting postcondition through normal status observation.

### Coarse Protocol Ownership

Where possible, one replica-side coordinator should own multi-step runtime
protocols:

- add or rebuild a replica;
- remove and retire a replica;
- install CatchUp and Current configurations;
- wait for catch-up quorum;
- persist local commit evidence.

The operator selects and fences the operation, while the primary agent
performs the tightly coupled replica-to-replica sequence. This avoids exposing
every internal protocol step as an operator phase.

## Observation Snapshot

Each reconcile builds an immutable normalized snapshot containing:

- resource UID and generation;
- desired spec;
- Pods, PVCs, Services, labels, readiness, and UIDs;
- status from every reachable replica agent;
- last accepted topology and transition intent;
- quorum metadata;
- current time;
- explicit observation failures.

Unreachable and absent are different states. A missing observation must never
be represented as a default replica report.

The normalization layer validates:

- duplicate logical replica IDs;
- duplicate incarnations;
- unsupported agent protocol versions;
- malformed configurations;
- epoch regressions;
- conflicting primary claims;
- configuration IDs that do not match their descriptors.

## Pure Evaluation

The core planner should be a pure function:

```text
evaluate(desired, accepted_status, observations, now) -> Plan
```

It performs:

1. Validate the accepted topology and transition.
2. Establish the strongest configuration supported by durable evidence.
3. Detect conflicting or stale authority.
4. Determine whether an existing transition completed, remains in progress,
   needs another action, or became unsafe.
5. If stable, compare the accepted topology with desired state and health.
6. Return one action, wait, stable status, or unsafe result.

Pure evaluation enables exhaustive table tests, model-based testing, and fault
injection without Kubernetes or gRPC.

## Reconciliation Flow

```text
Watch event or requeue
        |
        v
Load KubericSet and Kubernetes resources
        |
        v
Query available replica agents
        |
        v
Normalize and validate observations
        |
        v
Evaluate desired + accepted + observed state
        |
        +--> Stable: patch status projection and await change
        |
        +--> Wait: patch condition and requeue/watch
        |
        +--> Unsafe: fence routing, patch condition, await intervention/evidence
        |
        `--> Execute:
                 persist compact intent when required
                 issue one fenced idempotent command
                 requeue without assuming success
```

Kubernetes writes should use optimistic concurrency. A resource-version
conflict restarts observation and evaluation.

## Operation Sketches

### Bootstrap

1. Ensure the required Kubernetes objects exist.
2. Select the deterministic initial primary.
3. Ask that replica to establish the first configuration.
4. Observe the durable current configuration.
5. Add ready replicas through coarse add operations.
6. Publish routing only after the primary and accepted configuration are
   attested.

The operator may resume at any step by observing which configurations are
already committed.

### Add or Rebuild Replica

1. Ensure the target Pod and PVC exist.
2. Observe exact source and target identities.
3. Derive a deterministic operation ID from the source configuration and
   target incarnation.
4. Ask the primary agent to ensure the target is added.
5. Wait until the primary and target report the target configuration and build
   completion.
6. Accept the new topology and publish the serving label.

If the target incarnation changes, derive a new operation only after proving
that the old target is not committed or after safely removing it.

### Remove Replica

1. Select a non-primary target from the accepted topology.
2. Verify that retained members can satisfy the previous quorum and
   `minReplicas`.
3. Persist compact removal intent.
4. Ask the primary agent to ensure the reduced configuration and retire the
   target.
5. Accept the reduced topology only from durable commit evidence.
6. Delete or retain the target Pod/PVC according to policy.

An unavailable primary after an ambiguous Current-configuration installation
must not be interpreted as rollback. The replica-agent contract must provide
quorum-visible configuration evidence or a durable retained result.

### Switchover

1. Persist the requested target and source configuration.
2. Verify target health, progress, and incarnation.
3. Ask the current primary to coordinate a fenced switchover.
4. Observe the new epoch, configuration, and primary claim.
5. Require conflicting old-primary claims to be fenced or resolved.
6. Accept and publish the new stable topology.

`currentPrimary != targetPrimary` is sufficient as the compact transition
marker; individual demotion, promotion, and configuration phases need not be
stored in the CRD when their postconditions are durable.

### Failover

Failover remains the hardest protocol and should be migrated last.

1. Persist the failure start time when a delay is configured.
2. Fence routing to the old primary.
3. Collect election evidence from reachable replicas.
4. Require the configured read/quorum safety rule.
5. Persist the selected target and source election authority.
6. Ask the target to ensure the new epoch and primary configuration.
7. Handle data-loss epoch advancement only through a separately fenced,
   durable protocol.
8. Accept the topology after quorum and primary attestation.

The evaluator may change an uncommitted candidate when observations prove the
old candidate is unsuitable. It must not change the target after promotion or
configuration commit without beginning a new explicitly fenced transition.

## Ambiguous Effects

At-least-once command delivery is expected. Safety comes from identity,
fencing, durable results, and postcondition observation.

After a timeout or lost response:

1. Re-observe the target and relevant quorum.
2. If the postcondition is proven, advance.
3. If the same operation is active, wait.
4. If the retained terminal result proves failure, choose the permitted
   recovery action.
5. If the evidence is missing or contradictory, return `Unsafe`.

The operator must not issue a different operation merely because the previous
RPC result was lost.

## Role of DEX

DEX is optional in this design.

Use direct reconciliation when:

- the next action is derivable from current durable observations;
- commands are idempotent and fenced;
- the data plane exposes authoritative postconditions;
- waits naturally map to watches or requeues.

Consider an operation-scoped DEX workflow when:

- a bounded procedure remains clearer in direct async form;
- activity results are required later but are not naturally part of replica
  state;
- the operation has explicit compensation that cannot be reduced to
  convergence on a desired postcondition;
- strict-effect quarantine materially improves ambiguous-effect handling.

Do not use DEX for:

- the permanent reconcile loop;
- one execution per ordinary watch event;
- caching live observations;
- replacing quorum or election logic;
- storing stable topology that other controllers and users need.

If DEX is used, the CRD should expose only the operation kind, execution ID,
coarse phase, and conditions. DEX checkpoints remain private execution state.

## Comparison

| Property | Classic operator | Permanent DEX workflow | Proposed design |
|---|---|---|---|
| Reconcile style | Phase-driven | Replay-driven | Level-triggered |
| Workflow cursor | CRD status | DEX checkpoint | Usually none |
| Stable topology | CRD status | Must remain external | CRD cache or quorum-attested replicas |
| Retry history | CRD operation | DEX checkpoint | Agent ledger plus ordinary requeue |
| Primary protocol owner | Operator and agent | Workflow and agent | Replica agent |
| Recovery input | Status checkpoint plus replicas | Workflow history plus replicas | Durable replica/configuration evidence |
| Long-lived history | Large CRD status | Bounded active history | No controller history |
| Handling spec changes | Explicit phase rules | Requires execution lifecycle policy | Re-evaluate desired state each pass |

## Testing Strategy

### Pure Planner Tests

Table-test every meaningful combination of:

- desired replica count;
- accepted configuration;
- reachable and unreachable members;
- replica incarnations and agent generations;
- roles and epochs;
- current and catch-up configurations;
- active and terminal actions;
- transition intent;
- time and failover delay.

Each case asserts one exact plan and the safety reason supporting it.

### Crash-Boundary Tests

For every mutating command, inject a crash:

- before intent persistence;
- after intent persistence but before dispatch;
- after dispatch but before agent persistence;
- after agent persistence but before reply;
- after reply but before status update;
- during operator restart;
- during replica-agent restart under the same Pod UID.

The next reconcile must either converge to the correct postcondition or stop
without issuing an incompatible command.

### Model and Property Tests

Verify invariants across generated state transitions:

- at most one write-authoritative primary per accepted epoch;
- no configuration accepted without required quorum evidence;
- epochs never regress;
- an incarnation cannot inherit another incarnation's action result;
- an operation ID cannot authorize different input;
- committed removal never silently rolls back;
- routing never publishes an unattested primary.

### Kubernetes Integration Tests

KinD tests should cover:

- operator restart during every transition;
- replica-agent restart with unchanged Pod UID;
- Pod deletion and recreation with a new UID;
- stale status patch conflicts;
- delayed and duplicated watch events;
- network partitions and partial replica visibility;
- ambiguous gRPC responses;
- failover with and without quorum.

## Risks

### Hidden Workflow State Moves Into the Agent

Moving state out of the CRD is not simplification if the replica agent merely
reimplements the same large phase machine without a clearer contract. Agent
protocols should be coarse, local to the data plane, and represented by
durable postconditions.

### Observation Is Not Authority

The most responsive replica is not necessarily authoritative. Configuration
and election decisions require quorum and fencing evidence, not majority
voting over arbitrary status fields.

### Status Can Become Too Minimal

Removing the last stable topology from status may make recovery and diagnosis
depend on replica availability. Keep the compact topology cache until
quorum-based reconstruction has proven reliable operationally.

### Repeated Commands

Level-triggered reconciliation deliberately repeats commands. Any command that
cannot safely be repeated must be redesigned or handled by a narrow strict
effect with durable ambiguity resolution.

### Mixed Versions

During upgrades, replicas may expose different protocol versions. The planner
must detect unsupported evidence and block unsafe transitions rather than
silently downgrade guarantees.

## Open Questions

1. Is the accepted topology authoritative in CRD status, or a cache of a
   quorum-attested replica configuration?
2. Where should the replica agent persist its action ledger and configuration
   evidence?
3. Which configuration changes require evidence from a quorum versus only the
   primary?
4. Should transition intent be a status field, a separate namespaced resource,
   or both?
5. How are manual switchover requests represented without mixing desired
   commands into observed status?
6. What bounded retention is required for terminal action results?
7. Which operation, if any, remains materially simpler with DEX after the
   declarative command redesign?
8. How should operator and replica protocol versions be negotiated during
   rolling upgrades?

## Decision Gate

Adopt this design only after a prototype demonstrates:

1. restart-safe bootstrap or switchover without a per-step CRD checkpoint;
2. durable replica-agent evidence across same-Pod process restart;
3. safe recovery from a lost successful command response;
4. a substantially smaller CRD and reconciliation implementation;
5. equivalent or stronger invariant and fault-injection coverage than the
   classic operator.

Until those gates pass, the classic operator remains the behavioral reference
for replication safety.
