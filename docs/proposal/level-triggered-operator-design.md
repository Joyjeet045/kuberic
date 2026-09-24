# Level-Triggered Kuberic Operator

> **Status:** Implemented MVP; retained as the historical design proposal
>
> **Scope:** The independent operator and replica-agent contract. This design
> does not change the existing `kuberic.io/v1` operator or CRD.
>
> The as-built operational contract, deployment commands, supported behavior,
> and limitations are documented in the
> [level-triggered operator guide](../features/kuberic/level-triggered-operator.md).
> This document preserves the design rationale and does not define a migration
> from classic v1.

## Summary

Build the next Kuberic operator around Kubernetes-style level-triggered
reconciliation:

1. Observe the desired resource, Kubernetes objects, and every available
   replica.
2. Reconstruct the strongest safely provable cluster state.
3. Evaluate the complete state again on every reconcile.
4. Issue at most one authority-changing, fenced command.
5. Persist only domain authority, compact transition intent, and user-visible
   conditions.

The operator must not persist a phase cursor for every protocol step. A
reconcile may skip directly to the next action when durable observations prove
that earlier actions already completed. Missing or contradictory evidence must
produce `Wait` or `Unsafe`, never an inferred success.

The Kubernetes control loop follows the broad CloudNativePG pattern: the
controller continually re-evaluates desired and observed state while compact
coordination markers remain in status. Quorum, epoch, role, catch-up, and
reconfiguration semantics follow Service Fabric's Failover Manager,
Reconfiguration Agent, and replicator model.

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
7. Separate pure state classification and decision-making from Kubernetes and
   gRPC effects.
8. Reject operations whose effects cannot be reconstructed, resumed, or safely
   repeated by the replica protocol.
9. Prevent superseded primaries and delayed commands from committing write
   authority across configuration or epoch boundaries.
10. Guarantee bounded re-observation when progress depends on external replica
    state.
11. Match Service Fabric's Previous Configuration, Current Configuration,
    dual-quorum, epoch, LSN, catch-up, and quorum-loss semantics while
    explicitly refusing unsupported data-loss recovery.

## Non-Goals

- Changing the existing `kuberic.io/v1` CRD in place.
- Reusing the classic `DurableOperationStatus` in a new API.
- Deriving safety from Pod readiness or labels alone.
- Treating unavailable replicas as evidence that an action did not execute.
- Providing exactly-once command delivery.
- Moving quorum, election, fencing, or epoch rules into a generic workflow
  framework.
- Defining a new consensus or reconfiguration protocol where Service Fabric
  already provides the required semantics.
- Defining a Kubernetes Lease-based primary fencing mechanism.
- Using an external workflow engine for operator operations.
- Running one never-ending durable workflow per `KubericSet`.
- Removing all persisted coordination state.

## Protocol Reference: Service Fabric

Service Fabric is the normative reference for replicated-state transitions:

- the Failover Manager selects the desired replica set, target primary, roles,
  and new epoch;
- each node's Reconfiguration Agent durably owns local reconfiguration state;
- the primary replicator owns copy, replication, LSN progress, catch-up, and
  Previous Configuration/Current Configuration quorum enforcement;
- the stateful service receives role, epoch, access-status, and data-loss
  callbacks.

Kuberic maps these responsibilities as follows:

| Service Fabric | Kuberic |
|---|---|
| Failover Manager | Kubernetes operator |
| Reconfiguration Agent | Replica agent |
| Fabric Replicator | Kuberic runtime replicator |
| Failover unit description | Accepted topology plus compact transition intent |
| Replica ID and instance ID | Replica ID, Pod UID, and durable agent generation |
| Previous/Current Configuration | Previous/Current Kuberic configuration |
| Configuration/data-loss epoch | Kuberic configuration/data-loss epoch |
| ReadStatus/WriteStatus | Replica runtime access state |

The replica agent may use an internal reconfiguration state machine matching
Service Fabric:

```text
Phase 0: Demote old primary        (initial switchover side)
Phase 1: Gather LSN/progress       (failover and continued swap as required)
Phase 2: Catch up
Phase 3: Deactivate previous configuration
Phase 4: Activate current configuration
```

These are replica-local protocol stages, not CRD workflow phases. The
Kubernetes operator observes their durable postconditions and does not persist
or drive each stage.

The relevant Service Fabric implementation is under:

- `Reliability/Failover/common/FailoverUnitDescription.h`
- `Reliability/Failover/common/ReplicaDescription.h`
- `Reliability/Failover/ra/FailoverUnitReconfigurationStage.h`
- `Reliability/Failover/ra/ReconfigurationState.h`
- `Reliability/Replication/ReplicaManager.cpp`
- `Reliability/Replication/PrimaryReplicator.cpp`

## Controller Reference: CloudNativePG

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

Kuberic adopts CloudNativePG's reconciliation shape, while Service Fabric
defines the replication and reconfiguration rules.

## Design Principles

### Observations, Not Attempt History

The evaluator consumes current durable facts. It does not ask which line of the
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

### One Authority-Changing Action Per Reconcile

The evaluator returns at most one authority-changing protocol command:

```text
Stable(status)
Apply(kubernetes_changes)
Execute(protocol_command)
Wait(reason, wakeup)
Unsafe(reason, safety_changes)
```

`Apply` may contain a batch of independent, idempotent Kubernetes changes,
such as applying Services, Pods, PVCs, or status conditions. It must not
change replica authority, publish an unattested topology, or replace/delete
objects required by frozen PC or CC identities.

`Execute` contains exactly one command that can change role, epoch,
configuration, membership, retirement, or write authority. Required intent,
routing fences, and Kubernetes prerequisites must already be durably observed
before `Execute` is returned. The command is followed by a new observation
cycle rather than by assuming its result.

`Unsafe` may include only idempotent fail-closed changes, such as removing
write routing and publishing an unsafe condition. It never authorizes new
replica authority.

This boundary avoids unnecessary one-object-per-reconcile behavior for ordinary
Kubernetes convergence while preserving a fresh observation point between
authority-changing protocol actions.

### Durable Intent Is Smaller Than a Workflow Journal

Some transitions require a persisted intent marker to prevent a later
reconcile from choosing an incompatible target. This marker identifies what is
being attempted, but not the current implementation step.

Examples include:

- Previous Configuration and Current Configuration IDs;
- previous and current epochs;
- operation ID;
- operation kind;
- target replica and exact incarnation;
- exact Pod/PVC identity for a fresh agent store;
- a persisted failure-observation timestamp when failover delay applies.

### Fail Closed

The evaluator may advance only from evidence that satisfies the protocol's
quorum and fencing rules. Missing, stale, contradictory, or unsupported
evidence results in `Wait` or `Unsafe`.

## Sources of Authority

| State | Authority |
|---|---|
| Desired replica count, image, policy | `KubericSet.spec` |
| Kubernetes object existence and identity | Kubernetes API |
| Replica incarnation | Pod UID plus durable agent generation |
| Replica process session | Ephemeral agent session ID |
| Replica-local role, epoch, and configuration | Persisted replica-agent state |
| Committed topology | Quorum-attested configuration accepted into status |
| Current out-of-authority provisioning target | Compact `status.provisioning` intent |
| Current transition target | Compact `status.transition` intent |
| Effective transition safety inputs | Frozen `status.transition.effectivePolicy` |
| Reconfiguration authority | Previous/Current Configuration, epochs, roles, and exact replica incarnations selected by the operator |
| Write authority | Primary role and granted WriteStatus for the exact current epoch and configuration |
| User-visible health and progress | `status.conditions` and status projection |
| Command completion | Replica-agent reconfiguration state and observable runtime postcondition |

`status.topology` is the last operator-accepted topology projection. It is not
accepted from an RPC result alone: it must be supported by the required durable
replica and quorum evidence. Matching evidence confirms it, newer valid
evidence may advance it when no transition is active, missing evidence produces
`Wait`, and contradictory authority produces `Unsafe`. It remains domain state
rather than a record of individual operator actions.

While a non-bootstrap `status.transition` exists, `status.topology` is the
single durable Previous Configuration. It is frozen until reconfiguration
completes. The transition references it by configuration ID and stores the
proposed Current Configuration; it does not duplicate the full Previous
Configuration or its epoch.

## Proposed CRD Shape

The exact schema is deferred, but the API should resemble:

```yaml
apiVersion: operator.kuberic.io/v1alpha1
kind: KubericSet
metadata:
  name: example
spec:
  replicas: 3
  image: example:v1
  failoverDelaySeconds: 10
status:
  initialized: true
  observedGeneration: 4
  topology:
    configurationId: "cfg-..."
    epoch:
      dataLossNumber: 1
      configurationNumber: 8
    members:
      - replicaId: 1
        instanceId: "..."
        agentGeneration: "..."
        role: activeSecondary
      - replicaId: 2
        instanceId: "..."
        agentGeneration: "..."
        role: primary
      - replicaId: 3
        instanceId: "..."
        agentGeneration: "..."
        role: activeSecondary
    writeQuorum: 2
  provisioning:
    replaces:
      replicaId: 3
      instanceId: "old-pod-uid"
      agentGeneration: "old-generation"
    podUid: "new-pod-uid"
    pvcUid: "..."
    operationId: "..."
  transition:
    transitionId: "..."
    kind: failover
    specGeneration: 4
    effectivePolicy:
      replicaSetSize: 3
      writeQuorum: 2
      readQuorum: 2
      failoverDelaySeconds: 10
    previousConfigurationId: "cfg-..."
    currentConfiguration:
      configurationId: "cfg-next-..."
      epoch:
        dataLossNumber: 1
        configurationNumber: 9
      members:
        - replicaId: 1
          instanceId: "..."
          agentGeneration: "..."
          role: activeSecondary
        - replicaId: 2
          instanceId: "..."
          agentGeneration: "..."
          role: primary
        - replicaId: 3
          instanceId: "..."
          agentGeneration: "..."
          role: activeSecondary
  conditions:
    - type: Ready
      status: "true"
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
- a duplicated `primaryId` beside the uniquely validated `Primary` member;
- an `identity` wrapper around configuration-member identity fields.

`status.transition` is optional. Steady state has no transition. Before
bootstrap, Kubernetes scaffolding derives from `spec.replicas`; afterward it
uses the frozen accepted replica-set size. Every replacement or primary
authority change requires explicit Previous/Current Configuration intent and a
new current epoch.

`status.provisioning` is also optional and identifies at most one exact fresh
Pod/PVC target that remains outside replication authority. It is persisted
before `InitializeAgentStore` or build dispatch. It does not count toward
quorum, does not modify topology, and may be abandoned only before that
incarnation enters an outstanding CC and after any old work is proven unable to
complete. Bootstrap uses its persisted Bootstrap transition directly instead
of a separate provisioning marker. Beginning the replacement PC/CC transition
atomically clears the matching provisioning intent.

Configuration members expose `replicaId`, `instanceId`, `agentGeneration`, and
`role` directly. The public CRD derives the primary from the single member with
role `primary`; it does not persist a duplicate `primaryId`. Canonical runtime
types may retain that derived value internally and the protobuf compatibility
field remains populated. Deserialization accepts the former nested-member and
explicit-primary JSON shapes so existing durable agent metadata remains
readable, while all new status serialization uses the compact shape.

Transition ownership is asymmetric:

- the operator uses the frozen `status.topology` as Previous Configuration and
  writes the complete proposed Current Configuration, target roles, exact
  replica incarnations, and new epoch in `status.transition`;
- the replica-side coordinator performs the transition;
- replica and quorum observations establish the resulting current state;
- the operator accepts that evidence into `status.topology`;
- routing is published only from the accepted topology.

The following invariants apply:

1. For non-bootstrap transitions, `transition.previousConfigurationId` must
   equal `topology.configurationId`. Bootstrap is the only transition with an
   empty Previous Configuration.
2. The Previous Configuration epoch and members are read only from
   `status.topology`.
3. Every replica command expands the reference and carries the complete
   Previous and Current Configuration.
4. The operator must not modify `status.topology` while the transition is
   active.
5. After the Current Configuration and its postconditions are proven, one
   optimistic-concurrency status patch replaces `status.topology` with the
   Current Configuration and clears `status.transition`.
6. If a non-bootstrap Previous Configuration is missing or contradictory,
   reconciliation returns `Unsafe`. It may be reconstructed only from
   authoritative replica quorum evidence, never from the current spec.
7. `transition.currentConfiguration` is an outstanding membership obligation.
   A newer epoch may change its primary and roles, but must preserve its member
   set until the interrupted reconfiguration is resolved.
8. During an interrupted reconfiguration, election and quorum-loss evaluation
   use the applicable read-quorum rules for both PC and the outstanding CC.
9. If replicas prove that CC became effective but the topology patch was not
   published, reconciliation accepts that CC as topology and clears the
   transition rather than discarding it.
10. A subsequent membership change may begin only after the outstanding CC is
    resolved into an accepted stable topology. Data-loss supersession is
    outside the minimum contract.
11. `transition.specGeneration` records provenance only.
    `transition.effectivePolicy` freezes every safety-affecting input required
    to authorize or complete the reconfiguration, including replica-set size,
    derived read/write quorums, and failover delay.
12. A newer resource generation does not mutate active PC, CC, effective
    policy, or Kubernetes objects required by either configuration. It is
    evaluated after the transition becomes stable.
13. `status.initialized` is monotonic. Once true, missing topology or transition
    state must not be interpreted as permission to create a new genesis
    configuration.
14. After initialization, PC, CC, and accepted topology always contain the
    frozen `effectivePolicy.replicaSetSize`.
15. A changed `spec.replicas` value does not authorize Kubernetes deletion,
    creation, or replication membership changes.
16. `status.provisioning` never grants role, membership, quorum, or write
    authority. Its old exact identity, new Pod UID, new PVC UID, and operation
    ID are persisted. The resource UID, logical replica ID, initialization ID,
    and durable generation are derived and must match the
    `InitializeAgentStore` command.
17. At most one provisioning intent exists, and no unrelated membership
    transition begins while it is active.
18. `status.observedGeneration` advances only after the accepted exact
    incarnations report the requested image and the requested fixed policy
    matches the frozen effective policy. Unsupported image, replica-count, or
    failover-delay drift remains visible without mutating authority.

### Initialization Authority

Bootstrap has an explicit never-initialized authority case:

- `status.initialized == false`;
- no accepted topology exists;
- no bootstrap transition exists;
- no owned PVC or reachable replica reports durable Kuberic configuration,
  epoch, or incarnation evidence for this resource UID.

Only that complete condition permits creation of genesis authority. Before any
replica runtime action, the operator creates all intended Pods/PVCs so their
exact incarnations are known, then persists a Bootstrap transition with an
empty PC, deterministic full-size genesis CC, initialization ID, and frozen
effective policy.

The initial runtime Primary remains write-closed while it builds the other
replicas as Idle Secondaries outside the installed configuration. Once every
genesis member satisfies the build boundary, the agent installs the full
genesis CC. The operator then atomically accepts that CC as `status.topology`,
sets `initialized = true`, and clears the Bootstrap transition. WriteStatus and
write routing remain disabled until the full configuration and derived write
quorum are proven.

If status is missing or says never initialized while durable replica or storage
evidence exists, reconciliation returns `Unsafe`; it must not create a fresh
genesis configuration.

### Fixed Replica Count and Quorum Policy

The minimum contract supports one replica-set size:

```text
N = frozen spec.replicas
W = floor(N / 2) + 1
R = N - W + 1
```

`N`, `W`, and `R` are frozen in `transition.effectivePolicy`. After bootstrap
begins, a different `spec.replicas` value is unsupported. The operator keeps
the accepted topology unchanged and publishes an unsupported-spec condition.
An already-authorized transition continues under its frozen policy, and
same-cardinality repair needed to maintain accepted authority remains allowed.
The operator rejects only transitions that would implement the requested size
change.

Every accepted topology contains exactly `N` logical members. Ordinary
failover changes roles and epochs but not membership. Repair replaces one
incarnation with another in a same-cardinality PC/CC transition. Public
scale-up, scale-down, and user-configurable quorum policies are outside the
minimum contract.

Pod count is desired Kubernetes scaffolding, not replication membership.
Creating or replacing a Pod does not admit it to CC; build, catch-up, and PC/CC
reconfiguration remain mandatory.

The operator-issued reconfiguration description is accepted when its epoch is
newer than the replica's durable epoch, or when it is an identical retry of the
already accepted epoch and configuration. Replicas reject stale epochs, stale
instance IDs, and conflicting role assignments or configuration contents at
the same epoch.

The operator must not report a requested target as the current primary merely
because a command returned successfully.

## Replica-Agent Contract

The level-triggered design depends on strengthening the replica agent.

### Durable Local State

The agent must persist enough state across container and process restarts to
report:

- replica ID and incarnation;
- agent generation;
- process session ID and session-scoped monotonically increasing report
  sequence;
- role and access state;
- epoch;
- exact Previous and Current Configuration descriptors and roles;
- current progress, catch-up capability, committed LSN, and deactivation
  information;
- reconfiguration type and enough durable state to reconstruct the internal
  stage;
- pending runtime action identity and bounded terminal result;
- build or retirement state.

The durable state is stored in a replica-local SQLite database on the replica
PVC, under a Kuberic-owned metadata directory separate from application data.
The database records the owning resource UID, logical replica ID, exact replica
incarnation, durable agent generation, and schema version so persisted
authority cannot be silently rebound to a different Pod incarnation.

SQLite transactions define the agent's local commit boundary:

- command intent and pending runtime-action identity are committed before the
  corresponding runtime effect is issued;
- authority and terminal evidence required for recovery are committed before
  command completion is acknowledged;
- related epoch, configuration, role, access, deactivation, and action updates
  are committed atomically;
- schema migration is explicit and transactional;
- missing, corrupt, incompatible, or identity-mismatched storage is reported
  as `Unsafe` rather than recreated as empty authority.

The agent is the single writer. SQLite journaling and synchronization settings
must provide durable commit semantics on the supported PVC filesystem and are
validated by crash-boundary tests. The database does not contain application
state. A process session ID remains ephemeral and changes on every agent
process start; only the durable agent generation survives a process restart.

Database absence has two distinct meanings:

1. **Fresh provisioning:** The exact Pod and PVC were created for a
   never-initialized bootstrap member or for a replacement that is still
   outside PC and CC. The agent reports an uninitialized store and does not
   create authority by itself. After the operator persists the corresponding
   bootstrap or replacement intent, it may issue one fenced
   `InitializeAgentStore` command naming the resource UID, logical replica ID,
   Pod UID, PVC UID, initialization ID, operator-assigned durable agent
   generation, and effective policy. The generation is deterministically
   derived from the persisted initialization identity, allowing the complete
   exact configuration to be persisted before store creation. The agent
   atomically creates the schema and adopts that generation.
2. **Established storage missing:** The Pod incarnation or accepted/outstanding
   authority requires an existing durable generation, but the database is
   absent, unreadable, incompatible, or identity-mismatched. The agent and
   operator report `Unsafe`; they do not create a replacement database on that
   PVC.

A newly created PVC UID and persisted transition intent are therefore
provisioning evidence, not authority by themselves. A retained PVC must not be
rebound to a new Pod UID in the minimum contract.

Runtime-effect recovery follows an explicit ordering:

1. validate the command against current durable authority;
2. commit operation intent and the pending runtime-effect identity;
3. issue or reissue the idempotent runtime effect;
4. observe its durable or reconstructible postcondition;
5. atomically commit the resulting authority and terminal evidence;
6. acknowledge command completion.

Phase 3 implements this boundary in `kuberic-agent`. Process hosting and
runtime effect sequencing live in the agent crate; unpublished
`kuberic-runtime-internal` owns the narrow persistence and postcondition data
contracts. `SqliteStore` is created only from validated bootstrap or
replacement initialization authority, records exact storage identity, uses
WAL with `synchronous=FULL`, and implements the narrow authority, replication,
local-write, build-authorization, and build-progress capabilities. Reopen
rejects missing established metadata, corruption, schema mismatch, and
identity mismatch. Pending effect intent and retained terminal results are
durable independently of process-session identity.

A committed intent does not imply that its runtime effect ran. After a crash,
the agent distinguishes pending intent from durable completion and resumes from
the observed postcondition. Runtime authority that gates replication is
persisted before the runtime can acknowledge work in that authority. In
particular, a secondary must durably accept the exact epoch, configuration,
replica incarnation, and agent generation before acknowledging replication in
that epoch.

Application state and RA metadata do not share a transaction. Recovery
therefore uses conservative postconditions: application acknowledgements prove
durable application progress, runtime operations are safe to repeat, and RA
completion is recorded only after the corresponding application/runtime
postcondition is observed. A crash between those commits causes re-observation
or repetition, never inferred success or rollback of acknowledged progress.

The application API follows Service Fabric V1 semantics with Rust async
interfaces. Service lifecycle (`Open`, `ChangeRole`, `Close`, and `Abort`) is
separate from state-provider callbacks (`UpdateEpoch`, committed progress,
copy context/state, and data loss). Durable operation acceptance is an explicit
acknowledgement on a service-owned operation stream, not a state-provider
callback. Copy state is
an opaque chunk stream installed through a captured LSN boundary; incremental
replication remains a distinct ordered stream. Exact replica identity, epoch,
configuration fencing, quorum accounting, and build sequencing remain runtime
and agent responsibilities rather than application authority.

The runtime MUST preserve the exact SF V1 interface divisions from
`FabricRuntime.idl:495–539,577–633,687–758`, collapsing COM Begin/End pairs
into async Rust methods:

- `Replicator`: Open returning the replication address, ChangeRole(epoch,
  role), UpdateEpoch, Close, Abort, CurrentProgress, and CatchUpCapability.
- `PrimaryReplicator: Replicator`: OnDataLoss, UpdateCatchUpReplicaSetConfiguration,
  WaitForCatchUpQuorum, UpdateCurrentReplicaSetConfiguration, BuildReplica,
  and RemoveReplica.
- `StateReplicator`: Replicate(operation data), GetReplicationStream,
  GetCopyStream, and UpdateReplicatorSettings.
- `StateProvider`: UpdateEpoch(epoch, previous epoch last LSN),
  GetLastCommittedSequenceNumber, OnDataLoss, GetCopyContext, and GetCopyState.
  Copy context and copy state are operation-data streams.

`StatefulServiceReplica::open` MUST return the control `Replicator`, matching
EndOpen. During Open the service selects a factory through its stateful
partition and calls CreateReplicator with its state provider and settings.
CreateReplicator returns both control and state interfaces. The service keeps
the `StateReplicator` and consumes its copy/replication streams; `PodRuntime`
retains and drives exactly the returned control interface. The primary
interface is a separate, explicit Rust interface reference, corresponding to
COM interface discovery. Constructor injection into `PodRuntime` is not the
public replicator ownership model.

`DefaultReplicatorFactory` provides the built-in exact-authority replicator;
custom factories use the same partition boundary. The default factory
selects a non-COM durable-storage adapter, independently of the SF
`StateProvider`. Services and custom replicators MUST NOT be required to
implement default-engine storage callbacks merely to implement the SF API.
The factory constructs one complete shared `DefaultReplicatorInner` during
CreateReplicator. Hosting retains application lifetime, one-shot registration,
effect ordering, exact returned-interface identity, and control/primary
discovery. A primary-capable bundle derives control and primary views from one
implementation; independently supplied control and primary objects are not a
valid construction. The public factory context exposes immutable identity and partition
access capabilities, never a concrete hosting or default-engine root.
A managed bridge carries Kuberic hosting/default-replicator integration
through agent-owned registration and is not returned in the
application-visible interface bundle. The construction token comes from the
unpublished runtime-internal package so ordinary runtime consumers cannot
forge the hosting boundary.
User code constructs only the SF-shaped control, primary, and state interface
bundle. Custom replicators own their data plane independently.
Reservations, retries, exact ACK handling, authority admission, durable
quorum finalization, queues, and copy/build bookkeeping belong to a distinct
replication engine and MUST NOT be added to the public SF traits.

Replica-local persistence uses least-authority interfaces even when one SQLite
database implements them: replica authority, agent effect intent/result,
replication progress, local-write journal, build authorization, and build
execution progress. Build execution cannot authorize itself, replication code
cannot mutate agent effect state, and application durability remains a
separate transaction boundary.

Delivery acknowledgement is explicit and one-shot. Dropping a delivered
operation is not durable acceptance. Application acceptance precedes durable
authority/build applied progress, which precedes an applied peer ACK; quorum
readiness precedes application commit and retry-record completion, which
precede client success. A receive-only ACK may precede application acceptance
and never grants quorum credit.
Agent status `currentProgress` is observation and retained-history repair input,
not certified replication progress. It MUST NOT grant catch-up or client-commit
quorum credit. `verifiedReplicationLsn` is a separate durable certificate bound
to the report's exact identity, process session, epoch, and PC/CC authority.
The agent accepts it only from the authenticated live peer session, and the
replicator revalidates the authority before granting progress credit. Only an
authority-bound applied replication ACK, verified-progress certificate, or
completed build handoff may advance another replica's quorum slot.
Transport remains caller-supplied, including build request dispatch and ACK
delivery. Waiting for a build or catch-up quorum requires observed completion,
not successful enqueueing.

Lifecycle ordering follows Service Fabric: role changes fence access as
required and drive the replicator before notifying the service. Primary
promotion performs `ChangeRole`, replicator/state-provider `UpdateEpoch`, and
then application `ChangeRole`. The completed role is published only after
every required stage succeeds; partial completion remains explicit recovery
evidence. Graceful close fences access, closes the replicator, and then closes
the service. Abort stops the returned control before application teardown.
Failed/cancelled Open aborts created interfaces. Failures, cancellation, epoch
regression, and stale ACKs MUST fail closed.

Copy context and copy state retain their multi-item stream semantics.
Snapshot chunks, the captured copy boundary, and post-boundary replication use
one bounded ordered build stream. Provider enumeration does not hold the global
runtime effect/write lock. Retained-operation enumeration also occurs outside
that lock after the build is installed as a catching-up target, so concurrent
writes enter its pending handoff lane. Exact duplicate durable snapshot chunks
are acknowledged without application redelivery; conflicting contents are
rejected.

Phase 4 implements the replica-local RA boundary. `EnsureConfiguration`
commands are admitted against exact resource, incarnation, durable generation,
epoch, PC/CC, policy, and operation identity. The agent persists private
Demote, GetLSN, Catchup, Deactivate, ReplicatorRole, Epoch,
ApplicationRole, and Activate stages. Each runtime effect has a durable
sequence and retained postcondition, so restart resumes the first incomplete
stage rather than exposing a controller workflow cursor.

Partition information and independent read/write access are available through
the application partition. Load and fault reports are accepted by the hosting
owner and included in agent observations. Primary promotion uses separate
durable replicator-role, provider-epoch, and application-role effects.

Runtime replication and copy messages are implementation-neutral contracts in
`kuberic-runtime-internal`; protobuf validation and conversion belong to the
agent. The runtime crates no longer depend on `kuberic-wire`. The agent binds
separate authenticated control/peer and replication listeners, opens the
runtime only after both listeners bind, assigns a fresh process session,
rejects retired sender or receiver sessions, and exposes bounded reliable send
windows with reconnect, cancellation, truthful retained-range capability, and
full-copy fallback.

Phase 4 hardening rejects unequal same-epoch authority under a new operation
ID while permitting only exact current-only completion of an admitted PC/CC
transition. Changed authority cannot restore access during admission; primary
read activation waits for local catch-up. Managed catch-up releases the
progress lock so ACKs can complete it, and matching commands are serialized
and revalidated at durable stage boundaries.

Serving starts fail-closed listeners before reconstructing live hosting from
durable authority, role, access, pending effect, and retained stage evidence.
Readiness is revoked and the runtime is aborted on shutdown. Session
replacement holds an owned delivery lease, so it cannot return while an
old-session mutation remains in flight.

Reliable windows require full copy when retained history is absent or
cancelled. Older duplicate replication returns a cumulative received
watermark compatible with applied progress. Reports retry until durable
authority and repeated live snapshots agree, and deactivation retains its own
epoch. Build selection is admitted by the agent before source execution; the
engine cannot manufacture build permission. Returned copy-stream drop
propagates cancellation into provider iteration and removes the build.

The Phase 5 controller is isolated in `kuberic-controller` and watches only
`operator.kuberic.io/v1alpha1`. Each reconcile loads the latest CR, owned Pods,
PVCs, write Service, exact Kubernetes UIDs and resource versions, and available
agent reports before normalizing one immutable `ObservationSnapshot`. The pure
evaluator remains the only authority-selection owner. Status replacement uses
optimistic resource-version fencing, routing changes use UID and
resource-version tests, and one observed snapshot can dispatch at most one
agent command. Stable, waiting, and recoverable unsafe states all have bounded
re-observation intervals; agent startup `Unavailable` is a wait rather than
permission to issue another authority command.

Controller evidence is monotonic across effect failures: a missing, invalid,
or unreachable report cannot erase the last accepted process-session
watermark. Raw collection preserves every Pod incarnation for a logical
replica, allowing accepted and out-of-authority replacement evidence to
coexist without last-writer-wins loss. Never-initialized authority requires an
explicit `Uninitialized` report from every exact Pod/PVC pair; unavailable or
absent metadata is unknown storage, not proof that genesis is safe.

Write routing has an independently observed Service-existence postcondition.
Missing Services are recreated, unresolved selectors are fenced before
publication, and `Ready` requires the Service selector to match the exact
label on the attested primary Pod. A failed Service-list observation cannot be
treated as confirmed routing absence.

The Phase 6 vertical slice adds `examples/kvstore2` and
`kuberic-level-tests`. Fresh Pods first expose an authenticated
`Uninitialized` control service. `InitializeAgentStore` carries the exact
full genesis configuration and creates `.kuberic/agent.sqlite3`; the process
then reopens the same PVC through the normal durable agent and application
runtime. Application state is stored separately under the application data
directory.

Bootstrap installs the deterministic full-size genesis configuration on every
exact incarnation with WriteStatus closed. Because every genesis member has
just proven a fresh empty store and progress zero, this initial slice uses a
proof-based empty-state build: no copy payload exists to transfer, but every
member must durably attest the same full configuration and assigned role.
Only then does one status replacement set `initialized`, freeze the effective
policy, accept the topology, and clear the transition. A separate fenced
command grants primary WriteStatus; routing publication follows only after the
granted report is observed.

The agent supplies the authenticated gRPC dispatcher for replication and copy
traffic. Every exact incarnation has a derived ClusterIP Service selected by
its Pod UID, so an old and replacement incarnation remain concurrently
addressable until CC is accepted. Process sessions are discovered before
enqueue, receivers admit the authenticated exact sender session, and
acknowledgements return through the runtime quorum tracker. Controller-created
per-set Secrets distribute the same credential used by the controller.
Immutable local image tags and explicit `IfNotPresent` policy keep the KinD
harness offline.

The isolated KinD scenario proves a three-member accepted topology, controller
restart, exact secondary-container restart with the Pod UID and durable agent
generation preserved, and a quorum-replicated write. Cluster-dependent tests
remain ignored by default and require the owned `KUBECONFIG`,
`KUBE_CONTEXT`, and `KIND_CLUSTER_NAME` tuple.

Post-review hardening resumes the enclosing durable configuration command
after reconstructing any pending runtime effect. Bootstrap topology acceptance
requires the exact terminal install operation, no pending operation, and zero
application progress on every member. Fresh metadata is not sufficient by
itself: surviving application files produce unsafe storage, and established
metadata must match the process's resource, replica, Pod, and PVC identity
before runtime reconstruction.

The KV provider publishes new in-memory state only after the candidate state
is durably written and synced. Failed persistence therefore cannot be reused
as `verify_applied` or progress evidence. The public state replicator also
releases a newly reserved request when a different durable pending operation
owns recovery, allowing the original write to resume.

Outbound replication is independently retried per peer. One unavailable
secondary cannot block delivery to another quorum member or terminate the
primary. Re-observing the same peer process session preserves its retained
window. Peer discovery and session admission are agent-owned, while the
application supplies only deployment endpoint configuration. The controller
observes and independently reconverges the peer Service and credential Secret
in bootstrap, transition, and stable states.

`ReplicaHost` is the reusable process boundary. It owns fresh/existing agent
metadata, exact process identity validation, `PodRuntime`, `AgentService`,
process sessions, authenticated peer transport, discovery, readiness,
reconstruction, shutdown, and replica diagnostics. A stateful application
supplies its `StatefulServiceReplica`, its application-storage classification,
and endpoint configuration; it no longer constructs agent or transport
internals.

Phase 7 implements same-cardinality replacement. Definitive loss of a
non-primary Pod with surviving exact storage creates one deterministic
out-of-authority replacement Pod/PVC pair. The controller persists the exact
provisioning identity before initialization. The primary admits a durable
build authority, the target opens as Idle Secondary, and copy plus the
post-snapshot replication gap must be durably acknowledged before status can
freeze PC/CC. Empty replication gaps are represented by an empty stream rather
than an invalid range.

The evaluator installs PC/CC on reachable non-primary members before issuing
the primary command, waits for the privately derived CC catch-up predicate,
then installs current-only authority. PC and CC retain fixed cardinality and
the old incarnation remains accepted until a current-only quorum and granted
primary WriteStatus are observed. A missing target after CC is never
substituted in place; current-only completion may proceed with another valid
CC quorum, after which a later serialized replacement can repair the missing
member.

Current-only completion durably retires the replacement build on source and
target before the command becomes terminal. The controller then removes the
old exact endpoint and UID-fenced Pod/PVC scaffolding. Cleanup-only orphan
storage cannot be mistaken for failure of the healthy accepted replacement.
Pre-CC target loss clears provisioning and returns to deterministic
provisioning without creating a second authority.

Retained sender windows, ACK retirement, reconnect capability, and retry
cadence now live in `kuberic-runtime::replicator::sender`; the agent retains
only DNS, authentication, gRPC, and process-session discovery. A changed peer
process session preserves domain payload windows and refreshes session fences
on demand. Replacement builds reconstruct from durable build authority and
application state, and registered writes are re-registered and republished if
an authority refresh closes their process-local completion channel.

The replacement KinD scenario proves pre/post-replacement quorum writes,
copy/build handoff, equal-cardinality PC/CC acceptance, old PVC retirement,
exact replacement process restart, authority reconstruction, and another
quorum write. The original fresh-bootstrap scenario remains a separate
regression gate.

The post-Phase-7 SF alignment review tightened supported recovery paths.
Exact pending current-only commands are classified before fresh admission, so
their own durable PC-removal postcondition cannot invalidate replay. Internal
outbound and discovery workers start before peer-dependent reconstruction,
while external command/data admission and application readiness remain
closed. Exact peer reports under the installed fence supply truthful durable
progress; a cold primary replays retained operations to a lagging current
member rather than waiting for a new client write.

A replacement member returning before acceptance first receives its missing
PC/CC installation before current-only completion. If it returns behind after
acceptance, the controller treats the non-primary lag as a serialized
same-cardinality replacement instead of withdrawing usable-quorum routing.
Bootstrap incarnation supersession allocates a newer configuration epoch, so
surviving partial installation at the old write-closed epoch can converge
without weakening same-epoch conflict rejection.

Build retries retain the same durable build authority but cancel abandoned
process-local streams before retry. Providers must reproduce identical ordered
copy bytes for the same captured boundary; `kvstore2` reconstructs that
snapshot from retained operations. Copy chunks and directory entries are
synced before durable acknowledgement, final-copy completion is insufficient
until the replication gap reaches current source progress, and payload-bearing
delivery tasks are bounded.

The durable local-write journal includes the original committed watermark.
Restart reconstruction verifies and republishes the exact registered
operation under restored valid primary authority without requiring the
original client future. Ordinary KV reads consume the independent partition
ReadStatus and return a retryable denial unless access is Granted.

Phase 8 implements non-destructive ordinary failover and quorum loss.
`status.primaryFailure` binds the first failure observation to the exact
accepted primary and persists the frozen-delay start time. Write routing is
removed before a newer epoch is allocated. `status.quorumLoss` separately
records loss of the accepted configuration's write quorum; the surviving
primary publishes `NoWriteQuorum`, fences pending writes, and restores
`Granted` automatically when the same configuration quorum returns without
changing the data-loss number. Kuberic does not implement SF-style elapsed-time
replica dropping or destructive data-loss recovery. Persistent quorum loss
therefore remains write-closed unless the same quorum returns or separately
validated permanent-fault evidence authorizes an exact replacement.

Failover first preserves the accepted PC and any outstanding replacement CC,
including its build authority. PC and CC read quorum must remain observable
before the evaluator persists a newer write-closed election epoch. Reachable
members durably accept that epoch before their progress is eligible. The
agent's failover path changes the replicator role and updates the primary
epoch before GetLSN, then retains exact deactivation epoch and LSN evidence.
Candidate selection filters and orders this epoch-fenced evidence; changing a
provisional candidate allocates another configuration epoch rather than
rewriting same-epoch authority. The transition persists the selected
`electionLsn`. Each replica durably authorizes only
`min(localAppliedLsn, electionLsn)` under the new fence, preventing arbitrary
old suffix credit while allowing the next contiguous operation after
failover.

The selected runtime Primary remains write-closed while retained-history
repair runs. If a configured reachable member is behind the selected
primary's retained range, the transition persists one exact
`status.transition.repair` authority and performs a full-copy build under the
failover configuration. PC/CC deactivation quorum, CC catch-up, current-only
installation, granted primary WriteStatus, and a current quorum are required
before status accepts and publishes the new topology. A returned stale former
primary is admitted only as evidence for an exact newer-epoch correction; its
old epoch cannot receive quorum credit.

The level-triggered control protocol is version 3. `EnsureConfiguration`
carries the intended primary access state rather than a write-grant boolean,
allowing `ReconfigurationPending`, `NoWriteQuorum`, and `Granted` to remain
distinct durable postconditions. Current-only completion can retire every
build authority carried by replacement plus failover repair.

Phase 9 adds generated authority traces, process-termination persistence
boundaries, ambiguous-command replay, bounded no-watch resynchronization, and
a fresh-cluster adversarial matrix. The live matrix composes replacement,
quorum loss and healing, controller restart, one-replica network isolation,
replica process reconstruction, failover, and stale former-primary direct
access. Every client probe has a fixed timeout, and failure deadlines are
bounded so the matrix fails with diagnostics rather than hanging. Scheduled
CI runs the complete matrix twice on separate fresh clusters.

The following contract blocks an end-to-end Service Fabric equivalence claim:

| Contract | Required owner and phase |
|---|---|
| Destructive data-loss recovery, PC/CC abandonment, and non-intersecting authority recovery | Explicitly unsupported; requires separate design |

Phase 10 added an exhaustive source-public signature inventory, including
private-module and `#[doc(hidden)]` declarations, plus adversarial compile-fail
fixtures for agent-owned runtime capabilities. The operational guide records
the remaining cross-crate hidden surface and does not treat rustdoc visibility
as access control.

The current classic design treats loss of process-local role, epoch, or action
correlation under the same Pod UID as a stale replica requiring removal and
rebuild. The level-triggered operator must not depend on volatile correlation
state for normal recovery.

The durable agent generation identifies persisted authority across process
restarts. The process session ID identifies a specific running agent process.
A session change requires fresh observation but must not invalidate correctly
persisted configuration or action evidence.

### Declarative Fenced Commands

Commands should describe a desired postcondition and include exact authority:

```text
InitializeAgentStore {
    initialization_id,
    resource_uid,
    local_replica_id,
    expected_instance_id,
    expected_pod_uid,
    expected_pvc_uid,
    assigned_agent_generation,
    effective_policy,
    bootstrap_configuration
}
```

```text
EnsureConfiguration {
    operation_id,
    previous_configuration,
    current_configuration,
    previous_epoch,
    current_epoch,
    effective_policy,
    local_replica_id,
    expected_instance_id,
    expected_agent_generation,
    grant_write
}
```

For every command, the agent must reject stale incarnation, generation, epoch,
configuration, or role assignment and expose the resulting durable
postcondition through normal status observation.

For commands that use a retained action record, the agent must also:

1. return the retained result for an already completed operation ID;
2. continue or report an in-progress matching operation;
3. reject reuse of an operation ID with a different Previous/Current
   Configuration, epoch, role, or replica incarnation;
4. persist terminal evidence before acknowledging completion.

Commands whose result is uniquely reconstructible from durable state may omit
a terminal action result, but they must still be fenced and safe to repeat.
For example, an `EnsureRole` command may be recoverable solely from an
unambiguous durable role, epoch, and configuration postcondition.

### Effect Classes

Every authority-changing effect belongs to one of three classes:

1. **Convergent ensure:** Durable state uniquely proves whether the desired
   postcondition holds. Repetition is safe, and a terminal ledger entry is not
   required.
2. **Reconfiguration action:** The replica agent owns the internal
   reconfiguration stage and reconstructs completion from epoch,
   Previous/Current Configuration, role/access state, LSN progress, and
   catch-up results.
3. **Unsupported effect:** The result cannot be reconstructed or safely
   repeated from durable replica and runtime state. It must not be dispatched.

The evaluator and command schema must identify the effect class explicitly.
An effect must not be treated as convergent merely because retrying it usually
works.

### Replica-Agent Reconfiguration State

The replica agent owns the Service Fabric-style reconfiguration state:

```text
ReconfigurationState {
    operation_id,
    reconfiguration_type,
    previous_configuration,
    current_configuration,
    previous_epoch,
    current_epoch,
    stage?,                    // private implementation choice
    catch_up_lsn?,
    pending_runtime_action?
}
```

The internal stages are `Demote`, `GetLSN`, `Catchup`, `Deactivate`, and
`Activate`. Only the stages required by the reconfiguration type are used.
They are not copied into CRD status.

Previous/Current Configuration, epochs, replica identities, deactivation
information, catch-up boundary, and pending runtime action identity must be
durable. Persisting the exact stage enum is optional: the agent may reconstruct
it from those facts and runtime postconditions. A persisted stage cursor is a
private Kuberic implementation choice, not a claimed Service Fabric wire or
storage contract.

After an agent process restart, a pending action from the prior session is not
assumed to still be running. The agent reconstructs progress from its durable
reconfiguration state, runtime role and access status, installed
Previous/Current Configuration, epoch, replication progress, and catch-up
result. It then:

1. skips an action whose durable postcondition already holds;
2. reissues the same idempotent runtime action with identical configuration
   and epoch;
3. resumes the same catch-up or build operation; or
4. marks the local replica unsafe if the result cannot be reconstructed.

The RA-style retry contract uses one stable operation identity, suppresses
duplicate in-progress work, and returns only after the runtime postcondition is
durable. A non-reconstructible effect is unsupported.

Runtime results required for recovery are retained until the corresponding
Current Configuration is accepted and `status.transition` is cleared.
Retention after that point is outside this design.

### Epoch and Incarnation Supersession

The current epoch is the reconfiguration version. Normal reconfiguration
increments `configurationNumber`. `dataLossNumber` remains part of epoch
comparison for compatibility, but this design does not advance it. Epoch
comparison is lexicographic, with data loss taking precedence.

Every replica durably retains its highest accepted epoch and exact replica
instance identity. It rejects:

- a command with an older epoch;
- a command naming a stale Pod UID, instance ID, or agent generation;
- conflicting Previous/Current Configuration or role assignments at the same
  epoch;
- replication operations from a primary using an older epoch.

Changing the target or configuration requires a newer configuration epoch.
Absence of the old postcondition is not revocation. Delayed commands from the
abandoned reconfiguration remain harmless because replicas and the replicator
reject the older epoch.

The operator must not reuse an epoch for different configuration contents.
Kubernetes resource versions serialize status updates but do not replace epoch
or incarnation fencing.

### Coarse Protocol Ownership

Where possible, one replica-side coordinator should own multi-step runtime
protocols:

- build a replacement replica;
- replace and retire an old incarnation without changing cardinality;
- install CatchUp and Current configurations;
- wait for catch-up quorum;
- persist local commit evidence.

The operator selects and fences the operation, while the primary agent
performs the tightly coupled replica-to-replica sequence. This avoids exposing
every internal protocol step as an operator phase.

### Global Write Exclusion

The safety invariant is:

> At most one replica can successfully acknowledge writes across all
> concurrently usable epochs and configurations.

This is stronger than allowing one primary per epoch. Routing and labels are
traffic-management mechanisms, not write-authority fences.

A replica starts write-closed after every process restart. It may acknowledge
writes only when all of the following hold:

1. the local replica has the Primary role for its exact current epoch and
   incarnation;
2. the replica agent computes `WriteStatus = Granted`;
3. the Current Configuration has its required write quorum;
4. while a Previous Configuration exists, that configuration also has its
   required write quorum;
5. the replication operation is acknowledged under the same epoch by both
   quorums before success is returned.

The runtime replicator includes the primary in each write quorum and tracks
secondary acknowledgements by exact replica incarnation. During
reconfiguration, replication is sent to active replicas in both Previous and
Current Configuration, and an operation commits only after satisfying both
write quorums.

Secondaries durably accept the newer epoch before acknowledging replication in
that epoch and reject all later operations from an older epoch. Consequently,
an old primary cannot commit after an intersecting quorum has advanced.

If a valid Previous Configuration quorum or required read quorum cannot be
formed, the system enters quorum loss and blocks writes. It does not create a
new writer merely because the old primary is unreachable.

Existing client connections or direct Pod access cannot bypass this rule. An
old primary may receive a request, but it must reject or fail the write when it
does not have granted WriteStatus or cannot satisfy both applicable write
quorums.

### Layered Fencing

Safety must not depend on a single fence:

1. **Routing fence:** Remove write routing to an old or ambiguous primary
   before authorizing replacement write authority.
2. **Protocol fence:** Reject commands with stale incarnation, generation,
   configuration, or epoch authority.
3. **Runtime write gate:** The RA-style access-status calculator grants writes
   only for the exact Primary role, epoch, frozen replica-set size, and
   applicable Previous/Current Configuration quorums.
4. **Isolation self-fence:** A primary may stop serving when it loses both its
   control-plane connection and all required peer connectivity.

Normal reconfiguration relies on PC/CC quorum intersection and epoch fencing.
Additional self-fencing is defense in depth. Non-intersecting data-loss
recovery is unsupported and remains `Unsafe`.

### Quorum and Progress Evidence

Quorum evidence comes from the runtime replicator's durable acknowledgement and
progress state, not from arbitrary health reports or an operator-generated
certificate.

Every acknowledgement is scoped to:

- replica ID and exact instance ID;
- agent generation;
- epoch;
- Previous/Current Configuration membership;
- received, applied, and committed LSN as applicable.

The progress ordering is `received >= applied >= committed`. Ordered receiver
admission may publish received progress before the service durably applies the
operation. Received progress may retire transport resend work, but only applied
progress may contribute replication quorum credit.

The required Service Fabric predicates are:

| Operation | Required durable evidence |
|---|---|
| Steady write | Current Configuration write quorum acknowledged the operation in the current epoch |
| Reconfiguration write | Both Previous and Current Configuration write quorums acknowledged the operation in the current epoch |
| Bootstrap | Deterministic initial primary remained write-closed while every intended member was built outside the installed configuration; the full-size genesis CC and derived write quorum were installed before WriteStatus was granted |
| Build idle replica | Exact target incarnation acknowledged the final copy operation and the captured replication LSN that closed the copy/replication gap |
| Replace active secondary | Replacement build completed outside quorum; PC names the old incarnation and CC names the replacement with equal cardinality and quorum; catch-up, PC deactivation, CC activation, and end-reconfiguration completed before current-only installation and old-incarnation retirement |
| Failover | Eligible replicas accept the newer epoch before GetLSN; PC and outstanding CC read-quorum evidence includes deactivation history; selected target becomes runtime Primary write-closed; lagging exact CC members use incremental catch-up or full-copy fallback before deactivation, activation, and WriteStatus grant |

The replicator tracks two different progress quantities:

- **CC quorum progress:** the LSN acknowledged by the Current Configuration
  write quorum; catch-up uses this value.
- **Client commit progress:** while PC exists, the LSN satisfying both PC and
  CC write quorums; successful client writes use this value.

Catch-up records the primary replication queue's last sequence number when the
catch-up configuration is installed. Write-quorum catch-up completes only when:

1. CC quorum progress reaches that recorded LSN; and
2. every privately derived `must_catch_up` replica individually reaches the
   current CC quorum progress.

`must_catch_up` is internal RA/replicator state derived from the
reconfiguration. It is not CRD authority.

Missing required acknowledgements or progress produces `Wait`. Conflicting
epochs, instance identities, roles, or LSN histories produce `Unsafe`.

## Observation Snapshot

Each reconcile builds an immutable normalized snapshot containing:

- resource UID and generation;
- desired spec;
- Pods, PVCs, Services, labels, readiness, and UIDs;
- status from every reachable replica agent;
- last accepted topology and transition intent;
- frozen effective transition policy;
- quorum metadata;
- agent session IDs, report sequences, and operator receipt times;
- the Kubernetes resource versions used for the observation pass;
- current time;
- explicit observation failures.

Replica observations are keyed by logical replica ID plus exact Pod
incarnation, not by logical replica ID alone. A replacement snapshot may
therefore contain the accepted old PC incarnation and a new provisioning or CC
incarnation simultaneously. Report watermarks are scoped to that exact
incarnation and process session.

Unreachable and absent are different states. A missing observation must never
be represented as a default replica report.

The normalization layer validates:

- duplicate logical replica IDs;
- duplicate incarnations;
- unsupported agent protocol versions;
- malformed configurations;
- epoch regressions;
- conflicting primary claims;
- configuration IDs that do not match their descriptors;
- reports spanning incompatible configuration or epoch boundaries;
- stale report sequences or session changes requiring re-observation.

Each decision defines its required observation set. The evaluator must not
authorize a protocol action from a convenient subset when the operation
requires evidence from specific replicas or a quorum.

## Guaranteed Re-observation

Kubernetes watches are an optimization, not the sole liveness mechanism.
Replica state, quorum progress, connectivity, and recovered actions may change
without modifying a watched Kubernetes object.

The controller must therefore provide bounded periodic re-observation:

- `Stable` uses watches plus a configurable safety resync interval.
- `Wait` always includes a bounded wake-up deadline.
- `Execute` requeues immediately and observes the command postcondition.
- recoverable `Unsafe` states continue low-frequency re-observation and leave
  `Unsafe` automatically when authoritative evidence resolves the conflict.
- unresolved unsafe local state remains fail-closed until new authoritative
  evidence, spec change, or explicit administrative resolution occurs, but is
  still periodically observed for that resolution.

Agent event streams and callbacks are outside this design. Every external state
change is discovered through a complete bounded observation and evaluation
cycle.

## Pure Evaluation

The core evaluator should be a pure function:

```text
evaluate(desired, accepted_status, observations, now) -> Plan
```

It performs:

1. Validate the accepted topology, transition, and frozen effective policy.
2. Establish the strongest configuration supported by durable evidence.
3. Detect conflicting or stale authority.
4. Determine whether an existing transition completed, remains in progress,
   needs another action, or became unsafe.
5. If stable, compare the accepted topology with desired state and health.
6. Return stable status, Kubernetes convergence, one protocol command, wait,
   or an unsafe result.

Pure evaluation enables exhaustive table tests, model-based testing, and fault
injection without Kubernetes or gRPC.

A stable `Ready=True` projection requires fresh evidence for the exact accepted
primary, granted WriteStatus, the accepted epoch and Current Configuration,
the required write quorum of healthy exact members, and write routing to that
attested primary. Accepted status without that evidence produces bounded
`Wait`; it is not sufficient to infer readiness.

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
        +--> Stable:
        |        patch status projection
        |        watch and schedule bounded safety resync
        |
        +--> Apply:
        |        apply independent Kubernetes/status changes
        |        requeue and observe again
        |
        +--> Wait:
        |        patch condition
        |        requeue by bounded deadline
        |
        +--> Unsafe:
        |        fence routing and patch condition
        |        periodically re-observe without granting authority
        |
        `--> Execute:
                 issue one fenced idempotent command
                 requeue without assuming success
```

Compact intent, routing fences, and Kubernetes prerequisites are established
through `Apply` and observed before `Execute`.

Kubernetes writes should use optimistic concurrency. A resource-version
conflict restarts observation and evaluation.

## Protocol Behaviors

These sections describe required convergence and safety semantics. They are not
an implementation sequence or phased delivery plan.

### Bootstrap

1. Prove the never-initialized authority condition.
2. Ensure all `N` intended Pods, PVCs, and supporting Kubernetes objects exist
   so every genesis incarnation is known.
3. Select the deterministic initial primary incarnation.
4. Persist a Bootstrap transition with empty PC, full-size genesis CC, initial
   epoch, initialization ID, and frozen effective policy.
5. Initialize each exact fresh agent store using the persisted Bootstrap
   transition, resource UID, Pod UID, PVC UID, logical replica ID, and
   initialization ID.
6. Ask the selected replica agent to open the runtime and remain write-closed.
7. Establish every other genesis member. When all stores have just proven
   fresh empty state at progress zero, no copy payload exists; installing the
   same full genesis CC on every member is the build postcondition. A
   non-empty genesis source would instead require the normal Idle Secondary
   copy and replication-gap path.
8. Observe every exact member attesting the full genesis CC and assigned role
   while WriteStatus remains denied.
9. Atomically set `initialized = true`, accept genesis CC as
   `status.topology`, and clear the Bootstrap transition.
10. Observe granted WriteStatus and publish write routing.

The operator may resume at any step by observing which configurations are
already committed.

If a genesis incarnation is lost before the full genesis CC is installed, the
operator may supersede the Bootstrap transition with a newer bootstrap epoch
and a fresh incarnation only after proving:

- `initialized` remains false;
- no accepted topology exists;
- WriteStatus was never granted;
- the old incarnation did not install the genesis CC or acknowledge writes;
- any old build operation is canceled or durably unable to complete.

This exception is limited to never-writable bootstrap. It does not permit
identity substitution in an accepted or outstanding non-bootstrap
configuration.

### Build or Replace Replica

1. Select the non-primary logical member and exact old incarnation to replace.
2. Ensure the replacement Pod and PVC exist and observe its exact identity.
3. Derive a deterministic operation ID from PC and the replacement
   incarnation.
4. Persist `status.provisioning`, then initialize the exact fresh agent
   store using the resource UID, Pod UID, PVC UID, logical replica ID, and
   initialization ID.
5. Open the replacement as an Idle Secondary outside PC and CC.
6. Ask the primary replicator to build the exact replacement through copy plus
   concurrent replication.
7. Observe acknowledgement of both the final copy operation and the captured
   replication boundary.
8. Allocate a newer configuration epoch. Atomically clear provisioning and
   persist the transition: PC contains the old incarnation; CC replaces it
   with the new incarnation and retains exactly `N` members.
9. Install the catch-up configuration and wait for the required CC quorum
   progress. A replacement secondary is not unconditionally
   `must_catch_up`.
10. Enter deactivation and collect the required PC deactivation/read-quorum
   evidence.
11. Activate CC and complete the replica-agent reconfiguration.
12. Install CC without PC and accept the same-cardinality topology.
13. Close, retire, and delete or retain the old incarnation only after it is
    absent from accepted topology and outstanding transition authority.

Before CC is persisted, a failed build target may be abandoned after proving
it was never admitted to authority, and a new operation may name a fresh
incarnation.

If the replacement incarnation disappears after CC becomes an outstanding
obligation, do not substitute a new identity into that CC. When PC/CC quorum,
deactivation, and activation predicates can be satisfied without the missing
secondary, complete and accept CC with that member down, then begin another
serialized replacement. Otherwise return `Wait` or `Unsafe`.

### Failover

Failover is the hardest protocol.

1. Persist the failure start time when a delay is configured.
2. Fence routing to the old primary.
3. Preserve the accepted PC and any outstanding CC membership.
4. Require the applicable read-quorum safety rule for both PC and outstanding
   CC.
5. Select only a provisional reachable coordinator and allocate a newer
   configuration epoch. This is not yet the authoritative primary election.
6. The coordinator and eligible replicas accept the newer epoch before
   reporting election progress.
7. The RA-equivalent coordinator performs epoch-fenced GetLSN:
   `UpdateEpoch` precedes progress collection, and reports include current
   progress, catch-up capability, exact incarnation, and deactivation history.
8. Select the authoritative candidate using Service Fabric-compatible
   deactivation filtering and LSN ordering.
9. If the authoritative candidate differs from the provisional target, persist
   another newer epoch with the same PC and outstanding CC membership and the
   corrected roles.
10. Establish the selected target as runtime Primary with WriteStatus denied.
11. Compare every required CC member's progress with the selected primary's
    catch-up capability.
12. For a reachable exact incarnation whose missing history is no longer
    retained, perform full-copy rebuild during the active transition. Exclude
    it from acknowledgement and quorum-progress counting until build
    completion.
13. A missing CC secondary may remain down only when the remaining exact
    members satisfy CC quorum and all deactivation/activation predicates; it is
    replaced after the reconfiguration becomes stable.
14. Install the catch-up configuration and wait for the required catch-up
    quorum.
15. Deactivate the Previous Configuration and resolve any outstanding CC
    obligation.
16. Install the Current Configuration and complete activation.
17. Grant WriteStatus only after epoch, frozen replica-set size, and quorum
    checks pass.
18. Accept and publish the topology.

Ordinary operator status polling may nominate a provisional coordinator but
must not substitute for epoch-fenced GetLSN. Changing an unactivated candidate
requires a newer configuration epoch and preserves outstanding membership.
The target must not change after activation without beginning another
reconfiguration.

### Quorum Loss and Unsupported Data-Loss Recovery

Quorum loss and data loss remain distinct:

1. If Current Configuration write quorum is unavailable, set
    `WriteStatus = NoWriteQuorum` and block writes.
2. Persist a configuration-bound quorum-loss marker and continue bounded
   re-observation.
3. If quorum returns, resume without changing the data-loss epoch.
4. If recovery would require abandoning PC or outstanding CC quorum, report
    `Unsafe` and remain write-closed.
5. Do not increment `dataLossNumber`, invoke the data-loss callback, construct
    a smaller authoritative configuration, or grant replacement writes.

Actual data-loss recovery and its external fencing provider are outside the
minimum contract.

## Ambiguous Effects

At-least-once command delivery is expected. Safety comes from identity,
fencing, postcondition observation, and retained results where the effect class
requires them.

After a timeout or lost response:

1. Re-observe the target and relevant quorum.
2. If the postcondition is proven, advance.
3. If a matching action belongs to the current process session and is
   executing, wait.
4. If the pending action belongs to an older session, reconstruct its result
   from durable reconfiguration facts, installed epoch and configuration,
   role/access status, deactivation information, and replication progress.
5. Reissue only the same idempotent runtime action for the same epoch,
   configuration, and replica incarnation.
6. If the evidence is missing or contradictory, return `Unsafe`.

The operator must not issue a different operation merely because the previous
RPC result was lost. A different target or configuration requires a newer
configuration epoch.

## Comparison

| Property | Classic operator | Proposed design |
|---|---|---|
| Reconcile style | Phase-driven | Level-triggered |
| Workflow cursor | CRD status | Usually none |
| Stable topology | CRD status | Operator-accepted, quorum-attested topology |
| Retry history | CRD operation | Replica-agent reconfiguration state plus postconditions |
| Primary protocol owner | Operator and agent | Replica agent |
| Recovery input | Status checkpoint plus replicas | Epoch, PC/CC, role/access state, LSN progress, and agent state |
| Long-lived history | Large CRD status | No controller history |
| Handling spec changes | Explicit phase rules | Re-evaluate each pass; freeze effective inputs during an active transition |

## Testing Strategy

### Pure Evaluator Tests

Table-test every meaningful combination of:

- desired replica count;
- initialization authority and durable-artifact presence;
- accepted configuration;
- reachable and unreachable members;
- replica incarnations and agent generations;
- process session IDs and report sequences;
- roles and epochs;
- Previous and Current Configuration;
- current progress, catch-up capability, committed LSN, and `must_catch_up`;
- reconfiguration type, internal stage, and pending runtime action;
- transition intent;
- frozen effective policy and newer desired generations;
- time and failover delay.

Each case asserts one exact plan and the safety reason supporting it.

### Crash-Boundary Tests

For every authority-changing command, inject a crash:

- before intent persistence, when intent is required;
- after intent persistence but before dispatch, when intent is required;
- after dispatch but before agent persistence;
- after durable reconfiguration-state update but before the runtime call;
- after a runtime configuration update but before the corresponding durable
  state update;
- during build or catch-up;
- after deactivation but before activation;
- after role or epoch change but before reply;
- after agent persistence but before reply;
- after reply but before status update;
- during operator restart;
- during replica-agent restart under the same Pod UID;
- after execution-serialization acquisition or loss;
- after routing is fenced but before a new primary is authorized.

The next reconcile must either converge to the correct postcondition or remain
fail-closed without issuing an incompatible command. When connectivity and the
required quorum recover, every supported case must eventually converge.

### Model and Property Tests

Verify invariants across generated state transitions:

- at most one replica can successfully acknowledge writes across all
  concurrently usable epochs and configurations;
- no configuration accepted without required quorum evidence;
- epochs never regress;
- an incarnation cannot inherit another incarnation's action result;
- an operation ID cannot name different PC/CC, epochs, roles, or incarnations;
- `initialized = true` can never return to never-initialized authority;
- durable replica or PVC evidence prevents genesis recreation when status is
  missing;
- an epoch cannot be reused for different configuration contents;
- a stale epoch cannot commit replication or change role;
- GetLSN evidence used for failover is collected only after eligible replicas
  accept the election epoch;
- reconfiguration writes satisfy both PC and CC write quorums;
- catch-up uses CC quorum progress while client commit uses dual PC/CC progress;
- outstanding CC membership survives primary retargeting and operator restart;
- active PC and CC do not change when `metadata.generation` advances;
- active replica-set size and quorum predicates come from frozen effective
  policy, not the latest spec;
- every accepted PC and CC has exactly the frozen replica-set size;
- a changed `spec.replicas` value cannot create, delete, or admit members;
- no second reconfiguration begins while one PC/CC transition is unresolved;
- a replica being built does not count toward quorum before build completion;
- replacement installs the same-cardinality Current Configuration before
  closing the old incarnation;
- replacement completes PC deactivation and CC activation before removing PC;
- loss of an outstanding replacement target never substitutes a new identity
  into frozen CC;
- failover establishes the target runtime Primary write-closed before catch-up;
- a CC member with a log gap is rebuilt by full copy or excluded from quorum
  until a later replacement;
- bootstrap does not grant WriteStatus before the full frozen replica set and
  write quorum are established;
- unsupported data-loss recovery always remains `Unsafe` and write-closed;
- accepted replacement never silently rolls back to the old incarnation;
- routing never publishes an unattested primary;
- at most one authority-changing protocol command is issued per observation
  cycle;
- session changes cannot inherit volatile in-progress state;
- every recoverable external-state wait is re-observed within its configured
  bound;
- incompatible mixed protocol versions always produce `Unsafe`.

### Kubernetes Integration Tests

KinD tests should cover:

- operator restart during every transition;
- operator restart during never-initialized and write-closed genesis states;
- genesis Pod deletion before full genesis installation;
- missing or manually cleared status while durable replica/PVC evidence exists;
- replica-agent restart with unchanged Pod UID;
- Pod deletion and recreation with a new UID;
- stale status patch conflicts;
- spec changes arriving during an active reconfiguration;
- unsupported `spec.replicas` changes while stable;
- delayed and duplicated watch events;
- absence of Kubernetes events while replica evidence changes;
- network partitions and partial replica visibility;
- old quorum partition retaining peer connectivity during attempted data-loss
  recovery;
- ambiguous gRPC responses;
- delayed older-epoch commands arriving after retargeting;
- interrupted reconfiguration where CC became effective before status
  publication;
- replacement-target deletion before and after CC becomes outstanding;
- failover where a surviving CC member is behind the elected primary's
  catch-up capability;
- overlapping reconciles selecting competing targets;
- stale-primary restart with retained client connections and direct Pod access;
- effect completion followed by a crash before result persistence;
- failover with and without quorum.

## Risks

### Hidden Workflow State Moves Into the Agent

The replica agent intentionally owns a bounded Service Fabric-style
reconfiguration state machine. The simplification is not removal of all
protocol stages; it is moving them to the RA-equivalent boundary and removing
per-RPC orchestration from the public CRD and Kubernetes controller. Agent
stages must map to PC/CC, epoch, LSN, role/access, and catch-up postconditions
rather than reproduce the classic operator's generic activity journal.

### Observation Is Not Authority

The most responsive replica is not necessarily authoritative. Configuration
and election decisions require quorum and fencing evidence, not majority
voting over arbitrary status fields.

### Accepted Topology Is Mandatory Domain Authority

The last accepted topology remains in status under this design. It is the
durable Previous Configuration during reconfiguration and is required for
restart recovery, quorum evaluation, and diagnosis. It is not an optional
cache that may later be removed.

### Repeated Commands

Level-triggered reconciliation deliberately repeats commands. Any command that
cannot safely be reconstructed or repeated from durable RA/replicator state is
unsupported.

### Mixed Versions

Protocol-version negotiation is outside this design. Every participant in PC
and CC must expose a version supported by the operator and by the other
participants. The evaluator returns `Unsafe` for incompatible mixed versions
rather than silently downgrading guarantees.

## Appendix: Minimum Viable Feature Set

The minimum supported contract contains:

- level-triggered watch, observe, normalize, evaluate, `Apply`, and `Execute`;
- bounded polling in stable, waiting, and recoverable unsafe states;
- monotonic initialization authority and a write-closed genesis topology;
- accepted topology as durable authority;
- one frozen PC and one outstanding CC;
- exact replica incarnations and configuration epochs;
- frozen effective replica-set size, quorum, and failover-delay inputs;
- replica-agent-owned reconstructible reconfiguration state;
- stale epoch, incarnation, and same-epoch conflict rejection;
- Current Configuration write quorum in steady state;
- dual PC/CC write quorum during reconfiguration;
- separate CC catch-up progress and dual-quorum client commit progress;
- exact-incarnation idle build with copy/replication-gap closure;
- serialized build/replacement with unchanged cardinality, PC deactivation,
  and CC activation barriers;
- safe completion when an outstanding replacement secondary disappears;
- epoch-fenced GetLSN with deactivation-aware ordinary failover;
- runtime Primary establishment while write-closed before failover catch-up;
- full-copy fallback for CC members outside the elected primary's retained log;
- quorum-loss write blocking and automatic recovery when quorum returns;
- explicit `Unsafe` refusal of data-loss recovery;
- optimistic-concurrency topology acceptance and transition clearing;
- crash-boundary, invariant, and stale-reconcile validation for every supported
  authority-changing operation.

## Appendix: Safely Deferred Scope

The following features are outside the minimum contract. Their absence does
not weaken supported Service Fabric safety invariants because unsupported
operations remain fail-closed:

- pre-write-status-revoke catch-up optimization; mandatory post-revoke
  catch-up remains;
- agent push notifications or event streams; bounded polling remains the
  correctness mechanism;
- a separate transition CRD; transition intent remains in
  `KubericSet.status`;
- planned switchover and its public request API;
- applying new spec changes during an active reconfiguration; they are
  evaluated after the current PC/CC transition becomes stable;
- stateful successful-write trace generation across delayed effects and
  concurrent retained client connections; current model validation covers
  authority observations and the live matrix covers bounded sequential
  histories;
- more than one unresolved reconfiguration; exactly one PC and one outstanding
  CC are supported;
- mixed-version protocol negotiation; incompatible versions return `Unsafe`;
- archival and pruning policy for completed reconfiguration results beyond the
  active transition;
- advanced failure-domain-aware placement and balancing policy;
- destructive data-loss recovery and its external fencing provider;
- rebinding retained PVC authority to a replacement Pod UID; the minimum uses
  a fresh incarnation and safe rebuild;
- public scale-up, scale-down, rolling-upgrade, and balancing orchestration;
  the safe same-cardinality replacement primitive remains;
- connectivity-based primary self-fencing as defense in depth;
- catch-up-capability election optimization; deactivation filtering and valid
  progress selection remain mandatory, and full-copy fallback for an
  unrecoverable log gap remains required.
