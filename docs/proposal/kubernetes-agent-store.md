# Kubernetes-Backed Agent Store

## Status

**Deferred proposal.** This design is not planned for implementation. The
local `.kuberic/agent.sqlite3` database remains the durable replica-agent
authority.

Moving the agent store into Kubernetes would make API-server, etcd and
admission-webhook availability part of replica recovery and authority
transitions, add per-replica control-plane load, and require infrastructure
execution fencing and offline disaster-recovery procedures. Those costs are
not currently justified by removing the local database.

The proposal is retained as an architectural option for fresh deployments. It
may be reconsidered after the stateless-default-replicator ownership changes
are implemented and Kuberic has measured control-plane scale and availability
requirements. It does not describe behavior that is already implemented.

## Overview

Kuberic currently stores replica-agent authority and workflow state in a
SQLite database on each replica PVC. The database makes agent transitions
atomic and crash-recoverable, but it also makes the runtime PVC a durable
control-plane owner. A replica process cannot be reconstructed from Kubernetes
control state and application storage alone.

The target design replaces the SQLite-backed `AgentStore` with a
Kubernetes-backed implementation. A namespaced per-storage-slot custom
resource stores the durable replica-agent aggregate in Kubernetes. The runtime
accesses that resource directly through the Kubernetes API using its
ServiceAccount. Application bytes and replication history remain in the
application state provider; the custom resource is not an application log.

The resulting durable ownership is:

- Kubernetes owns cluster intent and the replica-agent control aggregate;
- the application state provider owns application bytes, applied progress,
  committed progress and retained operations;
- the default replicator owns only reconstructable in-memory replication
  mechanics;
- the runtime process owns only a fenced execution session and disposable
  caches.

This is a broader change than making a custom application replicator
stateless. The PostgreSQL stateless-metadata proposal removes
application-specific metadata while retaining generic runtime authority. This
proposal relocates that remaining generic authority from local SQLite to
Kubernetes.

## Goals

1. Remove `.kuberic/agent.sqlite3` as a recovery-authoritative store.
2. Preserve the crash consistency, idempotence and permanent fencing currently
   provided by `AgentStore`.
3. Make an agent process reconstructable from one durable Kubernetes aggregate
   plus application-owned durable state.
4. Preserve a single atomic commit boundary for each agent transition.
5. Use Kubernetes optimistic concurrency to reject stale process and workflow
   updates.
6. Bind durable state to a stable replica and PVC identity rather than a Pod
   lifetime.
7. Fail closed when durable agent state cannot be read, validated or updated.
8. Keep application data and high-frequency replication progress out of
   Kubernetes API storage.
9. Preserve the existing public `Replicator`, `PrimaryReplicator` and
   `StateProvider` interfaces.
10. Require exactly one Kubernetes-backed metadata authority from initial
    provisioning; no deployment may activate both SQLite and CRD state.

## Non-Goals

- Storing application bytes, SQLite pages, PostgreSQL WAL, copy streams or
  retained replication operations in a custom resource.
- Turning a Kubernetes `Lease` into durable authority or workflow history.
- Replacing the existing controller evaluation and command protocols in the
  first implementation.
- Providing atomic transactions across multiple Kubernetes objects.
- Allowing a replica to continue authority transitions while disconnected from
  the Kubernetes API.
- Preserving unbounded command, effect, build or removal history.
- Treating the API server watch cache as recovery authority.
- Making controller intent sufficient to grant application access without
  agent admission and application reconciliation.
- Migrating an existing SQLite-backed replica or preserving compatibility with
  an older runtime.
- Automatically continuing existing PVCs after CRD loss or an etcd rollback.

## Current Local Store

The private `AgentStore` interface provides narrow operations for:

- loading and validating storage identity;
- loading admitted authority;
- beginning, applying, completing and cancelling runtime effects;
- beginning and advancing reconfiguration;
- retaining command results for idempotent retries;
- recording build intent and retirement;
- recording removal, switchover and topology evidence;
- recording reporting state.

`SqliteStore` implements each operation as a transaction over one local
database. A transition reads the current `AgentState`, validates its expected
phase and identities, writes the complete next state and commits before the
caller executes or acknowledges the next action.

The database also implements private authority, replication-progress,
local-write, build-authority and build-progress capabilities outside the
`AgentStore` trait. Before SQLite is removed, every table and private
capability must receive an explicit disposition:

- generic identity, authority, workflow or safety evidence moves into the CRD
  aggregate;
- application progress and exact retained operations move to
  `DurableState`;
- process-local replication and build continuation is removed;
- obsolete state is deleted because the corresponding behavior no longer
  exists.

An `AgentState`-only inventory is insufficient because current safety checks
compose records stored through several private capabilities in one SQLite
transaction.

That atomic state-machine behavior must remain. Replacing each SQLite table
with an independently updated Kubernetes object would weaken the design
because Kubernetes does not provide multi-object transactions.

## Target Architecture

```text
Kuberic controller
    |
    | creates identity binding and supplies desired commands
    v
ReplicaRuntimeState CRD
    ^
    | get + resourceVersion-guarded status replacement
    |
KubernetesAgentStore : AgentStore
    |
    +-- replica-agent coordinator and effect journal
    +-- custom/default replicator hosting
    +-- process-session fencing

Application StateProvider / DurableState
    |
    +-- application bytes
    +-- applied and committed progress
    +-- retained operations and copy state
```

`KubernetesAgentStore` is a private implementation of the existing
`AgentStore` boundary. The coordinator, recovery owner and runtime adapter
continue to call operations such as `begin_effect`,
`advance_configuration` and `complete_effect`. They do not issue unstructured
Kubernetes patches themselves.

The controller creates the custom resource. The runtime is not allowed to
create, delete or rename its durable state object.

## Custom Resource Model

The proposed resource is a namespaced `ReplicaRuntimeState` in the existing
`operator.kuberic.io` API group. There is one resource for each stable replica
storage slot, not one resource for each Pod process.

Its name is deterministic from the `KubericSet` and replica ID. Code must still
validate UIDs and generations; the name alone is not an identity fence.

### Controller-Owned Spec

The controller owns immutable or generation-controlled identity binding:

```yaml
apiVersion: operator.kuberic.io/v1alpha1
kind: ReplicaRuntimeState
metadata:
  name: example-r1
  namespace: default
  labels:
    operator.kuberic.io/set-uid: "..."
    operator.kuberic.io/replica-id: "1"
spec:
  setRef:
    name: example
    uid: "..."
  replicaId: 1
  storage:
    pvcName: example-r1
    pvcUid: "..."
    generation: 3
  protocolVersion: 1
  replacement:
    generation: 8
    claimNonce: "..."
    expectedSession: "..."
    successorPodUid: "..."
    fenceCertificate: {}
```

The spec does not grant role or access. It binds the object to the cluster,
replica slot and application storage generation for which status may be
admitted.

`replacement` is a controller-issued, single-use session-claim authorization.
It is absent while the current process remains authoritative. The controller
may publish it only after completing the execution-fencing protocol. Its
generation is monotonic for the lifetime of the resource.

The existing controller and command protocol may continue to deliver desired
configuration in the first implementation. A future design may place desired
agent commands in spec, but that is not required to remove SQLite and must not
create a second competing command authority.

### Agent-Owned Status

Status contains the durable agent aggregate:

```yaml
status:
  lifecycle: Active
  observedSpecGeneration: 4
  observedReplacementGeneration: 8
  storageIdentity: {}
  activeSession: {}
  nextCommandSequence: 47
  compactedThroughCommandSequence: 31
  lastSessionMutation: {}
  authorityHealth: {}
  admittedAuthority: {}
  previousPolicy: {}
  admittedPolicy: {}
  previousConfiguration: {}
  currentConfiguration: {}
  highestEpoch: {}
  role: Secondary
  access:
    read: NotPrimary
    write: NotPrimary
  reconfiguration: {}
  pendingEffect: {}
  retainedResult: {}
  removal: {}
  switchover: {}
  retirement: {}
  buildIntent: {}
  certifiedProgressEvidence: {}
  provisionalOperations: {}
  destructiveWorkReceipts: {}
  promotionEvidence: {}
  conditions: []
```

This is a logical schema, not a requirement to expose the current Rust
`AgentState` serialization unchanged. The Kubernetes API type should use
explicit versioned fields and validation. Internal compatibility fields and
SQLite table structure must not leak into the API merely to simplify the
implementation.

The CRD Rust types must live in a lightweight shared API crate or protocol
module used by both the controller and runtime. The runtime must not depend on
the controller crate, and the shared type layer must not depend on host
implementation details. Conversion between the versioned CRD status and
private `AgentState` belongs inside `KubernetesAgentStore`.

### State Placement

| State | Target owner |
|---|---|
| Set UID, replica ID, PVC UID and storage generation | CRD spec |
| Current process session and predecessor retirement | CRD status |
| Admitted authority, policy, epoch and PC/CC configuration | CRD status |
| Role and intended access postcondition | CRD status |
| Pending effect intent, stage and exact operation ID | CRD status |
| Reconfiguration stage and retained idempotent result | CRD status |
| Removal, switchover and retirement evidence | CRD status |
| Build necessity, target and authorized boundary | CRD status |
| Historical acknowledgement policy and certified-progress receipts | CRD status |
| Provisional authority, receiver/source fences and destructive-work receipts | CRD status |
| Promotion continuation and completion evidence | CRD status |
| Partial copy cursor, peer session and send window | Runtime memory |
| Application applied and committed progress | `DurableState` |
| Retained application operations and copy bytes | Application provider |
| Load metrics and ordinary observations | Existing reporting status or events |

Only recovery-relevant build intent remains durable. A copy-stream cursor or
engine-private progress record is not moved from SQLite to the CRD; it is
removed under the stateless-default-replicator design.

Application progress and control-plane certificates that refer to progress are
different. The provider owns WAL or operation history and its applied and
committed boundaries. The generic agent owns bounded evidence proving which
historical policy could acknowledge those boundaries and which exact witnesses
certified them. This preserves the PostgreSQL stateless-metadata contract
without making ordinary SQL acknowledgement perform a CRD write per
transaction.

## Kubernetes API Access

### Client Construction

The runtime Pod uses an in-cluster Kubernetes client initialized from its
bound ServiceAccount token, namespace and cluster CA. The resource name and
expected identity are supplied through trusted Pod configuration generated by
the controller. The runtime performs an exact-name `GET`; it does not discover
its durable state by listing resources.

Conceptually:

```text
Replica agent
  -> GET /apis/operator.kuberic.io/v1alpha1/namespaces/<ns>/
         replicaruntimestates/<name>
  -> validate CRD UID, Set UID, replica ID, PVC UID and storage generation
  -> replace /status with the observed resourceVersion
```

The Pod does not receive `create`, `delete` or spec-update permission for
`ReplicaRuntimeState`.

### Reads

Recovery and every transition begin from a fresh API read. A local cache may
reduce repeated decoding and may support diagnostics, but it is never durable
authority.

Watches are notification mechanisms only. A watch event may trigger
reconciliation, but a safety decision uses a validated object read and a
conditional update. Watch closure, compaction or event loss cannot imply a
transition.

### Writes

The store replaces the complete status aggregate with the
`metadata.resourceVersion` returned by the read. A matching resource version is
the compare-and-swap precondition corresponding to the current SQLite
transaction lock.

JSON merge patches that omit unrelated fields and server-side apply field
ownership are not the primary transition mechanism. The agent state machine
must validate and atomically publish one coherent next aggregate rather than
allow independent field managers to assemble authority.

For each `AgentStore` operation:

1. Read the current resource.
2. Validate CRD and storage identity.
3. Validate the active process session.
4. Validate the operation ID, expected stage and immutable command content.
5. Derive one complete next status.
6. Replace status using the observed `resourceVersion`.
7. Return success only after the API server confirms the update.

An HTTP `409 Conflict` means another update won. The operation reloads the
object and reevaluates its semantic preconditions. It must not blindly apply
the previously calculated status to the newer object.

### Ambiguous API Results

A timeout, connection loss or unavailable response after sending an update is
an ambiguous completion, like losing a SQLite commit acknowledgement. The
store assigns every mutation a session-scoped monotonic mutation sequence and
enters `CommitUnknown`. No dependent effect or transition may execute while
that mutation remains unresolved.

The store resolves the mutation through strongly consistent reads comparing:

- mutation sequence;
- operation ID;
- immutable effect or command identity;
- expected predecessor stage;
- target stage;
- process session;
- resulting authority and receipt.

If the exact target state is present, the operation returns its idempotent
committed result. If the predecessor remains, it may retry the conditional
transition with the same mutation identity. If a later compatible state proves
that the mutation committed and was subsequently advanced, the retained
mutation floor and operation identity still establish success. Any
incompatible state is an explicit durable conflict.

An unavailable read-back keeps the operation in `CommitUnknown`; it is not
reported as failure or retried as a new operation. Request cancellation does
not retract a request already sent to the API server. Tests must cover an
initial predecessor read followed by a delayed commit of the timed-out update.

Mutation and command sequences are separate domains:

- a **command sequence** is allocated monotonically per replica by the
  controller, survives process replacement and participates in retention and
  permanent rejection;
- a **mutation sequence** is allocated monotonically within one active
  session, forms the mutation identity `(session, mutationSequence)` and is
  used only to resolve conditional API writes.

`compactedThroughCommandSequence` applies only to controller commands and
their retained results. It does not prove that an arbitrary session mutation
succeeded. A `CommitUnknown` mutation remains represented by the exact pending
operation and `lastSessionMutation` until it is resolved; neither the command
result nor its operation identity may be compacted while an ambiguous mutation
can refer to it.

## Atomic Transition Model

The CRD status is one serialized replica-agent aggregate. A single status
replacement may atomically:

- move a pending effect from intent committed to applied;
- install a resulting authority and role;
- update access postconditions;
- retain the operation result;
- clear the pending effect;
- advance the durable effect sequence.

This matches the current property that these fields change together in one
SQLite transaction.

The application side effect remains external to both stores. It continues to
use an intent/effect/completion protocol:

```text
status: IntentCommitted
    -> execute the exact runtime or application effect
status: EffectApplied
    -> validate the observed postcondition and receipt
status: Completed with the new admitted aggregate
```

Restart at any point reads the stage and either executes, observes, completes
or compensates the same operation. A newer process never infers completion
only from desired controller state.

## Process Sessions, Claims and Execution Fencing

Durable replica identity and live execution identity are different:

- the CRD and PVC binding identify the durable storage slot;
- `activeSession` identifies the process generation currently allowed to
  mutate its workflow;
- a controller-created Kubernetes `Lease` provides liveness information;
- a controller-issued replacement authorization proves that execution fencing
  completed before a successor may claim the slot.

A Lease is not authority history. Expiration does not erase admitted
configuration, complete an effect or prove that a predecessor stopped. It is
never sufficient by itself to authorize replacement.

### Ordinary Mutations

Every ordinary status mutation is bound to:

- the exact active session identity;
- its Pod UID;
- the storage generation;
- the next mutation sequence;
- the expected status resource version.

The admission layer rejects a mutation if the authenticated bound
ServiceAccount token does not identify the active Pod or if the new object
changes `activeSession` through an ordinary transition. A stale process cannot
reread a newer resource and continue because its authenticated Pod/session
pair is no longer admitted.

### Execution Fence

Replacing a session requires proof that the predecessor cannot continue
application execution, not merely proof that it cannot update the CRD.
Metadata fencing and execution fencing are distinct.

For a healthy node, the controller obtains a hard fence by:

1. removing the Pod from client and peer routing;
2. requesting graceful termination of the exact Pod UID;
3. observing kubelet-confirmed container termination and Pod deletion without
   force deletion;
4. observing detachment or release of the predecessor's storage attachment;
5. recording that exact evidence in the replacement authorization.

If the node or kubelet is unavailable, Pod deletion and Lease expiry are not
proof of termination. Replacement remains blocked until an infrastructure
fence has made the node unable to execute, such as provider-confirmed power
fencing together with CSI storage fencing. A force-deleted Pod on an
unreachable node is not replaceable without that evidence.

Peer protocols reject acknowledgements, copy traffic and topology evidence
from the predecessor process session. Client routing removal prevents new
connections, but existing direct connections are considered unsafe until the
hard execution fence completes. The successor cannot grant access, attach the
storage or continue a provisional operation before that fence.

Kuberic supports exactly one authority-bearing runtime process for the entire
Pod lifetime. Same-Pod restart of the runtime or an application child is not
supported because Pod authentication cannot distinguish process generations.
The authority-bearing Pod uses `restartPolicy: Never`; any process exit makes
the Pod terminal. Recovery requires controller-driven creation of a new Pod UID
after the predecessor passes the hard-fence protocol. A sidecar or node-level
supervisor may report diagnostics, but it cannot restart the authority process
or issue fencing evidence.

### Replacement Authorization

After fencing, the controller writes a `replacement` authorization into spec
containing:

- monotonic replacement generation and unpredictable claim nonce;
- expected predecessor session and Pod UID;
- exact successor Pod UID;
- Set, replica and storage generation;
- the hard-fence certificate;
- any nonterminal operation the successor is authorized to continue.

The successor performs a distinct `ClaimSession` transition. Admission checks
the authenticated Pod identity and compares `oldObject`, `newObject` and the
controller-owned authorization. One atomic status replacement:

- consumes the exact replacement generation and nonce;
- installs a fresh random session identity;
- records the successor process generation;
- preserves all pending workflow and safety evidence;
- limits continuation to the remaining phase authorized by the certificate.

The claim cannot change role, access, authority or mark an effect complete.
Replaying an already consumed replacement generation is rejected. A
replacement authorization whose successor has the same Pod UID as its
predecessor is invalid.

On startup, the process:

1. reads and validates the CRD;
2. verifies the exact replacement authorization for its Pod and storage;
3. claims the controller-created Lease without treating it as a fence;
4. performs `ClaimSession`;
5. reconstructs the runtime from admitted status and application state;
6. continues or compensates only the authorized pending operation;
7. opens access only after normal reconciliation.

Every later store mutation is conditional on that exact active session.
Installing a successor session rejects predecessor metadata outcomes; the hard
fence established before the claim rejects predecessor execution outcomes.

### Authority Health and Lease Failure

Lease renewal and agent-store health are independent. Successful Lease renewal
does not prove that the CRD, status subresource or admission webhook remains
available.

While serving, the runtime maintains a bounded authority-serving deadline. It
extends that deadline only after one health cycle completes both:

1. a strongly consistent `GET` of the exact `ReplicaRuntimeState`, validating
   resource identity, storage binding, active session and admitted authority;
2. a resource-version-guarded status heartbeat that passes the normal
   validating admission webhook and updates only `authorityHealth` and the
   session mutation receipt.

The heartbeat is low frequency and is unrelated to application transaction
rate. A `409 Conflict` requires semantic reread and reevaluation. A timeout is
resolved through the normal `CommitUnknown` protocol. A successful Lease
renewal cannot extend the authority-serving deadline.

If either the authority health cycle or Lease renewal cannot complete with
sufficient margin, the private host access-enforcement path closes read/write
application access, stops replication acknowledgements and quiesces new
effects before the deadline. This enforcement includes separately running
PostgreSQL processes and does not depend on a cooperative client connection.
An effect that was already durably admitted remains pending and is reconciled
after connectivity returns; no new irreversible phase begins while health is
expired.

The controller still cannot replace the process until the hard
execution-fencing protocol succeeds.

Ordinary application replication under an already admitted authority does not
write the CRD for every operation. PostgreSQL and other custom replicators must
close write access when the API-backed agent store or execution Lease becomes
unavailable; they may not continue relying on an in-memory copy of certified
authority. No new authority, access grant, workflow transition or process
replacement may be admitted without API connectivity.

## Identity and Admission Security

The runtime must not trust a resource merely because it has the expected name.
Every read and write validates:

- resource UID;
- namespace;
- owning `KubericSet` name and UID;
- replica ID;
- PVC name and UID;
- application storage generation;
- protocol/schema version;
- active Pod UID and process session for mutations;
- admitted operation and configuration generations.

The resource should be owned by the `KubericSet`, not the Pod. Pod replacement
must not delete durable state. Binding the CRD lifetime directly to the PVC is
also insufficient unless finalization prevents accidental loss of retirement
and cleanup evidence.

The status lifecycle is explicit:

- `Uninitialized`: controller-created identity exists but no application
  storage has been admitted;
- `Active`: one admitted storage generation and agent aggregate exist;
- `Terminating`: access is closed and retirement or cleanup is incomplete;
- `Retired`: permanent rejection evidence is complete;
- `Abandoned`: the controller proved hard fencing but storage or the agent was
  lost, so completion is impossible and the generation can never be reused.

Initial status creation requires a controller provisioning authorization bound
to the Set UID, replica ID, PVC UID, storage generation, initial policy and
application-path binding. The first Pod performs `InitializeSession`, which
atomically consumes that authorization, installs the first active session and
moves `Uninitialized` to `Active`. It does not require a predecessor or
replacement authorization. An empty status is never interpreted as an
initialized replica.

A finalizer keeps the resource until the controller has:

1. closed access;
2. completed or terminally resolved pending effects;
3. retired the replica incarnation;
4. processed required removal evidence;
5. authorized deletion of the associated storage generation.

If no agent can complete termination, the controller may move the resource to
`Abandoned` only after the hard execution fence and permanent storage
retirement are proven. It does not fabricate successful effect completion.
Finalizer removal follows `Retired` or `Abandoned`; the finalizer itself is not
an execution fence.

### RBAC

The runtime requires only:

- `get` and, if used, exact-name `watch` for its state object;
- `get`, `patch` or `update` on its status subresource;
- `get`, `patch` or `update` for its controller-created execution Lease.

It must not update spec or delete the object.

Kubernetes RBAC alone may not provide convenient per-Pod isolation for a
dynamically named status object. The implementation must use one of:

1. a per-replica ServiceAccount and Role scoped to the exact resource name;
2. a validating admission policy or webhook that binds the authenticated Pod
   UID, Set UID, replica ID and PVC UID to the target object;
3. a trusted Kuberic persistence gateway that performs the same validation and
   CRD update on behalf of the Pod.

The direct-access design requires bound Pod ServiceAccount tokens and a
validating admission webhook. The supported Kubernetes version must expose the
verified Pod name and UID token claims to that webhook. The webhook validates
authenticated identity, `oldObject`, `newObject`, controller-owned storage
binding, active session and replacement authorization. A shared credential
that permits one compromised replica to update every sibling's authority is
not acceptable.

## Controller and Agent Ownership

The controller and replica agent must not race as field-level co-owners of the
same admitted aggregate.

| Surface | Writer | Meaning |
|---|---|---|
| `ReplicaRuntimeState.spec` | Controller | Stable storage binding and protocol generation |
| `ReplicaRuntimeState.status` | Active replica agent, except narrow controller abandonment | Admitted local authority and workflow state |
| `KubericSet.status` | Controller | Cluster-level accepted topology and observed convergence |
| Execution Lease | Active agent, under controller replacement rules | Live process ownership |
| Application progress | Application provider | Durable application history |

The controller may observe replica status and issue the next exact command.
It cannot directly edit agent status to manufacture completion. Recovery
preserves the existing ordering:

1. controller commits cluster intent;
2. agent durably admits exact local intent;
3. runtime executes the effect;
4. agent durably records the result and admitted postcondition;
5. controller observes the result and advances cluster status.

The admission webhook enforces the allowed actor and field set for each
exceptional transition:

| Transition | Actor and precondition | Allowed status change |
|---|---|---|
| `InitializeSession` | Exact first Pod named by an unused provisioning authorization; lifecycle is `Uninitialized` | Consume provisioning authorization, install initial identity/session and move to `Active` |
| `ClaimSession` | Exact successor Pod named by an unused replacement authorization backed by a hard fence | Consume replacement authorization and replace only session/continuation ownership fields |
| Ordinary mutation | Authenticated active Pod and exact active session | Advance the admitted aggregate without changing lifecycle ownership or session |
| `Abandon` | Controller identity after hard fence and permanent storage retirement; no live active session | Preserve all evidence, record terminal reason and move `Terminating` to `Abandoned` |

The controller has no general status-update bypass. `Abandon` cannot grant
authority, complete an effect successfully, reduce a compaction floor or
remove retained safety evidence.

## State Size and Retention

Kubernetes API storage is not an append-only journal. The durable aggregate
must remain bounded.

- every controller command carries a monotonic per-replica command sequence in
  addition to its immutable operation ID;
- every conditional status write carries a session-scoped mutation sequence;
- status retains `compactedThroughCommandSequence`, and any command or result
  at or below that floor is permanently rejected rather than rediscovered;
- exact completion results remain until the controller durably acknowledges
  them and advances the compaction floor;
- superseded configuration and policy evidence is compacted only after a
  durable certificate proves that no active or recoverable workflow can refer
  to it;
- retirement and storage-generation floors permanently reject reuse without
  retaining every historical operation;
- schema validation bounds membership, identifier lengths, evidence
  cardinality and receipt sizes;
- publish metrics and diagnostics through reporting surfaces rather than
  accumulating them in the authority object.

If independent long-lived operation resources become necessary, they may be
modeled as child CRDs. The parent aggregate must first record the authoritative
operation reference, generation and phase. Safety cannot depend on atomically
updating the parent and child because Kubernetes does not provide that
transaction.

Before admitting an effect, the store reserves enough object budget for the
largest valid completion and compensation receipts for that effect. Admission
fails before irreversible work if that capacity is unavailable. The
implementation enforces an object-size budget below Kubernetes and etcd
request limits and never discards recovery evidence to make an update fit.
Compaction floors, retained evidence and the new command admission are
published in the same atomic aggregate.

## Recovery

### Process Restart

1. Construct the in-cluster Kubernetes client.
2. read the exact `ReplicaRuntimeState`;
3. validate resource, Set, replica and storage identity;
4. verify the controller-issued replacement and hard-fence certificate;
5. atomically claim a fresh authorized process session;
6. load the admitted agent aggregate;
7. open the application state provider;
8. reconstruct the default or custom replicator;
9. reconcile pending agent effects and application progress;
10. replay admitted role, epoch and configuration;
11. open access only after authority and application state agree.

No local metadata file is consulted. A missing, deleted, malformed or
identity-mismatched resource closes the replica and requires controller-driven
recovery; it is not treated as a fresh replica.

### API Server Unavailability

A process that has not loaded and validated its durable aggregate cannot
start serving. A process that cannot durably record a required transition
cannot execute or acknowledge that transition.

An already running process closes application access and replication
acknowledgements before the earlier of its Lease deadline and independent
authority-serving deadline. It may finish only local shutdown and observation
work that cannot create a new durable external effect. Reconnection must
complete both authority-health checks and verify that its session and
authority remain current before reopening.

### CRD Loss and etcd Disaster Recovery

Loss of the CRD is loss of generic control-plane durability. The runtime must
not reconstruct authority from Pod labels, controller desires or application
progress alone.

Automatic reuse of existing PVCs after CRD loss or etcd rollback is
unsupported. `resourceVersion`, finalizers, Leases and generations stored in
the same restored etcd snapshot cannot prove that newer authority or
retirement evidence once existed.

Disaster recovery is therefore an offline administrative procedure:

1. stop the Kuberic controller and all replica workloads;
2. hard-fence every node or process that could retain access to application
   storage or client/peer networks;
3. restore or recreate the Kubernetes control plane;
4. prohibit all restored Kuberic Pods from starting;
5. inspect surviving application storage offline;
6. either rebuild replicas from a selected authoritative source or import
   selected storage under a new `KubericSet` UID, replica identities and
   storage generations;
7. create fresh `ReplicaRuntimeState` objects through ordinary provisioning;
8. reopen only after application-level lineage and progress reconciliation.

A restored old CRD never directly authorizes an existing PVC. In-place restore
of application bytes also requires a new storage generation and explicit
application lineage validation; PVC UID equality does not prove byte identity.
Permanent retirement is guaranteed within one live control-plane history.
Across etcd rollback, safety depends on the mandatory offline fencing and
reauthorization procedure rather than automatic rollback detection.

## Relationship to Application Durability

Removing the agent SQLite database does not move application durability into
the CRD.

The stateless default-replicator design still requires the provider to expose:

- durable applied tail;
- durable committed boundary;
- exact retained operations above the committed boundary;
- deterministic copy state;
- explicit false-progress correction or rebuild behavior.

The agent CRD stores which authority may interpret those facts. It does not
store or duplicate the facts themselves.

For PostgreSQL, PGDATA remains PostgreSQL's durable state and the custom
replicator remains restart-stateless. The generic CRD aggregate, however,
persists the bounded control evidence that PGDATA cannot reconstruct:

- historical acknowledgement policy and its exact members and quorum;
- certified-progress boundaries and witness receipts;
- receiver/source fences;
- provisional authority and generation-safe continuation;
- destructive-work phases and receipts;
- promotion continuation and completion evidence.

These records are exposed through private generic store capabilities used by
the custom-replicator host; they do not change `Replicator`,
`PrimaryReplicator` or `StateProvider`. Evidence is updated at policy,
certification, promotion and destructive-work boundaries, not for every SQL
transaction. Loss of API-backed agent-store access closes PostgreSQL writes
under the same rule as other authority-bearing transitions.

For SQLite applications, the SQLite application database or a future V2
transactional log remains the application recovery authority. Neither
application may create a replacement private agent metadata database.

## Alternatives

### Keep Agent SQLite

This retains local atomicity, low latency and startup independence from the API
server. It also retains a second persistent lifecycle on every PVC and
requires explicit backup, corruption, schema migration and filesystem
semantics for runtime metadata.

This remains a valid alternative architecture. It is not a supported source
for conversion into the CRD-backed deployment.

### Controller-Mediated Store

The runtime could call a Kuberic persistence service, which validates and
updates the CRD. This centralizes Kubernetes credentials and admission logic
but introduces a new protocol, service availability dependency and
ambiguous-request recovery boundary.

The `AgentStore` interface permits this implementation later. Direct API
access is simpler initially because Kubernetes already supplies conditional
updates and durable storage.

### One CRD per Record

Separating authority, effects, builds and receipts into independent resources
would allow narrower updates, but it would lose the current single-transaction
aggregate. Cross-resource generation protocols would be required for nearly
every transition.

The default design therefore uses one aggregate per storage slot and adds
child resources only for independently recoverable, bounded workflows.

### ConfigMaps or Leases

ConfigMaps do not provide a typed status contract or appropriate ownership
semantics. Leases are intentionally small liveness records. Neither is a
replacement for the agent authority aggregate.

## Implementation Plan

This design has no compatibility or migration mode. It applies only to fresh
deployments created with the Kubernetes-backed metadata protocol. Existing
SQLite-backed replicas are not upgraded, imported or started by the new
runtime.

### Phase 1: Finalize State Ownership

- complete the stateless-default-replicator ownership changes;
- move application progress and retained operations to `DurableState`;
- remove local-write recovery and engine-private build continuation;
- produce a disposition matrix covering all current tables:
  `agent_state`, `schema_migrations`, `replica_authority`,
  `runtime_lifecycle`, `replication_progress`, `local_writes`,
  `build_authority` and `build_progress`;
- include every implementation of `AgentStore`, `ReplicaAuthorityStore`,
  `ReplicationProgressStore`, `LocalWriteJournal`, `BuildAuthorityStore` and
  `BuildProgressStore`, including lifecycle-key families and cross-record
  atomic checks;
- assign each record to the CRD aggregate, application durability or deletion
  with a replacement safety proof;
- retain build-selection and retirement-started fences unless the matrix proves
  an equivalent replacement; they are not presumed to be disposable cursors;
- define private generic capabilities for PostgreSQL safety evidence;
- define bounded sequences, compaction floors and worst-case receipt sizes.

The detailed row/key mapping is deliberately deferred to this Phase 1
artifact because it depends on the final stateless-replicator implementation.
No CRD schema implementation or SQLite deletion may begin until the matrix is
reviewed and accepted as the Phase 1 exit criterion.

### Phase 2: Define Kubernetes Authority

- add the versioned `ReplicaRuntimeState` CRD in a shared API crate;
- define lifecycle, provisioning and replacement-authorization schemas;
- define OpenAPI limits for identities, membership and evidence;
- implement the validating admission webhook;
- provision per-replica ServiceAccounts, exact-resource RBAC and
  controller-created Leases;
- define hard-fence certificates for healthy-node termination and
  infrastructure-fenced node loss.

### Phase 3: Implement `KubernetesAgentStore`

- implement every store operation with fresh read, semantic validation and
  resource-version-guarded whole-status replacement;
- implement distinct provisioning, `ClaimSession` and ordinary-mutation
  transitions;
- implement mutation sequences and `CommitUnknown` reconciliation;
- reserve completion capacity before effect admission;
- keep coordinator and runtime code independent of Kubernetes API types.

### Phase 4: Fresh-Cluster Integration

- install the CRD definition and admission webhook before storage
  provisioning;
- create the PVC, observe its UID, then create the corresponding
  `ReplicaRuntimeState` instance in `Uninitialized`;
- create the controller-owned Lease and first-Pod provisioning authorization;
- admit storage and application paths through the provisioning transition;
- configure authority-bearing Pods with `restartPolicy: Never`;
- start only binaries that understand the exact protocol version;
- reject any PVC containing `.kuberic/agent.sqlite3`;
- never include SQLite store code or a backend-selection fallback in the new
  deployment image.

### Phase 5: Remove SQLite

- delete runtime SQLite schema, startup and private store implementations;
- remove the `.kuberic` metadata directory from the storage contract;
- retain application-owned SQLite databases where SQLite is the application;
- document that old clusters must be redeployed and rebuilt rather than
  upgraded.

## Safety Invariants

1. A resource name alone never establishes replica or storage identity.
2. Only the active process session may advance agent status.
3. Every transition validates an expected predecessor and publishes one
   coherent next aggregate.
4. A resource-version conflict causes semantic reevaluation, not blind retry.
5. An ambiguous API result is resolved by exact operation identity and
   read-back.
6. Controller desired state cannot directly grant role or access.
7. A Lease cannot replace admitted authority, effect history or retirement
   evidence.
8. CRD loss or API unavailability fails closed for startup and authority
   transitions.
9. Application bytes and replication logs are never stored in the agent CRD.
10. Application access opens only after agent authority and provider state are
    reconciled.
11. A successor claim requires a controller authorization backed by a hard
    execution fence; session replacement alone is not a fence.
12. A successor session rejects predecessor metadata, peer and execution
    outcomes.
13. Within one live control-plane history, retired replicas and storage
    generations cannot be reused.
14. After etcd rollback, no restored Kuberic object or existing PVC is used
    until the offline fencing and reauthorization procedure completes.
15. The aggregate remains bounded without discarding required recovery
    evidence.
16. Capacity for the largest valid completion or compensation is reserved
    before admitting irreversible work.
17. One replica's credentials cannot mutate another replica's authority.
18. A deployment has exactly one metadata backend from initial provisioning;
    the CRD-backed runtime contains no SQLite fallback.
19. An authority-bearing process is never restarted within the same Pod UID.
20. Serving authority expires unless both Lease renewal and the independent
    CRD/status-admission health cycle succeed within their deadlines.

## Testing Strategy

### Store Contract

Run the existing store behavior suite against `KubernetesAgentStore` and add
coverage for every private capability formerly implemented by `SqliteStore`:

- begin, retry, apply and complete effects;
- reject mutated operation IDs;
- retain exact idempotent results;
- advance only from the expected coordinator stage;
- preserve retirement and removal fences;
- validate identity on every operation;
- preserve PostgreSQL certification and destructive-work evidence;
- verify every former SQLite record has an explicit owner.

### Concurrency

- race two updates from the same resource version;
- replace a process session while an old update is in flight;
- retry after `409 Conflict`;
- verify a stale session cannot reread and continue;
- verify admission rejects sibling-resource updates;
- reject `ClaimSession` without an exact unused replacement authorization;
- reject ordinary mutations that change the session;
- reject a replacement authorization that reuses the predecessor Pod UID;
- compact and restart watches without changing decisions.

### Ambiguous Results

- commit the status update and drop the HTTP response;
- drop the request before it reaches the API server;
- time out while the API server is persisting the update;
- commit after an initial read-back still observes the predecessor;
- cancel the caller while the API request remains in flight;
- keep dependent work blocked while read-back is unavailable;
- verify read-back distinguishes predecessor, later compatible completion and
  conflict.

### Restart Boundaries

Crash before and after every durable stage of:

- initialization;
- authority admission;
- role and access changes;
- failover and switchover;
- scale-up and build authorization;
- scale-down, removal and retirement;
- provisional custom-replicator operations.

Each restart must acquire a new process session, recover the same operation or
compensate it, and reject predecessor completion.

### Execution Fencing

- replace a healthy Pod only after exact-UID process-tree termination and
  storage release;
- suspend the predecessor process across Lease expiry;
- partition the predecessor node from the API server while retaining client
  and storage access;
- verify force deletion does not authorize a successor;
- require infrastructure power and storage fencing for an unreachable node;
- reject predecessor client work, replication acknowledgements and effect
  completion after successor claim;
- verify the authority container is not restarted under the same Pod UID;
- verify any same-Pod replacement claim is rejected.

### Kubernetes Failures

- API server unavailable at startup;
- API server unavailable before and after intent commit;
- watch closure and resource-version compaction;
- admission webhook unavailable;
- Lease renewal failure;
- Lease renewal succeeds while CRD reads fail;
- Lease renewal and CRD reads succeed while status admission fails;
- authority health heartbeat response is lost after commit;
- verify serving closes when either independent deadline expires;
- accidental CRD deletion blocked by finalization;
- status growth approaching its configured budget;
- completion receipt at the maximum reserved size.

### Provisioning and Finalization

- initialize from an exact provisioning authorization;
- verify `InitializeSession` installs the first session without a predecessor;
- reject an empty status as an established replica;
- reject a PVC containing runtime SQLite metadata;
- move through `Active`, `Terminating`, `Retired` and `Abandoned`;
- verify finalizer removal requires retirement or hard-fenced abandonment;
- verify abandonment does not manufacture successful effect completion.

### Retention

- acknowledge a result and atomically advance the compaction floor;
- permanently reject delayed commands below the floor;
- verify session mutation sequences reset only with a new session and never
  advance the command compaction floor;
- retain ambiguous operation evidence until `CommitUnknown` resolves;
- reserve maximum completion and compensation size before intent admission;
- reject oversized intent before executing an effect;
- compact historical policy only after its supersession certificate;
- run unbounded command retries without unbounded object growth.

### Disaster Recovery

- restore etcd older than PVC/application state and verify no workload starts;
- reject restored CRDs as direct authority for existing PVCs;
- require hard fencing of all former writers;
- import selected application storage only under new Set, replica and storage
  identities;
- validate application lineage before reopening.

## Operational Consequences

- Runtime startup and authority transitions depend on Kubernetes API
  availability.
- API latency is acceptable only because ordinary application replication does
  not update the agent CRD per operation.
- The controller must install and maintain the CRD, RBAC, admission enforcement
  and finalizer behavior.
- etcd restore does not resume existing PVC-backed replicas; it triggers the
  offline fencing and fresh-identity recovery procedure.
- PVC or etcd backup alone is insufficient for complete cluster recovery.
- Local scratch may still be used for logs, sockets and disposable caches, but
  it is never a recovery authority.

## Limitations and Future Work

The proposal does not define a general distributed transaction between etcd
and application storage. Kuberic continues to use explicit intent, effect,
observation and completion stages across that boundary.

Direct Kubernetes access increases the importance of per-replica credential
isolation and admission validation. A future persistence gateway may improve
that boundary if it preserves the same conditional-update and ambiguous-result
semantics.

Moving desired commands into the per-replica CRD could eventually replace part
of the direct controller-to-agent command protocol, but that is a separate
change. It should be considered only after the CRD-backed store has proven the
same recovery behavior as the current agent store.

The design deliberately does not turn Kubernetes into a transactional
replication log. Applications needing Service Fabric V2-like recovery still
require an explicit transactional log and provider checkpoint model outside
the agent CRD.

## Related Documents

- [Stateless Default Replicator](../features/kuberic/stateless-default-replicator.md)
- [Service Fabric Alignment and Runtime Simplification](../features/kuberic/service-fabric-alignment.md)
- [Level-Triggered Operator](../features/kuberic/level-triggered-operator.md)
- [PostgreSQL Stateless Metadata](../features/postgres/stateless-metadata.md)
- [SQLite V2 Transactional Replicator](../features/sqlite/v2-transactional-replicator.md)
