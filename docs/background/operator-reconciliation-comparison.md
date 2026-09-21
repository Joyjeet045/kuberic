# Operator Reconciliation Architecture Comparison

This document compares several storage and SQL Server Kubernetes operators with
Kuberic's proposed
[level-triggered operator design](../proposal/level-triggered-operator-design.md).
The goal is to identify useful precedents for the controller shape and, more
importantly, determine whether any of these operators demonstrate the complete
replication-safety architecture proposed for Kuberic.

The comparison covers:

- [CloudNativePG](https://github.com/cloudnative-pg/cloudnative-pg)
- [RustFS Operator](https://github.com/rustfs/operator)
- [DH2i DxOperator](https://support.dh2i.com/dxoperator/v1.0.76.0/guides/dxoperator-qsg/)
- [DotKube KubeSQLServer Operator](https://github.com/DotKube/KubeSQLServer-Operator)
- [SQL on Kubernetes Operator](https://github.com/nocentino/sql-on-k8s-operator)

The source links below are pinned to the revisions reviewed on 2026-09-19.
DxOperator's controller implementation is proprietary, so its conclusions are
limited to publicly available documentation, CRDs, and Helm resources.

---

## Executive Conclusion

All five operators provide some evidence for using a conventional Kubernetes
watch, observe, converge, and requeue loop. None demonstrates the complete
Kuberic proposal.

CloudNativePG is the strongest precedent for the proposal's broad architecture.
It combines compact current/target-primary markers, PostgreSQL and
instance-manager observations, replica-side promotion and demotion,
fail-closed quorum checks, optimistic status concurrency, and layered
split-brain protection. It still does not implement Kuberic's general
replica-incarnation fencing, Service Fabric-style Previous/Current
Configuration quorum rules, replica-agent reconfiguration state, or pure
evaluator boundary.

RustFS and `sql-on-k8s-operator` are the next closest comparisons: both
repeatedly observe application state in addition to Kubernetes resources. They
still keep multi-step protocol ownership in the controller, perform multiple
authority-changing mutations without a common evaluation boundary, and lack
Kuberic's Service Fabric-style epoch, PC/CC, and replica-agent transition
contract.

DxOperator has the most relevant coarse protocol split. The Kubernetes operator
deploys and integrates the workload, while DxEnterprise owns runtime cluster
management and automatic failover. Public evidence does not reveal whether the
operator or DxEnterprise uses operation identities, durable retained results,
postcondition-driven recovery, or an authority-action evaluation boundary.

KubeSQLServer is a straightforward provisioning operator. It deliberately
deploys one SQL Server replica and does not implement an availability-group or
replication protocol, so it is not evidence for the difficult safety properties
in the Kuberic proposal.

The strongest combined lesson is:

> A Kubernetes-style reconciliation envelope is common. Safe reconstruction of
> replicated authority after ambiguous effects is not. Kuberic still needs to
> verify its replica-agent durability, fencing, PC/CC quorum behavior, and
> fail-closed evaluation through invariant and fault-injection tests.

---

## Evaluation Criteria

The Kuberic proposal is more specific than ordinary declarative resource
reconciliation. It requires:

1. Rebuilding a normalized observation snapshot on every pass.
2. Reconstructing the strongest safely provable topology and authority.
3. Separating pure evaluation from Kubernetes and replica effects.
4. Returning `Stable`, `Apply`, `Execute`, `Wait`, or `Unsafe`.
5. Issuing at most one authority-changing fenced command before re-observing,
   while allowing batches of independent Kubernetes convergence changes.
6. Persisting compact transition intent rather than a per-step workflow cursor.
7. Recovering from operator and replica-agent restarts using durable evidence.
8. Using Service Fabric-style Previous/Current Configuration, epochs, replica
   identities, LSN progress, catch-up, and replica-agent reconfiguration state.

These requirements are described in the proposal's
[design principles](../proposal/level-triggered-operator-design.md#design-principles),
[replica-agent contract](../proposal/level-triggered-operator-design.md#replica-agent-contract),
[pure evaluation model](../proposal/level-triggered-operator-design.md#pure-evaluation),
and [ambiguous-effect handling](../proposal/level-triggered-operator-design.md#ambiguous-effects).

---

## Comparison Matrix

| Property | CloudNativePG | RustFS | DxOperator | KubeSQLServer | SQL on Kubernetes |
|---|---|---|---|---|---|
| Watch/requeue controller | Strong match | Strong match | Not publicly provable | Strong match | Strong match |
| Re-observes application state | Strong | Partial/strong | Runtime state is owned by DxEnterprise, operator behavior unknown | Limited | Strong |
| Normalized whole-system snapshot | Partial; no immutable snapshot boundary | No | Unknown | No | No |
| Compact CR status | Strong match for primary transitions | Mixed | Yes, but extremely coarse | Yes | Mostly |
| Pure evaluator/effect separation | No | No | Unknown | No | No |
| One authority-changing command per observation cycle | Partial | No general boundary | Unknown | Not applicable to HA | No |
| Data-plane protocol participant | PostgreSQL instance manager | RustFS APIs, but no general agent contract | DxEnterprise | None | SQL Server plus a small `preStop` participant |
| SF-style replica-agent reconfiguration state | PostgreSQL state provides a narrower analogue | Selected lifecycle checkpoints only | Unknown | No | No |
| Incarnation/PC/CC/epoch fencing | Layered primary fencing, but no Service Fabric PC/CC protocol | No equivalent | Runtime quorum exists; command fencing unknown | No | Partial SQL-role fencing only |
| Fail-closed ambiguous-effect result | Partial through durable PG postconditions and quorum checks | No general mechanism | Unknown | No | No |
| Useful precedent for Kuberic | Closest broad architectural precedent | Controller convergence and postcondition checks | Coarse operator/runtime ownership split | Simple declarative provisioning | SQL-state observation and guarded retries |

`Strong match` in this table means a match to one aspect of the proposal, not
that the complete architecture is equivalent.

---

## CloudNativePG

**Reviewed revision:** [`0d65c259`](https://github.com/cloudnative-pg/cloudnative-pg/commit/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da)

The architectural claims were subsequently revalidated against the local
checkout at `8f824a4aa60050ab0b85c48fdb4d32452ef4e3db`. The links remain pinned to
the original reviewed revision; revision-specific differences are called out
below.

See also the more detailed
[CloudNativePG architecture study](cloudnative-pg-architecture.md).

### Strong Architectural Matches

CloudNativePG is the closest public example of the permanent reconciliation
model proposed for Kuberic:

- It watches the Cluster and owned Pods, Jobs, Services, PVCs, PDBs, and
  Leases, along with referenced and related resources
  ([`cluster_controller.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/internal/controller/cluster_controller.go#L1319-L1373)).
- It lists Kubernetes resources and queries every instance manager for
  PostgreSQL role, WAL positions, timeline, readiness, fencing, errors, and
  instance-manager session identity
  ([`cluster_status.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/internal/controller/cluster_status.go#L130-L175),
  [`status.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/pkg/postgres/status.go#L35-L99)).
- Cluster status stores compact `CurrentPrimary` and `TargetPrimary`
  coordination markers, transition timestamps, and user-visible phase data
  ([`cluster_types.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/api/v1/cluster_types.go#L893-L955)).
- The operator selects and persists a target, while the target instance
  manager acquires the primary Lease, promotes PostgreSQL if needed, and
  writes the current-primary marker
  ([`instance_controller.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/internal/management/controller/instance_controller.go#L1282-L1337)).
- Former primaries derive shutdown, rewind, and demotion work from PostgreSQL
  state and the current/target markers
  ([`instance_startup.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/internal/management/controller/instance_startup.go#L102-L167)).

This is the clearest precedent for Kuberic's proposed split between an
observation-driven Kubernetes controller and a replica-local runtime
participant. The operator persists transition intent, but the instance manager
and PostgreSQL own the local promotion, demotion, rewind, and durable role
state.

### Failover, Quorum, and Fencing

CNPG's failover flow closely resembles the proposal's broad shape:

1. Evaluate optional failover-quorum evidence before initiating failover.
2. Persist a pending-failover target.
3. Remove the old primary from service routing.
4. Wait for WAL receivers connected to the old primary to stop.
5. Rank viable replicas using role, fencing, errors, received LSN, replayed
   LSN, and deterministic naming.
6. Persist the selected target and requeue.
7. Let the target instance manager acquire the Lease and promote.

The transition is implemented in
[`replicas.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/internal/controller/replicas.go#L118-L201),
with candidate ordering in
[`status.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/pkg/postgres/status.go#L286-L342).

When failover quorum is enabled, the primary instance manager publishes
synchronous replication method, standby membership, required count, and
reporting-primary identity in a dedicated status object
([`failoverquorum_types.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/api/v1/failoverquorum_types.go#L45-L78)).
The operator fails closed when that evidence is absent or inconsistent and
permits failover only when `R + W > N`
([`replicas_quorum.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/internal/controller/replicas_quorum.go#L35-L133)).

Split-brain prevention is defense in depth:

- old-primary routing is removed before promotion;
- instances may be explicitly fenced;
- a Kubernetes Lease acts as the primary-election mutex;
- an isolated primary fails liveness when it can reach neither the API server
  nor any peer;
- Lease preemption terminates the instance manager and PostgreSQL.

See
[`primary_lease.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/internal/controller/primary_lease.go#L47-L94),
[`liveness.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/pkg/management/postgres/webserver/probes/liveness.go#L50-L123),
and the Lease
[`runnable.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/internal/cmd/manager/instance/run/lease/runnable.go#L416-L453).

These mechanisms strongly support Kuberic's fail-closed and replica-side
participation principles. CNPG's evidence remains PostgreSQL-specific rather
than a general quorum-attested configuration and epoch contract.

### Restart and Ambiguous-Effect Recovery

CNPG demonstrates meaningful recovery from durable postconditions:

- the designated target acquires the primary Lease in instance reconciliation
  before promotion; this is not a universal pre-PostgreSQL-start gate;
- an empty current-primary marker can be repaired from durable local role
  state;
- a former primary reconstructs rewind and demotion from PostgreSQL data plus
  the current/target markers;
- if promotion completed but its status patch was lost, the next pass observes
  `IsPrimary`, skips promotion, and repairs `CurrentPrimary`.

See
[`instance_startup.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/internal/management/controller/instance_startup.go#L40-L167)
and the promotion path in
[`instance_controller.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/internal/management/controller/instance_controller.go#L1301-L1374).

The central cluster-status helper uses resource-version optimistic locking and
retries conflicts
([`patch.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/pkg/resources/status/patch.go#L34-L80)).
Some instance-side primary publication paths use ordinary merge patches, so
this is not universal across all status writes.

This validates observation-based recovery from a lost response for operations
whose results are naturally durable in PostgreSQL. It does not provide a
general retained operation result keyed by exact command input.

### Differences from the Kuberic Proposal

CNPG's reconciler is imperative rather than a pure
`evaluate(desired, accepted, observed, now) -> Plan` boundary. It can mutate
status and Kubernetes resources before and after collecting instance-manager
reports
([`cluster_controller.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/internal/controller/cluster_controller.go#L306-L600)).
Failover may patch phase, target markers, and routing labels in one call path.

The promotion operation also has no Kuberic-style:

- stable operation ID;
- expected Pod UID and durable agent generation;
- expected source configuration and epoch;
- retained terminal action result;
- rejection of operation-ID reuse with different inputs.

CNPG's primary Lease identity is the Pod name rather than the combination of
Pod UID and persistent agent generation proposed for Kuberic
([`runnable.go`](https://github.com/cloudnative-pg/cloudnative-pg/blob/0d65c259fd5cc6a9a1e2ae067c0c1db41529f5da/internal/cmd/manager/instance/run/lease/runnable.go#L124-L136)).

### Assessment

CloudNativePG strongly validates:

- a permanent level-triggered loop rather than a permanent durable workflow;
- compact transition intent;
- data-plane participation in primary transitions;
- recovery from durable database and Kubernetes postconditions;
- fail-closed quorum evidence;
- layered routing, Lease, liveness, and instance fencing.

It does not prove that Kuberic can omit its stronger fenced-operation and
action-ledger contract. Kuberic applies similar reasoning to arbitrary
configuration installation, replica add/remove/build, incarnations, epochs,
and ambiguous remote effects, not only PostgreSQL promotion whose result is
durably visible in the data directory.

---

## RustFS Operator

**Reviewed revision:** [`d947750f`](https://github.com/rustfs/operator/commit/d947750fa53788548f7d58c4445e27ac83395892)

### What Matches

RustFS has a conventional level-triggered controller envelope:

- It watches Tenants, referenced Secrets and ConfigMaps, Pods, and owned
  Kubernetes resources, mapping those events into the same reconciliation
  stream
  ([`src/lib.rs`](https://github.com/rustfs/operator/blob/d947750fa53788548f7d58c4445e27ac83395892/src/lib.rs#L224-L321)).
- Every reconcile refetches the current Tenant rather than continuing from
  process-local state
  ([`src/reconcile.rs`](https://github.com/rustfs/operator/blob/d947750fa53788548f7d58c4445e27ac83395892/src/reconcile.rs#L64-L90)).
- It builds desired Kubernetes resources and applies them with server-side
  apply
  ([`phases.rs`](https://github.com/rustfs/operator/blob/d947750fa53788548f7d58c4445e27ac83395892/src/reconcile/phases.rs#L457-L579),
  [`context.rs`](https://github.com/rustfs/operator/blob/d947750fa53788548f7d58c4445e27ac83395892/src/context.rs#L619-L632)).
- Pool reconciliation checks live RustFS pool identity and decommission state
  rather than advancing only from an attempted-step cursor
  ([`pool_lifecycle.rs`](https://github.com/rustfs/operator/blob/d947750fa53788548f7d58c4445e27ac83395892/src/reconcile/pool_lifecycle.rs#L400-L564)).
- Policy provisioning normalizes desired and observed policy data before
  comparing hashes
  ([`provisioning.rs`](https://github.com/rustfs/operator/blob/d947750fa53788548f7d58c4445e27ac83395892/src/reconcile/provisioning.rs#L773-L864)).

These are good precedents for Kuberic's outer controller loop and for deriving
progress from application-visible postconditions.

### What Does Not Match

RustFS does not isolate a pure whole-system planner. The main reconciler
performs cleanup, TLS, ServiceAccount, Service, lifecycle, StatefulSet,
provisioning, and status effects in sequence
([`src/reconcile.rs`](https://github.com/rustfs/operator/blob/d947750fa53788548f7d58c4445e27ac83395892/src/reconcile.rs#L64-L125)).
One pass may apply several Services or reconcile every policy, user, and bucket.

Pool lifecycle status also persists more procedural state than Kuberic's
proposed compact transition marker: phase, start and polling times, completion
time, progress, cleanup phase, and errors
([`status/pool.rs`](https://github.com/rustfs/operator/blob/d947750fa53788548f7d58c4445e27ac83395892/src/types/v1alpha1/status/pool.rs#L119-L223)).

RustFS has useful effect-specific fencing:

- destructive Kubernetes operations use UID and resource-version
  preconditions;
- user provisioning persists an ownership checkpoint containing Tenant UID,
  username, and access-key hash.

It does not expose a general equivalent to Kuberic's replica incarnation,
agent generation, epoch, exact source configuration, and retained operation
result. For example, a pool decommission request sends only a pool ID to the
RustFS API
([`pool_ops.rs`](https://github.com/rustfs/operator/blob/d947750fa53788548f7d58c4445e27ac83395892/crates/rustfs-admin/src/pool_ops.rs#L49-L66)).

### Assessment

RustFS is a strong precedent for Kubernetes resource convergence and selected
postcondition-driven operations. It is not a close precedent for Kuberic's
replication authority, command fencing, pure authority evaluation, or
ambiguous-effect recovery.

---

## DH2i DxOperator

**Reviewed release:** `1.0.76.0`
**Public Helm revision:** [`7a58c147`](https://github.com/dh2i/helm/commit/7a58c14752ebf635dd9d093e989655fb57f5884c)

### Publicly Visible Architecture

DxOperator accepts a `DxEnterpriseSqlAg` resource describing synchronous,
asynchronous, and configuration-only replicas, the availability-group name and
type, and listener/load-balancer configuration
([CRD](https://github.com/dh2i/helm/blob/7a58c14752ebf635dd9d093e989655fb57f5884c/source/dxoperator/crds/crd.yaml#L24-L76)).

Each database Pod contains both DxEnterprise and SQL Server configuration, with
persistent storage configured for both components
([resource template](https://github.com/dh2i/helm/blob/7a58c14752ebf635dd9d093e989655fb57f5884c/source/dxenterprisesqlag/templates/dxenterprisesqlag.yaml#L39-L63)).
DH2i describes the responsibility split as:

- DxOperator deploys and integrates Kubernetes resources.
- SQL Server provides availability-group replication behavior.
- DxEnterprise provides cluster management and automatic failover.

See the
[quick-start introduction](https://support.dh2i.com/dxoperator/v1.0.76.0/guides/dxoperator-qsg/)
and the chart
[README](https://github.com/dh2i/helm/blob/7a58c14752ebf635dd9d093e989655fb57f5884c/source/dxoperator/README.md#L21-L41).

This split resembles Kuberic's goal of moving tightly coupled runtime protocols
out of the Kubernetes controller. It may be the most relevant high-level
precedent among the SQL Server operators reviewed.

### Quorum and Fencing Evidence

DxEnterprise documents a cluster-level quorum mechanism that prevents minority
groups from keeping applications active. When a group lacks the configured
node majority, applications are shut down. For SQL Server external
availability groups, DH2i documents topologies such as three synchronous
replicas or two synchronous replicas plus a configuration-only replica for
automatic failover.

See DH2i's
[quorum considerations](https://support.dh2i.com/docs/v23.0/kbs/sql_server/availability_groups/quorum-considerations-for-sql-server-availability-groups/).
Cluster-level quorum is disabled by default when `quorum.size` is zero, so the
split-brain guarantee depends on configuration.

This is evidence of runtime arbitration, not evidence of Kuberic-style command
fencing. Public material does not show operation IDs, expected incarnation or
configuration authority, retained terminal results, or stale-command
rejection.

### Status and Recovery

The public CRD status is a single coarse value: `Ready`, `Deploying`, or
`Error`
([CRD status](https://github.com/dh2i/helm/blob/7a58c14752ebf635dd9d093e989655fb57f5884c/source/dxoperator/crds/crd.yaml#L2924-L2938)).
It does not expose a large workflow cursor, but it also does not expose accepted
topology, current primary, observed generation, transition identity, or
conditions.

The operator Deployment has no public durable workflow store. Its declared
operator-side volumes are an `emptyDir` and a ConfigMap
([deployment](https://github.com/dh2i/helm/blob/7a58c14752ebf635dd9d093e989655fb57f5884c/source/dxoperator/templates/deployment.yaml#L11-L18),
[`volumes`](https://github.com/dh2i/helm/blob/7a58c14752ebf635dd9d093e989655fb57f5884c/source/dxoperator/templates/deployment.yaml#L104-L110)).
This does not prove that recovery is unsafe: durable state may be held by the
Kubernetes API, SQL Server, or DxEnterprise. The reconstruction algorithm is
not public.

### Evidence Boundary

DH2i publishes charts and deployment configuration but not the DxOperator
controller implementation. RBAC shows that it can list, watch, and update the
CRD and related resources, but RBAC cannot establish its actual watch graph,
requeue behavior, planner structure, or number of effects per reconcile
([cluster role](https://github.com/dh2i/helm/blob/7a58c14752ebf635dd9d093e989655fb57f5884c/source/dxoperator/templates/clusterrole.yaml#L9-L41)).

The following properties therefore remain unknown:

- whether reconciliation is level-triggered internally;
- whether it observes durable runtime postconditions after effects;
- whether commands are idempotent and fenced;
- how it reconstructs interrupted operations after restart;
- whether it performs one or many mutations per pass;
- how it handles a command that succeeded but returned no response.

### Assessment

DxOperator provides evidence for a promising ownership boundary: the operator
deploys and integrates, while a dedicated runtime component owns HA and
failover. It cannot be treated as validation of Kuberic's detailed controller
or replica-agent contract because the relevant implementation is not public.

---

## DotKube KubeSQLServer Operator

**Reviewed revision:** [`6ccdbeee`](https://github.com/DotKube/KubeSQLServer-Operator/commit/6ccdbeeedc6f1e736da71fce8b3c255e557f334b)

### Controller Model

KubeSQLServer uses KubeOps controllers and timed requeues. Its SQLServer
controller ensures a password Secret, ConfigMap, one-replica StatefulSet, and
Services, then requests another reconciliation in five minutes
([`SqlServerController.cs`](https://github.com/DotKube/KubeSQLServer-Operator/blob/6ccdbeeedc6f1e736da71fce8b3c255e557f334b/src/OperatorTemplate.Operator/Controllers/V1Alpha1/SqlServerController.cs#L18-L28)).
Database and external-server controllers similarly use periodic success and
failure requeues.

The statuses are intentionally simple. `SQLServer.status` contains only state
and message, while database resources add a last-checked time
([`SqlServer.cs`](https://github.com/DotKube/KubeSQLServer-Operator/blob/6ccdbeeedc6f1e736da71fce8b3c255e557f334b/src/OperatorTemplate.Operator/Entities/V1Alpha1/SqlServer.cs#L30-L38),
[`Database.cs`](https://github.com/DotKube/KubeSQLServer-Operator/blob/6ccdbeeedc6f1e736da71fce8b3c255e557f334b/src/OperatorTemplate.Operator/Entities/V1Alpha1/Database.cs#L22-L33)).
This avoids a workflow journal but also provides no compact topology or
transition authority.

### Declarative SQL Effects

Database, login, and schema creation use `IF NOT EXISTS` guards, making their
basic desired-existence operations retry-friendly
([`DatabaseController.cs`](https://github.com/DotKube/KubeSQLServer-Operator/blob/6ccdbeeedc6f1e736da71fce8b3c255e557f334b/src/OperatorTemplate.Operator/Controllers/V1Alpha1/DatabaseController.cs#L87-L119)).
The operator also queries SQL for selected observations, such as external
database existence and server connectivity.

Most mutation controllers still mark the resource ready immediately after the
SQL call returns rather than waiting for a later reconcile to prove a durable
postcondition. Exceptions become an error status followed by a retry. There is
no operation identity, retained result, or explicit ambiguous outcome
([`SqlExecutor.cs`](https://github.com/DotKube/KubeSQLServer-Operator/blob/6ccdbeeedc6f1e736da71fce8b3c255e557f334b/src/OperatorTemplate.Operator/Controllers/Services/SqlExecutor.cs#L5-L35)).

### No HA Protocol

This operator explicitly deploys one SQL Server replica and documents
`Single replica only (no HA)`
([SQLServer reference](https://github.com/DotKube/KubeSQLServer-Operator/blob/6ccdbeeedc6f1e736da71fce8b3c255e557f334b/docs-site/docs/reference/crds/sqlserver.md#L175-L180)).
The StatefulSet is hard-coded to one replica and contains no replica agent
([controller](https://github.com/DotKube/KubeSQLServer-Operator/blob/6ccdbeeedc6f1e736da71fce8b3c255e557f334b/src/OperatorTemplate.Operator/Controllers/V1Alpha1/SqlServerController.cs#L61-L95)).

It consequently has no comparison point for:

- primary election or failover;
- quorum-attested topology;
- epoch or configuration authority;
- replica incarnation fencing;
- durable multi-replica operation results.

### Assessment

KubeSQLServer is a useful example of simple, periodically reevaluated
provisioning and retry-friendly `ensure exists` SQL. It does not exercise the
distributed protocol problems that motivate Kuberic's proposal.

---

## SQL on Kubernetes Operator

**Reviewed revision:** [`96eb0285`](https://github.com/nocentino/sql-on-k8s-operator/commit/96eb028572206c87e0f875e044eceee00a7a526c)

### What Matches

This operator has the strongest SQL Server match to the outer Kuberic loop:

- It watches the availability-group CR, owned Kubernetes resources, and
  managed Pods
  ([controller setup](https://github.com/nocentino/sql-on-k8s-operator/blob/96eb028572206c87e0f875e044eceee00a7a526c/internal/controller/sqlserveravailabilitygroup_controller.go#L2915-L2943)).
- It uses periodic and condition-specific requeues during bootstrap,
  failover-delay, and recovery.
- It reconstructs desired Kubernetes resources from the CR on every pass
  ([controller](https://github.com/nocentino/sql-on-k8s-operator/blob/96eb028572206c87e0f875e044eceee00a7a526c/internal/controller/sqlserveravailabilitygroup_controller.go#L301-L396)).
- It queries every SQL Server replica for current availability-group role and
  synchronization health
  ([status observation](https://github.com/nocentino/sql-on-k8s-operator/blob/96eb028572206c87e0f875e044eceee00a7a526c/internal/controller/sqlserveravailabilitygroup_controller.go#L1201-L1252)).
- Bootstrap waits until secondaries are both `SECONDARY` and `CONNECTED`
  instead of treating `JOIN` success as the final postcondition
  ([SQL query](https://github.com/nocentino/sql-on-k8s-operator/blob/96eb028572206c87e0f875e044eceee00a7a526c/internal/sqlutil/agscripts.go#L329-L342),
  [polling](https://github.com/nocentino/sql-on-k8s-operator/blob/96eb028572206c87e0f875e044eceee00a7a526c/internal/controller/sqlserveravailabilitygroup_controller.go#L877-L924)).

SQL Server itself supplies durable availability-group configuration, current
roles, synchronization state, and protection against unsafe synchronized
failover. This makes later re-observation materially stronger than relying on
Pod readiness alone.

### Observation and Authority Gaps

The operator does not construct a validated, immutable observation snapshot.
Some SQL query errors are discarded and represented through empty/default
fields. When no primary can be observed, it may infer that the first
unreachable Pod is the primary
([status inference](https://github.com/nocentino/sql-on-k8s-operator/blob/96eb028572206c87e0f875e044eceee00a7a526c/internal/controller/sqlserveravailabilitygroup_controller.go#L1224-L1281)).

That is weaker than Kuberic's requirement to distinguish unavailable, absent,
stale, and contradictory evidence and return `Wait` or `Unsafe` rather than
infer authority.

The CR status is smaller than Kuberic's classic workflow journal, but it still
contains workflow gates, retry counters, and timers such as
`InitializationComplete`, bootstrap attempts, diagnostics failures, and the
failover start time
([status schema](https://github.com/nocentino/sql-on-k8s-operator/blob/96eb028572206c87e0f875e044eceee00a7a526c/api/v1alpha1/sqlserveravailabilitygroup_types.go#L248-L329)).
It contains no operation ID, source configuration identity, selected failover
authority, replica incarnation, or fencing token.

### Controller-Owned Protocols

There is no durable replica-agent protocol. The controller runs `sqlcmd`
inside Pods through Kubernetes exec
([`executor.go`](https://github.com/nocentino/sql-on-k8s-operator/blob/96eb028572206c87e0f875e044eceee00a7a526c/internal/sqlutil/executor.go#L36-L124)).
It directly owns certificate exchange, endpoint configuration, availability
group creation, replica joins, convergence polling, failover, replica reseating,
and endpoint recovery.

A Pod `preStop` script is a limited data-plane participant: it queries local
SQL state, selects a connected secondary, and initiates planned failover
([`ag_helpers.go`](https://github.com/nocentino/sql-on-k8s-operator/blob/96eb028572206c87e0f875e044eceee00a7a526c/internal/controller/ag_helpers.go#L59-L98)).
This is much narrower than the durable coarse-operation coordinator in the
Kuberic proposal.

### Fencing and Ambiguous Effects

The implementation has useful local guards:

- several SQL objects are created with `IF NOT EXISTS`;
- availability-group creation is preceded by an existence check;
- replica joins check local HADR state;
- SQL Server rejects a synchronized failover when the target is not suitable.

These guards do not carry Kuberic's complete replica identity, Previous/Current
Configuration, epoch, and role-assignment authority.

After a timeout, Kubernetes exec cannot distinguish a command that did not run
from one that succeeded but lost its response. Later reconciliation often
rediscovers SQL role state, but there is no durable action ledger or explicit
`Unsafe` quarantine. A generic failover error requeues without persisting a
target operation identity, and successful failover updates status before a
fresh role observation
([failover handling](https://github.com/nocentino/sql-on-k8s-operator/blob/96eb028572206c87e0f875e044eceee00a7a526c/internal/controller/sqlserveravailabilitygroup_controller.go#L2596-L2667)).

Some recovery escalation state is also process-local, including endpoint
restart cooldowns and retry timing
([controller state](https://github.com/nocentino/sql-on-k8s-operator/blob/96eb028572206c87e0f875e044eceee00a7a526c/internal/controller/sqlserveravailabilitygroup_controller.go#L54-L67)).
An operator restart resets that state.

### Assessment

`sql-on-k8s-operator` is a valuable example of combining Kubernetes watches
with repeated observation of durable SQL Server availability-group state. It
is not an implementation of the proposed Kuberic architecture because the
controller owns multi-step protocols, performs many effects per pass, relies
partly on volatile recovery state, and has no general fenced operation or
ambiguous-effect ledger.

---

## Implications for Kuberic

### Retain the Level-Triggered Controller Envelope

CloudNativePG, RustFS, KubeSQLServer, and `sql-on-k8s-operator` all reinforce
the value of ordinary Kubernetes controller mechanics:

- watch the desired resource and owned/dependent objects;
- periodically requeue when an external system has no watch interface;
- reconstruct desired resources from the current spec;
- observe application state rather than relying only on Pod readiness;
- project conditions and health into status.

This part of the Kuberic proposal is conventional and well supported.

### Make the Runtime Ownership Boundary Explicit

DxOperator and CloudNativePG suggest the most promising division of labor:

- the Kubernetes operator selects a target and supplies authority;
- a runtime-local component owns tightly coupled database or replication
  transitions;
- the data plane persists the evidence needed to determine the result.

Kuberic should not merely move its current detailed phase machine into the
replica agent. Coarse operations must have stable inputs, durable
postconditions, and a bounded retained-result contract.

### Do Not Copy Weak Observation Semantics

The reviewed operators sometimes:

- collapse query failure into an empty or disconnected state;
- infer a primary from an unreachable member;
- treat command return as completion;
- retry after an error without retaining the previous operation identity;
- keep escalation timers only in process memory.

Those shortcuts may be acceptable for provisioning or when the database has
independent safety enforcement. They are not sufficient for Kuberic's
configuration, epoch, and write-authority invariants.
