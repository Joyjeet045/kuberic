# SQL Server External-Replication Adapter

> **Status:** Contract only. No SQL Server process, query, topology, or failover
> effects are enabled by this change.
>
> **Support level:** Experimental. This is not a Microsoft-supported Kubernetes
> high-availability solution.

## Decision

Kuberic will treat SQL Server as an external replication engine. SQL Server
owns log transport, hardening, redo, and automatic seeding. Kuberic may
eventually own the distributed intent and reconciliation around those native
operations.

The first supported target is deliberately narrow:

| Area | Initial contract |
|---|---|
| Engine | SQL Server 2022, major version 16 |
| Platform | Linux x86-64 containers |
| Edition | Developer for tests; Enterprise for a future production profile |
| Availability group | Full AG, one managed user database |
| AG naming | `EXTERNAL` limit of 64 characters, enforced as 64 UTF-16 code units |
| Cluster type | `EXTERNAL` |
| Topology | Three synchronous data replicas |
| Failover mode | `EXTERNAL` |
| Seeding | Automatic |
| Commit protection | `REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = 1` |
| Write fencing | SQL Server external write lease plus a verified fence receipt |
| Default behavior | Observe only |

Images must be pinned by digest. Accepting the SQL Server EULA remains an
explicit deployer action and is never implied by the adapter.

The AG name limit is narrower than the general `sysname` limit.
`CREATE AVAILABILITY GROUP` documents 128 characters for `cluster_type = WSFC`
but 64 for `cluster_type = NONE` and `EXTERNAL`, and the engine enforces it as
error 19544. Before SQL Server 2022 CU23 an over-length name raised an assertion
failure rather than that error, so client-side rejection avoids a crash-class
failure on older builds. Microsoft states the bound in "characters" without
naming a unit; the adapter counts UTF-16 code units, which is never more
permissive than counting scalar values.

`CLUSTER_TYPE = NONE` is intentionally excluded. Microsoft documents it as a
read-scale configuration without high availability. It allows manual T-SQL
failover but has no cluster-manager arbitration, so using it would not
demonstrate the safe failover requested by issue #80. Windows/WSFC, Basic AGs,
distributed AGs, asynchronous disaster recovery, configuration-only replicas,
multiple managed databases, and mixed major versions are also out of scope for
the initial profile.

## Current Implementation

The `examples/sqlserver` crate currently implements only the reviewable
contract needed before database effects are introduced:

- validation of the supported SQL Server profile;
- immutable image and Kubernetes Secret references;
- native AG, database, replica, and incarnation identities;
- separate desired replica identities for bootstrap, before SQL Server generates
  native replica GUIDs;
- exact SQL Server `numeric(25,0)` progress values without narrowing to
  Kuberic's current `i64` progress type;
- observations that distinguish present, absent, stale, and failed evidence;
- versioned operation envelopes with canonical SHA-256 input signatures;
- explicit destructive approvals and fence references;
- duplicate-operation and operation-ID-reuse classification.

The crate does not yet connect to SQL Server, create an AG, seed a database,
renew a write lease, change a role, or integrate with either Kuberic operator.
Mutation configuration is therefore only a contract for later stages, not an
enabled execution path.

Unlike the PostgreSQL and SQLite examples, `examples/sqlserver` is a pure
library with no binary target and no dependency on `kuberic-core`. The contract
is deliberately expressible and testable without the replication runtime, so
that stage 2 can introduce a runtime against a contract that is already fixed.

Two capabilities named in this design are defined but not yet enforceable, and
each is assigned to a later stage rather than half-built now:

- **Encoding and decoding.** The canonical writer produces the bytes that the
  input signature covers, but there is no reader and no `serde` support, so an
  envelope cannot yet be persisted or sent between processes. Stage 2 owns the
  decoder together with the durable result journal that needs it.
  `OperationRequest::from_decoded_parts` exists as the seam that decoder will
  use, and is the only path on which the contract-version check is reachable.
- **Proof validity and issuance.** `DestructiveApproval` and `FenceReference`
  currently bind a receipt to an exact operation ID and canonical input
  signature. They carry no issue or expiry time and no issuer verification, so
  the type system cannot yet express an expired or forged proof, and any caller
  able to construct an envelope can construct a receipt for it. Stage 4 owns
  receipt lifetime and issuer authentication, alongside the external lease
  handling that gives those fields their meaning. Until then, no code path may
  treat the presence of a receipt as evidence that fencing actually occurred.

## Why the PostgreSQL Adapter Is Not a Drop-in Template

The PostgreSQL example establishes the useful control-plane/data-plane
boundary, but its current executable and safety behavior are incomplete:

- its binary is still a placeholder;
- direct PostgreSQL clients bypass Kuberic's in-memory write-status flag;
- epoch updates do not establish a database-native fence;
- monitor failures retain last-known progress without freshness evidence; and
- its LSN can be represented as a 64-bit scalar, while SQL Server AG DMV
  positions are `numeric(25,0)` and have field-specific meanings.

SQL Server must therefore use native identity, progress, lease, and role
evidence instead of being forced through the current PostgreSQL adapter or the
generic scalar election path.

## Native HA Boundary

For `CLUSTER_TYPE = EXTERNAL`, normal failover belongs to the external cluster
manager. Kuberic must not issue an isolated `ALTER AVAILABILITY GROUP ...
FAILOVER` and then infer success from an RPC response.

Microsoft publishes the `mssql-server-ha` resource-agent source specifically so
other clustering systems can follow the SQL Server protocol. Its AG helper:

- monitors the SQL Server instance and AG;
- reads native configuration sequence numbers;
- computes promotion eligibility;
- promotes and demotes through the external-cluster session contract; and
- maintains the external write lease.

The mutation implementation should invoke a pinned, tested helper build or
faithfully implement that published protocol. Reimplementing only its visible
T-SQL statements is not sufficient because sequence-number arbitration,
required synchronous secondaries, lease renewal, health thresholds, and action
ordering jointly establish safety.

The external write lease is the database-native write fence for direct TDS
clients. Kuberic may renew it only while the accepted primary still has valid
cluster authority. Before promotion, the planner must prove that the old
primary's lease expired or present a verified infrastructure fence that
prevents that exact incarnation from writing or restarting. A Service update,
readiness failure, Pod deletion request, Kubernetes Lease, or in-memory
`PartitionState` flag is not such proof.

## Observation Contract

An observation snapshot must retain provenance and freshness and must
distinguish:

- a catalog object that is absent;
- a replica that is unreachable;
- permission-denied or malformed output;
- a stale last-known observation; and
- an unsupported engine or protocol state.

Every observation records when the attempt was made, including failed attempts,
so that a caller can reason about how long evidence has been unavailable. A
failed observation is never fresh, and an observation stamped in the future is
also reported as not fresh: clock skew must degrade toward refusing to act, not
toward trusting an unverifiable sample.

The observer will read and cross-check at least:

- `SERVERPROPERTY(...)` and `@@SERVERNAME`;
- `sys.availability_groups`;
- `sys.availability_replicas`;
- `sys.dm_hadr_availability_replica_states`;
- `sys.availability_databases_cluster`;
- `sys.dm_hadr_database_replica_states`;
- `sys.databases`; and
- automatic and physical seeding DMVs.

Replica and database rows are joined by their native GUIDs. Local observations
must not be confused with a primary's potentially stale report about a remote
replica. The AG identity and local role are sampled again around a multi-query
observation; a SQL transaction does not make the DMVs a globally atomic
snapshot.

SQL Server exposes multiple progress concepts. Hardened-block, redone-record,
and committed-record positions remain distinct exact decimal values. They
cannot be truncated into `kuberic-core`'s current `i64`, compared across
different databases, or compared across incompatible recovery histories.
Every progress-bearing transition therefore includes both the database GUID
and recovery-fork GUID.

## Command Contract

Each SQL Server operation is bound to:

- contract version;
- Kuberic resource identity;
- operation identity;
- source configuration identity;
- source and target epochs;
- native AG/database/replica identities;
- target Pod incarnation;
- database recovery lineage for progress-bearing transitions;
- all operation-specific safety inputs;
- destructive authorization, when required; and
- a verified fence reference, when required.

Canonical input encoding uses fixed field order, explicit variant and option
tags, big-endian integers, length-prefixed UTF-8 strings, and sorted set-like
collections. The input signature is SHA-256 over that encoding. It is an
idempotency and conflict-detection value, not authentication.

A second digest, the effect signature, covers the same encoding minus the
operation identity under a distinct domain-separation prefix. It answers a
different question: whether two requests ask SQL Server for the same thing. The
input signature cannot answer it, because the operation ID is part of the input
it covers.

Approval and fence receipts are evidence about a request, not request input.
Each carries the exact operation ID and canonical input signature that it
authorizes. They are excluded from that signature to avoid a circular encoding
and to permit replacement of an expired proof without changing the requested
database effect.

An exact duplicate returns the retained result. Reusing an operation ID with
different canonical input is rejected. A planner that crashes after dispatch but
before persisting its intent can regenerate the same native effect under a fresh
operation ID, so a request whose effect signature matches a retained result
under a different operation ID is reported distinctly and requires reobserving
the native postcondition. A lost response is resolved the same way; it is not
permission to issue a conflicting operation.

The initial operation vocabulary is:

- `EnsureAvailabilityGroup`;
- `EnsureReplicaJoined`;
- `EnsureReplicaSeeded`;
- `ReseedReplica`;
- `PlannedSwitchover`; and
- `ForcedFailover`.

Reseeding and forced failover require operation-bound destructive approval.
Reseeding additionally fences the target being replaced. Both switchover and
failover fence the exact old-primary incarnation.

## Security Contract

There are three separate trust boundaries:

1. TDS administration with verified TLS and separate observation and mutation
   principals.
2. AG endpoint encryption and certificate authentication between replicas.
3. Authenticated command authority between the future operator and replica
   agent.

Only Kubernetes Secret references belong in desired state. Passwords,
certificate private keys, connection strings containing credentials, and SQL
batches containing secrets must not be stored in CRD status, operation
signatures, logs, or command-line arguments. Observation permissions and
mutation permissions must use distinct Secret keys. SQL Server 2022 observation
permissions, including `VIEW SERVER PERFORMANCE STATE`, must be validated
explicitly; metadata hidden by insufficient permission must not be treated as
absence.

Endpoint certificates, system databases, and user data all require durable
storage and explicit rotation procedures. Availability Groups do not replicate
server logins or other server-level objects, so those need separate
provisioning.

## Level-Triggered Integration Boundary

The SQL Server library can be developed before issue #79, but it must not be
wired into the current operator2 prototype until that work provides:

- durable replica and incarnation evidence;
- versioned declarative commands and retained terminal results;
- normalized observation and typed proof transport;
- compact accepted topology and transition intent;
- a pure planner that emits at most one authority-changing command; and
- separate persistence and dispatch reconciliation cycles.

Until then, the SQL Server code remains an independently testable adapter
library and laboratory tool. It must not modify the classic operator or claim
automatic Kubernetes failover.

## Delivery Sequence

1. **Support and safety contract** — the current change: types, validation,
   canonical operation identity, tests, and this design.
2. **Runtime and observation** — a replaceable TDS executor, immutable DMV
   snapshots, freshness, startup capability checks, and an observe-only CLI.
3. **Bootstrap, join, and reseed** — pure convergence decisions, one native
   effect at a time, durable SQL-specific result journal, and automatic-seeding
   postconditions.
4. **Switchover, failover, and fencing** — external lease handling, sequence
   arbitration, verified fence receipts, explicit data-loss recovery, and live
   fault tests. Automatic failover remains disabled until every safety gate
   passes.
5. **Operator2 integration** — Pod/PVC/Secret/Service convergence and the #79
   command/evidence boundary.
6. **Kubernetes E2E and operations** — pinned licensed test environment,
   three-replica failures, process/operator restarts, client reconnection,
   upgrades, and runbooks.

Stages 1-4 do not require operator2. Stages 5-6 require the minimum #79 vertical
slice, but do not need to wait for every #79 operation to be complete.

## Test Gates

Ordinary workspace tests stay independent of SQL Server. Server-free tests
cover profile rejection, exact progress, malformed observations, canonical
operation vectors, duplicate/reused operation IDs, epoch regression,
destructive approval, and fence binding. They run in the ordinary CI job as
`cargo test -p sqlserver-replicated`, alongside the other example crates.

Live tests require a separate explicit job because the current CI installs
PostgreSQL but not SQL Server. That job must pin the engine, tools, and helper
artifacts; accept the EULA explicitly; isolate credentials; and fail rather
than skip when requested prerequisites are missing. Mutation support cannot be
declared complete until live tests cover crash points before and after intent
persistence, native dispatch, postcondition observation, result persistence,
reply, and routing publication.

## References

- [Issue #80](https://github.com/youyuanwu/kuberic/issues/80)
- [Level-triggered operator proposal](../../proposal/level-triggered-operator-design.md)
- [SQL Server availability groups on Linux](https://learn.microsoft.com/en-us/sql/linux/business-continuity/availability-groups/overview)
- [Configure an availability group for high availability on Linux](https://learn.microsoft.com/en-us/sql/linux/business-continuity/availability-groups/configure)
- [Manage availability group failover on Linux](https://learn.microsoft.com/en-us/sql/linux/business-continuity/availability-groups/failover-high-availability)
- [`sys.dm_hadr_database_replica_states`](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-objects/sys-dm-hadr-database-replica-states-transact-sql)
- [`CREATE AVAILABILITY GROUP`](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-availability-group-transact-sql) — `EXTERNAL` AG name limit
- [Microsoft `mssql-server-ha` resource agents](https://github.com/microsoft/mssql-server-ha)
- [Microsoft SQL Server AGs on Kubernetes with DxOperator](https://learn.microsoft.com/en-us/sql/linux/business-continuity/containers/tutorial-kubernetes-dxoperator)
