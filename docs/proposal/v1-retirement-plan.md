# Kuberic v1 Retirement Plan

> **Status:** In progress — Workstream 1 implemented and validated; Workstream 2
> has implemented and validated sequential scale-up and secondary scale-down.
> Primary-removal behavior and broader retirement remain outstanding.
>
> **Goal:** Make the level-triggered stack the default Kuberic implementation,
> deprecate the classic v1 stack, and eventually remove it.
>
> **Migration contract:** Clean break. Existing v1 resources, status, storage,
> and application data are not converted. Users delete the v1 application and
> deploy a new v2 application.

## Summary

Kuberic v1 should be retired after the level-triggered stack provides the
required user outcomes for KVStore, SQLite, and PostgreSQL and is ready for
normal distribution. Retirement does not require API compatibility,
state-machine compatibility, mixed-version operation, or preservation of
existing application data.

V2 behavior may intentionally replace v1 behavior. In particular, stronger
fencing and fail-closed recovery are acceptable even where v1 attempted a more
available or destructive recovery path.

The work should proceed in this order:

1. Add planned switchover to v2 — implemented and validated.
2. Add scale-up and scale-down to v2 — sequential scale-up and secondary
   scale-down complete; primary-removal behavior remains.
3. Add a v2 SQLite application.
4. Move the PostgreSQL application to v2.
5. Publish and make v2 deployment assets the default.
6. Deprecate and freeze v1.
7. Remove v1 after all removal gates are satisfied.

Automatic rolling upgrades and broader concurrent successful-write model
validation remain separate, deferred work.

## Goals

- Provide v2 replacements for the classic KVStore, SQLite, and PostgreSQL
  applications.
- Preserve the existing v2 guarantees for exact identity, durable authority,
  quorum credit, epoch fencing, and stale-primary rejection.
- Add the operational capabilities needed to replace normal v1 usage:
  planned switchover and replica-count changes.
- Publish supported controller and application images for v2.
- Make v2 documentation, manifests, and examples the default entry points.
- Give users an explicit delete-and-redeploy transition procedure.
- Freeze v1 before deleting its source and deployment assets.

## Non-Goals

- Converting `kuberic.io/v1` resources to
  `operator.kuberic.io/v1alpha1` resources.
- Reusing or translating v1 status, epochs, replica identities, or durable
  operation state.
- Preserving KVStore, SQLite, or PostgreSQL application data across the
  transition.
- Rebinding v1 PVCs to v2 replicas.
- Running v1 and v2 participants in one replication group.
- Supporting mixed protocol versions or rolling protocol negotiation.
- Reproducing every v1 failure-recovery decision.
- Porting `NodeMaintenanceRequest` orchestration before v1 deprecation.
- Adding configurable v2 storage size or PVC retention policy before v1
  deprecation.
- Changing the SQL Server application as part of v1 retirement. It does not
  depend on `kuberic-core` or `kuberic-operator` and is outside this migration
  path.
- Adding automatic rolling image upgrades.

## Current State

The level-triggered stack already supports:

- full-set, write-closed bootstrap;
- quorum replication with authority-bound progress;
- exact-incarnation, same-cardinality replica replacement;
- ordinary primary failover;
- explicit named-target planned switchover with durable handoff, restoration,
  newer-epoch compensation, and terminal write-closed Unsafe outcomes;
- deterministic secondary-only scale-down to a target/minimum as low as one,
  sequential reductions, and exact post-commit Pod/PVC/endpoint cleanup;
- sequential one-at-a-time scale-up with fresh ordinal restoration,
  PVC-before-Pod allocation provenance, copy plus catch-up closure, PC/CC
  admission, exact pre-admission cleanup, and failover recovery;
- retained-history repair and full-copy fallback;
- explicit quorum-loss write blocking and non-destructive recovery;
- restart recovery for the controller, replica agent, runtime, and
  application;
- bounded level-triggered reconciliation and stale-command fencing;
- the `kvstore2` conformance application;
- isolated KinD bootstrap, replacement, failover, quorum-loss, healthy
  switchover, secondary scale-down, sequential scale-up, and adversarial
  restart/target-loss scenarios.

The principal retirement gaps are:

| Area | Classic v1 | Level-triggered v2 | Retirement disposition |
|---|---|---|---|
| Planned switchover | Supported | Explicit named-target request supported and validated | Workstream 1 complete |
| Scale up/down | Supported | Sequential scale-up and secondary-only scale-down implemented/validated; no primary removal | Workstream 2 partially complete |
| KVStore | Supported | `kvstore2` supported | Make v2 the default |
| SQLite | Supported | Not ported | Add a v2 application |
| PostgreSQL | Depends on `kuberic-core` and `kuberic-operator` | Not ported | Move to v2 before deprecation |
| Destructive data-loss recovery | Supported | Fails closed | Keep v2 behavior |
| API and status compatibility | Existing v1 contract | Independent contract | No compatibility required |
| Data migration | Existing data remains in v1 | No import path | No migration required |
| Image publication | Published | Local/CI only | Publish before deprecation |
| Node maintenance | `NodeMaintenanceRequest` orchestration | Not supported | Deferred; not a retirement blocker |
| Storage and PVC policy | Configurable size and retention | Fixed policy; removed v2 replica PVCs are permanently deleted | Configurable retention deferred |
| Automatic rolling upgrades | Not supported | Not supported | Deferred |

## Workstream 1: Planned Switchover

**Implemented and validated.** The
[planned-switchover guide](../features/kuberic/level-triggered-operator.md#planned-switchover)
describes the as-built request and operational contract.

- `spec.switchover` names a unique request ID and committed logical secondary;
  acceptance freezes exact source/target identities, membership, and policy.
  An active request cannot be cancelled or retargeted. Identical active or
  latest-receipted requests are idempotent; status retains only the latest receipt.
- Exact protocol 7 retains the process-session dispatch fences introduced in
  version 4 and the generation-bound preparation/retirement added in version 5.
  Preparations bind the accepted spec generation with durable
  retirement high-water marks. Durable preparation closes source writes and records a handoff
  certificate; target catch-up must use authority-verified progress.
- Write-closed PC/CC and current-only convergence precede accepted topology,
  write grant, and exact-Pod routing. Membership and data-loss authority remain
  unchanged; configuration authority only advances.
- Definitive target loss before newer-authority admission permits
  original-authority restoration. After admission, only evidence-proven,
  strictly newer-epoch compensation can restore the old primary. Impossible
  completion converges every possible writer to closed or absent before
  publishing Unsafe. There is no destructive data-loss recovery.
- Durable replica-local effects and fresh observation resolve restarts and
  ambiguous replies without a controller phase journal. Writes and connections
  may be briefly interrupted; zero downtime and a fixed completion time are not
  promised.

Validation includes pure evaluator/recovery models, runtime write-completion
races, durable agent subprocess crash boundaries, controller ambiguity/session
tests, and isolated healthy/adversarial KinD scenarios. The standalone healthy
run took 15.1 s total / 8.2 s after readiness. Two fresh full matrices measured
healthy 4.2/4.3 s and adversarial 44.5/48.8 s; the strict retained-session
rejection adversarial rerun took 45.2 s. All owned clusters and kubeconfigs were
cleaned. These are scenario timings, not outage guarantees.

This completes only Workstream 1, not v1 retirement. Workstream 2 is partially
complete as described below, followed by SQLite, PostgreSQL, distribution,
deprecation, and separately approved removal. Automatic target selection,
cancellation/retargeting, node maintenance,
rolling upgrades, destructive recovery, and data migration/import remain
unsupported or deferred.

## Workstream 2: Scale Up and Scale Down

**Scale-up and secondary scale-down are implemented and validated; the
workstream remains partially complete because primary removal is not
implemented.** The
[scale-up guide](../features/kuberic/level-triggered-operator.md#sequential-scale-up)
and
[secondary-scale-down guide](../features/kuberic/level-triggered-operator.md#secondary-scale-down)
describe the as-built contracts. Both use typed PC/CC authority, but they are
separate membership-increase and membership-decrease protocols rather than
replacement or one generic scaling state machine.

Sequential scale-up is enabled in the production v2 controller configuration;
classic `kuberic.io/v1` remains unchanged. Its as-built behavior is:

- Increasing `spec.replicas` from accepted size `N` adds exactly one missing
  positive ordinal through `N+1`, fully settles it, and only then begins another
  addition. Scaling down and back up restores the missing ordinal with fresh
  PVC, Pod/incarnation, generation, session, endpoint, build, and operation
  identities.
- Allocation authority is persisted before resource creation. The controller
  creates and freezes the annotated canonical PVC, revalidates it, and creates
  the bound Pod afterward. Same-name resources without exact operation
  provenance are neither adopted nor deleted.
- The exact accepted primary supplies immutable snapshot enumeration and
  continued replication. A distinct post-enumeration catch-up boundary is
  frozen, and the candidate must durably apply the full snapshot plus every
  contiguous operation through that boundary before admission.
- The candidate remains outside accepted membership and supplies no quorum
  credit before typed scale-up PC/CC authority. PC retains the previous policy;
  expanded CC uses its independently validated majority policy. Healthy
  same-primary writes may continue only while both configurations remain
  writable and authorized.
- Expanded current-only authority is irreversible. Candidate-local acceptance,
  `ActiveSecondary` role, expanded write quorum including the primary, write
  grant, routing, and late-member receipt convergence remain separate. Status
  reports committed-degraded rather than rolling back an accepted member.
- Desired reduction before PC/CC cancels and exactly cleans the unadmitted
  candidate. After admission starts, scale-up completes before newer desired
  state is evaluated. Pre-admission primary failure uses ordinary failover,
  exact cleanup, and a fresh retry; post-PC/CC failure carries independent
  previous/expanded recovery evidence.
- Failed or cancelled unadmitted work is cleaned endpoint→Pod→PVC using frozen
  UID/resource-version provenance and authoritative absence. Different-UID
  resources survive, and partial PVCs are not resumed.

Protocol 8 and agent schema 3 are exact coordinated-deployment boundaries.
Protocol 7 and earlier, plus schema 2, are rejected with no migration,
mixed-version mode, or rolling-upgrade contract. The generated CRD is currently 344,907 bytes under a
strict-below-350,000-byte regression guard; the largest current representative
18-member serialized scale-up status sample is carried failover at 26,353
bytes. These are growth guards, not a supported maximum replica-count budget.

Validation includes pure protocol/model traces, runtime and durable agent crash
boundaries, controller allocation/cleanup races, healthy and multi-count KinD
scale-up, writes held during live copy, cancellation/fresh retry,
pre-admission failover, post-PC/CC carried failover, accepted replacement and
ordinary failover composition, and scale-down→scale-up fresh restoration.
These results cover the tested scenarios; they are not outage, latency,
maximum-scale, or arbitrary-failure guarantees.

This is SF-inspired secondary scale-down using PC/CC quorum principles, with
Kuberic-specific target/minimum coupling, deterministic selection, write closure,
sequential cleanup, and Kubernetes resource deletion.

- Lowering `spec.replicas` selects one highest logical-ID committed secondary
  at a time, preserving the exact primary and retained identities. Desired
  count is both target and minimum, down to one. This target=min coupling is a
  Kuberic policy choice; SF target and minimum are independently configurable.
- Before freezing intent or removing routing/closing writes, exact retained
  members must currently provide the previous read quorum under stable accepted
  current-only authority and fresh exact sessions. Otherwise
  `ScaleDownRetainedReadQuorumUnavailable` preserves existing service with bounded
  re-observation, without replacement or an alternate target. Healing the retained
  member admits the same highest-ID target; 2→1 still needs only the primary.
- After admission, durable write closure and a verified primary boundary precede retained
  previous-read-quorum evidence, write-closed reduced PC/CC, and fresh reduced
  current-only write-quorum evidence. Ordinary replication still requires both
  PC and CC write quorums when PC exists.
- Accepted reduced topology/policy and cleanup are published atomically.
  Separate local acceptance, write grant, and routing restore service.
  Endpoint→Pod→PVC deletion is exact-UID/resource-version fenced; unavailable
  targets can use post-commit exact Pod deletion instead of a retirement reply.
  PVC object deletion waits for authoritative Pod absence, with no retention or
  import path; it does not promise physical storage erasure. Exact original PVC
  provenance must be reconstructable before admission. If Pod/PVC disappearance
  prevents this, scale-down waits/fails closed rather than treating list omission
  as absence. Unavailable-target support requires frozen or reconstructable exact
  cleanup identity.
- Schema-3 retirement-started/tombstone recovery prevents application Open
  after retirement begins and additionally persists scale-up build/admission
  authority. Protocol 8/store schema 3 require fresh deployment, without
  migration or mixed-version support.
- Active removal cannot be cancelled or retargeted. Cleanup serializes later
  removals and other authority work. Primary loss or missing evidence waits
  fail-closed, including indefinite outage if the frozen primary is lost during
  removal or cleanup. Before superseding the bounded receipt, every retained
  member needs its original completed current-only witness or fresh completed
  local acceptance, so sequential reductions need more than quorum availability.
  Singleton operation deliberately has no redundancy.

Validation covers healthy 3→2, 2→1 and singleton restart, sequential 5→2,
unavailable-target 3→1, exact deletion races, durable process crashes, and
retained-client/session fencing. Two original seven-scenario matrices completed
in 18m16s/16m29s; the final post-fix matrix passed in 927.942s command wall time
(959.565s full lifecycle), plus separate failover in 18.71s. The guide records
per-scenario results. These measurements are not outage guarantees.

**Outstanding primary-removal behavior** must:

- define selection and durable authority for removing a primary;
- move primary authority first through an approved safe transition;
- compose that movement with removal, recovery, and exact cleanup.

The implemented scaling subset does not provide primary removal, explicit
user-selected removal, independent target/minimum policy, placement balancing,
concurrent additions, a validated maximum replica count, or full scaling
parity. It does not establish v1 retirement readiness; application ports,
distribution, deprecation, and the separately approved removal gates still
apply.

Only one authority-changing membership command may be issued from one
observation. Reconciliation must remain level-triggered and restart-safe.

### Deferred scale-down follow-ups

These are explicitly **deferred**, not merge requirements or claims of existing
SF parity. P1 is the first follow-up tier (availability/status boundaries), P2
is maintainability or policy expansion, and P3 needs a new recovery protocol.
Scale-up completion does not promote these follow-ups into the current scope.

| Priority | Follow-up | Why deferred / change class |
|---|---|---|
| P1 | Durable per-member Kubernetes resource provenance | Persist exact original Pod/PVC/endpoint identity before disappearance so pre-admission loss can converge. Requires a durable lifecycle/status contract, not permission to infer absence from lists. |
| P1 | CRD/status compaction and boundary redesign | Context-bound preparation/retirement records should reference their enclosing frozen intent; split replication proof (`RemovalAuthority` / `CommittedRemovalProof`) from Kubernetes `CleanupObligation` and request metadata. Add serialized status-size tests and an operational replica-count budget. This is compatibility/API-breaking status and potentially wire/store redesign, requiring coordinated validation/recovery changes, not a formatting refactor. |
| P2 | Shared candidate-selection helper, neutral exact-cleanup helpers, and static command-binding predicates | Mechanical refactors reduce duplicated policy/identity checks without changing selection or evidence. Keep layer-specific installed-authority and live-session checks; defer to isolate the admission fix from unrelated code movement. |
| P2 | Independent target/minimum policy and PLB/placement-aware selection | SF target and minimum are independently configurable. New API, placement inputs, and availability policy are needed; highest-ID selection and target=min remain the deliberately narrow Kuberic contract. |
| P1 design / P3 implementation | Frozen-primary recovery during removal/cleanup; possible overlapping cleanup and failover | Current serialization can cause indefinite outage. Safe continuation needs a new cross-epoch recovery and cleanup-ownership protocol; do not loosen frozen evidence or silently enable overlap. |
| P3 | Multi-member removal in one reconfiguration | Current reductions are sequential and require cleanup plus receipt/local-acceptance prerequisites between steps. Batch removal changes quorum, identity, evidence and cleanup semantics, not just the loop. |
| P3 | Move RA-style distributed phase scheduling from evaluator to a durable primary-agent coordinator | Explicitly defer: per-member ordering, witness freezing, PC/CC progression and restart replay require a durable cross-replica coordination protocol. Keep desired-count/target policy in the evaluator/controller, not the agent/runtime; keep local journals out of CR status and Kubernetes cleanup authority out of the replicator. |
| P2 | Scale-up operational budget and performance characterization | Existing linear serialized-status guards and bounded live scenarios do not define a supported maximum replica count, completion SLO, throughput target, or outage bound. Establish those only with explicit Kubernetes object-size, controller, copy, quorum, and application measurements. |

Compaction must preserve typed structural schemas, exact contextual evidence and
late-member recovery. Do **not** use opaque schemas, hash-only receipts, or TTL
evidence deletion to shrink status. Actual serialized status size, not only the
generated CRD's pretty-printed byte count, determines the replica-count budget.
No backward compatibility or migration is promised by this experimental stack,
but status/API-breaking redesign still needs an explicit coordinated deployment
contract; mechanical helper extraction does not.

## Workstream 3: SQLite on V2

The v2 SQLite application should validate that the SF-shaped runtime supports
an application with durable WAL-frame replication rather than only the
deterministic KVStore operation model.

The application must:

- use the public v2 application interfaces without access to managed
  authority internals;
- durably accept replicated WAL frames before acknowledging them;
- support full copy and incremental replication;
- reconstruct acknowledged state after process restart;
- participate in bootstrap, replacement, failover, switchover, and scaling;
- start write-closed and obey runtime access changes;
- use new storage and metadata directories with no v1 import path.

The existing v1 SQLite application is behavioral reference material only. The
new application must not depend on `kuberic-core`, classic wire types, or v1
operator state.

## Workstream 4: PostgreSQL on V2

The PostgreSQL application currently depends directly on `kuberic-core`, and
its tests depend on `kuberic-operator`. It must move to the level-triggered
runtime before those packages can be deprecated or removed.

The v2 PostgreSQL application must:

- integrate PostgreSQL process lifecycle with the public v2 application
  interfaces;
- continue using PostgreSQL-native streaming replication for its data plane;
- translate durable PostgreSQL LSN and recovery evidence into the v2 authority
  model without granting raw progress quorum credit;
- fence direct PostgreSQL clients when the replica is not the accepted
  writable primary;
- support fresh bootstrap, replica build, failover, planned switchover, and
  scaling;
- reconstruct process and replication authority after Pod or process restart;
- use new v2 metadata and deployment assets with no v1 status or storage
  import path.

Because PostgreSQL clients connect directly to the database port, routing
changes alone are not sufficient fencing. Promotion and demotion must enforce
PostgreSQL read-only/read-write state and reject writes through stale direct
connections.

The migration is a source and runtime port, not a data migration. Existing v1
PostgreSQL clusters are deleted and fresh v2 clusters are deployed.

SQL Server is not part of this workstream. Its current package has no
dependency on `kuberic-core` or `kuberic-operator`, so v1 retirement must not
create an artificial migration requirement for it.

## Workstream 5: Distribution and Defaulting

V2 cannot replace v1 while it remains local and CI-only.

Before deprecation:

- publish immutable controller, KVStore, SQLite, and PostgreSQL image tags;
- provide versioned CRD and controller installation assets;
- document image compatibility and the exact supported protocol version;
- provide clean deployment examples for KVStore, SQLite, and PostgreSQL;
- make v2 the primary path in the root README and feature documentation;
- keep v1 documentation available but clearly marked deprecated;
- retain isolated image identities, API groups, labels, and Services while
  both stacks coexist.

The user transition procedure must state clearly that deleting v1 may delete
PVCs and application data according to the v1 retention policy. It must not
imply automatic conversion, rollback, or data recovery.

## Workstream 6: V1 Deprecation and Freeze

After v2 satisfies the deprecation gates:

- mark the classic CRD, operator, runtime, examples, images, and deployment
  assets deprecated;
- stop adding v1 features;
- accept only critical correctness and security fixes in v1;
- direct new users and examples to v2;
- publish the delete-and-redeploy transition procedure;
- retain v1 tests while the deprecated implementation remains in the tree.

Deprecation is a distinct milestone from source removal. It provides a period
where v2 is the default while v1 remains available for users who have not yet
redeployed.

## Workstream 7: V1 Removal

Final removal must be a separately reviewed change. It may delete:

- `kuberic-core`;
- `kuberic-operator`;
- classic KVStore, SQLite, and PostgreSQL examples;
- classic CRDs, manifests, image publication, and integration tests;
- compatibility documentation and CI paths that only exercise v1.

Before deletion, every remaining workspace dependency and shipped adapter
must have an explicit disposition. PostgreSQL must already use v2. SQL Server
remains outside the v1 dependency boundary and should only change if its own
roadmap requires it.

The removal change must also simplify shared workspace dependencies, scripts,
Docker contexts, documentation links, and CI after the classic packages are
gone.

## Retirement Gates

### Deprecation Gates

V1 may be marked deprecated when:

- planned switchover passes unit, crash-boundary, and KinD validation
  (**satisfied by Workstream 1**; must remain green);
- scale-up and scale-down pass unit, crash-boundary, and KinD validation;
- KVStore, SQLite, and PostgreSQL v2 applications pass their applicable
  bootstrap, replacement, failover, quorum-loss, switchover, and scaling
  scenarios;
- controller, KVStore, SQLite, and PostgreSQL v2 images are published;
- installation and delete-and-redeploy documentation is complete;
- v2 is the default documented path;
- unsupported operations fail closed with actionable conditions.

### Removal Gates

V1 source may be removed when:

- all deprecation gates remain continuously green;
- no default documentation or manifest references v1;
- every workspace package depending on v1 has an approved disposition;
- the full v2 adversarial matrix passes on the removal commit;
- classic image publication can be stopped without affecting v2 releases;
- the removal diff includes no hidden dependency on classic generated code,
  manifests, or Docker build artifacts.

## Validation Strategy

Each new authority transition must be validated at four levels:

1. Pure protocol tables and generated traces for safety invariants.
2. Durable agent/runtime tests at intent, effect, acknowledgement, and
   completion boundaries.
3. Controller tests for stale observations, ambiguous effects, restart, and
   bounded healing.
4. Fresh isolated KinD scenarios using explicitly owned clusters.

The CI target should remain bounded. Pull requests should run one live
end-to-end path for each changed operation, while the repeated adversarial
matrix runs through scheduled and manual workflows.

## Risks and Guardrails

- **Unsafe feature equivalence:** Retirement is based on user outcomes, not
  copying v1 internals. V2 must not weaken fencing to mimic v1 behavior.
- **Accidental migration promise:** Documentation must consistently describe
  delete and redeploy, including data loss.
- **Primary movement races:** Switchover must preserve one-writer authority
  across restarts, cancellation, and delayed commands.
- **Membership/quorum mistakes:** Scaling must use intersecting
  configurations and exact identities; resource creation alone is not
  membership.
- **Application durability mismatch:** SQLite acknowledgement must mean its
  replicated state is durable and reconstructible.
- **Direct PostgreSQL clients:** PostgreSQL role and write-mode fencing must
  remain effective even when a client bypasses the controller-managed Service.
- **Premature removal:** V1 source deletion must not begin before package and
  adapter disposition is explicit.
- **Unbounded CI time:** Keep PR smoke focused and use the repeated matrix for
  broader crash and partition coverage.

## Deferred Work

The following do not block this retirement plan:

- automatic rolling image or protocol upgrades;
- node-maintenance preparation, placement exclusion, and recovery
  orchestration;
- configurable v2 storage size and PVC retention policy;
- in-place v1-to-v2 resource conversion;
- application data migration;
- mixed-version negotiation;
- broader stateful successful-write generation across delayed effects and
  concurrent retained-client connections;
- changes to the SQL Server application, which does not depend on v1.
