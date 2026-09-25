# Kuberic v1 Retirement Plan

> **Status:** In progress — Workstream 1 implemented and validated; broader
> retirement remains proposed.
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
2. Add scale-up and scale-down to v2.
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
- retained-history repair and full-copy fallback;
- explicit quorum-loss write blocking and non-destructive recovery;
- restart recovery for the controller, replica agent, runtime, and
  application;
- bounded level-triggered reconciliation and stale-command fencing;
- the `kvstore2` conformance application;
- isolated KinD bootstrap, replacement, failover, quorum-loss, healthy
  switchover, and adversarial restart/target-loss test scenarios.

The principal retirement gaps are:

| Area | Classic v1 | Level-triggered v2 | Retirement disposition |
|---|---|---|---|
| Planned switchover | Supported | Explicit named-target request supported and validated | Workstream 1 complete |
| Scale up/down | Supported | Fixed cardinality | Implement after switchover |
| KVStore | Supported | `kvstore2` supported | Make v2 the default |
| SQLite | Supported | Not ported | Add a v2 application |
| PostgreSQL | Depends on `kuberic-core` and `kuberic-operator` | Not ported | Move to v2 before deprecation |
| Destructive data-loss recovery | Supported | Fails closed | Keep v2 behavior |
| API and status compatibility | Existing v1 contract | Independent contract | No compatibility required |
| Data migration | Existing data remains in v1 | No import path | No migration required |
| Image publication | Published | Local/CI only | Publish before deprecation |
| Node maintenance | `NodeMaintenanceRequest` orchestration | Not supported | Deferred; not a retirement blocker |
| Storage and PVC policy | Configurable size and retention | Fixed v2 policy | Deferred; document the fixed behavior |
| Automatic rolling upgrades | Not supported | Not supported | Deferred |

## Workstream 1: Planned Switchover

**Implemented and validated.** The
[planned-switchover guide](../features/kuberic/level-triggered-operator.md#planned-switchover)
describes the as-built request and operational contract.

- `spec.switchover` names a unique request ID and committed logical secondary;
  acceptance freezes exact source/target identities, membership, and policy.
  An active request cannot be cancelled or retargeted. Identical active or
  latest-receipted requests are idempotent; status retains only the latest receipt.
- Protocol version 5 fences each control dispatch to the observed process
  session and preparations to the accepted spec generation with durable
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

This completes only Workstream 1, not v1 retirement. Scaling is next, followed
by SQLite, PostgreSQL, distribution, deprecation, and separately approved
removal. Automatic target selection, cancellation/retargeting, node maintenance,
rolling upgrades, destructive recovery, and data migration/import remain
unsupported or deferred.

## Workstream 2: Scale Up and Scale Down

Scaling should generalize the existing replacement, copy, and PC/CC machinery
rather than introduce an independent membership protocol.

Scale-up must:

- provision a fresh exact Pod/PVC incarnation;
- build it from copy plus replication-gap closure;
- add it through an intersecting PC/CC transition;
- update quorum policy only as part of accepted membership authority;
- recover safely from target loss and abandoned build attempts.

Scale-down must:

- select and durably identify the exact member to remove;
- move the primary first when the selected member is primary;
- preserve write quorum through an intersecting PC/CC transition;
- revoke the removed member before deleting routing, Pod, and PVC resources;
- reject scaling below the supported minimum.

Only one authority-changing membership command may be issued from one
observation. Reconciliation must remain level-triggered and restart-safe.

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
