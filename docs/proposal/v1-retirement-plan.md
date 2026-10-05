# Kuberic V1 Removal Record

> **Status: Complete.** The classic v1 runtime, operator, KVStore application,
> integration tests, deployment assets and image publication have been removed.
> This document records the completed migration/removal decision; it is not an
> operational guide.

## Decision

Kuberic v1 was removed before v2 distribution because there were no active
users or deployments requiring a compatibility period. The repository did not
need a deprecation release, user notification, resource conversion, application
data migration, fallback implementation or coexistence mode.

The previous sequence—publish v2, deprecate/freeze v1, then remove v1—was
superseded by this explicit no-active-users clean-removal decision. Existing
external classic registry artifacts, if still present, are unsupported
historical artifacts and were not deleted by the source change.

V2 distribution remains a separate future workstream. The repository currently
publishes no Kuberic controller or application images and provides no supported
release installation path. Source builds and isolated local/CI deployment
assets remain available.

## Removed Surface

The removal deleted:

- the classic replication/runtime package;
- the classic Kubernetes operator and its CRDs/RBAC;
- the classic KVStore application and generated API;
- classic Gateway, node-maintenance and lease-election integration tests;
- classic Dockerfiles, manifests, Gateway routes and external chart tooling;
- classic main/tag image publication;
- operational classic API, deployment and maintenance documentation.

Historical architecture and ADR material remains archived where it explains
past decisions.

## Completed V2 Workstreams

### Workstream 1: Planned Switchover

V2 supports explicit named-target planned switchover with durable handoff,
write closure, restoration before newer authority, and fail-closed terminal
outcomes.

### Workstream 2: Scale Up and Scale Down

V2 supports sequential one-at-a-time scale-up and secondary-only scale-down.
Move primary authority with planned switchover before reducing membership.

### Workstream 3: SQLite on V2

The existing SQLite application was migrated in place. It uses
quorum-before-publication WAL-frame replication, durable committed snapshots,
retained catch-up and restart/rebuild fencing. Fresh v2 storage is required.

### Workstream 4: PostgreSQL on V2

The existing PostgreSQL application was migrated in place as an SF-style
custom replicator. PostgreSQL owns WAL, physical replication/recovery,
synchronous policy, replay and promotion; Kuberic owns generic authority and
lifecycle choreography. Fresh protocol-9/schema-5 storage is required.

### KVStore2 Conformance Application

KVStore2 remains the isolated live conformance application for the
level-triggered controller, agent and runtime.

## Current Validation Contract

The surviving repository validates:

1. protocol, wire, runtime, agent and controller unit/model/durable behavior;
2. SQLite unit and in-process lifecycle/reconfiguration scenarios;
3. PostgreSQL host-local process, recovery, fencing and scaling scenarios;
4. DEX default, mocked-provider and isolated real-Kubernetes behavior;
5. KVStore2 bootstrap, replacement, failover, quorum-loss, switchover,
   scale-down, scale-up and adversarial KinD scenarios.

No current validation requires the removed classic stack.

## Distribution Status

V2 controller/KVStore2 Dockerfiles, manifests and Just recipes are retained as
experimental local/CI assets. SQLite and PostgreSQL do not have supported
application images or deployment manifests. Publishing immutable images,
versioned installation assets and compatibility guidance remains deferred.

## Deferred Scale-Down Follow-Ups

These remain v2 roadmap work, not prerequisites for the completed classic
source deletion. P1 identifies availability/status boundaries, P2 is
maintainability or policy expansion, and P3 requires a new recovery protocol.

| Priority | Follow-up | Deferred change class |
|---|---|---|
| P1 | Durable per-member Kubernetes resource provenance | Persist exact original Pod/PVC/endpoint identity before disappearance so pre-admission loss can converge. This requires a durable lifecycle/status contract rather than inferring absence from lists. |
| P1 | CRD/status compaction and boundary redesign | Reference context-bound preparation/retirement records from their frozen intent; separate replication proof, Kubernetes cleanup obligation and request metadata. Coordinate status, wire/store, recovery and serialized-size validation. |
| P2 | Shared candidate-selection, exact-cleanup and command-binding helpers | Mechanical refactoring may reduce duplicate policy/identity checks while retaining layer-specific installed-authority and live-session validation. |
| P2 | Independent target/minimum and placement-aware policy | New API, placement input and availability policy are required; current highest-ID selection and target=min remain deliberately narrow. |
| P2 design / P3 implementation | Automatic direct-primary removal composition | The supported composition is planned switchover followed by secondary scale-down. A single movement/removal/recovery request requires a new orchestration protocol. |
| P1 design / P3 implementation | Frozen-primary recovery during removal/cleanup | Safe continuation or overlap with failover requires a cross-epoch recovery and cleanup-ownership protocol; frozen evidence must not be weakened. |
| P3 | Multi-member removal in one reconfiguration | Batch removal changes quorum, identity, evidence and cleanup semantics; current reductions remain sequential. |
| P3 | Durable primary-agent phase coordinator | Per-member ordering, witness freezing, PC/CC progression and restart replay require a durable cross-replica coordination protocol. Desired policy remains controller-owned and Kubernetes cleanup authority stays outside the replicator. |
| P2 | Scale-up operational budget and performance characterization | Existing bounded tests do not define maximum replica count, completion SLO, throughput target or outage bound. |

Compaction must preserve typed structural schemas, exact contextual evidence and
late-member recovery. Opaque schemas, hash-only receipts and TTL evidence
deletion are not substitutes for a coordinated redesign.

Other deferred v2 work includes:

- configurable PVC retention and storage size;
- automatic rolling image/protocol upgrades;
- node-maintenance orchestration for v2;
- application data import or resource conversion.

## Preserved Independent Components

SQL Server observation moved to
[kuberic-mssql](https://github.com/youyuanwu/kuberic-mssql). DEX never depended
on the classic implementation and remains a supported source component.
Independent protobuf namespaces containing `v1`, Service Fabric V1 terminology,
DEX `kuberic.io` metadata and local `level-triggered-v1` development image tags
do not represent classic Kuberic compatibility.
