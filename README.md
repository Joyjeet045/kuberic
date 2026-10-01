# Kuberic

> **⚠️ Experimental** — This project is under active development and not ready for production use.

A stateful replication framework for Kubernetes. Provides quorum-based replication with automatic failover, switchover, copy-based replica building, and epoch-based fencing.

## Features

- **Quorum replication** — primary replicates to secondaries, blocks until write quorum ACKs
- **Automatic failover** — operator detects primary failure, promotes best secondary
- **Graceful switchover** — write revocation → demotion → promotion with rollback on failure
- **Copy protocol** — new replicas built from full snapshot + incremental replay
- **Epoch fencing** — stale primaries rejected via monotonic epoch numbers
- **Kubernetes native** — custom operator with `KubericSet` CRD, bare pod management

## Architecture

```
kuberic-core/          Core replication framework (replicator, driver, runtime)
kuberic-operator/      K8s operator (reconciler, CRD, pod management)
kuberic-dex/           Durable execution and deterministic replay kernel
examples/kvstore/      Replicated key-value store (HashMap + WAL)

kuberic-protocol/      Independent level-triggered domain model and evaluator
kuberic-runtime/       Independent application and replication runtime
kuberic-agent/         Durable replica-local authority and process hosting
kuberic-controller/    operator.kuberic.io/v1alpha1 controller
examples/kvstore2/     Level-triggered conformance application
examples/sqlite/       Existing SQLite example migrated in place to v2
examples/postgres/     V2 custom replicator using PostgreSQL-native replication
```

See [kuberic-core](kuberic-core/), [kuberic-operator](kuberic-operator/), and
[Kuberic DEX](kuberic-dex/) for classic crate-level documentation. The
[level-triggered operator guide](docs/features/kuberic/level-triggered-operator.md)
documents the independent experimental stack.

Classic [kvstore](examples/kvstore/) uses `PodRuntime` and `WalReplicator`.
The v2 [SQLite example](docs/features/sqlite/design.md) uses public
`StatefulServiceReplica`/`StateProvider` interfaces and agent-owned `ReplicaHost`.

## Quick Start

```bash
# Build
cargo check
cargo clippy --all-targets

# Test (no K8s required)
cargo test -p kuberic-core -p kvstore -p sqlite-replicated
# V2 SQLite unit/in-process validation (no cluster or child-process tests)
cargo test -p sqlite-commit-barrier -p sqlite-replicated --all-features -- --test-threads=1
# V2 PostgreSQL unit/host-local subprocess validation (local PostgreSQL required)
cargo test -p postgres-replicated --all-features -- --test-threads=1

# Run kvstore in demo mode (single node, no operator)
cargo run -p kvstore -- --demo
```

## Examples

### KVStore

Replicated `HashMap<String, String>` with gRPC Put/Get/Delete API. Demonstrates the full protocol: quorum writes, copy-based rebuild, failover, switchover, epoch rollback. 23 integration tests including an 8-test mock reconciler suite.

### SQLite

The existing `sqlite-replicated` package is migrated in place to v2, with no
classic runtime/operator dependencies or second SQLite application. Its gRPC
Execute/Query/ExecuteBatch API uses quorum-before-publication WAL-frame replication,
committed snapshots plus retained catch-up, and durable restart/reconciliation/
rebuild fencing. Unit and in-process tests cover replacement, failover, planned
switchover, sequential scale-up and secondary scale-down. Fresh v2 storage is
required: there is no v1 data import. SQLite image publication and deployment
assets remain future work; see the [design and local validation guide](docs/features/sqlite/design.md).

### PostgreSQL

The existing `postgres-replicated` package is migrated in place to v2 through
ordinary `Replicator`/`PrimaryReplicator`, not operation/copy streams. PostgreSQL
owns WAL, physical replication and recovery; Kuberic owns generic authority and
SF choreography. Host-local tests cover fencing, build, failover, switchover,
scaling and restart. Workstream 4 is complete; images/distribution remain
Workstream 5. Fresh deployment is required, with no v1 data import or PostgreSQL
KinD coverage. See the [design and trust boundaries](docs/features/postgres/design.md).

## Kubernetes Deployment

For local development and CI, use the [shared Gateway KinD setup](docs/features/envoy-gateway-kind.md).
It deploys the operator and two three-replica KVStore applications behind one
loopback port. Run `just prepare-external-dependencies` once, then
`just kvstore-deploy` installs this setup in the owned cluster without downloading
external manifests or Helm charts.

The operator watches `KubericSet` resources and manages the full lifecycle: pod creation, Open → Idle → Active → Primary promotion, failover, and scale up/down.

The independent level-triggered controller uses
`operator.kuberic.io/v1alpha1` and distinct deployment assets. It currently
supports full-set bootstrap, replacement, ordinary failover, explicit
named-target [planned switchover](docs/features/kuberic/level-triggered-operator.md#planned-switchover),
[secondary scale-down](docs/features/kuberic/level-triggered-operator.md#secondary-scale-down)
down to one, and non-destructive quorum-loss recovery for `kvstore2`.
This is SF-inspired secondary scale-down using PC/CC quorum principles, with
Kuberic-specific target/minimum coupling, deterministic selection, write closure,
sequential cleanup, and Kubernetes resource deletion. `spec.replicas` target=min
is Kuberic policy; SF target and minimum are independently configurable.
Retained read-quorum preflight preserves existing service when admission must
wait. Exact original PVC provenance must be reconstructable before admission;
PVC object deletion has no retention or import path, not a physical-erasure
guarantee. After admission, frozen-primary loss can block recovery indefinitely.
Reconfiguration may interrupt writes and connections with no duration guarantee.
V2 images remain local/CI-only;
the production v2 controller configuration enables
[sequential scale-up](docs/features/kuberic/level-triggered-operator.md#sequential-scale-up)
one fresh incarnation at a time. Classic v1 remains unchanged. Automatic
direct-primary removal is deferred; use planned switchover followed by
secondary scale-down. An independent minimum replica count, a validated
maximum replica count remain future work. SQLite and PostgreSQL are
ported in place and validated without Kubernetes; their distribution remains
separate from the existing KVStore2 live deployment.
Protocol 9 / agent schema 5 require a fresh coordinated v2 deployment.

## Continuous Delivery

After CI passes, pushes to `main` and version tags publish Linux AMD64 images to
GitHub Container Registry:

- `ghcr.io/${{ github.repository_owner }}/kvstore`
- `ghcr.io/${{ github.repository_owner }}/kuberic-operator`

Main-branch images receive immutable commit SHA tags. A semantic version tag
such as `v0.1.0` also publishes the exact version tag.

## Design

- [Core protocols](docs/features/kuberic/protocols.md) — replication, copy, failover, switchover
- [Operator design](docs/features/kuberic/operator.md) — reconciler, CRD, pod management
- [User API](docs/features/kuberic/user-api.md) — PodRuntime, lifecycle events, StateProvider
- [SQLite design](docs/features/sqlite/design.md) — WAL frame shipping, persist-then-ACK
- [PostgreSQL design](docs/features/postgres/design.md) — v2 authority, native replication, SQL fencing and host-local validation
- [SQL Server design](docs/features/sqlserver/design.md) — native AG contract and safety gates
- [SQL Server observation](docs/features/sqlserver/observation.md) — observe-only runtime, configuration, and tests
- [Design gaps](docs/features/kuberic/design-gaps.md) — tracked gaps and known limitations
- [Testing strategy](docs/features/kuberic/testing.md) — test layers and patterns
- [Level-triggered operator](docs/features/kuberic/level-triggered-operator.md) — independent stack deployment, authority, supported operations, and diagnostics
- [Secondary scale-down](docs/features/kuberic/level-triggered-operator.md#secondary-scale-down) — lower desired membership, singleton risks, and exact permanent cleanup
- [Sequential scale-up](docs/features/kuberic/level-triggered-operator.md#sequential-scale-up) — add or restore one fresh ordinal at a time through copy, catch-up, and PC/CC admission
- [V1 retirement plan](docs/proposal/v1-retirement-plan.md) — completed switchover and scaling workstreams, deferred direct-primary removal, and remaining application/distribution/deprecation gates
- [Kuberic DEX roadmap](docs/features/kuberic/kuberic-dex-roadmap.md) — durable execution kernel boundary and deferred work

## License

[MIT](LICENSE)
