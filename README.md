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
examples/sqlite/       Replicated SQLite database (WAL frame shipping)

kuberic-protocol/      Independent level-triggered domain model and evaluator
kuberic-runtime/       Independent application and replication runtime
kuberic-agent/         Durable replica-local authority and process hosting
kuberic-controller/    operator.kuberic.io/v1alpha1 controller
examples/kvstore2/     Level-triggered conformance application
```

See [kuberic-core](kuberic-core/), [kuberic-operator](kuberic-operator/), and
[Kuberic DEX](kuberic-dex/) for classic crate-level documentation. The
[level-triggered operator guide](docs/features/kuberic/level-triggered-operator.md)
documents the independent experimental stack.

The framework provides `PodRuntime` and `WalReplicator` — your service implements lifecycle event handlers and a gRPC API. See the [kvstore](examples/kvstore/) and [sqlite](examples/sqlite/) examples.

## Quick Start

```bash
# Build
cargo check
cargo clippy --all-targets

# Test (no K8s required)
cargo test -p kuberic-core -p kvstore -p sqlite-replicated

# Run kvstore in demo mode (single node, no operator)
cargo run -p kvstore -- --demo
```

## Examples

### KVStore

Replicated `HashMap<String, String>` with gRPC Put/Get/Delete API. Demonstrates the full protocol: quorum writes, copy-based rebuild, failover, switchover, epoch rollback. 23 integration tests including an 8-test mock reconciler suite.

### SQLite

Replicated SQLite database with gRPC Execute/Query/ExecuteBatch API. Ships WAL frames (page-level) after each commit — no determinism requirements. Secondaries persist frames to a durable log and apply on promotion. 9 integration tests covering replication, failover, and switchover.

## Kubernetes Deployment

For local development and CI, use the [shared Gateway KinD setup](docs/features/envoy-gateway-kind.md).
It deploys the operator and two three-replica KVStore applications behind one
loopback port. Run `just prepare-external-dependencies` once, then
`just kvstore-deploy` installs this setup in the owned cluster without downloading
external manifests or Helm charts.

The operator watches `KubericSet` resources and manages the full lifecycle: pod creation, Open → Idle → Active → Primary promotion, failover, and scale up/down.

The independent level-triggered controller uses
`operator.kuberic.io/v1alpha1` and distinct deployment assets. It currently
supports fixed-cardinality bootstrap, replacement, ordinary failover, and
non-destructive quorum-loss recovery for `kvstore2`; its images remain
local/CI-only.

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
- [SQL Server design](docs/features/sqlserver/design.md) — native AG contract and safety gates
- [SQL Server observation](docs/features/sqlserver/observation.md) — observe-only runtime, configuration, and tests
- [Design gaps](docs/features/kuberic/design-gaps.md) — tracked gaps and known limitations
- [Testing strategy](docs/features/kuberic/testing.md) — test layers and patterns
- [Level-triggered operator](docs/features/kuberic/level-triggered-operator.md) — independent stack deployment, authority, supported operations, and diagnostics
- [Kuberic DEX roadmap](docs/features/kuberic/kuberic-dex-roadmap.md) — durable execution kernel boundary and deferred work

## License

[MIT](LICENSE)
