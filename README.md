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
examples/kvstore/      Replicated key-value store (HashMap + WAL)
examples/sqlite/       Replicated SQLite database (WAL frame shipping)
```

See [kuberic-core](kuberic-core/) and [kuberic-operator](kuberic-operator/) for crate-level documentation.

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

Requires a K8s cluster (tested with [KinD](https://kind.sigs.k8s.io/)):

```bash
# Deploy operator + kvstore (3 replicas)
kubectl apply -f kuberic-operator/deploy/deployment.yaml
kubectl apply -f examples/kvstore/deploy/kubericset.yaml
```

The operator watches `KubericSet` resources and manages the full lifecycle: pod creation, Open → Idle → Active → Primary promotion, failover, and scale up/down.

## Design

- [Core protocols](docs/features/kuberic/protocols.md) — replication, copy, failover, switchover
- [Operator design](docs/features/kuberic/operator.md) — reconciler, CRD, pod management
- [User API](docs/features/kuberic/user-api.md) — PodRuntime, lifecycle events, StateProvider
- [SQLite design](docs/features/sqlite/design.md) — WAL frame shipping, persist-then-ACK
- [Design gaps](docs/features/kuberic/design-gaps.md) — tracked gaps and known limitations
- [Testing strategy](docs/features/kuberic/testing.md) — test layers and patterns

## License

[MIT](LICENSE)