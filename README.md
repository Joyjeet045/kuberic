# Kuberic

> **Experimental** — Kuberic is under active development and is not ready for
> production use. The classic v1 stack has been removed. Published v2 images
> and a supported release installation are not available yet.

Kuberic is a Service Fabric-inspired stateful replication framework for
Kubernetes. The current level-triggered stack provides quorum replication,
automatic failover, planned switchover, copy-based replica building,
sequential scale-up, secondary scale-down, and epoch/session fencing.

## Repository

```text
kuberic-protocol/          Authority, topology and command model
kuberic-wire/              Generated control and replication wire contracts
kuberic-runtime/           Stateful application and replicator interfaces
kuberic-runtime-internal/  Durable authority/effect implementation
kuberic-agent/             Replica-local authority and process hosting
kuberic-controller/        operator.kuberic.io/v1alpha1 controller
kuberic-level-tests/       Isolated KinD scenario harness
kuberic-dex/               Durable execution and checkpoint kernel

examples/kvstore2/         Level-triggered conformance application
examples/sqlite/           SQLite v2 application
examples/postgres/         PostgreSQL-native v2 custom replicator
examples/sqlserver/        Independent SQL Server observer
sqlite-commit-barrier/     SQLite durability helper
```

The former `kuberic-core`, `kuberic-operator`, classic KVStore and classic
integration-test packages were removed because they had no active users.
There is no compatibility layer, resource conversion or application-data
migration path.

## Local Validation

Install the pinned Rust toolchain and `protoc`. The repository installer pins
`cargo-nextest` and verifies its archive checksum:

```bash
just install-nextest
cargo check --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --doc --workspace --all-features
just nextest-test
```

PostgreSQL validation requires compatible host-local PostgreSQL binaries and an
unprivileged Linux account:

```bash
mkdir -p target/paw-tmp
export TMPDIR="$PWD/target/paw-tmp"
just nextest-postgres
```

Build once and verify the exact ordinary/PostgreSQL partitions with:

```bash
just nextest-archive
just nextest-validate-archive
```

The complete tier inventory, shard commands, SQL Server external fixtures and
DEX Kubernetes-provider commands are documented in the
[test strategy](docs/features/kuberic/testing.md).

## Experimental Kubernetes Validation

The retained controller and KVStore2 Dockerfiles are development/CI assets.
They are not published release images.

```bash
export KIND_CLUSTER_NAME=kuberic-level-dev
mkdir -p target/kuberic-level-dev
export KUBECONFIG="$PWD/target/kuberic-level-dev/kubeconfig"
export KUBE_CONTEXT="kind-${KIND_CLUSTER_NAME}"
export KUBERIC_AGENT_BEARER_TOKEN=local-level-triggered-token

just create-kind-cluster
just level-triggered-images
just level-triggered-install
just level-triggered-kind-test bootstrap replacement failover
just delete-kind-cluster
```

Use nondefault owned cluster coordinates. The recipes verify the exact
cluster/context/kubeconfig tuple before every mutation and deletion.

## Applications

### KVStore2

The conformance application exercises the complete level-triggered controller,
agent and runtime path through an HTTP key/value API. Its isolated live matrix
covers bootstrap, replacement, failover, quorum loss, planned switchover,
secondary scale-down, sequential scale-up and adversarial recovery.

### SQLite

`sqlite-replicated` uses quorum-before-publication WAL-frame replication,
committed snapshots and retained catch-up. Fresh v2 storage is required; no
classic data is imported. See the [SQLite design](docs/features/sqlite/design.md).

### PostgreSQL

`postgres-replicated` is an application-owned SF-style custom replicator.
PostgreSQL owns WAL streaming, physical recovery, synchronous policy and
promotion while Kuberic owns generic authority and lifecycle choreography.
Fresh protocol-9/schema-5 storage is required. See the
[PostgreSQL design](docs/features/postgres/design.md).

### SQL Server

`sqlserver-replicated` is an independent native availability-group observer. It
does not deploy or mutate SQL Server. See the
[SQL Server observation guide](docs/features/sqlserver/observation.md).

## Distribution Status

Classic source and image publication have been removed. Existing external
classic registry artifacts, if still present, are unsupported historical
artifacts.

V2 distribution is deferred. Main-branch and version-tag workflows currently
publish no Kuberic controller or application images. Build from source and use
the isolated development/CI assets above.

## Documentation

- [Level-triggered operator](docs/features/kuberic/level-triggered-operator.md)
- [Testing strategy](docs/features/kuberic/testing.md)
- [SQLite design](docs/features/sqlite/design.md)
- [PostgreSQL design](docs/features/postgres/design.md)
- [SQL Server design](docs/features/sqlserver/design.md)
- [Kuberic DEX roadmap](docs/features/kuberic/kuberic-dex-roadmap.md)
- [V1 removal record](docs/proposal/v1-retirement-plan.md)
- [Archived classic architecture](docs/archive/v1/README.md)

Service Fabric and other background material under `docs/background/` remains
technology reference material rather than a Kuberic compatibility promise.

## License

[MIT](LICENSE)
