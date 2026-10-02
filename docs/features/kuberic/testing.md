# Kuberic Test Strategy

Kuberic validation is split between Cargo-native local suites and explicitly
owned Kubernetes scenarios. The removed classic v1 stack has no remaining test
selection.

## Workspace Validation

```bash
cargo check --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --doc --workspace
```

The aggregate `cargo test --workspace --all-features` includes DEX's real
Kubernetes provider target and therefore requires its isolated cluster
prerequisites. It is not a cluster-free command.

## Level-Triggered Unit and Durable Validation

```bash
cargo test -p kuberic-protocol -p kuberic-wire \
  -p kuberic-runtime -p kuberic-runtime-internal \
  -p kuberic-agent -p kuberic-controller -p kvstore2 \
  --features kuberic-agent/testing

cargo test -p kuberic-agent --features testing \
  --lib --test runtime --test service --test coordinator \
  --test store --test transport --test crash_boundaries \
  -- --test-threads=1

cargo test -p kuberic-protocol --lib --test protocol --test model
cargo test -p kuberic-controller --lib --test controller
cargo test -p kuberic-runtime --test public_api_inventory
```

The agent crash suite's parent tests execute their ignored child helpers and
reopen durable stores. Top-level ignored process entries are not omitted
coverage.

Controller library tests verify that the checked-in CRD equals the generated
schema. Protocol tests guard representative status growth and reject quadratic
evidence expansion.

## Level-Triggered Live Validation

Live scenarios require Docker, KinD, kubectl, Just, the pinned Rust toolchain
and `protoc`. Use a nondefault cluster, repository-local kubeconfig and exact
matching context:

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
just level-triggered-kind-test switchover scale-down scale-up
just delete-kind-cluster
```

`all` expands to replacement, quorum-loss, adversarial switchover,
scale-down and scale-up scenarios. Run it on fresh bootstrap; standalone
failover requires another fresh cluster. `scale-up-full` expands to healthy,
multi-add and adversarial scale-up.

The `scale-up-adversarial` carried-failover receipt oracle currently fails
independently of classic v1 removal; it is tracked in [#102](https://github.com/youyuanwu/kuberic/issues/102).
The other nine full-matrix scenarios and all PR smoke selectors remain required.

On failure, run `just level-triggered-diagnostics` before deleting the owned
cluster. Every mutation and deletion verifies the ownership receipt and exact
context.

The live copy gate is injected only by the owned-cluster installation recipe.
It is absent from the checked-in user sample and is not a supported production
API.

## SQLite V2 Unit and In-Process Validation

SQLite uses actual WAL frames, public stateful application interfaces, durable
agent/application stores and in-process transport. It requires no Kubernetes
API, container runtime or external database process.

```bash
cargo test -p sqlite-commit-barrier -p sqlite-replicated \
  --all-features -- --test-threads=1
cargo clippy -p kuberic-agent -p sqlite-replicated \
  --all-targets --all-features -- -D warnings
```

Coverage includes bootstrap, replacement, failover, planned switchover,
sequential scale-up, secondary scale-down, quorum restoration, copy restart and
authority races. Fresh v2 storage is required; no classic data import exists.

See the [SQLite design](../sqlite/design.md).

## PostgreSQL V2 Host-Local Validation

PostgreSQL tests run real local subprocesses with durable stores and
exact-session authority. They require an unprivileged Linux account, pidfds,
subreapers, readable `/proc` and compatible PostgreSQL binaries. PostgreSQL 16
is the validated host major. Missing prerequisites fail rather than skip.

```bash
mkdir -p target/paw-tmp
export TMPDIR="$PWD/target/paw-tmp"
cargo test -p postgres-replicated --all-features -- --test-threads=1
cargo clippy -p postgres-replicated --all-targets --all-features -- -D warnings
```

The matrix covers physical build/rewind, fencing, failover, switchover,
replacement, scaling, quorum restoration, read-only secondaries and
application/agent restart. PostgreSQL has no application-specific KinD test.

See the [PostgreSQL design](../postgres/design.md).

## SQL Server Validation

The default SQL Server suite is server-free and uses local fake TDS fixtures:

```bash
cargo fmt -p sqlserver-replicated -- --check
cargo clippy --locked -p sqlserver-replicated \
  --all-targets --all-features -- -D warnings
cargo test --locked -p sqlserver-replicated --all-features
```

The ignored live observation target requires an externally provisioned SQL
Server and explicit image/EULA/TLS/credential configuration. It is independent
of Kuberic controller deployment.

## DEX Validation

DEX default tests are cluster-free:

```bash
CARGO_BUILD_JOBS=2 cargo test -p kuberic-dex --all-targets
CARGO_BUILD_JOBS=2 cargo test -p kuberic-dex --doc
CARGO_BUILD_JOBS=2 cargo clippy -p kuberic-dex \
  --all-targets -- -D warnings
```

The mocked provider and real API targets are:

```bash
CARGO_BUILD_JOBS=2 cargo test -p kuberic-dex \
  --features kubernetes --test kubernetes_checkpoint -- --nocapture
CARGO_BUILD_JOBS=2 cargo test -p kuberic-dex \
  --features kubernetes --test kubernetes_checkpoint_real
```

The real target requires explicit nondefault `KIND_CLUSTER_NAME`,
`KUBECONFIG`, and matching `KUBE_CONTEXT`. It performs access review and owns
its temporary namespace lifecycle.

## Distribution Boundary

No current workflow publishes Kuberic images. Controller/KVStore2 Dockerfiles
and installation recipes are experimental local/CI assets. Existing external
classic images, if present, are unsupported historical artifacts.
