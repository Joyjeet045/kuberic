# Development

Kuberic currently supports source builds and local/CI validation only. The
classic v1 stack and its image publication were removed; v2 release
distribution is deferred.

## Rust Prerequisites

Use the pinned toolchain in `rust-toolchain.toml` and install `protoc`.

```bash
cargo check --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## Level-Triggered Local Cluster

Install Docker, KinD, kubectl and Just. Always use a nondefault owned cluster:

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
just level-triggered-diagnostics
just delete-kind-cluster
```

`just level-triggered-kind-test all` runs the ten-scenario matrix from fresh
bootstrap. Run standalone failover on a separate fresh cluster. See
[tests and diagnostics](features/kuberic/level-triggered-operator.md#tests-and-diagnostics)
for selectors, ownership rules and cleanup.

The controller and KVStore2 images built by these recipes are local development
artifacts. They are not published or supported distribution images.

## PostgreSQL

PostgreSQL tests use host-local subprocesses, not containers or KinD. Use an
unprivileged Linux account with compatible server/client binaries, pidfds,
subreapers and readable `/proc`. PostgreSQL 16 is the validated major.

```bash
mkdir -p target/paw-tmp
export TMPDIR="$PWD/target/paw-tmp"
cargo test -p postgres-replicated --all-features -- --test-threads=1
cargo clippy -p postgres-replicated --all-targets --all-features -- -D warnings
```

See the [PostgreSQL validation guide](features/kuberic/testing.md#postgresql-v2-host-local-validation)
and [design](features/postgres/design.md).

## DEX Kubernetes Provider

Default DEX tests are cluster-free. The real provider test requires an isolated
owned Kubernetes endpoint:

```bash
CARGO_BUILD_JOBS=2 cargo test -p kuberic-dex --all-targets
CARGO_BUILD_JOBS=2 cargo test -p kuberic-dex --doc
CARGO_BUILD_JOBS=2 cargo test -p kuberic-dex \
  --features kubernetes --test kubernetes_checkpoint -- --nocapture
CARGO_BUILD_JOBS=2 cargo test -p kuberic-dex \
  --features kubernetes --test kubernetes_checkpoint_real
```

The real test uses `KIND_CLUSTER_NAME`, `KUBECONFIG` and `KUBE_CONTEXT`; it
creates and deletes its own namespace.
