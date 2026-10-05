# Kuberic Test Strategy

Kuberic validation is split into repository-wide nextest tiers, doctests and
explicitly owned Kubernetes scenarios. The removed classic v1 stack has no
remaining test selection.

## Workspace Validation

```bash
just install-nextest
cargo check --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --doc --workspace --all-features
just nextest-test
```

The `ordinary` profile is cluster-free and excludes PostgreSQL, DEX's real
Kubernetes target and build-only guards. It runs with four global slots; the
SQLite group has one slot, and each runtime host process-boundary test reserves
all four slots so its subprocess recovery deadlines are not competing with
another test. CI divides the current ordinary inventory into four slices.

The all-features archive includes every test binary, including resource-backed
tiers. Validate its exact-one disposition and partition union before relying on
it:

```bash
just nextest-archive
just nextest-validate-archive
```

The consolidated validator accounts for 1,109 tests: 873 ordinary, one build-only,
203 directly runnable PostgreSQL tests, 12 live KinD scenarios, one DEX live test
and 19 mapped parent-driven subprocess helpers.
Use its generated counts as the inventory evolves.

The ignored runtime `public_api_privacy` binary belongs only to the `build-only`
tier (`just nextest-list build-only`). It invokes Cargo and needs dependency
caches and workspace sources, so CI runs `scripts/check_runtime_public_api.sh`
in the build-artifact job before archiving, never on archive-only runners.
`just nextest-build-only` runs the same guard, and `just nextest-archive`
includes it as a prerequisite. Archive inventory validation still accounts for
the ignored test exactly once.

## Level-Triggered Unit and Durable Validation

Use `just nextest-test` for the complete cluster-free tier. For focused
debugging, nextest accepts Cargo package/target selectors in addition to the
repository profile:

```bash
cargo nextest run --profile ordinary -p kuberic-runtime --all-features
cargo nextest run --profile ordinary -p kuberic-controller
```

The runtime `host::tests::crash_boundaries` and `host::tests::recovery` parent
tests execute their ignored child helpers and
reopen durable stores. Top-level ignored process entries are not omitted
coverage.

Controller library tests verify that the checked-in CRD equals the generated
schema. Runtime protocol tests guard representative status growth and reject quadratic
evidence expansion.

## Level-Triggered Live Validation

Live scenarios require Docker, KinD, kubectl, Just, the pinned Rust toolchain,
the pinned nextest binary and `protoc`. The `kind` profile and `kind-live`
group select exactly the 12 ignored live scenarios and use one execution slot.
Use a nondefault cluster, repository-local kubeconfig and exact matching
context:

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
cargo test -p sqlite-replicated \
  --all-features -- --test-threads=1
cargo clippy -p kuberic-runtime -p sqlite-replicated \
  --all-targets --all-features -- -D warnings
```

The ordinary nextest tier includes the SQLite example and its private barrier
module's tests under one serial
`sqlite` group, so `just nextest-test` preserves the same resource boundary.

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
just nextest-postgres-smoke
just nextest-postgres
# Same deterministic four-way hash partition used by an isolated CI runner:
just nextest-postgres 1/4
cargo clippy -p postgres-replicated --all-targets --all-features -- -D warnings
```

The smoke target is intended for routine local checks and covers representative
real lifecycle paths. The full target remains the complete pre-push and CI
validation.

The matrix covers physical build/rewind, fencing, failover, switchover,
replacement, scaling, quorum restoration, read-only secondaries and
application/agent restart. The profile has one execution slot. Its complete
205-test inventory is 203 directly partitioned tests plus two ignored
subprocess helpers executed by mapped parent tests. PostgreSQL has no
application-specific KinD test.

See the [PostgreSQL design](../postgres/design.md).

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
its temporary namespace lifecycle. `just nextest-list dex-live` selects exactly
that target. Pull requests run it on the owned smoke cluster; main, version-tag
and CI-manual events run it on a separate receipt-gated cluster.

## Distribution Boundary

No current workflow publishes Kuberic images. Controller/KVStore2 Dockerfiles
and installation recipes are experimental local/CI assets. Existing external
classic images, if present, are unsupported historical artifacts.
