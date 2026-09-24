# Level-Triggered Kuberic Operator

The level-triggered stack is an independent, experimental implementation of
Kuberic stateful replication. It combines Kubernetes-style reconciliation
with Service Fabric-shaped replica, reconfiguration-agent, and replicator
boundaries.

> **Status:** implemented for development and CI validation, not production
> use. The controller and `kvstore2` images are not published release
> artifacts.

The stack coexists with the classic `kuberic.io/v1` operator. It uses a
different API group, controller deployment, image names, labels, Services,
source crates, and example. It does not migrate, reinterpret, replace, or
remove classic resources.

The historical rationale and protocol design remain in the
[level-triggered operator proposal](../../proposal/level-triggered-operator-design.md).

## Architecture

| Component | Responsibility |
|---|---|
| `kuberic-protocol` | Canonical identities, epochs, PC/CC configurations, validation, commands, normalized observations, and pure evaluation |
| `kuberic-wire` | Exact-version protobuf and tonic contracts for controller, peer, copy, and replication traffic |
| `kuberic-runtime` | Application interfaces, replication/copy streams, exact-authority acknowledgements, and quorum mechanics |
| `kuberic-agent` | Replica process hosting, durable local authority, reconfiguration coordination, authenticated listeners, and restart reconstruction |
| `kuberic-controller` | Kubernetes observation, normalization, pure-plan execution, routing fences, resource convergence, and bounded re-observation |
| `kvstore2` | Small HTTP key-value conformance application for the independent stack |

The controller is the Failover Manager-equivalent owner. The agent and its
coordinator own Replica Agent-equivalent local reconfiguration. `ReplicaHost`
and `PodRuntime` own hosting and lifecycle effects. The default runtime
replicator owns data-plane replication, copy, retained send windows, and
quorum credit.

Every reconcile observes the current CR, Pods, PVCs, Services, Secrets, and
available agent reports; normalizes them; validates the complete snapshot; and
evaluates a pure plan. A plan may converge independent Kubernetes resources,
but it contains at most one authority-changing replica command. Lost replies
are resolved by observing durable command identity and postconditions rather
than by trusting process-local controller history.

Stable and waiting states always requeue. Watch delivery is an optimization,
not the correctness mechanism.

## Authority and Identity

The following identities are intentionally distinct:

| Identity or state | Meaning |
|---|---|
| Replica ID | Stable logical member number within one `KubericSet` |
| Pod UID / replica instance ID | Exact Kubernetes Pod incarnation |
| PVC UID | Exact storage incarnation |
| Durable agent generation | Persisted replica-local authority identity; survives process restart |
| Process session | One running agent process; changes after every process restart |
| Epoch | Ordered data-loss and configuration authority |
| Previous/Current Configuration (PC/CC) | Quorum authority during reconfiguration |
| Runtime role | `Primary`, `ActiveSecondary`, `IdleSecondary`, or `None` |
| WriteStatus | Independent write-access gate; Primary role alone never grants writes |
| Routing | Kubernetes Service selection; changed only after durable authority postconditions |

An agent process restart changes the process session and resets report
sequencing, but it does not change the durable generation. A new Pod or PVC
incarnation cannot inherit old authority. Status, routing labels, command
responses, and Pod readiness are observations; none is sufficient alone to
grant replica authority.

Raw application progress is repair evidence only. Protocol version 3 carries a
separate authority-bound `verifiedReplicationLsn`. The current primary
revalidates that certificate before it can contribute remote quorum credit.

## Kubernetes API

The CRD is `operator.kuberic.io/v1alpha1`, kind `KubericSet`.

```yaml
apiVersion: operator.kuberic.io/v1alpha1
kind: KubericSet
metadata:
  name: kvstore2
spec:
  replicas: 3
  image: localhost/kvstore2:level-triggered-v1
  failoverDelaySeconds: 10
```

`spec.replicas` must be positive. The implemented operations preserve fixed
cardinality; changing the replica count after initialization is not a
supported scaling API. `spec.image` selects the application image.
`spec.failoverDelaySeconds` defaults to 30.

Status is controller-owned durable authority. Users must not author or repair
it manually. Important projections include:

- `initialized` and `observedGeneration`;
- `effectivePolicy`, frozen while authority is active;
- the last quorum-attested `topology`;
- at most one compact `transition` and one fresh `provisioning` intent;
- `primaryFailure` and same-configuration `quorumLoss`;
- conditions describing waiting or unsafe observations.

The checked-in CRD is generated from the Rust type and verified byte-for-byte
by `scripts/check_level_triggered_documentation.sh`.

## Supported Behavior

The implemented contract supports:

- write-closed full-set bootstrap and routing publication only after the full
  genesis configuration is accepted; the sample and live harness use three
  members;
- exact-incarnation, same-cardinality secondary replacement using copy plus
  replication-gap closure;
- epoch-fenced ordinary primary failover with election-safe progress;
- retained-history repair or full-copy fallback during supported failover;
- quorum-loss detection, `NoWriteQuorum`, and non-destructive write recovery
  when the same configuration quorum returns;
- controller restart, replica process restart, lost command replies, stale
  observations, and bounded no-watch healing;
- rejection of stale epochs, old Pod incarnations, retired process sessions,
  mixed protocol versions, and stale former-primary writes.

The following remain fail-closed and require separate design:

- destructive data-loss recovery, PC/CC abandonment, or non-intersecting
  recovery;
- planned switchover and user-requested primary movement;
- scale-up or scale-down;
- timed dropping of unavailable replicas;
- PVC authority rebinding to a new Pod UID;
- automatic rolling image/spec upgrades;
- mixed-version protocol negotiation;
- publication of the controller and `kvstore2` images as release artifacts.

The current generated model validates authority observations and single-writer
admissibility, while the live matrix exercises bounded sequential writes
through restart, partition, replacement, and failover. A stateful model of
successful writes across delayed/reordered effects and concurrent retained
client connections is deferred validation work; it is not evidence for
loosening any fencing invariant.

Quorum loss does not advance the data-loss number and does not call an
application data-loss callback. Unsupported evidence produces `Wait` or
`Unsafe`, never success-shaped recovery.

## Runtime and Storage Contract

Each replica receives one ReadWriteOnce PVC mounted at `/var/lib/kuberic`.
`kvstore2` uses:

```text
/var/lib/kuberic/
├── application/             application-owned state and retained operations
└── .kuberic/
    └── agent.sqlite3        agent authority, effects, commands, and progress
```

Application data and protocol metadata have separate lifecycles. The
application must durably apply a replication or copy operation before
acknowledging it. Transport receipt may advance resend state, but only durable
application acceptance can grant quorum credit.

The agent database is created only through authorized initialization. Opening
an established replica requires matching resource, Pod, PVC, replica,
generation, initialization, policy, and schema identity. Missing metadata,
identity drift, corruption, or an incompatible schema fails closed.

SQLite runs in WAL mode with `synchronous=FULL`, foreign keys enabled,
automatic WAL checkpoints disabled, and a five-second busy timeout. The agent
is the single database writer. The PVC filesystem must preserve fsync and
directory-entry durability and support SQLite WAL locking/shared-memory
semantics. Filesystems that cannot provide those semantics, including
unsupported network-filesystem arrangements, are not valid production
storage.

The current schema accepts only its exact version. The migration hook records
an idempotent current-version migration; it does not upgrade older schemas.
Future schema changes require an explicit, crash-safe migration before mixed
data can be opened.

Crash injection is test-only. `KUBERIC_CRASH_WRITER_PATH` and
`KUBERIC_CRASH_BOUNDARY` are consumed only by the
`kuberic-agent` crash-boundary test executable. Production binaries expose no
fault-injection switch.

## Local Deployment

Prerequisites:

- Docker;
- KinD and `kubectl`;
- `just`;
- the repository Rust toolchain and `protoc`.

Use a dedicated cluster, kubeconfig, and ownership receipt. The recipes refuse
the default `kind` cluster and `$HOME/.kube/config`.

```bash
export KIND_CLUSTER_NAME=kuberic-level-dev
export KUBECONFIG="$(mktemp -d)/kubeconfig"
export KUBE_CONTEXT="kind-${KIND_CLUSTER_NAME}"
export KUBERIC_AGENT_BEARER_TOKEN=local-level-triggered-token

just create-kind-cluster
just level-triggered-images
just level-triggered-install
```

The installation creates the `kuberic-system` namespace, controller
credentials, the CRD/RBAC/controller deployment, and the sample `kvstore2`
resource. The controller creates replica PVCs, Pods, exact-incarnation peer
Services, a headless discovery Service, credentials, and the
`kvstore2-write` Service.

Observe initialization:

```bash
kubectl --kubeconfig "$KUBECONFIG" --context "$KUBE_CONTEXT" \
  get kubericsets.operator.kuberic.io kvstore2 -o yaml
kubectl --kubeconfig "$KUBECONFIG" --context "$KUBE_CONTEXT" \
  get pods,pvc,services -l operator.kuberic.io/set-name=kvstore2
```

For a local client, forward the write-routed Service in one terminal:

```bash
kubectl --kubeconfig "$KUBECONFIG" --context "$KUBE_CONTEXT" \
  port-forward service/kvstore2-write 18080:80
```

Then use a second terminal:

```bash
curl --fail-with-body --max-time 10 -X PUT --data 'value' \
  http://127.0.0.1:18080/kv/key
curl --fail-with-body --max-time 10 \
  http://127.0.0.1:18080/kv/key
```

The PUT response is the committed LSN. A write may return HTTP 503 while the
replica is not Primary, reconfiguring, or lacks write quorum.

Clean up only through the ownership-checked recipe:

```bash
just delete-kind-cluster
```

Local images use fixed `level-triggered-v1` tags with `IfNotPresent`. After an
image change, use a fresh cluster or explicitly reload the images and recreate
the affected Pods.

## Tests and Diagnostics

Build the isolated packages:

```bash
just level-triggered-build
```

Run an explicit live scenario after installing the stack:

```bash
just level-triggered-kind-test bootstrap
just level-triggered-kind-test failover
just level-triggered-kind-test replacement
just level-triggered-kind-test quorum-loss
just level-triggered-kind-test adversarial
```

The bounded full matrix expands to replacement, quorum loss, and the composed
adversarial restart/partition/failover scenario:

```bash
just level-triggered-kind-test all
```

The PR workflow separately runs bootstrap and failover smoke tests. Scheduled
and manually dispatched full CI runs the matrix twice on separate fresh
clusters.

On failure, collect the CR, resource state, events, controller logs, and all
replica logs:

```bash
just level-triggered-diagnostics
```

Each replica also exposes `GET /status`, including its exact identity, durable
generation, process session, role, epoch, PC/CC IDs, progress, committed LSN,
write status, pending operation, and active builds.

## Controller Configuration

The controller deployment requires `KUBERIC_AGENT_BEARER_TOKEN`. These
optional environment variables tune bounded re-observation:

| Variable | Default | Purpose |
|---|---:|---|
| `KUBERIC_STABLE_RESYNC_SECONDS` | 30 | Re-observe stable resources without a watch event |
| `KUBERIC_WAIT_REQUEUE_SECONDS` | 5 | Re-observe incomplete but potentially recoverable work |
| `KUBERIC_UNSAFE_REQUEUE_SECONDS` | 30 | Re-observe unsafe evidence without mutating authority |
| `KUBERIC_RPC_DEADLINE_SECONDS` | 5 | Per-agent RPC deadline |

Authentication is a shared bearer token distributed through Kubernetes
Secrets. It is suitable for the isolated development harness, not a complete
production identity or key-rotation design.

## API and Coexistence Guards

The level-triggered crates have no source dependency on classic v1 crates.
`scripts/check_level_triggered_scope.sh` rejects changes under protected v1
paths, and `scripts/check_level_triggered_dependencies.sh` rejects manifest,
source-link, and include-based dependencies on them.

The runtime API guard has two reviewed inventories:

- generated rustdoc for the intended application-facing surface;
- an exhaustive `syn`-parsed inventory of every source `pub` signature,
  including declarations under private paths and `#[doc(hidden)]` paths.

Compile-fail fixtures additionally prove that safe external application code
cannot obtain the managed replicator, construct a host partition, inject
agent authority dependencies, register a managed runtime directly, or access
the private authority module. Hidden public declarations exist where Rust
cross-crate hosting requires nameable signatures; `#[doc(hidden)]` is not
treated as an access-control boundary.

Run all documentation and API checks with:

```bash
scripts/check_level_triggered_documentation.sh
```

Classic v1 remains the documented path for existing `KubericSet` resources,
switchover, scaling, and the SQLite/PostgreSQL examples. No migration or
classic-path removal is implied.
