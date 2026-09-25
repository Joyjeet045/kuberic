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

Raw application progress is repair evidence only. Protocol version 3
introduced a separate authority-bound `verifiedReplicationLsn`; protocol
version 4 additionally binds control commands to the exact observed target
process session. Version 5 adds accepted-spec-generation fencing for preparation
and retirement; all level-triggered components must use that exact version.
The current primary revalidates the progress certificate
before it can contribute remote quorum credit.

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
- `transition.switchover` for a frozen active request and handoff;
- `lastSwitchover` for the latest terminal receipt (not a history);
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
- explicit named-target planned switchover with durable write handoff,
  original-authority restoration, and evidence-gated newer-epoch compensation;
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
- automatic switchover target selection, cancellation/retargeting of active
  requests, and node-maintenance orchestration;
- scale-up or scale-down;
- timed dropping of unavailable replicas;
- PVC authority rebinding to a new Pod UID;
- automatic rolling image/spec upgrades;
- mixed-version protocol negotiation;
- publication of the controller and `kvstore2` images as release artifacts.

The generated model validates authority observations, single-writer
admissibility, and switchover recovery traces. Runtime race and subprocess
crash tests cover the handoff's durable boundaries; the live matrix exercises
bounded writes and retained TCP clients through restart, partition, replacement,
failover, and switchover. A broader concurrent successful-write history
generator remains deferred; the focused handoff tests do not claim that scope.

Quorum loss does not advance the data-loss number and does not call an
application data-loss callback. Unsupported evidence produces `Wait` or
`Unsafe`, never success-shaped recovery.

## Planned Switchover

Wait for a stable Ready partition, inspect `status.topology.members`, and choose
a healthy committed `activeSecondary` by its logical `replicaId`, not by Pod
name. Every exact member must have completed the accepted current-only
authority. No replacement, failover, or other authority transition runs
concurrently with an accepted switchover.

Submit a unique nonempty request ID and a positive target ID:

```yaml
apiVersion: operator.kuberic.io/v1alpha1
kind: KubericSet
metadata:
  name: kvstore2
spec:
  replicas: 3
  image: localhost/kvstore2:level-triggered-v1
  failoverDelaySeconds: 10
  switchover:
    requestId: move-to-replica-2-001
    targetReplicaId: 2
```

Here replica 2 must actually be an eligible secondary. Keep the existing image
and policy when updating an existing resource. For that resource, the equivalent
request-only patch is:

```bash
kubectl --kubeconfig "$KUBECONFIG" --context "$KUBE_CONTEXT" \
  patch kubericsets.operator.kuberic.io kvstore2 --type=merge \
  -p '{"spec":{"switchover":{"requestId":"move-to-replica-2-001","targetReplicaId":2}}}'
```

Acceptance freezes the request ID, exact source and target (logical ID, Pod
incarnation, durable generation), policy, starting configuration, and requested
configuration in `status.transition`. The controller removes write routing,
prepares the source by closing admission and resolving in-flight completions,
and persists the durable handoff boundary. The target must prove
`verifiedReplicationLsn` through that boundary under the starting authority;
raw application progress is insufficient.

The requested configuration preserves exact membership and data-loss number,
and advances the configuration number. Source demotion precedes write-closed
target promotion. PC/CC (previous/current configuration) quorums and certified
catch-up precede current-only completion. The controller then accepts topology
and the receipt, separately grants writes, and finally publishes exact-Pod
write-Service routing. Role, receipt, and routing are not interchangeable
proofs of write availability.

### Outcomes and retry contract

Inspect `status.lastSwitchover` and conditions, not just a successful patch
response. Receipts contain `requestId`, `requestedTargetReplicaId`,
`acceptedTarget`, `resultingPrimary`, and `outcome`:

| Outcome | Meaning |
|---|---|
| `requestedTargetCompleted` | Requested target's current-only authority accepted |
| `oldPrimaryRestored` | Target definitively lost before newer authority admission; exact preparation retired and starting authority retained |
| `oldPrimaryCompensated` | Target definitively lost after authority admission; old primary accepted at a strictly newer compensation epoch |
| `unsafe` | No safe completion remains and every possible writer has been proven closed or absent; no resulting primary |

The schema also defines `rejected`, but the evaluator currently reports
pre-acceptance rejection through `SwitchoverRejected` conditions rather than
writing that receipt. Conditions distinguish already-primary, unknown, or
ineligible targets, conflicting reuse of the latest receipt's ID, and active
request mutation. Kubernetes may accept the spec patch; that does not mean the
controller accepted its requested movement.

Keep an active request unchanged. Clearing it, changing its target, or submitting
another ID produces `ActiveRequestImmutable` and does not cancel or retarget the
frozen operation. Repeating the same active request or the latest receipted
request does not start a second transition. Only the latest receipt is retained,
so use a fresh ID for every new movement and never replay older IDs after another
receipt replaces them. An unaccepted ineligible request is re-evaluated; it may
become eligible later. Wait for a terminal outcome before submitting new work.

Temporary missing reports wait with observable reasons, not an arbitrary
switchover timeout. Before restoration, every extant exact member must prove idle
starting authority; retiring even an unobserved preparation fences delayed
dispatch. The accepted spec generation is frozen as `preparationGeneration`
in intent, commands, and certificates. A durable authority-bound high-water mark
rejects every older/equal retired preparation, including after multiple
same-authority restorations and process restarts; a newer request generation
remains admissible. After **any** newer authority admission—even before the application
demotion callback completes—restoring the old epoch is forbidden. Compensation
requires the source's whole handoff certificate, retained durable prefix, exact
read-quorum evidence, and enough survivors for write quorum. Once target
topology is accepted, lost replies or delayed routing converge forward.
During compensation, an extant permanently faulted non-primary is fenced by
exact UID/resourceVersion Pod deletion, preserving its PVC. The controller
re-observes absence before converging the surviving quorum; temporary
unavailability still waits, and a participant observed healthy before deletion
can converge normally.

An impossible operation first enters durable safety closure with routing
removed. Each possible writer must report non-granted access and no pending
operation under accepted or superseding authority, or its exact Pod must be
observed absent. Where safe command closure cannot be established, the
controller may delete the exact UID/resourceVersion-fenced Pod while preserving
its PVC. Only after closure is proven is the terminal `unsafe` receipt
published. The frozen unsafe intent remains; a new request or manual status edit
is not a recovery API.

### Client availability

A healthy movement may briefly interrupt writes and connections. There is no
zero-downtime, retained-session, or fixed-duration guarantee. The old primary
rejects new writes after revocation even on a retained/direct connection.
It can accept writes again only if safely restored/compensated and granted
access under the resulting accepted authority.
Every write admitted before revocation either fails or is durably covered by the
handoff boundary; delayed completion cannot acknowledge a write outside it.
Acknowledged values survive safe requested or compensated completion.
Client failure does not discard a durable registered operation. Before granting
the recovered primary access, the runtime verifies the original operation/data
identity and reconciles its journal using durable application commit evidence
or replication/quorum completion under the selected authority. Access stays
closed until this finishes, including across grant retries and restart. Reserved
operations are durably applied before certifying the handoff prefix.

Clients should tolerate HTTP 503 and disconnects, reconnect through the write
Service, and use bounded backoff. A Service port-forward may stay pinned to its
original Pod and need restarting. A lost client reply is ambiguous: retry only
according to application-level idempotence; switchover request idempotence is
not an exactly-once HTTP write guarantee. `failoverDelaySeconds` and controller
RPC/requeue intervals are not switchover completion deadlines.

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
mkdir -p target/kuberic-level-dev
export KUBECONFIG="$PWD/target/kuberic-level-dev/kubeconfig"
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
just level-triggered-kind-test switchover
just level-triggered-kind-test switchover-adversarial
```

The bounded full matrix expands to replacement, quorum loss, the composed
adversarial restart/partition/failover scenario, and both switchover scenarios:

```bash
just level-triggered-kind-test all
```

The PR workflow separately runs bootstrap, replacement, failover, and healthy
switchover smoke tests. Scheduled
and manually dispatched full CI runs the matrix twice on separate fresh
clusters.

`switchover` submits an explicit request ID and logical target to a stable
three-member `kvstore2`. It watches routing and receipts, keeps direct TCP
clients pinned to exact Pods, preserves every acknowledged test value, and
checks current-only authority, a single write grant, Service writes, and
former-primary rejection. It reports elapsed time and enforces a 270-second
after-ready scenario budget. Retained-client connect, write, and read operations
retry transient `WouldBlock`/EAGAIN without replaying partial HTTP messages or
extending that deadline; timeouts and other I/O or protocol errors still fail.
Each response is bounded by `Content-Length` or chunked framing (including
trailers), never by keep-alive connection closure. The same socket is reused for
former-primary checks; a deleted target must explicitly reject or disconnect,
not merely time out. Before submitting the request, the harness waits for each
exact secondary's durable applied progress to cover the primary's acknowledged
prefix. Secondary committed-LSN watermarks can lag until another replication
item arrives and are not a setup barrier.

`switchover-adversarial` runs three separately bounded subcases: controller
and target-process restart during a durable handoff; exact target deletion
before authority admission with original-authority restoration; and deletion
after source demotion with strictly newer compensation. Replication-only
partitions hold the boundaries without blocking control reports. The latter
case deliberately retains two exact survivors and their handoff evidence, so
an Unsafe receipt is a failure rather than an acceptable replacement for
compensation. Each subcase heals its exact network rules and waits for
three-member recovery before the next starts. No cluster is created by a test;
even direct `cargo test --ignored --exact` invocation requires the matching
ownership receipt and explicit context.

Recorded Phase 6 validation on fresh isolated clusters measured healthy
switchover at **15.1 s total / 8.2 s after readiness** in the standalone run.
Two complete repeated matrices measured healthy switchover at **4.2 / 4.3 s**
and adversarial switchover at **44.5 / 48.8 s**. The strict retained-session
rejection rerun completed the adversarial scenario in **45.2 s**. These are
scenario elapsed times, not write-outage measurements or production SLOs.
All owned clusters and kubeconfigs from those runs were cleaned up.

On failure, collect the CR, resource state, events, controller logs, and all
replica logs, including previous-container logs and live agent/runtime `/status`
evidence. The full CR includes the request, frozen handoff, and retained receipt;
resource YAML retains Pod/PVC UIDs and exact Service selectors:

```bash
just level-triggered-diagnostics
```

Each replica also exposes `GET /status`, including its exact identity, durable
generation, process session, role, epoch, PC/CC IDs, progress, committed LSN,
write status, pending operation, and active builds.
Agent reports expose `preparedSwitchover`; the full CR retains the frozen
handoff in `status.transition.switchover.handoff`. Conditions such as
`SwitchoverTargetCatchupPending`, `SwitchoverAuthorityAdmissionUnknown`, and
`SwitchoverCompensationReadQuorumPending` explain unmet evidence.
`SwitchoverSafetyClosure` precedes terminal Unsafe; absence of routing alone
does not prove all writers closed. Preserve diagnostics before any intervention;
do not edit status, rebind PVCs, or bypass session/version fences to force progress.

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

Classic v1 remains the documented path for existing `kuberic.io/v1` resources,
scaling, and the SQLite/PostgreSQL examples. V2 now supports its own explicit
planned switchover; no v1 conversion, data import, or classic-path removal is
implied. The [retirement plan](../../proposal/v1-retirement-plan.md) keeps scaling,
application ports, distribution, deprecation, and removal as separate workstreams.
