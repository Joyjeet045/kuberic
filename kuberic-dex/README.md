# Kuberic DEX

Kuberic DEX (`kuberic-dex`) is the project's durable execution kernel. It
provides deterministic, linear workflow replay, has no dependency on
`kuberic-core` or `kuberic-operator`. It is not currently integrated into the
operator and is not an end-user runtime.

Run the in-memory end-to-end example:

```bash
cargo run -p kuberic-dex --example hello_world
```

The controller-shaped mock demonstrates repeated reconcile turns and a fake
Kubernetes API mutation:

```bash
cargo run -p kuberic-dex --example reconciler_mock
```

## Typed orchestration and activity authoring

Application workflows are typed async closures registered under stable string
names. The body returns `Result`, allowing normal `?` propagation without
handling `ExactBytes` or constructing `TerminalOutcome`.

```rust
use kuberic_dex::{
    ActivityInvocationError, OrchestrationContext, OrchestrationRegistry,
};

let orchestrations = OrchestrationRegistry::builder()
    .register_typed::<GreetingInput, GreetingResult, ActivityInvocationError, _, _>(
        "GreetingWorkflow",
        |context: OrchestrationContext, input| async move {
            context
                .schedule_activity_typed::<GreetingInput, GreetingResult>(
                    "greeting",
                    &input,
                )
                .await
        },
    )
    .build()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`ExecutionSpec::typed` canonically encodes workflow input, and
`decode_workflow_result` decodes a terminal success or error value. The
exact-byte `Workflow` contract remains the hidden replay-kernel interface.

Typed activities use string names with generic input/output types, matching
Duroxide's authoring shape. DEX supplies the activity version and global
encoded payload limits. Workflow and store futures are `Send`, so a host turn
runs directly inside an asynchronous controller without a second executor.

```rust
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
struct GreetingInput {
    recipient: String,
}

#[derive(Deserialize, Serialize)]
struct GreetingResult {
    message_id: String,
}

```

Handlers use the same typed async closure shape as Duroxide while DEX verifies
the activity's name and framework-owned payload bounds:

```rust
use kuberic_dex::{ActivityContext, ActivityHandlerError, ActivityRegistry};

let activities = ActivityRegistry::builder()
    .register_typed::<GreetingInput, GreetingResult, _, _>(
        "greeting",
        |_context: ActivityContext, input: GreetingInput| async move {
            Ok(GreetingResult {
                message_id: input.recipient,
            })
        },
    )
    .build()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Inside an orchestration handler, the ordinary direct-style call is:

```rust
let sent = context
    .schedule_activity_typed::<GreetingInput, GreetingResult>(
        "greeting",
        &GreetingInput {
            recipient: "Ada".to_owned(),
        },
    )
    .await?;
```

An immutable `ActivityRegistry` validates names, versions, exact recorded
bounds, and typed input before invoking an owned handler.
`ScopedActivityRegistry<S>` provides the same registry-owned dispatch while
letting a handler borrow mutable embedding state, such as one reconciliation
snapshot, for the duration of an invocation. Identity, codec validation, and
handler selection remain framework-owned.
`ActivityInvocationRuntime` applies the same action-deadline, attempt-timeout,
retry-exhaustion, wait, result, and failure classification for both standalone
`ActivityRunner` users and reconciler-hosted scoped handlers.

Ordinary activities have practical at-least-once semantics. One logical record
contains bounded physical attempts; the default policy permits three attempts
with persisted exponential backoff. Retryable application failures and lost
ordinary results may consume an attempt. Codec, registration, nondeterminism,
storage, and timeout failures do not silently become retries. `ActivityOptions`
also carries action deadlines and attempt timeouts; all options participate in
replay identity and supply requeue wakeups to an embedding reconciler.
`ActivityRunner::with_timeout_runtime` accepts the embedding async runtime's
timer/cancellation adapter, so elapsed in-flight deadlines become durable
timeout failures without adding Tokio or another executor to the kernel.

Handlers must expect a crash after changing external state but before result
persistence. Read-only handlers reread evidence, naturally idempotent handlers
converge, and identity-fenced handlers preserve a stable action ID across
physical attempts. A narrow optional strict integration retains exact
prepared-command, one-use permit, observe-before-dispatch, and quarantine
machinery for operations where duplicate ambiguity would violate safety. It is
not the default workflow model.

This surface implements Duroxide-style direct workflow authoring, typed named
activities, replay, and at-least-once retry semantics. Kubernetes adapts
scheduling to existing watches and requeues. The crate intentionally omits an
embedded Duroxide runtime, workers, queues, leases, heartbeats, external
events, parallel orchestration, child workflows, and cancellation.

## Replay and checkpoint semantics

Typed calls first canonicalize JSON object-key order, then compare the encoded
input exactly on every replay. Low-level `ExactBytes` are compared without
normalization and encoded as validated base64 JSON strings. Workflow history
is a contiguous, zero-based sequence with a completed prefix and at most one
final pending activity. A requested activity must match the recorded sequence,
immutable framework-versioned name, exact input, and global result bound; a
mismatch is nondeterminism rather than a new dispatch.

Format version 4 stores JSON payload bytes in a versioned
`CheckpointEnvelope`. An immutable `ExecutionSpec` declares execution
identity, exact workflow input, and the maximum exact-byte terminal payload.
The persisted execution contract also records the active and terminal
encoded-checkpoint capacities under which the execution was admitted. Every
load validates that authority and rejects either changed limit before workflow
polling.

The payload has exactly one explicit lifecycle state:

- `Active` contains the complete bounded linear history, with records in
  scheduled, exposed/retry-wait, or completed state and a bounded physical
  attempt ledger under each logical activity.
- `Terminal` contains `Succeeded` or `Failed` exact bytes and the completed
  activity count. It has no activity history or digest.

Unsupported and prior formats are compatibility errors, not storage
conflicts. While active, completed activity results replay into workflow code
without a dispatch permit. Once terminal, a turn returns the stored outcome
and observed revision directly without polling workflow code.

Logical activity identity is the complete tuple of execution ID, sequence,
versioned activity name, exact activity input, framework input/result bounds,
and scheduling options. Its stable external action rendering contains the
execution ID, sequence, versioned name, exact input, and result bound without
hashing or normalization; replay separately authenticates the input bound and
options from the recorded specification. `AttemptId` is a separate host-epoch
and monotonic-counter identity, so a physical retry may change without
changing the logical activity.

## Checkpoint limits and result reservation

Every `DurableHost` requires `CheckpointLimits` for maximum activity records,
maximum canonical active-checkpoint bytes, and maximum canonical
terminal-checkpoint bytes. Exact configured boundaries are accepted; loaded or
proposed checkpoints beyond the applicable lifecycle boundary are rejected
before workflow progress or publication.

Before an absent execution is evaluated, the host projects both success and
failure terminal checkpoints at the execution's declared maximum payload and
a maximum-width completed activity count. The projection uses checked
base64/JSON length arithmetic without allocating the declared payload. The
larger form must fit the configured terminal encoded limit. The active and
terminal capacities are persisted as immutable admission authority. Capacity
failure therefore precedes even the first schedule and every possible
external-effect permit. Later hosts must use the same admitted limits; changing either lifecycle limit
for a persisted operation is incompatible.

Before committing dispatch exposure, the host projects the completed
checkpoint containing a result at exactly the activity's declared maximum.
The projection calculates base64 and JSON lengths without allocating that
maximum result. If it cannot fit, exposure is rejected and no permit exists.
An observation larger than the declaration is rejected without mutation. This
ensures that a permitted activity result within its declaration cannot later
fail solely because its completed checkpoint lacks capacity.

## Storage revisions

`CheckpointStore` exposes runtime-neutral asynchronous `load` and
`compare_and_swap` operations. `load` distinguishes an absent checkpoint from
a portable classified `StoreError`; errors preserve a provider description
without exposing provider-specific types. `StorageRevision` is a validated
nonempty opaque string token with equality semantics and is unrelated to
checkpoint format version. It exposes no numeric, ordering, or increment API.

CAS returns only `Accepted`, `Conflict`, or `OutcomeUnknown`. A provider must
classify any result whose acceptance cannot be proven as `OutcomeUnknown`;
`StoreError` is reserved for definitely local or pre-request failures. A
conflict or unknown outcome returns `ReloadRequired`, while a provider error
returns `StoreFailed`. None grants dispatch permission. The included
`InMemoryCheckpointStore` keeps its numeric revision counter private, renders
opaque tokens, and can return the same unknown result with or without applying
the mutation.

## Kubernetes checkpoint provider spike

The default-off `kubernetes` feature adds `KubernetesCheckpointStore`. A caller
supplies a `kube::Client` and a namespace, retaining ownership of credential
discovery, the async runtime, namespace lifecycle, retries, and all workflow
effects:

```rust,no_run
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use kuberic_dex::KubernetesCheckpointStore;

let client = kube::Client::try_default().await?;
let store = KubernetesCheckpointStore::new(client, "durable-checkpoints")?;
# let _ = store;
# Ok(())
# }
```

`KubernetesCheckpointStore::new` uses independently retained checkpoints and a
786,432-byte ConfigMap data budget. Advanced callers can use
`KubernetesCheckpointStore::with_options` with
`KubernetesCheckpointStoreOptions` to select a budget from 1 through 983,040
bytes and optionally attach one `KubernetesCheckpointOwner`. The owner wraps a
Kubernetes `OwnerReference` plus either a namespaced or cluster-scoped
assertion. A namespaced owner must name the checkpoint namespace. Required
identity fields and the immutable UID must be nonempty, and controlling or
deletion-blocking references are rejected before any request. The provider
does not fetch the owner, so the caller remains responsible for the accuracy
of its scope, GVK, name, and UID. Before a replacement, the provider reads the
current object and rejects a stale revision or owner relationship that differs
from the store configuration, so reconstruction cannot silently change a
checkpoint between independent and owner-based retention.

Each execution maps to
`kuberic-checkpoint-<32-lowercase-execution-id-hex>` in that namespace. The
format-4 envelope's canonical JSON is stored in
`data["checkpoint.json"]`; the object is labeled
`kuberic.io/component=durable-checkpoint`. Initial CAS uses create-if-absent.
Successor CAS uses replace with the caller's exact, opaque
`metadata.resourceVersion`. Already-existing creates, stale replacements, and
replacements racing with deletion return the portable unit `Conflict`; the
host's next load obtains authoritative state.

Explicit API rejections map to portable `StoreError` kinds and diagnostics
retain only the operation plus API code/reason. Mutation transport failures,
5xx responses, and successful responses without a usable revision are
`OutcomeUnknown`, because acceptance cannot be disproved. The provider never
embeds Kubernetes error types, credentials, checkpoint contents, or arbitrary
transport text in its public errors.

The data budget is the aggregate UTF-8 byte length of every ConfigMap `data`
key and value. An exact-bound object passes this local gate; one byte over is
rejected before dispatch. This provider check is separate from and does not
weaken the kernel's activity-count and encoded-checkpoint limits. Passing the
budget does not guarantee API-server acceptance: metadata, managed fields,
annotations, owner references, admission mutation, and API-server policy can
consume capacity or reject the object independently.

Checkpoints have no provider-driven terminal deletion. Terminalization
CAS-replaces active history with a terminal record and retains the ConfigMap.
Without an owner reference, the checkpoint remains independently retained
until an explicitly authorized lifecycle actor removes it. With an owner,
ordinary Kubernetes garbage collection may remove the checkpoint after that
owner is deleted. Orphan selection, retention policy, and recovery-safety
checks remain responsibilities of a separate lifecycle actor, not this
provider.

The namespace-scoped
[writer RBAC example](deploy/checkpoint-writer-rbac.yaml) grants only ConfigMap
`get`, `create`, and `update`. The separate
[cleanup RBAC example](deploy/checkpoint-cleanup-rbac.yaml) grants only
ConfigMap `list` and `delete`; it is optional and intentionally not granted to
the writer identity. These examples do not alter or broaden the existing
operator permissions.

`store.metrics().snapshot()` reports only writes confirmed by a response with a
usable authoritative revision. It includes accepted-write count, canonical
checkpoint JSON bytes, canonical typed server-returned ConfigMap JSON bytes,
and measurement failures. Object bytes include server metadata and are not raw
HTTP-wire bytes. The validation watch measurement similarly canonicalizes each
delivered typed `WatchEvent<ConfigMap>` and excludes HTTP framing and transport
overhead.

## Turns and dispatch permission

Awaiting a host turn evaluates and commits no more than one persistence
boundary:

1. A new ordinary activity accepts one exposed physical attempt under its
   stable logical record.
2. The registered handler observes or invokes that attempt. Completion,
   retry-wait, and lost-result recovery update the same logical record.
3. Accepted completion may fuse the result with the next exposure, while each
   additional physical attempt adds at most one exposure and one result write.
4. After replay completes, the host CAS-replaces active history with a compact
   terminal checkpoint.
5. A newly accepted terminal returns an awaiting-reload outcome. Completion is
   published only after a later authoritative load observes and validates the
   terminal revision.

A zero-activity workflow skips schedule, exposure, and observation. Its first
turn validates terminal capacity and CAS-creates terminal state directly from
absence before returning `WorkflowCompleted`.

Construction requires the checkpoint store, caller-supplied host epoch, and
validated `CheckpointLimits`; callers supply the executor used to await turns
and observations.

The permit is evidence that this in-process host observed acceptance of the
exposure boundary. It is **not** proof of exactly-once execution: a crash may
occur after exposure persistence and before, during, or after handler
invocation.

### Internal prepared exposure

Strict-effect adapters may pass a `PreparedActivityResolver` to
`turn_and_expose_with` and `observe_and_turn_with`. For a new logical request,
the resolver supplies the complete bounded `ActivitySpec` before checkpoint
encoding, result-capacity reservation, and the exposure CAS. For replay, it
validates the complete recorded specification against the logical request;
the kernel then performs its normal exact specification comparison. Derivation,
validation, encoding, or bound failure occurs before persistence and produces
no permit.

The accepted `DispatchExposed` checkpoint is therefore the authority for the
exact command. A permit is created only after that checkpoint CAS is accepted
and identifies the same logical activity and attempt. Conflict, definite store
failure, and `OutcomeUnknown` never produce a permit. If an unknown outcome was
applied, reload sees the complete exposed specification as quarantined; if it
was not applied, reload sees the predecessor. An unresolved exposure remains
quarantined until the host receives authoritative completion or
effect-specific proof that permits a safe retry. Process-local knowledge that
a permit or reply was lost cannot override checkpoint state.

This is an internal, opt-in host/evaluation behavior behind an ordinary
activity call. `OrchestrationContext::schedule_activity_typed`, immutable
name/version identity, canonical typed input/output encoding, exact replay
matching, and framework-owned input/result bounds remain the public model.

Completion conflict or `OutcomeUnknown` returns `ReloadRequired` and never a
permit or completion. If an unknown write applied, reload observes terminal
state. If it did not apply, reload replays completed active results and retries
terminalization without redispatch. Provider failures return `StoreFailed`;
every later attempt starts with a fresh load.

Callers with strict-effect authoritative recovery can opt into the fused
host methods without changing the low-level behavior above:

- `turn_and_expose` evaluates the next activity, reserves its maximum result,
  and persists it directly as `DispatchExposed` in one CAS. It creates the
  opaque permit only after that CAS is accepted.
- `observe_and_turn` validates one exact authoritative observation, replays
  deterministic workflow code, and uses one CAS to persist either the
  completed result plus the next `DispatchExposed` command or the compact
  terminal state.

Conflict, definite store failure, and `OutcomeUnknown` at either fused boundary
never return a permit. An unknown-after-apply next exposure reloads as
quarantined, including the case where this process knows it never received the
permit; generic recovery does not weaken that conservative rule. Ordinary activity handlers use the registry and bounded attempt lifecycle
instead.

Terminal success and failure payloads share the `ExecutionSpec` bound. An
exact-bound outcome is accepted. A larger outcome is an explicit checkpoint
contract violation and is not persisted or reported complete. This workflow
failure outcome does not add activity-failure behavior.

## Quarantine and observation recovery

Reloading an unresolved `DispatchExposed` activity returns `Quarantined` with
the persisted logical and attempt identities before workflow-definition
matching. Quarantine does not redispatch, compensate, mutate the checkpoint,
or schedule a later activity. After an authoritative observation resolves the
exposed activity, ordinary replay resumes and still reports any definition
mismatch as nondeterminism before a later dispatch.

The host leaves effect execution to its caller. A caller may later supply an
authoritative `ActivityObservation` for the exact exposed logical activity.
An accepted observation records the exact result, after which normal replay
continues. Missing, stale, or mismatched observations are rejected; competing
observations use the same CAS rule. The crate does not determine the
observation's trust source or transport.

## Validation

Run the bounded feasibility evidence and the complete crate/workspace gates:

```console
CARGO_BUILD_JOBS=2 cargo test -p kuberic-dex --test feasibility -- --nocapture
CARGO_BUILD_JOBS=2 cargo test -p kuberic-dex --all-targets
CARGO_BUILD_JOBS=2 cargo test -p kuberic-dex --doc
cargo fmt --all -- --check
CARGO_BUILD_JOBS=2 cargo check --workspace
CARGO_BUILD_JOBS=2 cargo clippy -p kuberic-dex --all-targets -- -D warnings
```

The standalone kernel exposes the Kubernetes provider behind its optional
`kubernetes` feature:

```console
CARGO_BUILD_JOBS=2 cargo test -p kuberic-dex --features kubernetes --test kubernetes_checkpoint -- --nocapture
CARGO_BUILD_JOBS=2 cargo test -p kuberic-dex --features kubernetes --all-targets
CARGO_BUILD_JOBS=2 cargo clippy -p kuberic-dex --features kubernetes --all-targets -- -D warnings
```

A feature-gated test performs authorization preflight, creates a temporary
namespace, validates the provider against the configured real Kubernetes API,
prints its measurement report, and waits for namespace deletion:

```console
CARGO_BUILD_JOBS=2 cargo test -p kuberic-dex --features kubernetes --test kubernetes_checkpoint_real
```

The repository's existing [CI workflow](../.github/workflows/CI.yml) enables
all crate features on its workspace-wide test command after
`helm/kind-action` has provisioned a uniquely named, one-control-plane KinD
environment with a dedicated kubeconfig/context and dynamic loopback port.
There is no second provider cluster or cleanup owner.
Ordinary local and default Cargo test runs without the Kubernetes feature do not select this test.

The test reports a failed endpoint or authorization precondition rather than
claiming real-API coverage. Its apply-then-unknown and no-apply-unknown cases
mask the store result around real persisted state; they validate host recovery,
not reproduction of an actual network fault.

The feasibility test reruns the sole conformance registry, emits every
assertion, measures the FR-012 surface, and applies the exhaustive FR-014
three-way classifier. The revised evidence contains 45 unique contiguous
scenarios and 135 structured assertions; all pass. All five FR-012 authoring
predicates and all five provider, bounding, lifecycle, and documentation
predicates also pass. The async runtime-neutrality predicate examines the
library `[dependencies]` table rather than test-only dependencies and retains a
negative fixture for a real library runtime dependency. The mechanically
derived result is **feasible** within this kernel's stated boundary.

## Embedding status

The framework currently has no production operator consumer. Previous
remove-replica and switchover integrations were removed after they failed to
meet the intended framework-level authoring and ownership goals. A future
embedding must be designed independently and must preserve the existing
operator safety properties without adding excessive Kubernetes API-server
state or traffic.

## Deferred usability roadmap

The crate intentionally stops at the kuberic-dex kernel.
Completion-only compaction and an isolated Kubernetes checkpoint-provider spike
are implemented. Operator integration, generic active-history compaction, and
continuation remain excluded. The remaining ordered deferred work is tracked in
[Kuberic DEX Roadmap](../docs/features/kuberic/kuberic-dex-roadmap.md).

## Limitations and exclusions

The kernel remains experimental as a general-purpose orchestration framework.
The ConfigMap provider remains optional and independently validated. The
isolated real-API evaluation does not establish generic persistence fitness,
distributed execution ownership, a worker, queue, lease, automatic observation
polling, or passive-observation transport. The
kernel does not establish a canonical exact-byte representation across
versions.

The classifier is recomputed from the current registry rather than assuming a
positive result. Provider cases distinguish absence from every portable error
class, exercise opaque revisions, and make applied and unapplied unknown CAS
outcomes indistinguishable to the host. The lost-effect-reply fixture still
invokes one synthetic effect under an opaque permit and discards its returned
result before restart.

Dynamic runtime discovery, passive convergence policy shared across products,
tracing/inspection, durable timers, parallelism, generic lifecycle APIs,
queries, external events, child workflows, workers, queues, leases, and
distributed runtime ownership are excluded. So are migrations,
upgrade guarantees, operator adoption, and production diagnostics.
