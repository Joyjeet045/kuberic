# Replicator boundary and native proof

Kuberic hosts built-in and application-owned replicators through the same
Service Fabric-shaped public interfaces. Standard lifecycle and primary
operations always cross `Replicator` or `PrimaryReplicator`; the built-in
engine additionally supplies an unpublished native capability for durable
replication proof.

The forward-looking comparison and phased simplification plan are documented
in [Service Fabric Alignment and Runtime Simplification](service-fabric-alignment.md).

## Ownership

The agent owns desired-state ordering, durable effect intent and completion,
process/session admission, peer transport, endpoint ownership, and the
externally visible read/write projection. The built-in replicator owns local
write journals, quorum commitment, copy progress, pending-write recovery,
committed-prefix reconciliation, native topology evidence, and local write
fencing.

These responsibilities do not move at a standard-operation boundary.
Successful public catch-up, build, and ordinary removal completion is the
built-in engine's durable completion contract. The agent validates that its
authority, sessions, target, and attempt are still current, then commits the
common effect; it does not fetch a second native receipt.

## Coherent interface bundle

`ReplicatorInterfaces` binds public control, optional primary/state interfaces,
and optional host-only capabilities to one creation identity. Cancellation,
failed attachment, or an Open identity mismatch abandons the whole creation.
Wrappers that retain the built-in engine must consume the original bundle with
`wrap_primary`; rebuilding a bundle with `primary` intentionally creates an
independent public bundle without the original native provenance.

Independent custom replicators return only the public interfaces they
implement. A custom `StateReplicator` does not imply the presence of the
built-in native capability.

## Standard operation routing

Open, role, epoch, Close, progress, data-loss, configuration, catch-up, build,
and removal use the public interfaces. Hosted build/remove transport remains
agent-owned:

- one admitted build invokes the returned public primary once;
- built-in copy delivery and public completion must both succeed;
- cancellation or either branch failing cancels the exact build attempt;
- native copy progress alone is not host-accepted completion;
- remove invokes the public primary before the common Remove dispatch.

Private lifecycle work is divided into consumer-specific process, authority,
access, build, cancellation, topology, observation and outbound capabilities.
Registration projects those capabilities from one shared managed or independent
owner. Reporting, build dispatch, peer discovery, cleanup, topology effects and
recovery receive opaque narrow views rather than a universal lifecycle facade.
The split changes reachability, not synchronization: authority, sessions,
generations, cleanup claims and the outbound queue remain shared.

There is no all-capability supertrait, generic lifecycle action hook, backend
enum, or getter that reconstructs the complete owner. Managed and custom
implementations continue to use explicit common routing and their distinct
proof mechanisms.

## Native topology receipts

Only Kuberic-specific topology operations retain private durable receipts:
certified-prefix settlement, switchover preparation, secondary removal, and
retirement. `TopologyReceipt` stores those canonical payloads directly in an
effect result. There is no catch-up/build/removal receipt layer and no generic
operation-evidence wrapper duplicating the same payload.

The host revalidates topology receipts against the durable intent and current
authority. Standard-operation staleness is validated from host admission plus
the native topology fence captured around the public call.

## Access publication

Native access admission does not itself grant application access. The host
reserves one common projection generation, validates public progress, publishes
native access, and atomically updates the common and external projection. One
already-running access task owns native/common fencing, final owner validation,
host-memory completion, and projection cleanup. Its ready transaction is
consumed exactly once to obtain an accepted transaction; that accepted owner is
then committed or rejected exactly once. If the caller disappears before a
decision, the task observes the closed channel and performs rollback. No
`Drop` implementation starts asynchronous correctness work.

Durable access effects persist applied and completed stages through the
intent-first protocol before authorizing the accepted transaction. Direct
lifecycle restoration, reconciliation, and Close use the same accepted owner
without creating another effect journal entry. A positive durable decision
transfers the exact applied effect to the access task, so cancellation of the
waiting observer cannot suppress host-memory recording after successful final
validation.

Durable completion and runtime commit remain distinct. Abort or supersession
can invalidate the owner after completion is retained but before final commit.
That execution returns an explicit error, cleans only its own publication, and
does not record host-memory success. The retained exact result remains
replayable without publishing another access generation.

Cancellation, publication failure, or configuration invalidation fences native
writes and clears an unaccepted projection. Rollback is generation-scoped, so
it cannot revoke a newer accepted grant. Final effect acceptance is serialized
with projection invalidation. Common ownership validates authority,
configuration, peer sessions, role, and access generations. The built-in engine
also validates its native session, generation, progress, and fence. Independent
custom implementations instead complete their public progress/revocation
callback contract; they do not acquire managed-native receipts.

## Custom authority admission

Independent custom replicators use the existing current/joint configuration
callbacks to accept an exact proposed topology before the host persists its
authority. Those callbacks apply configuration and can mutate live or durable
application state. They are not pure validation operations, and the interfaces
provide neither an atomic application/authority-store transaction nor rollback.
No new replicator method, capability, registration or wrapper requirement is
introduced.

Each runtime host owns one `CustomAuthorityContainment`. It contains the
independent authorization latch, exact pending recovery, and the shared
configuration-callback and report-restoration serialization locks. Each
admission creates a short-lived `CustomAuthorityAttempt`; nested staging and
host effect completion share the same entered-attempt state so invalidation or
caller loss cannot publish or record stale success
(`kuberic-runtime/src/host/custom/authority.rs:17-117,358-399`,
`kuberic-runtime/src/host/hosting.rs:2379-2385,2641-2648`).

The containment owner first checks structural, durable-conflict and required
build evidence. It then invalidates old work and saved grants, explicitly
closes both read/write projections, and completes bounded native access
revocation before calling the application. This also interrupts
identical-authority replay and same-primary scale-up access. Report/deferred
restoration accepted earlier is serialized before staging; its host-owned
continuation retains ownership even if the requesting reporter is dropped.
Already-entered incidental configuration callbacks likewise finish before
candidate application. Independent direct and deferred configuration share the
containment callback owner; dropping a peer-discovery observer does not release
a stateful callback into a newer authority. Subsequent stale restoration cannot
reopen access
(`kuberic-runtime/src/host/custom/authority.rs:119-327`,
`kuberic-runtime/src/host/custom.rs:1929-1933,2020-2024`).
Successful admission requires callback acceptance, authority-store success and
owned generation/session-valid publication. It remains closed until fresh
explicit access authorization.

| Boundary | Authority and containment |
|---|---|
| Pre-application structural/conflict rejection | No candidate callback/admission; existing session need not abort. |
| Callback rejection, including partial mutation | No candidate authority admission; entered attempt aborts with access closed. Application rollback is not claimed. |
| Store error before write | Candidate may already be applied by the custom engine; host authority remains old/absent. Current session aborts. |
| Store commits then returns an error | Candidate may be durable despite failure. Current session aborts; no read-back-as-success, authority erasure or compensating old configuration. |
| Dropped entered attempt or stale completion | Synchronous Abort contains the session; late external mutation/durable commit is possible, but invalidated work cannot record host effect success. |
| Host success followed by journal failure | Exact cached success can finish journal replay without another configuration callback in the same host. |

Effect intent remains durable before staging. A fresh independent host with
pending authority intent reconciles the exact candidate before applying obsolete
stored configuration, reconstructs role/epoch/application state, then performs
ordinary exact effect replay and journal completion. Startup access stays closed
and an old persisted grant is not restored. Current/previous descriptors remain
exact; process-session descriptions are fresh. Callbacks can repeat across
recovery. A conflicting newer/same-epoch authority, missing required fresh build
proof, or a custom engine refusing exact reconciliation fails closed rather than
substituting a proposal or promising universal recovery liveness. Dropping an
entered callback/store future leaves its recorded pending intent; a returned
cancellation may instead cancel that intent under the existing adapter rules.

Managed built-in admission retains its distinct durable-authority-before-
configuration order, exact durable configuration check, same-primary continuity
and existing recovery sequence. The custom containment policy does not weaken
or reorder that managed path.

## Persistence, reporting, and recovery

SQLite persists effect intent before execution and completion only after the
canonical topology receipt, when present, and the lifecycle postcondition
validate. Existing
JSON fields remain backward-readable through serde defaults; this refactor does
not introduce a database schema migration.

Reporting uses an opaque reconciliatory view rather than the complete hosting
runtime. It composes durable agent authority with current public/native
observations and can retry deferred access restoration. It preserves
`live_builds_only`, so old-session build evidence is not resurrected. Catch-up
composition retains an accepted boundary only while the native engine still
reports catch-up complete.
Independent custom authority staging supersedes old restoration authorization:
report-driven grant restoration remains disabled for that host instance.
An earlier durable/deferred grant cannot reopen the new configuration; initial
and later regrants require fresh explicit access effects.

Recovery reconstructs authority and native stores, reissues or reobserves
pending effects, and keeps access closed until public/native proof is accepted.
A previously persisted granted snapshot is never sufficient to reopen writes.

## Compatibility

The application-facing service, partition, factory, public replicator, primary,
and optional state-replicator contracts remain SF-shaped. Native receipts are
not requirements on independent custom replicators. The intentional wrapper
migration boundary is limited to preserving the coherent factory-created
bundle.

The runtime source-public inventory and compile-fail API guards cover hidden
bundle and receipt signatures. Stable Rust visibility lints and runtime-local
Clippy restrictions enforce broad type ownership in ordinary builds. The
focused lifecycle source test retains only relationship rules that those tools
cannot express, including capability aggregation, escape paths, fixture gating
and the single admitted-build coordinator.

## Validation

The repository validates this boundary through:

```bash
cargo test -p kuberic-runtime --test public_api_inventory
cargo test -p kuberic-runtime --test lifecycle_capability_boundaries
scripts/check_runtime_public_api.sh
cargo nextest run --profile ordinary -p kuberic-runtime --features testing --lib \
  -E 'test(/^host::tests::(runtime|store|coordinator|crash_boundaries)::/)'
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Use the repository `just nextest-*` recipes for the complete ordinary,
PostgreSQL, archive-partition, and KinD matrices.
