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

Those facts are exposed internally through explicit owner types. Durable
`AgentState` is wrapped as `DurableAgentObservation`; process-local
adapter-local `ReplicaRuntimeState` produces `HostProxyObservation`; and
managed/native progress
is exposed as `ReplicationEngineObservation`. `ReportObservation` composes the
host and engine owners without becoming durable state
(`kuberic-runtime/src/host/observation.rs:1-244`).

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

The managed capability exposes explicit replication operations for executable
configuration, peer sessions, build proof, progress refresh, acknowledgement,
access, certified-prefix settlement, switchover, every secondary-removal stage
and retirement. Each removal and retirement stage has a distinct outcome type
containing only that stage's evidence. There is no all-capability supertrait,
generic lifecycle action hook,
backend operation enum, compatibility dispatcher or getter that reconstructs
the complete owner. Application role, replicator role and epoch remain separate
application/public operations; access retains its private prepare/publish/
decision protocol.

Durable `AdmittedAuthority` is host-owned. Before managed admission, the host
derives `ManagedReplicaConfiguration`, which contains only executable current/
previous configuration and replication-required handoff, removal and scale-up
evidence plus explicit executable build policy. The engine, replication log
and quorum tracker do not import
controller effects, transition kinds, durable authority or broad runtime
snapshots. A narrow managed store view projects the already-durable
configuration and exposes only replication-specific removal and retirement
persistence.

## Native topology receipts

Only Kuberic-specific topology operations retain private durable receipts:
certified-prefix settlement, switchover preparation, secondary removal, and
retirement. `TopologyReceipt` stores those canonical payloads directly in an
effect result. There is no catch-up/build/removal receipt layer and no generic
operation-evidence wrapper duplicating the same payload.

The engine returns transient operation-specific outcomes fenced by executable
configuration, engine session and engine generation. The host binds the outcome
to the exact captured durable authority, reconstructs the existing durable
receipt/token shape and revalidates it against the intent. Standard-operation
staleness is validated from host admission plus the typed native fence captured
around the public call.

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
also validates its executable configuration, native session, generation,
progress, and fence. An access preparation contains no durable authority.
Independent custom implementations instead complete their public progress/
revocation callback contract; they do not acquire managed-native receipts.

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
configuration-callback and recovery-restoration serialization locks. Each
admission creates a short-lived `CustomAuthorityAttempt`; nested staging and
host effect completion share the same entered-attempt state so invalidation or
caller loss cannot publish or record stale success
(`kuberic-runtime/src/host/custom/authority.rs:17-117,358-399`,
`kuberic-runtime/src/host/hosting.rs:2379-2385,2641-2648`).

The containment owner first checks structural, durable-conflict and required
build evidence. It then invalidates old work and saved grants, explicitly
closes both read/write projections, and completes bounded native access
revocation before calling the application. This also interrupts
identical-authority replay and same-primary scale-up access. Deferred
restoration accepted earlier is serialized before staging; its host-owned
continuation retains ownership even if the requesting recovery observer is
dropped.
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

Managed built-in admission uses an explicit prepared boundary. The engine first
validates the executable candidate, replication progress, handoff/build proof
and log epoch, then advances its native generation and returns one owned
preparation. The host persists the exact durable authority, the engine consumes
that exact preparation through prevalidated, non-fallible log/quorum mutation,
then completes the remaining fallible public configuration callbacks under the
existing exact-effect recovery contract. The host publishes common
configuration and the engine synchronizes the final host generation. Dropping a
preparation synchronously invalidates it and advances the native generation. A
before-write
store failure leaves the old durable authority and permits exact retry; an
ambiguous post-write failure remains write-closed and leaves the durable
candidate for recovery. Same-primary continuity preserves the access projection
while generation fencing rejects obsolete completion. The custom containment
policy does not weaken, replace or reorder this managed-only protocol.

## Persistence, reporting, and recovery

SQLite persists effect intent before execution and completion only after the
canonical topology receipt, when present, and the lifecycle postcondition
validate. Existing
JSON fields remain backward-readable through serde defaults; this refactor does
not introduce a database schema migration.

Reporting is strictly read-only. `ReportRuntime` exposes only owner observation
capture and live partition observation; it has no progress-refresh, access,
retry, store-write or recovery capability. Both `GetAgentStatus` and the report
appended to an accepted command use the same reporter
(`kuberic-runtime/src/host/hosting.rs:466-527`;
`kuberic-runtime/src/host/report.rs:14-246`;
`kuberic-runtime/src/host/service.rs:816-946`).

The reporter applies one composition rule per field:

| Field group | Owner and composition |
|---|---|
| Protocol version, storage constants and reporter sequence | Protocol constants plus `AgentReporter::ProcessSession`; they are not host or engine lifecycle facts |
| Identity, epoch, previous/current configuration, durable topology and retained work | Durable `AgentState` only |
| Exact admitted authority | Durable `ReplicaAuthorityStore`; the host projection must match the complete stored authority, including transition, switchover, removal and scale-up evidence |
| Address, actual application role/access, open state and authority projection | `HostProxyObservation` only |
| Replication, verified, committed, quorum and catch-up progress | `ReplicationEngineObservation` only |
| Builds | Live engine progress wins by build ID; durable fallback is admitted only for a still-live authority-free command or retained scale-up completion when the host is not in live-build-only mode; host receipt-currentness and retirement filtering decide whether live completion is reportable |
| Peer sessions and addresses | Host/session registries own them; they fence report/recovery capture but are not copied into the local status except the reporter's own process session |
| `healthy` | Live host fault, fail-closed immediately |
| `reported_fault` and load | Durable state after background persistence |
| Catch-up capability | Managed engines supply it directly with each engine observation; independent custom runtimes use the recovery-owner cache only while its observation fence remains current |

Report capture links the host and managed engine through executable
configuration, engine session/generation and the host generation acknowledged
by the engine. Host lifecycle/generation/session fields and engine topology
fences must remain stable across the capture; progress and builds may advance.
An exact pending restoration may temporarily have durable desired access
`Granted` while actual access remains `ReconfigurationPending`. Reporting emits
the actual closed value and accepts that mismatch only while the desired pair,
authority, configuration/access generations, peer sessions and engine fence
remain exact
(`kuberic-runtime/src/host/observation.rs:135-244`;
`kuberic-runtime/src/host/report.rs:199-303`).

Lifecycle advancement belongs to service-owned background owners.
`RecoveryOwner` reconciles exact access first when no effect or
reconfiguration is pending, then maintains one separately cancellable
diagnostic task for progress and catch-up capability. Retryable access and
open independent-custom observation use a bounded 100 ms timer; idle managed
replicas wait for explicit lifecycle revisions. It reloads selection around
publication and compensates fail-closed after supersession. Managed peer
discovery contributes a deferred exact restoration witness rather than
publishing access from its observer task. `PartitionReportOwner` independently
persists changed load/fault revisions so a stalled callback cannot starve
observation durability
(`kuberic-runtime/src/host/recovery.rs:14-192`;
`kuberic-runtime/src/host/hosting.rs:1694-1927`).

`RecoveryTaskOwner` retains access transactions, diagnostic work and peer
recovery producers after their observing caller disappears. Deferred custom
configuration separately retains an abort handle and join handle before its
callback may start. Shutdown signals the background owners, closes descendant
ingress, aborts diagnostic/peer helpers, cooperatively rolls back and joins
access transactions, joins the owners, cancels and joins deferred
configuration, quiesces partition-report producers,
persists the final stable load/fault revision, and only then aborts the
runtime. Startup failure drops reconstruction ownership before following the
same descendant-cancellation and persistence ordering
(`kuberic-runtime/src/host/hosting.rs:57-87,259-300,1600-1665`;
`kuberic-runtime/src/host/service.rs:511-621`).

Recovery still reconstructs authority and native stores, reissues or
reobserves pending effects, and keeps access closed until public/native proof
is accepted. A previously persisted granted value is never sufficient to
reopen writes. Managed recovery validates the full durable authority in the
host, derives executable configuration and reuses the same prepare/commit
protocol before common restoration or access publication.

`RuntimeSnapshot` remains only in the existing effect-evidence,
`RuntimePostcondition` and opt-in testing paths. Report, build, peer-discovery,
outbound and restart-inspection views carry owner observations or narrow
projections. No durable schema change was required; engine-only fence and
diagnostic fields do not implement durable serialization
(`kuberic-runtime/src/effects.rs:104-181`;
`kuberic-runtime/src/host/observation.rs:50-244`;
`kuberic-runtime/tests/lifecycle_capability_boundaries.rs:1266-1460`).

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
  -E 'test(/^host::tests::(runtime|store|coordinator|recovery|crash_boundaries)::/)'
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Use the repository `just nextest-*` recipes for the complete ordinary,
PostgreSQL, archive-partition, and KinD matrices.
