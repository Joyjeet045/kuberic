# PostgreSQL: Native Replication on Kuberic V2

The existing `postgres-replicated` package is **migrated in place** to the
level-triggered v2 runtime. PostgreSQL Workstream 4 is complete with unit and
host-local subprocess validation. Images, deployment assets and distribution
remain deferred; there is no PostgreSQL KinD or live-cluster coverage.
This is experimental software, not a production deployment guide.

Use a **fresh deployment** with protocol 9 / agent schema 5. There is no v1 data
import, metadata conversion, mixed-version operation or rolling-upgrade contract.

## Architecture and Authority

`PgService` implements `StatefulServiceReplica`; its `open` returns the
service-created `PgReplicator` through the existing `ReplicatorFactory` and
`ReplicatorInterfaces`. PostgreSQL implements ordinary `Replicator` and
`PrimaryReplicator`. It supplies neither an operation/copy `StateReplicator` nor
a dummy `StateProvider`. There is no alternate native/external public mode or
driver hierarchy.

| Owner | Responsibility |
|---|---|
| Kuberic protocol/controller | Generic SF-inspired configuration, role, failover, handoff and membership choreography |
| Private agent lifecycle host | Durable identity, authority, effects, selected builds, exact process sessions, proof-before-publication access, callback fencing, Close/Abort and restart reconstruction |
| PostgreSQL custom replicator | WAL, system identity, timeline/LSN evidence, physical backup/rewind/recovery, synchronous policy, receiver drainage, replay and promotion |

The agent delivers exact incarnation, session, endpoint, role and frozen progress
descriptions through `ReplicaInformation` / `ReplicaSetConfiguration` and the
existing configuration/build/catch-up callbacks. Authority stores and mutation
capabilities remain private. Unmanaged custom replicators remain fail-closed for
managed admission.

PostgreSQL uses the same common lifecycle owner as the built-in engine but does
not register the built-in replication/copy data plane. Delayed native callback
completion is accepted only under the original authority, process sessions and
lifecycle generation. Authority/session replacement, Close and Abort cancel or
stale the result before generic progress, access, build or removal credit is
published.

`GetCurrentProgress` exposes durable WAL end for ordering, not proof of replay;
catch-up capability reports the beginning of retained WAL. A raw peer scalar,
process readiness or successful backup cannot mint quorum/build credit.
PostgreSQL validates its own lineage and completion evidence before callbacks
complete. Generic reports and controller status contain no PostgreSQL recovery
certificates. Protocol 9 advertises the custom replicator's actual endpoint;
database evidence travels only over application-owned `pgdata.v2`.

## SQL Access and Acknowledged Writes

The retained partition handle's independent read/write statuses govern access,
not role notifications or PostgreSQL promotion alone. Progress observation
reconciles SQL access before private hosting acknowledges an access effect.
Accepted secondaries serve read-only SQL while PostgreSQL recovery rejects
writes. Idle/unadmitted replicas remain closed.

Ordinary clients use the non-superuser database-owner role `kuberic_app`, database
`kuberic`, and `synchronous_commit=remote_apply`. PostgreSQL owns synchronous
standby selection using exact session-qualified application names. Before a
policy change, it durably invalidates recovery metadata, applies and reads back
the native policy, then distributes the matched policy to the exact eligible
standbys. SQL cannot reopen on stale policy metadata after process restart.
Deliberately selecting local/asynchronous acknowledgement is outside the
acknowledged-data guarantee.

Closing previously granted access stops and reaps the entire owned PostgreSQL
run, then restarts with closed managed HBA rules before acknowledging closure.
HBA reload and `pg_terminate_backend` alone miss accepted, unauthenticated
sockets. The stop fence disconnects ordinary clients, retained administrative
connections and pre-authentication sockets; later regrant cannot revive them.
Grant/close serialize, and grant confirms a real application-role login.
Cancelling an unfinished grant still requires the full fence.

External-client closure can leave a restarted PostgreSQL available for internal
control and replication; a completed **PostgreSQL-stopped** postcondition is
stronger and leaves no SQL session usable. Both deliberately interrupt
connections. A transaction already inside PostgreSQL and waiting for synchronous
acknowledgement when connectivity is lost has an **unknown outcome**, not a
promised rollback. A unique write attempted after the completed fence is
definitively rejected without a database side effect.

### Administrative Trust and Supervisor Loss

Database and OS administrators are trusted. This example uses local trust
authentication, loopback application access, local administrative sockets and
trust-authenticated physical replication (including an IPv4 replication HBA
rule). PostgreSQL listens on all addresses; this is not hardened network
isolation. Do not expose the database/replication port to untrusted networks.
Coordination RPCs use the agent bearer credential over HTTP, not a supplied TLS
deployment. Managed settings must not be replaced or bypassed.

Completed fences disconnect retained administrative test sessions, but do not
protect against hostile administrators changing configuration or relaunching
PostgreSQL. API unreachability is not physical termination evidence. Abrupt
supervisor loss, including a host killed while PostgreSQL independently survives,
is outside the containment guarantee. The supervisor and database must share a
managed lifetime; there is no independent orphan self-fencing mechanism.

## Native Build and Catch-Up

Private hosting durably selects one immutable build generation per logical
target slot. Completion receipts bind that authority to both current sessions
and the local attempt. Supersession/replacement withdraws the old description;
one scalar cannot certify multiple builds or resurrect completion after reopen.
If supersession arrives while the old native callback is unwinding, the new
configuration is deferred under a bounded generation-owned task. It can return
durable admission without waiting behind the stale callback, but must complete
within its deadline and revalidate authority/session/terminal state before the
configuration becomes visible.

The target validates its locally installed SF description and source evidence
before destructive work. Configuration replacement cancels and joins stale work.
PGDATA clearing runs in a private generation-owned helper, not Tokio filesystem
workers. Cancellation, timeout and dropped build futures terminate and reap that
helper before releasing the build lock; failed cleanup remains retained and
blocks replacement. Recursive directory removal, file unlinking and the final
directory fsync share this ownership, so no stale deletion can resolve paths in
a successor's live PGDATA. Interrupted clearing leaves `Copying` durable and
re-admitted retries take a fresh backup, as with interrupted backup/rewind.
`pgdata.v2` carries authenticated, canonical JSON envelopes bounded to 64 KiB,
with exact resource, identities, sessions, build boundary and PostgreSQL lineage.
Unknown, missing, duplicate, oversized and wrong-version fields are rejected.

Durable build stages are `Intent`, `Copying`, `Installed`, `Recovering` and
`Complete`. Native `pg_basebackup` supplies the copy. Compatible established
lineage permits `pg_rewind`; failed rewind explicitly switches to a fresh backup,
while unrelated system identities/incompatible histories are rejected.
Interrupted destructive work resumes only under re-admitted exact authority;
installed data is reused. Old sessions cannot reconnect on reopen.

Completion requires compatible system/timeline history, flushed WAL **and replay**
at or beyond the frozen boundary, including equality. Checkpointed installed
recovery and durable completion precede admission. Receipt or flush without
replay is insufficient. `WriteQuorum` uses policy-certified replay; SF `All`
requires every required exact session to replay its frozen boundary. Waits release
the lifecycle lock between probes and cancel on authority/access/shutdown changes.

Physical replication slots belong to exact incarnations, not quorum votes.
Slots reserve WAL before build and survive internal reconnection/restart.
Session-qualified names separately determine acknowledgement eligibility.
Retired consumers drain before slot deletion; an in-use slot is not reported
removed. Unix sockets live in `PGDATA/pg_stat_tmp`, excluded by backup/rewind.

## Failover and Planned Switchover

Generic hosting installs write-closed authority and awaits the existing primary
role callback. PostgreSQL, not a shared native selector, performs recovery:

1. Collect fresh exact-session observations for the last accepted synchronous
   policy. Require **`R + W > N`**: observed responders plus acknowledgement
   count must intersect every eligible synchronous standby acknowledgement set.
   Missing, restarted or incompatible optional peers contribute nothing to `R`
   but remain in persisted `N`; neither `N` nor `W` is reduced. Local authority
   and policy must still be exact.
2. Journal that initial set. Responders persist revocation, remove old-primary
   connection settings, verify WAL receiver exit, retain received/replay progress
   and restart closed.
3. Re-observe the same sessions/policy and compatible lineage after receiver
   drainage; revalidate the intersection before final candidate ordering by
   received WAL, then replay WAL, then exact identity.
4. The selected candidate replays the selected boundary, promotes, checkpoints
   and verifies lineage before completing `ChangeRole(Primary)`. An unsafe or
   non-selected candidate refuses activation.

Draining the intersecting set prevents further supported synchronous
acknowledgements from the old source; it is not an attestation that the old
postmaster died. Surviving standbys must reconnect to the exact promoted primary
and timeline before regaining synchronous eligibility. Former primaries remain
stopped until an authorized rewind or fresh build.

Planned switchover uses the existing two SF `All` catch-up calls around access
revocation. Source demotion checkpoints and persists a handoff intent, completes
verified shutdown, and distributes a durable stopped receipt before returning.
The target revalidates that receipt and replay boundary before promotion.
Generic hosting journals only successful callback/effect receipts; access grant
is separate. Restart can resume the same evidence or stay closed, never infer
success from a role flag.

## Scaling, Replacement and Restart

Sequential scale-up builds/adopts one exact fresh identity at a time. PC/CC
configuration precedes current-only admission, and candidates stay fenced until
their selected build completes. Secondary removal closes writes, freezes a
boundary under SF `All`, checks exact-session witnesses, commits reduced authority
and persists terminal retirement only after application Close. Witness scalars
do not advance custom quorum progress.

Replacement uses a fresh incarnation and storage root for the same ordinal;
delayed old-session work is rejected. A former primary can only rejoin through
a selected rebuild. Direct primary removal is not implemented: use planned
switchover followed by secondary removal.

Agent SQLite metadata, checksummed PostgreSQL metadata and PGDATA have distinct
owners. Schema 5 binds canonical application/PGDATA paths before authorized
creation. First-open permission survives interruption but is consumed before
readiness. Valid completed `initdb` is reused and managed settings repaired;
partial/invalid established storage is never silently erased or reinitialized.
Missing files, changed paths/symlink targets, corrupt identity/lineage or
ambiguous evidence fail closed with explicit faults.
Unexpected durable-role/recovery-signal mismatches or incomplete recovery without
an authorized native build stop and reap the captured PostgreSQL generation before
publishing a permanent fault. A private reader/writer gate serializes progress/status
validation with native role changes and durable role publication, without blocking
access regrant behind delivery of an old reader's helper error. Before
native promotion, the private election journal records the exact candidate,
process session and process generation after final quorum/replay verification.
An interrupted role-publication gap is recognized only with that intent, unchanged
authority and accepted policy, exact final responders/boundary, closed access and
compatible promoted lineage. Same-generation retries complete the existing election;
cold storage inspection does not authorize a retired session to activate. Build and
demotion transitions retain their existing validation. A delayed mismatch from a
retired generation cannot fence its successor, and unrelated recovery metadata
never exempts a true role/signal mismatch.

Application-only and whole-agent-host restart reconstruct durable state with
fresh sessions. Accepted standbys wait for validated current source connections.
On whole-agent restart a persisted custom-replicator grant remains desired, not
immediately effective: transient access reconciliation keeps the control service
live and reports access pending while application SQL stays closed. Progress/status
reconciliation retries that grant only under the restored authority until exact
peer discovery, native policy readback and acknowledgement availability converge.
New access/authority fencing supersedes the deferred grant; other startup errors
remain fatal. The default replication engine's restart behavior is unchanged.
Lost quorum closes writes without discarding acknowledged data; accepted
secondary reads remain possible. Restoration requires fresh evidence and native
policy readback, not old scalar journals.

## Process Ownership and Failure Semantics

Linux pidfds, child-subreaper support and readable `/proc` are prerequisites.
The package binary re-executes a dedicated single-threaded subreaper for each
PostgreSQL/helper launch; library hosts need `postgres-replicated` beside their
executable. No separate installed helper binary is required.

Readiness verifies the actual postmaster's ownership. Shutdown signals retained
pidfds, not mutable PID-file targets, and uses bounded fast shutdown/escalation.
Success requires reaping every owned identity, not merely observing exit.
Helpers (`initdb`, control-data/readiness probes, promotion, backup and rewind)
share cancellation-safe ownership. Abort, signals, dropped futures and partial
startup join cleanup; unrelated processes are never targets.

Durable process-generation high-water marks prevent reuse after reopen; delayed
cleanup/results/faults cannot affect a successor generation. Exhaustion is
terminal, not wrapping. Fatal ownership/cleanup failures remain retained.
Transient helper deadlines stop the exact run closed and permit a safe retry.
Fault reporting awaits bounded durable agent acknowledgement; acknowledgement
or cleanup failure produces a nonzero executable exit.

Application metadata commits serialize cancellation with rename/fsync and
publication. Once commit wins, cancellation joins it. Active/suspended builds
are bounded to 16, retained IDs to 64 per admitted epoch, IDs to 512 bytes and
metadata to 4 MiB. New epochs compact old IDs into a rejection watermark, never
TTL-based forgetting. Exhausted budgets fail closed.

## Local Usage and Configuration

Run as an unprivileged Linux account with compatible local PostgreSQL server and
client binaries (`initdb`, `postgres`, `pg_ctl`, `pg_basebackup`, `pg_rewind`,
`pg_controldata`, `pg_isready`). PostgreSQL 16 is the validated host major;
there is no multi-major compatibility claim.

```sh
cargo run -p postgres-replicated -- --help
cargo run -p postgres-replicated -- \
  --resource-uid example --replica-id 1 --pod-uid replica-1 --pvc-uid storage-1 \
  --data-root ./postgres-state --pg-bin /usr/lib/postgresql/16/bin \
  --bearer-token "$KUBERIC_AGENT_BEARER_TOKEN"
```

This starts an authority-waiting host, **not** a writable standalone database.
Authenticated `InitializeAgentStore`, exact `EnsureConfiguration` and a separate
write grant are required. The tests provide local choreography; no deployment
controller or bootstrap CLI is supplied for PostgreSQL.

| Options / environment | Meaning |
|---|---|
| `--resource-uid`, `--replica-id`, `--pod-uid`, `--pvc-uid` / corresponding `KUBERIC_*` | Exact resource/storage incarnation; replica ID must be positive |
| `--data-root` / `KUBERIC_DATA_ROOT` | Agent root (`.kuberic/agent.sqlite3`); default application and PGDATA roots beneath it |
| `--application-root` / `KUBERIC_APPLICATION_ROOT`, `--pg-data` / `PGDATA` | Optional separately bound application metadata and PostgreSQL directories |
| `--pg-bin` / `KUBERIC_PG_BIN`, `--pg-port` / `PGPORT` | Required binary directory and SQL port (default 5432) |
| `--control-address`, `--replication-address`, `--application-address` | Agent control/operation transport and PostgreSQL coordination listeners (loopback 50051/50052/50053 by default); corresponding `KUBERIC_*_ADDRESS` variables |
| `--control-endpoint`, `--replication-endpoint`, `--application-endpoint` | Advertised endpoints; corresponding `KUBERIC_*_ENDPOINT` variables |
| `--bearer-token` / `KUBERIC_AGENT_BEARER_TOKEN` | Shared agent and application-coordination credential |
| `--peer-routes` / `KUBERIC_PEER_ROUTES` | Optional JSON file of exact `identity`, `control`, `replication` routes, at most 32 entries / 64 KiB |

Route identities contain `replicaId`, `instanceId`, `agentGeneration`; there is
no ordinal-only fallback. PostgreSQL coordination uses the endpoint returned by
the custom replicator, not the agent's operation-stream listener.
The application repairs managed port/socket, `wal_log_hints`, `hot_standby`,
`wal_level=replica`, synchronous durability and closed access settings on startup.
Configuration is implementation-owned, not a general PostgreSQL tuning API.

## Local Validation

```sh
just nextest-postgres-smoke
just nextest-postgres
# One deterministic shard, normally run on its own isolated CI runner:
just nextest-postgres 1/4
# Smaller native build/recovery and reconfiguration selections:
cargo test -p postgres-replicated --all-features --test native_build --test failover -- --test-threads=1
cargo test -p postgres-replicated --all-features --test reconfiguration --test authority_races --test switchover_checkpoints --test validation_oracles -- --test-threads=1
cargo clippy -p postgres-replicated --all-targets --all-features -- -D warnings
```

The smoke target is the routine local gate and exercises representative real
PostgreSQL bootstrap/fencing, native build, failover, planned switchover,
scaling and explicit failure handling. The full target remains the complete
pre-push gate.

Tests discover common PostgreSQL installation directories (16, 17, 15 in that
order on Debian-style hosts), then `pg_config --bindir`. `--pg-bin` configures the
executable, not a test-runner override. Missing binaries fail the prerequisite;
tests do not silently skip. Fixtures own their worker stacks; no extra runner
stack override is required.

`testing::PgGroup` drives real agent stores and SF hosts with local PostgreSQL
subprocesses; it supplies generic decisions rather than invented WAL receipts.
Coverage includes 1→2→3 growth, 3→2→1 removal, replacement, repeated
failover/switchover across identities, retained/pre-auth/admin fencing, quorum
restoration, admission-cut application/agent restart and storage/process faults.
Unique-write and complete-SQL-set oracles distinguish definitive fences from
timeouts, generic SQL errors and unknown outcomes. Teardown checks retained
pidfds, allocated listener addresses and exact fixture roots under
`target/postgresql-v2-tests/<unique-id>`.
Compact IDs encode the Linux PID and a bounded process-local counter to leave
room for PostgreSQL Unix sockets in the hosted CI checkout. Existing directories
are skipped, never removed to allocate a new fixture.

CI builds one all-features nextest archive, then runs four deterministic hash
partitions on isolated runners. The `postgres` profile and test group both
enforce one test process at a time within each runner; the measured unsafe
same-runner four-way mode is not used. The 172-test inventory consists of 170
directly partitioned tests and two subprocess helpers reached through their
mapped parent tests. PostgreSQL is absent from KinD/live jobs and selectors.
Shared regressions, partition validation and guard commands are in the
[testing guide](../kuberic/testing.md#postgresql-v2-host-local-validation).

## Limitations and Future Work

- Images, distribution and deployment assets remain future work. No PostgreSQL
  Kubernetes/container dependency, manifest or live test is part of this port.
- Physical-slot lifecycle is implemented, but WAL-retention limits
  (`max_slot_wal_keep_size`) and free-space headroom admission are not. Unavailable
  configured replicas or retryable selected builds can retain unbounded WAL.
  Capacity policy must include invalidated-slot recovery/rebuild, not just a cap.
- Network authentication/TLS hardening, independent orphan containment,
  automatic rolling upgrades and direct-primary-removal orchestration are not
  supplied. Administrative trust and supervisor-loss boundaries above apply.
- Tests prove bounded local traces, not an availability SLO, maximum replica
  count, throughput guarantee or complete failure-interleaving model. Fencing,
  catch-up, restart and handoff may interrupt service without a duration promise.

See the [v1 removal record](../../proposal/v1-retirement-plan.md#workstream-4-postgresql-on-v2),
[SF callback reference](../../background/service-fabric/references.md) and
[runtime interface boundary](../../../kuberic-runtime/README.md).
