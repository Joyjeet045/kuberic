# kuberic-agent

Custom-replicator build receipts are bound to a durably selected immutable
authority and generation for one logical target slot. Selection supersedes old
builds/incarnations atomically; live session/attempt bindings are checked before
progress persistence and reporting. Restart never promotes old scalar build
journals into current-session completion. This hosting support is private and
uses the existing SF replica-set configuration and build callbacks.

On shutdown, retained control/replication streams are cancelled and joined before
the durable fault acknowledgement. Peer-controlled graceful connection draining
must not delay that acknowledgement; interrupted durable command intents remain
replayable.

Replica-local hosting and durable authority for the level-triggered Kuberic
stack.

`kuberic-agent` owns the Service Fabric Replica Agent/FUP-equivalent process
boundary:

- application lifetime, `Open` registration, role/close/abort ordering, and
  exact returned-replicator identity;
- one SQLite metadata database under `.kuberic/agent.sqlite3` on the replica
  PVC, separate from application data;
- exact resource, Pod, PVC, replica-incarnation, durable-generation,
  initialization, policy, and schema identity;
- intent-before-effect and terminal-result-before-reply ordering;
- restart recovery from pending intent or retained terminal evidence;
- ephemeral process sessions and session-scoped report sequences.

The database is created only by an authorized `InitializeAgentStore` path.
Missing established metadata, corruption, incompatible schema, or identity
mismatch fails closed instead of creating empty authority. SQLite uses WAL and
`synchronous=FULL`; the agent is the single writer.

Primary promotion follows the SF host sequence: replicator role and epoch,
including any provider-specific work that replicator owns, then application role.
Abort stops the returned control
before application teardown. The default replicator's managed capability is
transferred directly into agent registration and is not returned to
application code.

Lifecycle hosting is selected independently of the optional `StateReplicator`.
Every independently implemented `PrimaryReplicator` receives the same private
SF configuration, role, build, catch-up and durable-effect support, whether or
not it also supplies operation/copy streams. Its application calls that optional
state capability directly. Only the registered default engine supplies
`ManagedReplicator` internals for Kuberic's operation transport and write journal;
custom lifecycle hosting does not implement or impersonate that capability.

Custom-primary scaling uses the existing generic authority and effect records.
Candidate admission checks the exact live build selection and boundary.
Secondary-removal preparation and commit require SF catch-up callbacks; peer
witnesses are session/configuration checked but never projected into application
quorum progress. Retirement persists intent before Close and its tombstone only
after the returned replicator and service have closed. These paths contain no
database-specific evidence or protocol fields.

The agent now owns fenced `EnsureConfiguration` admission, durable private
Demote/GetLSN/Catchup/Deactivate/Activate stages, restart-safe runtime effect
sequencing, independent read/write access, partition/load/fault reports,
separate authenticated control and replication listeners, fresh process
sessions, stale-session rejection, protobuf conversion, and bounded reliable
send windows. Missing retained replication payloads explicitly require full
copy rather than overstating catch-up capability.

Authority replacement requires a newer epoch, except for the exact
Previous/Current-to-current-only completion of one admitted transition.
Changed primary authority remains access-closed until provider epoch and
catch-up postconditions complete. Catch-up waits permit ACK progress, and
matching commands are serialized and revalidated at each durable stage.
Failover commands update the selected primary epoch before GetLSN, retain
deactivation epoch/LSN evidence, durably authorize only the elected safe
prefix under the new fence, and can publish `NoWriteQuorum` independently from
role. Configured lagging members may receive an explicitly authorized
full-copy build before current-only completion.

Serving starts fail-closed listeners before reconstructing live hosting from
durable authority, role, access, pending effects, and retained stage evidence.
An interrupted planned primary demotion restores only the fenced target
replicator role until the journal completes the application transition. It must
not reactivate the old Primary after handoff authority has replaced it; genuine
Primary activation still settles the authority-verified prefix before its callback.
Session replacement holds a delivery lease through runtime mutation. Shutdown
revokes readiness and aborts the owned runtime and workers.

Build authorization is admitted by the agent before source copy execution.
Dropping a returned copy stream cancels blocked provider iteration and releases
the build. Reports retry until durable authority and live progress form one
compatible snapshot and carry deactivation epoch with its LSN.

The controller remains responsible for selecting configurations,
re-observing command postconditions, routing fences, and distributing
deployment authentication material. Concrete outbound peer dialing is wired
through the agent's `OutboundDispatcher` contract.

## In-process application tests (opt-in)

The `testing` Cargo feature exposes `testing::InProcessTransport`; it is absent
from default production builds. Register each opened `PodRuntime` with its exact
process session. Registration derives the full replica identity from the runtime,
not just its numeric replica ID. A fresh registration after restart replaces old
polling/delivery futures and rejects reuse of retired sessions; it does not
reconstruct or transfer application data, authority, or write journals.

`pump()` performs a bounded nonblocking poll in identity/delivery order.
`next().await` waits on real queue/ACK wakers rather than sleeping or busy-polling.
Reports distinguish received ACKs from application-applied ACKs, include pending
delivery counts, and report idle only when nothing is ready and no delivery is
outstanding. Transport idle does not imply cluster convergence or write quorum.
One transport must be the sole outbound consumer of its registered runtimes.

For prepared copy streams or deliberate delayed delivery, call `bind(message)`
at emission time, retain that session-bound public wire message, then `enqueue`
it. Copy ACKs return through the source runtime only after durable receiver
acceptance. Missing exact endpoints, changed sessions, rejected operations, and
dropped stream operations produce explicit errors/rejection events, never fake
applied progress. `Build`, `Remove`, and `Evict` outputs are surfaced to the
caller without interpreting them or granting authority. Tests remain responsible
for agent-side admission, storage directories, process lifetime, and recovery.

## Storage and recovery contract

`ReplicaHost` places metadata at
`<data-root>/.kuberic/agent.sqlite3`; application state remains under an
application-owned sibling directory. SQLite uses WAL, `synchronous=FULL`,
foreign keys, disabled automatic WAL checkpoints, and a five-second busy
timeout. The agent is the single writer.

The PVC filesystem must provide durable fsync and directory-entry semantics
and SQLite WAL locking/shared memory. Opening an established store validates
integrity, exact storage identity, and the exact schema version. The current
migration hook is idempotent only for that version; it is not an older-schema
upgrade path.

Schema **4** intentionally rejects schemas 2 and 3 and unknown versions without
migration. Initialization identity (including its original policy) remains
immutable. Admitted PC/CC policies are separate durable authority; reduced or
expanded policy does not rewrite initialization replay or Pod/PVC validation.
Schema 3 introduced exact scale-up initialization, build-boundary,
PC/CC/current-only admission, carried-failover, and bounded completion evidence.
Schema 4 retains that evidence and establishes committed snapshot boundaries:
the exact applied suffix is delivered separately through retained catch-up.
Source restart reuses immutable build authority; final-marker commitment must
equal that boundary. This is a fresh-storage contract, not an old-schema upgrade.

The controller enables SF-inspired secondary scale-down using PC/CC quorum
principles, with Kuberic-specific target/minimum coupling, deterministic
selection, write closure, sequential cleanup, and Kubernetes resource deletion.
`spec.replicas` target=min is Kuberic policy; SF target and minimum are
independently configurable. Before freezing intent or closing routing/writes,
controller preflight requires the previous read quorum from retained exact
members under stable accepted current-only authority and fresh exact sessions.
Otherwise `ScaleDownRetainedReadQuorumUnavailable` preserves existing service
with bounded re-observation and no removal preparation or alternate target.
The agent never owns desired-count or target-selection policy.

The agent executes `PrepareSecondaryRemoval`, write-closed dual-policy
`EnsureConfiguration`, `AcceptSecondaryRemovalCommit`, and `RetireReplica`
through durable command/effect intent. Preparation preserves the verified boundary and its original
session/sequence; exact duplicates return retained evidence even after a
process restart. Current-only completion retains the full frozen evidence
after PC disappears. Operation mutation and unrelated authority work are
rejected while removal is pending. Accepted current-only evidence is separately
persisted before write regrant; coordination alone never grants reduced writes.

Retirement validates and persists an exact `retirement-started` record before
revoking access, fencing traffic, driving role None, and closing hosting.
Finalization atomically persists the tombstone, removes active authority, and
clears the started record. Restart checks both lifecycle records before
application `Open`: a started retirement is finalized with **zero Open calls**
because process termination already closed the prior host. Failed finalization
stays closed. The control plane continues reporting role None and the exact
terminal receipt under a fresh process session. Excluded peers cannot regain sessions through
delayed discovery. Pending removal preparation reconstructs access closed and
resolves interrupted local writes under their original operation identities
without waiting for an unavailable old write quorum.

Frozen certificates survive retained-peer restart but do not reinstate
obsolete-session credit; acceptance revalidates current-session progress.
An already committed reduction has a separate live-progress path: exact
current-only peers retain the same immutable commit proof, cover the prepared
boundary, and report completed work in their current session. The primary may
already have granted access. Pre-commit witnesses remain write-closed. Startup
keeps a pending commit-acceptance effect replayable while discovery restores
fresh peer evidence, rather than requiring obsolete-session credit before
the control plane can start.

After accepted topology advances, an exact `localRecovery` commit replay can
clear a retained secondary's still-pending historical removal fence. It requires
the immutable completed certificate, matching installed current-only authority,
exact resource/incarnation and verified boundary, plus the current dispatch session.
It cannot run on the primary or excluded target, replace unrelated work, or grant
access. SQLite retains the exact local effect for pending/applied/completed startup
replay without creating a live runtime commit or restoring peer-session credit.
Only after the acceptance report may ordinary newer-authority correction proceed;
the general pending-removal admission fence remains unchanged.

`ReplicaDiagnostics.retired` is a compact terminal-retirement indicator from
durable or runtime authority. Unlike role `None` or denied access alone, `true`
means a retirement tombstone exists. The JSON field is additive; older diagnostic
responses may omit it. Diagnostics do not expose managed certificates.
Controller admission and exact Kubernetes cleanup are enabled; these local
contracts never select the target or authorize arbitrary Pod/PVC deletion.
Use a fresh coordinated protocol-9/schema-5 deployment, not a rolling upgrade.
Exact original PVC provenance must be reconstructable before admission; if Pod
and PVC already disappeared without that provenance, scale-down waits/fails
closed rather than treating list omission as absence. Unavailable-target support
requires frozen or reconstructable exact cleanup identity. PVC object deletion
has no retention or import path, not a physical storage erasure guarantee.
Frozen-primary loss during removal/cleanup can cause indefinite outage. Sequential
cleanup must finish, and every retained member needs its original completed
current-only witness or fresh completed local acceptance before superseding the
bounded receipt. Sequential scale-up uses durable schema-5 build and admission
authority, preserves healthy same-primary writes only while both PC/CC quorums
remain authorized, and retries failed unadmitted candidates only after exact
endpoint→Pod→PVC cleanup. Carried failover first installs a write-closed
provisional epoch, then uses fresh fenced PC/CC progress for final primary and
safe-prefix selection before activation. Final commands carry a separate exact
post-fence election certificate. Its typed witness table records replica/session,
sequence, durable progress, deactivation progress, and the deterministic fence
operation; quorum IDs and frozen authority reconstruct exact incarnations,
configuration, policy, final epoch, and safe prefix. The completion receipt keeps
that proof with a provisional-primary reference instead of duplicating the full
intent/configuration, so a returning provisional member can advance through
final PC/CC and current-only without accepting unrelated stale authority. Protocol
9/schema 5 require a fresh coordinated deployment; classic v1 remains unchanged.
These limits and the explicitly deferred
durable primary-agent phase coordinator are recorded in
[scale-down follow-ups](../docs/proposal/v1-retirement-plan.md#deferred-scale-down-follow-ups).

Crash-boundary environment variables exist only in the test executable.
Production agent and application binaries expose no fault-injection mode.

See the
[level-triggered operator guide](../docs/features/kuberic/level-triggered-operator.md)
for identity semantics, deployment, diagnostics, and unsupported recovery.
