# PostgreSQL Restart-Stateless Metadata

## Status

Proposed architecture. This document defines a CloudNativePG-inspired storage
and recovery model for the PostgreSQL custom replicator. It does not describe
behavior that is currently implemented.

The proposal removes the PostgreSQL-specific `state-v2.json` metadata file as
an independent recovery authority. PostgreSQL state is reconstructed from
PGDATA, while topology and workflow authority remain owned by Kuberic's generic
agent and controller.

The primary design requirement is:

> The PostgreSQL application replicator, which is implemented outside
> `kuberic-runtime`, must not persist any replicator state.

It may keep queues, observations, helper ownership, callback context, and other
execution state in process memory. It may read and modify PostgreSQL through
PGDATA. It may submit typed evidence to Kuberic runtime for durable acceptance.
It must not own a metadata file, database, journal, checkpoint, receipt store,
workflow cursor, fence store, or other durable recovery state of its own.

This change does not require changing the public `Replicator`,
`PrimaryReplicator`, or `StateProvider` interfaces.

## Overview

The PostgreSQL application currently has three durable owners:

1. the Kuberic controller and generic replica agent;
2. PostgreSQL PGDATA;
3. a separate checksummed PostgreSQL application metadata file.

The application metadata records identity, lineage, WAL progress, native
synchronous policy, build state, recovery elections, process state, access
state, and workflow cursors. Some of those values duplicate facts already
available from PostgreSQL. Others duplicate generic authority already accepted
by the agent or controller.

This additional store makes crash continuation precise, but it also creates a
second interpretation of PostgreSQL and reconfiguration state. Startup must
compare the metadata file with PGDATA, reject mismatches, and resume private
workflow journals that overlap the level-triggered controller.

CloudNativePG uses a simpler model:

- PostgreSQL storage is the database recovery authority;
- the Kubernetes API is the cluster-control authority;
- the per-pod instance manager keeps only disposable runtime state;
- temporary logs, sockets, certificates, backup files, and WAL spools use
  pod-scoped scratch storage;
- after restart, the instance manager observes PostgreSQL and reconciles it
  with the desired cluster state.

Kuberic should adopt the same ownership principle while preserving its stronger
epoch, quorum, exact-incarnation, and proof-before-publication protocols. The
PostgreSQL custom replicator must be restart-stateless: all of its process state
is disposable. It reconstructs from PGDATA plus generic agent/controller
authority rather than opening any private durable store.

This is a component-boundary requirement, not merely a file-layout preference.
Moving `state-v2.json` into another PostgreSQL-owned file, embedded database,
PGDATA subdirectory, Kubernetes object, or remote service would violate the
design. Durable records required by the protocol must be defined, validated,
and persisted by Kuberic runtime's generic agent/controller facilities.

## Goals

1. Make the PostgreSQL application replicator persist no state of its own.
2. Remove `application/state-v2.json`, its lock files, and the separately bound
   PostgreSQL application-metadata root.
3. Prevent replacement of that file with any other application-owned durable
   metadata mechanism.
4. Make PGDATA the sole local durable authority for PostgreSQL lineage, WAL,
   recovery state, and database contents.
5. Make the generic agent and `KubericSet.status` the durable authority for
   identity, topology, epochs, configurations, role intent, access intent,
   builds, fencing, and reconfiguration workflows.
6. Reconstruct the PostgreSQL custom replicator from fresh PostgreSQL
   observations and replayed generic authority.
7. Keep SQL access closed until reconstructed PostgreSQL evidence and admitted
   authority satisfy the normal activation invariants.
8. Replace resumable application-owned build and recovery cursors with
   level-triggered restart or reconciliation.
9. Move sockets, logs, temporary backup data, rewind staging, and other
   disposable files to pod-scoped scratch storage outside PGDATA.
10. Preserve acknowledged-write safety across process, pod, and controller
   restarts.
11. Preserve exact replica/PVC incarnation fencing and stale-session rejection.

## Non-Goals

- Removing the generic agent's durable store.
- Making Kubernetes API availability optional for granting write access.
- Treating local PostgreSQL flush as proof of quorum commitment.
- Weakening the current synchronous acknowledgement policy.
- Resuming a partially completed `pg_basebackup` or `pg_rewind` after pod
  replacement.
- Moving PostgreSQL WAL or database pages into the Kubernetes API.
- Adding a PostgreSQL-specific transactional log beside PGDATA.
- Allowing the PostgreSQL application replicator to persist generic receipts
  on behalf of Kuberic runtime.
- Changing the public replication or state-provider interfaces.
- Providing mixed-version metadata migration or rolling upgrade in the first
  implementation.
- Replacing PostgreSQL physical replication with Kuberic operation replication.

## Current Storage Model

### Durable roots

The PostgreSQL service currently binds two application paths:

```text
postgres-application/
  state-v2.json
  state-v2.lock
  state-v2.owner

postgres-pgdata/
  PG_VERSION
  global/
  base/
  pg_wal/
  postgresql.conf
  postgresql.auto.conf
  pg_hba.conf
  standby.signal
  ...
```

The generic agent additionally stores identity, authority, effects, builds,
access state, removal, and transition state in `.kuberic/agent.sqlite3`.

`PgDurableState` currently combines several categories:

| Category | Representative fields |
|---|---|
| Storage identity | Resource UID and exact replica identity |
| PostgreSQL lineage | System identifier, timeline, timeline-history digest |
| PostgreSQL progress | Current, flush, receive, replay, and certified LSNs |
| Runtime condition | Role, process stopped, access closed, recovery state |
| Native policy | Synchronous policy and policy generations |
| Build continuation | Accepted build, inbound/outbound build stages, attempts, retired IDs |
| Catch-up continuation | Configuration and catch-up boundary |
| Recovery continuation | Membership, elections, source fences, promotion and connection state |

The file is checksum protected, bounded, atomically replaced, directory-fsynced,
and locked against concurrent owners. Those mechanisms make it a real recovery
authority rather than a cache.

### Problems

#### Duplicate PostgreSQL truth

System identity, timeline, role, WAL positions, recovery signals, and timeline
history are available from PostgreSQL control functions and PGDATA. Persisting
copies requires startup mismatch logic and can turn a recoverable interruption
into a permanent metadata fault.

#### Duplicate control-plane truth

Membership, epochs, accepted configurations, builds, access intent, and
transition workflows are already admitted by the generic agent and represented
in controller status. A private PostgreSQL journal creates a second workflow
owner that must advance in lockstep.

#### Cursor-oriented recovery

The metadata attempts to resume individual build, rewind, catch-up, policy, and
election steps. Kuberic's controller is level-triggered: it should reissue the
desired effect, observe the current state, and continue from externally visible
postconditions rather than restore an application-private program counter.

#### Operational coupling

The application metadata root must be provisioned, identity-bound, backed up,
restored, monitored, and kept consistent with PGDATA. Losing either root makes
the other insufficient even when PostgreSQL itself is recoverable.

## Reference Model

CloudNativePG does not keep a durable per-instance metadata database beside
PGDATA. Its instance manager uses pod-scoped scratch storage for:

- PostgreSQL logs;
- Unix sockets;
- generated certificate files;
- WAL archive and restore spools;
- backup and recovery temporary files.

Cluster state such as current primary, target primary, topology, instance
status, PVC inventory, promotion state, and primary leasing is stored in
Kubernetes objects. PostgreSQL identity and recoverable database state remain
in PGDATA.

Kuberic cannot copy this model literally. It has stronger exact-incarnation,
PC/CC configuration, epoch, quorum-intersection, and callback-completion
contracts. It can, however, use the same ownership rule:

> Durable database facts belong to PostgreSQL. Durable orchestration facts
> belong to the generic control plane. The instance manager owns no third
> metadata authority.

Two CloudNativePG behaviors are particularly relevant:

1. During synchronous-replication configuration changes, CloudNativePG resets
   the `FailoverQuorum` status. Failover is denied while the active
   acknowledgement basis is indeterminate. The primary republishes quorum
   metadata only after reading the applied PostgreSQL configuration.
2. A restarted instance is rediscovered through stable Kubernetes and storage
   identity, then reports fresh PostgreSQL state. Dead process-local sessions
   are not required to return before the instance can rejoin.

Kuberic adopts both principles while retaining stronger exact-session fencing:

- policy handover has an explicit unavailable phase and catch-up barrier before
  new failover metadata is published;
- a fresh process session can reattach to the same admitted storage incarnation
  only through a durable runtime-owned reattachment protocol.

## Target Ownership

| State | Target owner | Reconstruction |
|---|---|---|
| Resource UID, replica ID, instance ID, agent generation | Generic agent | Reload from admitted storage identity |
| Pod UID, PVC UID, canonical PGDATA binding | Controller and agent | Validate against current Kubernetes resources |
| Previous/current configuration and epoch | Generic agent/controller | Replay through existing callbacks |
| Desired role and access state | Generic agent/controller | Replay and reconcile |
| Build authorization and terminal receipt | Generic agent/controller | Reissue or restart from declared intent |
| Removal, replacement, switchover, and failover intent | `KubericSet.status` and agent effects | Reconcile level-triggered postconditions |
| Historical acknowledgement policy | Generic control plane | Retain until a replacement policy is durably published |
| Receiver revocation and former-primary fence | Local generic agent | Enforce before PostgreSQL startup |
| Destructive storage-work intent and installation receipt | Local generic agent | Distinguish reusable PGDATA from interrupted mutation |
| Selected promotion candidate and final recovery evidence | Controller transition and candidate agent | Recognize an authorized physical promotion without admitting a stale process |
| PostgreSQL system identifier | PGDATA | Query `pg_control_system()` or `pg_controldata` |
| Timeline and timeline history | PGDATA | Query control state and hash history files when needed |
| Primary/standby state | PostgreSQL | Query `pg_is_in_recovery()` and inspect recovery signals while stopped |
| Current and flushed WAL | PostgreSQL | Query native WAL functions |
| Received and replayed WAL | PostgreSQL | Query native recovery functions |
| Database contents | PGDATA | PostgreSQL crash recovery |
| Physical replication slots | PGDATA | Query PostgreSQL catalogs |
| Synchronous configuration readback | PostgreSQL | Query the active native configuration |
| Quorum-certified progress | Generic control-plane receipt | Replay a validated certificate or recertify from fresh peers |
| Process IDs, helper ownership, sessions, and generation leases | Process memory | Recreate |
| Socket, log, backup, restore, and rewind staging | Pod scratch volume | Recreate or discard |

The generic agent may continue using `.kuberic/agent.sqlite3`. That store is not
PostgreSQL application metadata: it is the local durable replica-agent
authority shared by all application types. The PostgreSQL application
replicator accesses it only through typed private runtime protocols; it never
opens, queries, mutates, or interprets the store directly.

## Startup Reconstruction

A PostgreSQL application restart follows this sequence:

1. Start with external SQL access closed.
2. Open and validate the generic agent store.
3. Validate the resource, replica, Pod, PVC, and canonical PGDATA binding.
4. Load admitted epoch, PC/CC configuration, desired role, access intent, and
   pending generic transition effects.
5. Load local receiver-revocation, former-primary, destructive-work, and
   promotion-continuation fences from the generic agent store.
6. Inspect PGDATA without assuming the former process role.
7. Reconcile pre-start fences into managed recovery and connection
   configuration. A revoked receiver must not be allowed to reconnect merely
   because the process restarted.
8. Repair managed PostgreSQL configuration while retaining closed HBA rules.
9. Start PostgreSQL for internal control only when the admitted effect and
   pre-start fences permit it.
10. Query system identifier, timeline, recovery state, WAL positions, slots,
   receiver state, and synchronous-policy readback.
11. Compare observed lineage with the controller-authorized topology and build
   lineage.
12. Reconcile any pending build, rewind, demotion, promotion, or policy effect
    from its externally visible postconditions.
13. Publish fresh progress and application evidence to the generic agent.
14. Grant read or write access only after authority, lineage, policy, quorum,
    and process-session checks pass.

Startup must not infer write authority from PostgreSQL being out of recovery.
Promotion changes PostgreSQL state, but only the generic admitted role and
completed fencing protocol authorize client writes.

### PostgreSQL stopped at startup

When PostgreSQL is stopped, the instance may use `pg_controldata`, signal files,
timeline history, WAL filenames, and controller-owned receipts for preliminary
classification. Values that require a live server are confirmed after a
closed-access startup.

The instance must not publish optimistic progress from an old in-memory
observation. If safe progress cannot be established, it reports an unknown or
conservative boundary and remains closed until reconciliation completes.

### API or agent authority unavailable

PGDATA alone never authorizes activation. If the instance cannot obtain and
validate generic authority:

- PostgreSQL may remain stopped; or
- PostgreSQL may run for internal recovery with external access closed.

It must not serve ordinary SQL or accept a promotion request.

## Historical Acknowledgement Policy

### Why desired policy is insufficient

The policy that is desired now is not necessarily the policy under which the
last acknowledged write completed. A crash can occur while replacing:

- the previous/current configuration;
- the exact eligible standby sessions;
- acknowledgement count `W`;
- eligible set size `N`;
- native `synchronous_standby_names`;
- distributed policy validity.

Survivors must retain the last policy that could have acknowledged writes until
its replacement has been safely established. Missing or restarted members do
not reduce historical `N` or `W`.

The current `synchronous` policy, its generation, and the accepted recovery
policy are therefore additional non-reconstructable safety evidence. They move
to the generic control plane rather than disappearing.

### Policy record

A durable historical policy record binds:

- resource UID and configuration epoch;
- policy generation;
- primary identity and process session;
- exact eligible standby identities and process sessions;
- `N` and `W`;
- compiled native synchronous configuration;
- validity state;
- predecessor policy when a PC/CC transition requires both sets.

The record remains the failover acknowledgement basis until a replacement
policy reaches `Published`.

### Policy transition protocol

Policy replacement uses these durable states:

```text
Accepted
  -> Invalidating
  -> Invalidated
  -> Applied
  -> CatchingUp
  -> BarrierCertified
  -> Published
```

1. The controller admits the desired policy effect.
2. The primary agent durably records `Invalidating`.
3. The controller marks failover-quorum metadata unavailable for this policy
   transition. Automated failover and new write grants are denied until a
   replacement policy is published.
4. The application fences new client writes, drains in-flight synchronous
   commits, and persists a handover certificate covering the highest write
   acknowledged under the old policy.
5. The application invalidates the old local policy and distributes
   invalidation to the exact old eligible sessions.
6. Each reachable responder durably records the invalidation before
   acknowledging it. Unreachable old sessions remain represented in historical
   `N` and `W`; they are not silently removed.
7. The primary agent commits `Invalidated`.
8. The application applies and reads back the new PostgreSQL policy.
9. The primary submits the readback and exact eligible-session evidence to its
   agent.
10. The agent commits `Applied`, then `CatchingUp`.
11. The new policy is distributed to the exact new eligible sessions; each
   responder commits its accepted historical policy before acknowledging.
12. Every new eligible standby must replay through the handover boundary before
    it may contribute to the internal publication barrier.
13. The primary executes an internal barrier write with
    `synchronous_commit=remote_apply` forced by the trusted application path,
    independent of ordinary client session settings. Successful completion
    proves the required `W` new standbys have replayed the complete prior WAL
    prefix as well as the barrier.
14. The primary submits the barrier LSN and exact witness replay evidence. The
    agent commits `BarrierCertified`.
15. The controller publishes the replacement failover-quorum metadata from the
    applied policy and accepted barrier evidence.
16. The primary agent commits `Published`; only then may the generic policy
    effect complete and writes reopen.

The design distinguishes three eligibility levels:

1. **Recovery observer:** may report its actual durable lineage and WAL
   position while externally fenced. It need not have reached the certified
   boundary.
2. **Barrier participant:** has the authorized source, has accepted the
   candidate policy, and has replayed through the handover boundary. PostgreSQL
   may count it for the trusted internal barrier while ordinary client writes
   remain closed.
3. **Ordinary acknowledgement member:** belongs to the published policy and may
   acknowledge ordinary client commits after write access reopens.

`Published` depends on barrier participants, not ordinary acknowledgement
members. This removes the circular dependency while ensuring no ordinary
client write can use the candidate policy before the handover is certified.

Until `Published`, the last policy that may have acknowledged writes remains
historical recovery authority, but automated failover is denied while the
handover is indeterminate. If failure occurs during handover, recovery uses the
old policy and retained handover boundary, or enters quorum loss; it does not
pretend the new policy was published.

This is the Kuberic equivalent of CloudNativePG resetting `FailoverQuorum`
during configuration changes, with the additional guarantee that publication
is conditioned on a WAL catch-up and acknowledgement barrier.

A process restart replays the durable phase; it does not skip directly to the
current desired policy.

## Quorum-Certified Progress

### Why PGDATA is insufficient

PostgreSQL can prove that WAL is locally flushed or replayed. It cannot, after
a restart, prove solely from local PGDATA that a particular primary LSN was
acknowledged under Kuberic's exact synchronous policy.

The current `policy_certified_lsn` fills this gap. It is monotonic local evidence
that an LSN satisfied the configured acknowledgement policy. Replacing it with
the local flush LSN would be unsafe:

```text
local flush >= LSN
```

does not imply:

```text
the required exact standby sessions replayed LSN
```

This value and its historical acknowledgement policy cannot simply be queried
back from PostgreSQL after all relevant sessions have disappeared.

### Three different boundaries

The design distinguishes:

1. **Historical certified progress:** the highest LSN proven replayed by the
   required `W` exact witnesses under a historical published policy.
2. **Failover recovery boundary:** the WAL boundary selected after quorum
   intersection, receiver revocation, and final observations. It determines
   which history may be promoted but is not itself proof that `W` witnesses
   replayed every LSN through that point.
3. **Current write-readiness boundary:** progress certified under the current
   process sessions and active policy before write access opens.

These records are not interchangeable. `R + W > N` proves that the recovery
responder set intersects every possible acknowledgement set. It does not prove
that a chosen LSN was replayed by `W` responders.

### Target certificate

Certified progress becomes a generic control-plane receipt rather than a local
PostgreSQL scalar. A receipt binds:

- resource UID;
- configuration ID and epoch;
- acknowledgement policy generation;
- primary identity and process session;
- certified LSN;
- exact witness identities and process sessions;
- each witness's replay evidence;
- the operation or transition that collected the evidence.

The application computes the candidate certificate using PostgreSQL semantics:
sort exact eligible witness replay positions and choose the `W`-th highest
position, capped by the primary's local flush position. The generic agent
validates the identity, policy, session, and monotonicity envelope and persists
the receipt before acknowledging it. PostgreSQL reports the evidence but does
not own its durable lifecycle.

On restart, locally usable committed progress is:

```text
min(local durable PostgreSQL progress, admitted certified boundary)
```

This minimum describes what the local copy currently contains; it does not
erase the retained certified obligation. If local PGDATA is behind an admitted
certified boundary, the replica must catch up or rebuild before activation.

The receipt remains historical durability evidence after its sessions cease to
be eligible for new writes. It is invalid for a new write grant if its
configuration, epoch, identity, process session, lineage, or policy no longer
matches current authority, but it is not discarded when evaluating recovery of
already acknowledged data.

### Fresh recertification

A missing or stale certificate does not permit substituting local flush. The
instance instead:

1. keeps writes closed;
2. obtains fresh observations from the surviving exact sessions named by the
   historical policy and admitted recovery transition;
3. uses the historical policy and `R + W > N` only to establish safe recovery
   intersection;
4. revokes receivers before final candidate selection;
5. computes a failover recovery boundary from the final responder set;
6. completes recovery or promotion;
7. installs the current policy on fresh sessions;
8. computes a new write-readiness certificate from the `W`-th highest exact
   replay position;
9. persists that certificate before granting writes.

If sufficient witnesses are unavailable, the cluster enters quorum loss rather
than guessing a committed boundary.

For a valid singleton policy, `N = 0` and `W = 0`. Local flush is sufficient
only because the admitted policy explicitly requires no remote acknowledgement.
This is a policy-specific rule, not a fallback used when configured witnesses
are missing.

### Session retirement and reattachment

Process sessions are execution fences, not durable storage identities. A full
process restart can replace every session while preserving every PGDATA volume
and replica identity.

The generic runtime owns a durable reattachment operation that binds:

- resource, replica, Pod, and PVC identity;
- old retired process session;
- new process session;
- admitted epoch and configuration;
- observed PostgreSQL system identifier and timeline;
- local durable WAL/replay position;
- historical policy and certified obligation;
- receiver-revocation, source-fence, and destructive-work state.

Reattachment follows these rules:

1. The old session is durably retired before the new session can contribute
   authoritative callback completion or native acknowledgements. The new
   session may submit fenced recovery observations after retirement.
2. The new session must prove the same admitted storage incarnation and
   compatible PGDATA lineage.
3. Historical `N`, `W`, and certified obligations are retained. Session
   replacement does not reduce them.
4. A reattached standby immediately becomes a **recovery observer** at its
   actual durable position. Lagging storage remains part of `R` when it is
   reachable, compatible, fenced from the old source, and able to participate
   in the exact recovery protocol.
5. Reaching the certified boundary is required for candidate activation, not
   for submitting a recovery observation.
6. A reattached standby becomes a **barrier participant** only after it
   establishes the authorized source, replays through the handover boundary,
   and accepts the candidate policy.
7. It becomes an **ordinary acknowledgement member** only after policy
   publication and write-access reconciliation.
8. An unchanged primary restart is reconstruction, not failover. It starts
   closed, reattaches to its existing PGDATA, reconstructs standbys with fresh
   sessions, completes a policy handover barrier, and obtains a fresh
   write-readiness certificate without promotion.
9. A failover uses whichever historical members have been safely reattached or
   remain live, but still evaluates recovery against the original historical
   `N` and `W`.

Historical policy membership is retained by replica/storage slot as well as by
the process session that last occupied it. An old session leaves eligibility
only through either:

- a durable invalidation acknowledgement from that exact session; or
- durable session retirement proving that the runtime-owned process generation
  cannot resume, followed by reattachment of the same storage slot.

Retirement does not shrink `N` or `W`. A storage-validated reattached session
assumes the same historical member slot for recovery observation at its actual
durable position. Replay through the retained handover or certified boundary is
required only before that slot can be activated or acknowledge new writes.
This allows lagging but useful replicas to contribute to quorum intersection
and a full process restart to reconstruct policy without waiting for dead
sessions to respond.

Fresh recertification after full restart observes the reattached storage slots,
not the retired sessions. Failover quorum still uses the historical member-set
cardinality, while current write readiness uses the newly published exact
sessions.

This follows CloudNativePG's stable instance/PVC identity model while retaining
Kuberic's stronger rule that a dead process session can never return and
complete old work.

### Future simplification

After the failover protocol is proven to derive every safe boundary from fresh
quorum observations, persistent progress certificates may be reduced to
transition receipts. The first migration should retain an explicit generic
certificate because it preserves the current committed-progress contract while
removing the PostgreSQL-specific store.

## Access and Fencing

`external_access_closed` should not be persisted as a Boolean. Closure is an
enforced postcondition:

- managed HBA starts closed;
- ordinary sessions are absent;
- pre-authentication sockets from the former process are gone;
- only the current process generation owns the PostgreSQL process tree;
- service routing does not target an unadmitted instance.

Every restart begins closed, making an interrupted close idempotent. A generic
access effect remains pending until the instance observes the required
postcondition and acknowledges it.

Write access requires all of:

- current exact storage and replica identity;
- admitted primary role and epoch;
- valid current configuration;
- compatible PostgreSQL lineage;
- current synchronous-policy readback;
- valid certified-progress evidence;
- required peer process sessions;
- no pending build, rewind, recovery, removal, or fencing effect;
- successful application-role login after opening HBA.

No persisted local role flag is needed.

## Native Policy Reconstruction

The desired policy belongs to admitted configuration. The historical accepted
policy belongs to durable generic authority. The active native policy is read
from PostgreSQL.

After restart:

1. load the historical policy and its durable transition phase;
2. obtain the desired policy and exact eligible sessions;
3. read PostgreSQL's current synchronous settings;
4. keep writes closed;
5. retire and reattach any replaced process sessions;
6. resume invalidation, application, catch-up, barrier certification, or
   publication from the durable phase;
7. treat unmatched or unowned native settings as invalid;
8. apply the desired policy only after the old policy's invalidation
   requirements have been satisfied;
9. read back the new native policy;
10. distribute it to exact eligible sessions and persist their acceptance;
11. require catch-up through the handover boundary;
12. certify a barrier under the new policy;
13. publish replacement failover metadata and commit `Published`;
14. obtain a current-session write-readiness certificate;
15. acknowledge the policy effect.

Process-generation ownership remains transient. A restarted process can
reconcile a historical policy transition, but it never inherits the
predecessor's right to complete a callback without fresh validation and agent
acknowledgement.

## Build and Catch-Up Recovery

### Build authorization

The controller and generic agent retain durable build intent:

- source and target exact identities;
- build ID;
- authorized lineage;
- frozen copy boundary;
- destructive-work state;
- installed-image receipt;
- required terminal postcondition.

The PostgreSQL application does not retain a private build-stage journal.

### Destructive-work protocol

Lineage and WAL positions do not prove that an interrupted `pg_basebackup` or
in-place `pg_rewind` produced a complete data directory. Before destructive
mutation, the target agent durably records:

- operation and build IDs;
- target PVC and PGDATA identity;
- expected source and lineage;
- mutation kind;
- state `Prepared`.

The application may mutate PGDATA only after the agent acknowledges `Prepared`.
It then advances the generic record through:

```text
Prepared -> Mutating -> Installed -> Validated
```

`Installed` requires the mutation process to have completed, all resulting
files and required directory entries to be durable, and PostgreSQL control data
to be readable. `Validated` additionally requires compatible lineage and replay
at the authorized boundary.

An alternative implementation may build into a separate directory and
atomically install it. It must provide the same durable distinction between the
old image, incomplete staging, and the installed image.

### Restart behavior

An interrupted physical build is handled using the generic work record plus
observable PGDATA:

| Durable state and observation | Action |
|---|---|
| `Prepared` or `Mutating` after restart | Treat PGDATA as non-reusable; clean it through the generation-owned path and perform an authorized fresh backup |
| `Installed` with unreadable or wrong control identity | Reject or perform an authorized fresh backup |
| `Installed` with compatible lineage but behind boundary | Start closed recovery and wait for replay |
| `Installed` at or beyond boundary | Validate and advance to `Validated` |
| No work record but established PGDATA changed unexpectedly | Fail closed as unexplained storage damage |
| Authority or build ID changed | Cancel old work and start only under the new intent |

Temporary backup and rewind directories are disposable. A new pod or process
may repeat `pg_basebackup`. An interrupted in-place rewind is not inferred
complete and falls back to an authorized fresh backup unless the chosen staging
protocol proves atomic installation.

### Source-side state

Physical slots survive in the source's PGDATA and can be enumerated from
PostgreSQL. Their desired ownership derives from current build and membership
intent. Orphan slots are removed only after the controller-authorized consumer
is terminal or retired.

Outbound attempt counts and suspended-build lists do not require a private
store. The controller retries the declared build with bounded policy and a new
process-local attempt.

## Failover Recovery

The controller-owned failover transition records durable intent and accepted
receipts. The PostgreSQL custom replicator executes idempotent observations and
effects.

A restart during failover repeats the current level-triggered phase:

1. close external access;
2. load the admitted configuration and failover intent;
3. collect fresh exact-session PostgreSQL observations;
4. verify quorum intersection and compatible lineage;
5. durably install responder-local receiver-revocation records before changing
   receiver configuration;
6. remove old-source connection settings, drain receivers, restart closed, and
   acknowledge revocation only after the receiver cannot reconnect;
7. durably install the former-primary/source fence where applicable;
8. collect the final observation round from the same exact responder sessions;
9. persist the selected candidate, original process session/generation,
   historical policy, responders, recovery boundary, expected lineage
   transition, and source-fence evidence in the generic transition;
10. persist the candidate-local promotion intent and wait for its agent commit;
11. promote only that exact candidate;
12. verify promoted lineage and checkpoint completion;
13. persist the physical-promotion result before acknowledging the effect;
14. admit a current policy and fresh process session separately before opening
    writes.

The selected candidate, responder set, source fence, and promotion boundary are
transition authority. They must not be recoverable only from a private file on
the candidate.

### Receiver revocation

Receiver revocation is local agent authority and is consulted before PostgreSQL
startup. It binds the old source, receiver identity, process session, epoch,
historical policy, and failover operation.

It is cleared only by an exact authorized follow, rebuild, or terminal
transition that names the permitted source and lineage. Pod or process restart
does not clear it. Consequently, a responder cannot reconnect to the old source
and begin acknowledging writes while another candidate is being promoted.

### Interrupted promotion

Recognition that a prior physical promotion occurred is distinct from
authorizing the restarted process to serve. The physical result is recognized
from:

- generic promotion intent;
- unchanged exact candidate identity;
- original promoting process session and generation;
- unchanged accepted historical policy;
- exact final responders and their sessions;
- admitted recovery boundary and source fence;
- PostgreSQL no longer being in recovery;
- compatible system identity and the expected promoted timeline transition;
- closed external access.

The restarted process may finish checkpointing or publish the previously
authorized physical result after fresh validation. It does not inherit the old
session's service authority. The agent/controller must separately admit the new
session, current policy, certified progress, and write grant.

Without all of those historical and fresh facts, an unexpected primary-looking
PGDATA remains fenced.

## Planned Switchover Recovery

The existing planned-switchover protocol retains controller/agent ownership of:

- source and target identities;
- catch-up boundaries;
- access-revocation completion;
- source stopped receipt;
- durable former-primary fence;
- target promotion intent;
- terminal handoff receipt.

PostgreSQL proves checkpoints, replay, shutdown, and promotion by fresh
observation. Before source shutdown, the source agent commits a former-primary
fence bound to the switchover operation and target. That fence is checked before
every subsequent PostgreSQL startup and is cleared only by an authorized
follow/rebuild transition.

A restarted source sees the generic effect and local fence and reconciles to
stopped or the exact authorized standby state. A restarted target does not
promote from a local role flag: it requires the admitted promotion effect,
durably accepted source-stopped evidence, and the same interrupted-promotion
rules used by failover.

## Scratch Storage

The PostgreSQL pod should mount a dedicated scratch volume, using `emptyDir` by
default and optionally a Kubernetes generic ephemeral volume.

Suggested layout:

```text
/controller/
  run/                 # Unix sockets and pidfd-adjacent runtime files
  log/                 # PostgreSQL and supervisor logs
  backup/              # pg_basebackup staging
  rewind/              # pg_rewind staging and transient source data
  recovery/            # temporary recovery files
  certificates/        # generated or copied runtime certificates
  wal-spool/           # optional archive/restore staging
```

The volume is not a recovery authority. Its loss may cause work to repeat but
must not change the admitted topology, certified boundary, or PGDATA lineage.

Moving Unix sockets out of `PGDATA/pg_stat_tmp` also removes the need for
backup/rewind exclusions that exist only for live runtime files.

## Kubernetes and Agent Data Model

The controller and local agent have distinct durable roles.

### Controller authority

`KubericSet.status` is authoritative for cluster-wide intent and accepted
transition decisions:

- admitted topology, epochs, and PC/CC configuration;
- failover or switchover operation ID;
- historical acknowledgement policy selected for recovery;
- required responder set;
- selected promotion candidate and recovery boundary;
- build source, target, lineage, and frozen boundary;
- transition completion and terminal receipts.

The controller uses optimistic concurrency and operation IDs. It does not
directly validate PostgreSQL file formats or WAL semantics.

### Local agent authority

The existing local generic agent store is the linearization authority for
per-replica facts that must survive before an irreversible local action:

- provisional authority admission;
- accepted historical policy and policy-transition phase;
- process-session retirement and reattachment;
- receiver revocation;
- former-primary/source fence;
- destructive-work intent and installation state;
- candidate-local promotion intent and physical-result receipt;
- certified-progress evidence accepted from the application;
- effective access closure and effect acknowledgement.

The application may proceed past a required durability boundary only after the
agent has committed the corresponding record and acknowledged that commit. An
agent commit whose response is lost is retried by operation ID and returns the
same accepted result.

### Provisional authority admission

The current custom-replicator path invokes configuration callbacks before
publishing final admitted authority. PostgreSQL callbacks may need to durably
fence writes, invalidate a policy, or prepare receiver state before they can
return. Evidence cannot be accepted under the old authority, but final authority
cannot be activated before the callback succeeds.

The runtime resolves this ordering with a durable provisional-admission
capability:

- it is created by the generic agent before invoking the configuration
  callback;
- it binds the exact proposed authority, operation ID, previous authority,
  callback kind, and process generation;
- it permits only preparation evidence required by that callback;
- it cannot grant read/write access, publish generic progress, complete a
  build, promote a replica, or become current authority;
- application evidence submitted under it is stored by the generic agent, not
  by the application replicator;
- callback success allows the agent to atomically promote the exact provisional
  record to admitted authority and publish the prepared evidence;
- callback failure or cancellation leaves access fenced and either retries the
  same operation or runs an explicitly authorized compensation path;
- restart restores the provisional record and resumes or compensates it before
  admitting unrelated authority.

This mirrors CloudNativePG's declarative ordering: desired cluster intent is
durable before the instance manager reconciles PostgreSQL, while effective
primary/readiness status is published only after reconciliation succeeds.

### Provisional-operation continuation

A provisional capability is an execution lease for one process generation,
not a transferable durable credential. After a crash, the replacement process
must not reuse the predecessor's capability, even though the provisional
operation and its evidence remain durable.

The generic runtime performs an explicit continuation transition:

1. durably retire the predecessor process session and fence its PostgreSQL
   process tree and replication connections;
2. load the nonterminal provisional operation by operation ID;
3. verify that its proposed authority, previous authority, callback kind,
   storage incarnation and immutable parameters still match controller intent;
4. attach fresh PostgreSQL observations to the existing pre-action and
   completion evidence without rewriting that evidence;
5. issue a new execution capability bound to the successor process generation,
   the same operation ID and only the remaining legal phase;
6. permit the successor to revalidate postconditions and either finish the
   exact operation or enter its typed compensation transition;
7. revoke the continuation capability when the operation becomes terminal.

Historical evidence survives generation replacement; execution rights do not.
The successor cannot broaden the proposed authority, repeat an irreversible
phase whose committed postcondition is already present, or submit evidence for
another operation. If retirement of the predecessor or ownership of its
PostgreSQL process tree cannot be proven, no successor capability is issued
and access remains closed.

Compensation uses the same rule. The agent durably changes the provisional
operation to a specific compensation phase and issues a generation-bound
capability for only that phase. A lost acknowledgement is resolved by replaying
the same operation ID and returning the already committed phase; it never
revives the predecessor capability.

### Private evidence protocol

The public `Replicator` and `PrimaryReplicator` callbacks remain unchanged. The
private hosting boundary gains a typed PostgreSQL evidence protocol:

```text
application observes or prepares evidence
    -> submit typed evidence to local agent
    -> agent validates admitted authority or an exact provisional capability
    -> agent durably commits or returns the existing idempotent result
    -> application receives commit acknowledgement
    -> application performs the authorized irreversible action
    -> application submits observed completion evidence
    -> agent durably commits completion
    -> callback may return success
```

PostgreSQL-specific code remains responsible for validating lineage, replay,
native policy, checkpoint, receiver, and promotion semantics. The generic agent
validates operation identity, admitted authority, exact sessions, monotonicity,
bounded representation, and stale-generation rejection.

A successful generic callback receipt alone is too late for pre-action
fencing. The private protocol supplies the required mid-callback durable
acceptance without exposing agent SQLite to the custom replicator.

Preparation evidence accepted under a provisional capability is invisible to
ordinary access and progress decisions until that exact provisional admission
is promoted. Evidence from a failed or superseded proposal remains fenced and
cannot be reinterpreted under a later authority.

### Status publication

The agent reports accepted local evidence to the controller. Controller status
publication can lag local safety commits:

- an unreported local fence remains effective;
- the controller does not treat the transition as globally advanced until the
  corresponding status update is accepted;
- replay after controller restart republishes the same operation-bound record;
- stale status cannot clear a newer local fence.

Clearing a local fence requires a newer controller-authorized operation and a
new local agent commit. Merely observing desired status or losing API
connectivity cannot clear it.

New PostgreSQL-specific durable data must be limited to evidence that the
generic controller or local agent needs to make a safe transition decision.

Potential additions are:

- a bounded certified-progress receipt;
- a bounded historical acknowledgement-policy record;
- receiver-revocation and former-primary fence records;
- destructive-work and installed-image receipts;
- PostgreSQL lineage attached to a build or promotion receipt;
- exact witness replay evidence attached to a failover decision;
- observed PGDATA identity attached to instance status.

All records are bounded and operation-scoped. Large timeline histories, WAL
payloads, backup manifests, full peer transcripts, private workflow cursors, or
unbounded build attempts remain outside both stores.

### Commit ordering

The required ordering is:

1. controller commits cluster intent when the decision is cluster-wide;
2. local agent commits an exact provisional-admission capability;
3. application callback prepares PostgreSQL under that capability;
4. local agent commits pre-action evidence or fence;
5. application performs the authorized irreversible PostgreSQL action;
6. local agent commits completion evidence;
7. callback returns success;
8. local agent promotes the provisional record to admitted authority;
9. controller accepts the reported completion and advances global intent.

If any required commit is unavailable, the action does not advance. If the
action completed but its completion acknowledgement was lost, restart
recognizes it only through the committed pre-action record plus fresh
PostgreSQL observation; it never infers authorization from PGDATA alone.

If restart occurs between any steps, the old process capability is retired and
the agent performs the provisional-operation continuation protocol before
reissuing only the remaining step to the successor generation.

Final authority publication and access activation remain separate. Promoting a
provisional record does not itself open SQL access; ordinary proof-before-
publication access reconciliation still applies.

## Failure Semantics

### Process crash

- PGDATA remains authoritative.
- Scratch state is discarded.
- The agent replays desired effects.
- The agent retires the old process session and durably reattaches the new one
  to the same validated storage incarnation.
- A nonterminal provisional operation receives a new generation-bound
  continuation capability only after predecessor retirement and postcondition
  revalidation.
- PostgreSQL is observed and reconciled from a new process generation.
- SQL remains closed until the new generation is admitted.

### Full cluster process restart

- Historical policy, `N`, `W`, and certified obligations remain unchanged.
- Every old process session is retired.
- Each new process session reattaches through its own local agent.
- The unchanged primary is reconstructed from its PGDATA; it is not promoted.
- Standbys reconnect and replay through the retained handover/certified
  boundary.
- Failover metadata remains unavailable and writes remain closed while policy
  publication is reconstructed.
- A new policy barrier and current-session certificate are required before
  writes reopen.

### Pod replacement with the same PVC

- Kubernetes and agent identity validation must authorize the attachment.
- The new pod receives a new process session.
- Old peer sessions and callback completions are rejected.
- PGDATA is inspected; application metadata is not required.

### PVC replacement

- The new PVC UID creates a new storage incarnation.
- Old PGDATA evidence and old certificates cannot authorize it.
- It must initialize or complete an authorized build.

### Controller restart

- `KubericSet.status` and Kubernetes resources reconstruct transition intent.
- The controller re-observes instances.
- Local pre-action fences remain effective and are republished by operation ID.
- No globally completed transition relies only on an unreported local receipt.

### Kubernetes API partition

- An already effective write grant may continue only while its local agent
  authority remains readable, the current process generation remains owned,
  native policy remains valid, and no local revocation or fence is installed.
- A restart cannot reacquire write authority from PGDATA alone.
- Promotion and new access grants require current authority.
- Local safety fences and receiver revocations continue to apply.
- Effects requiring a new cluster-wide decision remain pending.

### Agent-store failure

- Failure to read accepted authority prevents startup activation.
- Failure to commit a required pre-action record prevents the irreversible
  action.
- Failure to commit completion keeps the callback pending or failed even if
  PostgreSQL appears to have completed the action.
- A running writable instance whose agent store becomes unreadable or
  unwritable closes writes and reports a fault; it does not continue
  indefinitely on cached memory.
- Lost commit acknowledgement is resolved by an idempotent operation-ID retry.

### Status commit and acknowledgement races

- If the controller commits intent but delivery acknowledgement is lost, it
  redelivers the same operation.
- If the agent commits a local fence but its acknowledgement is lost, the
  application retries and receives the same committed result.
- If PostgreSQL completes an action but completion was not committed, restart
  uses the pre-action record and fresh observation to finish or remain fenced.
- If agent completion commits but controller status publication is lost, the
  agent republishes it; the local fence remains until the controller advances a
  newer authorized operation.

### Provisional-admission failure

- Failure before the provisional capability commits invokes no application
  callback.
- Failure after provisional commit but before PostgreSQL mutation restores the
  same preparation on retry.
- Failure after mutation uses the provisional pre-action record and fresh
  PostgreSQL observation to resume or compensate.
- Restart never transfers the predecessor's execution capability. It issues a
  successor capability for the same immutable operation and remaining phase
  only after durable predecessor retirement.
- A failed proposal cannot grant access or publish progress.
- Unrelated authority cannot be admitted until the provisional record is
  terminal or explicitly superseded by a typed compensation transition.

### Corrupt PGDATA

- PostgreSQL startup or lineage validation fails explicitly.
- The absence of a metadata file does not permit automatic reinitialization.
- Recovery requires an authorized rebuild or operator action.

### Lost scratch volume

- Sockets, logs, and temporary operations are recreated.
- Incomplete backup, rewind, or spool work restarts.
- No committed data or authority is lost.

### Lost generic authority

- PGDATA remains intact but fenced.
- The instance cannot infer topology or primary authority.
- Recovery requires restoring or reconstructing controller/agent state under a
  separately defined disaster-recovery procedure.

## Safety Invariants

1. The PostgreSQL application replicator persists no state. All of its
   process-local state may be discarded without losing durable authority or
   safety evidence.
2. Only PGDATA and Kuberic runtime-owned generic stores may contain durable
   state used to reconstruct the PostgreSQL application replicator.
3. PGDATA alone never grants primary or write authority.
4. Kubernetes or agent authority alone never proves PostgreSQL lineage.
5. Local flush LSN never substitutes for quorum-certified progress.
6. Historical policy is retained until its replacement is durably published.
7. Replacement policy publication requires catch-up through the old-policy
   handover boundary and a barrier acknowledged under the new policy.
8. Failover metadata is unavailable while synchronous policy is indeterminate.
9. `R + W > N` establishes recovery intersection, not an LSN certificate.
10. Process-session replacement retains historical `N`, `W`, and certified
    obligations.
11. Every write grant is bound to current identity, epoch, configuration,
   process generation, native policy, peer sessions, and current-session
   certified progress.
12. Every restart begins externally closed.
13. Receiver revocation and former-primary fences are enforced before
   PostgreSQL startup.
14. A stale process or delayed callback cannot publish progress or completion
   for a successor generation.
15. Interrupted destructive work is never inferred complete from lineage and
    WAL position alone.
16. A build completion requires an installed-image receipt, compatible lineage,
    and replay at or beyond the authorized boundary.
17. A promotion requires the exact admitted candidate, original promotion
    intent, final quorum evidence, and expected lineage transition.
18. Recognizing a historical physical promotion does not grant the restarted
    process service authority.
19. An unexpected primary-looking PGDATA is fenced until reconciled.
20. Disposable scratch loss may repeat work but cannot change durable
    authority.
21. No PostgreSQL-specific application store duplicates controller or agent
    transition state.
22. Required agent persistence is acknowledged before irreversible local
    actions.
23. Provisional authority can prepare but can never activate a replica.
24. Missing evidence fails closed rather than being synthesized.

## Migration Plan

### Phase 1: Classify and observe

1. Introduce a reconstruction view that reads all derivable lineage, role,
   process, and WAL fields directly from PostgreSQL.
2. Compare reconstructed values with `PgDurableState` in tests and diagnostics.
3. Identify every decision that still depends exclusively on local metadata.
4. Classify historical policy, certified progress, receiver revocation,
   destructive-work provenance, and promotion continuation as separate
   evidence types.
5. Define the private application-to-agent evidence protocol.
6. Define provisional admission and process-session reattachment.

The existing file remains authoritative during this phase.

### Phase 2: Move workflow authority

1. Add typed controller records for recovery intent, selected candidates,
   responder sets, historical policy, and final boundaries.
2. Add local agent records for receiver revocation, former-primary fencing,
   destructive work, installed images, and promotion continuation.
3. Move build authorization and terminal evidence entirely into generic build
   intent and receipts.
4. Make policy invalidation, application, and publication a replayable generic
   effect with durable phases.
5. Add the policy handover boundary, catch-up requirement, and new-policy
   barrier before `Published`.
6. Add exact process-session retirement and storage-bound reattachment.
7. Add provisional authority admission before configuration callbacks.
8. Stop restoring application-private workflow cursors only after each cursor's
   safety fact has a replacement record.

### Phase 3: Externalize certified progress

1. Add separate bounded records for historical certified progress, failover
   recovery boundaries, and current write-readiness certificates.
2. Persist certificates only after exact policy and replay validation.
3. Reconstruct local progress from PGDATA without discarding a higher retained
   certified obligation.
4. Define singleton `N = 0`, `W = 0` behavior explicitly.
5. Exercise primary, standby, quorum-loss, session replacement, and
   total-process-restart cases.

### Phase 4: Restart-stateless application

1. Start every application process externally closed.
2. Reconstruct solely from PGDATA and generic authority.
3. Gate PostgreSQL startup on local receiver/source/destructive-work fences.
4. Stop reading `state-v2.json`.
5. Retain shadow comparison for one development cycle if useful, without
   allowing the shadow file to authorize behavior.

### Phase 5: Remove the metadata root

1. Remove `PgDurableStore`, its schema, locks, commit workers, and fault paths.
2. Remove `--application-root` and `KUBERIC_APPLICATION_ROOT`.
3. Remove the `postgres-application` storage binding.
4. Add the scratch-volume layout.
5. Update deployment, backup, restore, and operational documentation.

This project permits breaking API changes, so the first supported release may
require a fresh deployment rather than an in-place metadata conversion.

## Compatibility

The migration changes private PostgreSQL storage and recovery behavior. It does
not require changes to:

- `Replicator`;
- `PrimaryReplicator`;
- `StateProvider`;
- PostgreSQL physical data format;
- PostgreSQL client protocol.

Private PostgreSQL coordination messages and typed generic transition evidence
may change. Mixed old/new PostgreSQL application versions should remain
unsupported unless a separate compatibility design is added.

Backups should contain PGDATA and the cluster's generic control-plane backup.
They should not require a separately coordinated application metadata file.

## Testing Strategy

### Reconstruction equivalence

- Compare PGDATA-derived identity, timeline, role, and WAL progress with the
  current durable metadata across primary and standby states.
- Cover stopped PostgreSQL, crash recovery, promotion, rewind, and timeline
  changes.
- Verify that stale metadata is unnecessary for a healthy PGDATA restart.

### Certified progress

- Crash through every historical-policy phase.
- Verify missing and restarted peers do not reduce historical `N` or `W`.
- Verify failover metadata is unavailable throughout policy handover.
- Require every new policy member to replay through the handover boundary.
- Crash before and after the new-policy barrier acknowledgement.
- Lose the old primary and old witnesses immediately after `Published` and
  prove the new policy preserves the prior acknowledged prefix.
- Crash before and after quorum evidence is persisted.
- Restart primary and witnesses with changed process sessions.
- Reject local-flush-only progress.
- Reject certificates from old epochs, policies, identities, or sessions.
- Distinguish quorum intersection from the `W`-th-highest replay certificate.
- Preserve a certified obligation when local PGDATA is behind it.
- Cover valid singleton restart without inventing remote witnesses.
- Enter quorum loss when witnesses are insufficient.

### Session reattachment

- Restart one standby without changing its PVC or replica identity.
- Restart the primary without promotion.
- Restart every process simultaneously while retaining all PGDATA and agent
  stores.
- Verify old sessions are retired and cannot complete callbacks.
- Restart with a nonterminal provisional operation and verify the old
  capability is rejected while a successor capability is bound to the same
  operation ID and immutable proposed authority.
- Verify the successor may perform only the remaining phase and cannot repeat
  a completed irreversible action.
- Verify lagging reattached sessions can submit fenced recovery observations at
  their actual durable positions.
- Verify sessions cannot become barrier participants before reaching the
  handover boundary and accepting the candidate policy.
- Verify barrier participants can acknowledge the internal publication barrier
  while ordinary client writes remain fenced.
- Verify sessions cannot acknowledge ordinary client writes before policy
  publication and access reconciliation.
- Verify historical `N`, `W`, and certified obligations do not shrink.
- Distinguish unchanged-primary reconstruction from failover election.

### Access fencing

- Kill the application during access grant and closure.
- Restart with PostgreSQL still running.
- Verify managed HBA begins closed and old sessions cannot regain access.
- Verify an unexpected promoted data directory stays fenced.

### Builds

- Interrupt PGDATA clearing, `pg_basebackup`, rewind, receiver startup, and
  replay waits.
- Crash before and after every destructive-work and installation commit.
- Treat interrupted in-place rewind as non-reusable unless atomic installation
  is proven.
- Restart from PGDATA observation plus the generic installation record rather
  than a private stage cursor.
- Reject superseded build IDs and source sessions.
- Reuse compatible completed PGDATA only after full terminal validation.

### Failover and switchover

- Restart every participant after each externally visible transition phase.
- Restart the controller after intent persistence but before effect delivery.
- Restart responders after durable receiver revocation and verify they cannot
  reconnect to the old source.
- Restart the selected target immediately before and after promotion.
- Crash after physical promotion but before local completion commit.
- Replace the promoting process session before service admission.
- Verify only the admitted exact candidate can activate.
- Verify source-stopped and final-boundary evidence survives without local
  application metadata.

### Persistence ordering

- Lose acknowledgements after controller intent commits.
- Lose acknowledgements after local pre-action fence commits.
- Complete a PostgreSQL action before its completion record commits.
- Commit local completion while controller status publication fails.
- Make the agent store unreadable or unwritable while writes are open and
  verify fail-closed behavior.
- Replay every operation ID and verify idempotent acceptance.
- Crash before and after provisional-admission persistence.
- Cancel or fail a configuration callback after preparation and after native
  policy mutation.
- Verify provisional evidence cannot grant access or publish progress.
- Restart with a nonterminal provisional record and verify exact retry or typed
  compensation.
- Lose continuation-capability acknowledgements and verify idempotent replay
  never revives predecessor execution rights.

### Storage

- Delete scratch storage at every operation boundary.
- Replace Pod identity while retaining the PVC.
- Replace the PVC under the same replica ordinal.
- Corrupt or partially remove PGDATA and verify fail-closed behavior.
- Confirm no code recreates `state-v2.json` after final migration.

Routine PostgreSQL validation should use:

```sh
just nextest-postgres-smoke
```

The full `just nextest-postgres` suite remains reserved for explicit requests.

## Observability

Instance status should distinguish:

- PGDATA observed and compatible;
- waiting for generic authority;
- waiting for policy reconciliation;
- waiting for certified progress;
- rebuilding;
- rewinding;
- replaying to an admitted boundary;
- fenced due to lineage mismatch;
- quorum loss;
- safe for read access;
- safe for write access.

Diagnostics should report the source of every boundary:

- local PostgreSQL flush;
- local replay;
- admitted certified receipt;
- fresh quorum recertification;
- build or switchover boundary.

Operators must be able to tell whether an instance is unavailable because of
database recovery, missing control-plane authority, stale peer sessions, or
insufficient quorum evidence.

## Open Decisions

1. Whether a stopped primary must report a committed boundary immediately or
   may report progress pending until peers recertify it.
2. Which PostgreSQL observations should be persisted in status for diagnostics
   without becoming authority.
3. Whether build retry reuses a logical build ID with a new process attempt or
   always creates a new build ID.
4. Whether generic ephemeral PVC scratch should be supported in addition to
   `emptyDir` in the first deployment implementation.
5. What disaster-recovery procedure reconstructs generic authority when PGDATA
   survives but Kubernetes and agent state are both lost.
6. Whether builds use always-fresh backup after any interrupted mutation or an
   atomic staged-install implementation for selected storage backends.
7. Whether the handover barrier is implemented as a dedicated internal
   transaction, a checkpoint-plus-probe operation, or another PostgreSQL-native
   write that cannot be bypassed by ordinary client settings.

## Related Documentation

- [PostgreSQL: Native Replication on Kuberic V2](design.md)
- [CloudNativePG Architecture](../../background/cloudnative-pg-architecture.md)
- [Stateless Default Replicator](../kuberic/stateless-default-replicator.md)
- [Service Fabric Alignment and Simplification](../kuberic/service-fabric-alignment.md)
- [Service Fabric State Management and Persistence](../../background/service-fabric/state-management.md)
