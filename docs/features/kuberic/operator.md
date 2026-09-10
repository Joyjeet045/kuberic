# Kuberic: Operator Design

The Kuberic operator acts as SF's Failover Manager on Kubernetes.
It watches `KubericSet` CRDs and orchestrates pod lifecycle, failover,
switchover, and scaling through CRD-backed durable workflows.

> Part of the [Kuberic Design](../kuberic-replicator-design.md).
> Failure scenarios documented in [operator-failure-scenarios.md](../operator-failure-scenarios.md).

---

## CRD

**`KubericSet`** with:

**Spec fields:**
- `spec.replicas`, `spec.minReplicas`
- `spec.image`, `spec.storage`, `spec.pvcRetentionPolicy`
- `spec.failoverDelay`, `spec.switchoverDelay`
- client, control, and data ports

**Status fields:**
- `epoch`, `currentPrimary`, `targetPrimary`, `phase`
- `reconfigurationPhase`
- optional authoritative `stableSnapshot` with epoch, primary logical ID,
  complete member logical/incarnation identities and roles, write quorum, and
  optional last-known election progress/deactivation metadata
- optional compact versioned `operation` checkpoint for durable creation,
  switchover, replica add/rebuild, and configuration-first replica removal
- structured add-replica attempt with frozen primary/target generations,
  endpoints, configuration descriptors, semantic build key, deadlines, and
  commit observation
- structured remove-replica attempt with exact primary/target authority,
  previous/reduced configuration descriptors, three-attempt and deadline
  bounds, bounded coordinator phase/result, exact commit evidence,
  post-commit cleanup, and typed poison disposition
- Phase-1 failover/data-loss recovery, including optional
  previous/committed topology, target snapshot, one pending correlated action,
  failover observations/assessment/epoch intents, and optional pod-local
  generation/control-version/runtime-epoch fences and a frozen action payload
- optional `stableElectionMetadataRefresh` checkpoint for topology-scoped,
  write-ahead runtime configuration recording and live progress publication
- `primaryFailingSince`
- per-member stable replica ID and replica incarnation (Kubernetes Pod UID)
- `conditions`

**Reconciliation:** `PartitionDriver` performs read-only stable recovery with
`GrpcReplicaHandle`; CRD-backed state machines own every mutation. A stable
operation is complete only after its resulting snapshot is persisted. Durable
creation persists partial committed bootstrap topology after primary-only and
each expanded current configuration, so process loss rolls forward from live
committed authority instead of replaying `Open(New)`.

Reconciler-driven creation, switchover, replica add/rebuild, replica removal,
and failover persist a versioned operation checkpoint, reconstruct fresh
handles and observations on every reconcile, and advance one status transition
or one mutating activity at a time. Bounded read observations may precede
either, but a mutation and status patch never share a reconcile. Pending action
intent is durable before RPC, pod-label, or UID-fenced pod-delete mutation.
Status patches include the observed Kubernetes `resourceVersion`; no lock is
held across a durable activity.

---

## Reconciler Phases

| Phase | Description |
|-------|-------------|
| `Pending` | CRD created, no pods yet |
| `Creating` | Durable initial bootstrap, partial topology commit, and routing publication |
| `Healthy` | Normal operation — monitors health, handles scale, detects failures |
| `FailingOver` | Primary failed, running failover protocol (incl. data loss path) |
| `Switchover` | Planned primary change in progress |
| `AddingReplica` | Durable scale-up or stale-secondary replacement in progress |
| `RemovingReplica` | Durable healthy scale-down or stale/dead-secondary eviction in progress |

Quorum and best-candidate waits are persisted failover sub-phases rather than
top-level CRD phases. `DurableOperation` conditions use
`WaitingForBestCandidate` or `QuorumLoss`. Both wait indefinitely while
missing evidence could change the safe result; elapsed time never promotes a
lesser replica or authorizes data loss.

---

## Healthy Phase Responsibilities

The Healthy phase runs multiple checks on each reconciliation:

```
Healthy phase:
  │
  ├─ 1. Primary health check
  │    Pod ready? gRPC reachable?
  │    → If not: enforce failover delay → FailingOver
  │
  ├─ 2. Durable Phase-1 failover
  │    collect progress/incarnation/epoch/role/config/deactivation
  │    → validate previous/current read quorum
  │    → wait, confirm target, or negotiate data loss
  │
  ├─ 3. Switchover detection
  │    targetPrimary != currentPrimary?
  │    → If yes: → Switchover
  │
  ├─ 4. Secondary health check
  │    Ready replacement incarnation? → durable rebuild
  │    Old incarnation unreachable with retained quorum? → durable force-remove
  │
  ├─ 5. Missing pod detection
  │    driver.replica_ids() vs list_pods()
  │    → If replacement is ready: rebuild; otherwise durable force-remove
  │
  ├─ 6. Scale reconciliation
  │    spec.replicas vs current count
  │    → Scale up: create pods + add_replica
  │    → Scale down: durable configuration-first removal
  │
  ├─ 7. Stable election metadata refresh
  │    topology-scoped correlated config record + exact live progress
  │
  ├─ 8. Node drain detection
  │    Any pod's node unschedulable?
  │    → If primary: switchover to healthy node
  │
  ├─ 9. Multi-primary detection
  │    GetStatus on all replicas
  │    → If multiple primaries: close stale one
  │
  └─ 10. Condition updates
       Set Ready/Degraded/QuorumAvailable conditions
```

---

## gRPC Failure Tracking

Per-replica `grpc_failure_count` persisted in CRD status. Incremented
on Unavailable/DeadlineExceeded, reset on success. When count reaches
`spec.grpcFailureThreshold` (default 3), treat replica as unreachable
even if pod shows Ready.

### Replica Incarnations

The stable replica ID is derived from the pod ordinal and survives pod
recreation. Each concrete pod uses its Kubernetes UID as
`ReplicaInstanceId`. Runtime status, driver handles, replica-set
configurations, and CRD member status carry that incarnation.

Health reconciliation compares the runtime-reported incarnation with the
handle's expected pod UID. A ready replacement enters coarse add/rebuild,
whose primary coordinator retires the exact old connection before building
the new incarnation. A missing or unreachable old incarnation enters coarse
`Force` removal. In both paths, delayed cleanup for the old
`(ReplicaId, ReplicaInstanceId)` cannot remove a replacement connection.

### ReplicaAgent Dispatch Boundary

Every control request reaches a pod-local `ReplicaAgent` before
`PodRuntime`. The agent owns local admission, correlation, serialization and
bounded completion replay; the runtime owns ordered service/replicator
effects. This is intentionally narrower than Service Fabric RA: CRD status and
the operator remain the only owners of distributed workflow state.

Before a pending runtime action is dispatched, reconciliation requires
replica-agent control protocol version 3 and exact agreement among the
addressed, runtime, and pending Pod incarnations. It persists the observed
agent generation, agent control version, and runtime epoch.
Direct non-add/non-remove actions also freeze their exact encoded payload so
observation and retry signatures cannot drift with live progress. Add/rebuild
and removal use structured `operation.addIntent` and
`operation.removeIntent` as their payload authority.

Missing, malformed, or unsupported agent status fails closed. There is no
capability negotiation or old-peer fallback.

This fence write is a separate reconciliation step and does not consume or
reset the action attempt/deadline budget. The next reconcile reconstructs the
deterministic action and uses `ExecuteCorrelatedControlAction`. A stale
precondition or unavailable-continuity rejection itself executes no effect, so
the advisory fences are cleared and re-observed without consuming an attempt;
the agent makes no claim about whether an older unretained action executed.
Other errors remain counted. The only production mutation call is
`ExecuteCorrelatedControlAction`.

This is a coordinated deployment boundary: quiesce durable topology work and
deploy the operator and replica runtimes together.

---

## Operator Restart Recovery

On the first `Healthy` reconcile after process restart, the operator requires
`status.stableSnapshot`, derives logical IDs from required pod-index labels,
and creates handles from current pod UIDs and addresses. `PartitionDriver::
recover()` calls only `GetStatus`, then requires an exact logical/incarnation
bijection, epoch and stable-role agreement, one primary, complete membership,
and the persisted majority write quorum.

The snapshot is authoritative. `currentPrimary` remains compatibility output
and is refreshed from recovered driver state; it is never recovery input.
Legacy resources without a snapshot fail closed. A missing or inconsistent
stable primary routes directly into durable failover before driver recovery;
non-primary incarnation changes are handled by topology reconciliation or the
phase-specific failover fence. Durable `Creating`, `Switchover`,
`AddingReplica`, `RemovingReplica`, and `FailingOver` resume from
`status.operation`. Completed topology snapshots are refreshed with exact
election metadata before they are used as unavailable-candidate comparison
evidence. See `operator-failure-scenarios.md` §8.

A container restart can keep the Pod UID while resetting role, epoch and all
agent/runtime process-local state. Status exposes a new `AgentGeneration`, so
the new process cannot replay or impersonate the previous process's action.
On restart recovery, a stable secondary with the same Pod UID but unverifiable
runtime role/epoch is persisted into the established durable
force-remove/rebuild path before mutation. A stale primary enters durable
failover. Missing prior-generation local state is never proof that an
ambiguous effect did not run.

## Durable Partition Creation

All initial pods are created with `kuberic.io/role=bootstrap`, which does not
match the primary or secondary client Services. The durable operation first
fences any pre-existing initial serving labels back to `bootstrap`. After every
desired pod is ready, the operator validates pod-index-derived logical IDs and
UIDs, sorts by logical ID, selects the lowest ID as initial primary, and
persists a `CreatePartition` operation before runtime mutation.

The operation explicitly has no previous stable topology. It records the full
target and an optional committed bootstrap snapshot. Primary-only current
configuration is committed and checkpointed first. Each secondary then runs
Open(New), epoch update, idle role, build, active role, catch-up configuration,
write-quorum wait, and current configuration; the expanded partial topology is
persisted before the next secondary starts.

Controller replacement observes runtime postconditions and correlated
activity state. Committed members are never reopened or rebuilt. Failure
before the first commit cleans up and restarts from no topology; later failures
preserve committed members and clean up only the current candidate. Final
routing labels are published one member per reconcile only after the complete
target (and therefore `minReplicas`) is durably committed.

The fencing iterator addresses the exact member at its persisted index, not
the current runtime candidate field. Fence failure never schedules
RemoveReplica, demotion, close, or deletion. Same-incarnation unavailability
fails closed; an uncommitted replacement restarts with a new persisted target
UID while retaining committed members. All compensation phases reject a target
already present in the committed bootstrap snapshot.

## Durable Switchover

The internal durable layer separates pure observation/decision logic from
side-effecting activities. Its compact checkpoint pins operation version,
source and target stable snapshots, phase, frozen LSN, retry/deadline/error
metadata, and at most one pending action. Each action identifies the exact
replica ID, pod-UID incarnation, expected epoch, and desired postcondition.

`GetStatus` exposes write access, canonical configuration state,
`current_action`, and bounded `retained_terminal_actions`. Lost replies
therefore resume from the authoritative local ledger and runtime
postconditions rather than blind RPC repetition. Target-promotion failure can
durably restore the old primary; impossible or stale observations poison the
operation without publishing a new stable snapshot.

---

## Scale-Up Reconciliation

When `spec.replicas > current stable member count`:
1. Create new Pod (with ownership labels)
2. Wait for Pod Ready
3. Observe exact primary/target identities, agent generations, control/data
   endpoints, peer version, epoch, quorum, and `minReplicas`
4. Persist one structured add attempt and one coarse pending
   `AddReplicaIntent`
5. Dispatch only to the current primary ReplicaAgent
6. Observe primary coordinator phase, target peer/runtime postconditions, and
   current-configuration commit
7. Label the candidate and publish the target stable snapshot after exact
   attestation

The primary agent owns target Prepare, tracked copy, target Activate, catch-up
configuration, write-quorum wait, current configuration, and compensation.
The operator never sends a production mutation to the add target.
Prepare, Activate, and compensation Cleanup use the internal
`ReplicaLifecyclePeer` v2 service.

The structured attempt is the semantic source of truth. It freezes structural
configuration and derives the target progress from the acknowledged copy LSN.
No per-step add cursor or encoded add action payload is retained.
Copy and quorum wait are tracked asynchronously by `PodRuntime`; status remains
available and exact duplicate copy is suppressed only while target generation
continuity still matches.
Ordinary control effects retain the default 30-second reply bound; a direct
durable `BuildReplica` effect retains the 10-minute copy window used by
unchanged workflows while remaining observable through `GetStatus`.

Stale-secondary replacement uses the same operation after first removing the
old exact `(ReplicaId, ReplicaInstanceId)` connection from the primary. That
retirement is now primary-agent-owned and frozen together with the replacement
identity. The previous stable snapshot remains unchanged until the
replacement's current configuration commits.

Before commit, `Compensated` requires observed previous current configuration
and exact target-connection absence; otherwise status becomes poisoned and the
operator does not delete the target. After commit, recovery is roll-forward
only. If final serving attestation cannot be restored, the operator removes any
target serving label, publishes the proven topology with a
`CommittedDegraded` condition, and lets existing recovery repair it.

---

## Scale-Down Reconciliation

When `spec.replicas` is lower than stable membership, the operator selects the
highest-ID stable secondary. It never removes a primary; switchover must make
that replica a secondary first.

For healthy `ScaleDown`, the operator first requires an exact target
generation, endpoint, and lifecycle-peer v2 observation. If that pre-admission
fails, the set remains `Healthy`, no operation is persisted, and
`ScaleDownTargetUnavailable` is published without repeatedly rewriting
unchanged status. The stale/dead/missing-secondary health paths separately
authorize `Force`.

The operator then:

1. validates `minReplicas` and retained previous-write-quorum safety;
2. persists remove operation v2 with the previous and frozen reduced
   snapshots, exact target incarnation/pod UID, mode, deadlines, and a maximum
   of three pre-commit attempts;
3. freezes one generation-qualified `RemoveReplicaIntent` v1 and dispatches it
   only to the exact current primary through correlated control v3;
4. observes bounded primary coordinator evidence until the exact reduced
   Current configuration commits, compensation succeeds, redrive is safe, or
   the operation poisons;
5. persists the primary's exact commit timestamp, configuration signature, and
   the reduced workflow-scoped `committedSnapshot` before global cleanup;
6. fences the old pod's role label to `retired`, deletes only the frozen UID,
   and publishes the reduced `stableSnapshot`.

The primary `ReplicaAgent` owns the transient sequence:

```
reduced CatchUp + previous configuration
  → tracked WaitForCatchUpQuorum(Write)
  → reduced Current
  → remove exact old-incarnation connection
  → ReplicaLifecyclePeer Retire
  → final attestation
```

The target agent admits `Retire` only after validating exact sender, parent
action, target incarnation/generation, epoch, committed reduced projection,
and signed expiry. `PodRuntime` performs `ChangeRole(None)` and `Close` in
order. The operator never sends those production removal actions directly to
the target.

Exact reduced Current is the irreversible commit. Before commit, failure may
restore previous Current. After current-install dispatch, rollback requires
positive exact configuration evidence; ambiguous state poisons instead of
guessing. After commit, every path rolls forward and never republishes the
removed member.

`ScaleDown` and `Force` have identical quorum, minimum, commit, connection, and
publication safety. Only target retirement differs: `ScaleDown` starts with
frozen reachable peer authority, while `Force` permits missing authority and
degraded post-commit retirement. Neither mode weakens exact target admission
or quorum.

Primary progress is volatile and bounded; CRD status is the durable authority.
A new primary-agent generation may receive a new pre-commit attempt only when
exact previous Current or reduced CatchUp survives, up to three attempts.
The three terminal operator dispositions are:

- `FailedPreCommitIncomplete` for a known pre-commit state after deadline or
  attempt exhaustion;
- `InvalidRemovalState` for structurally impossible evidence or ambiguity
  after reduced-current dispatch; and
- `AmbiguousPrimaryRestart` when a complete same-Pod primary restart erases
  all commit-boundary evidence.

All pin the operation in `Poisoned`; their evidence and recovery meaning are
distinct. Post-commit cleanup is not attempt-capped.

**Selection heuristic:** Prefer the secondary with the highest replica ID
(newest). CNPG uses the same approach. SF uses PLB load balancing which
is more sophisticated but unnecessary for our initial implementation.

Removal freezes a 600-second overall deadline, 10-second call bounds,
30-second compensation grace (capped at overall plus 30), and a 60-second
post-commit retirement budget capped by the overall deadline. Status remains
responsive because quorum and peer work is tracked asynchronously. One
reconfiguration runs at a time.
