# ADR: Agent-Owned Replica Remove Protocol

> **Status:** Implemented
> **Date:** 2026-09-02

## Problem

Replica removal was durable and configuration-first, but the operator still
persisted and dispatched every primary configuration, quorum, connection, and
target runtime action. That made the FM analogue a remote runtime step
sequencer and tied restart recovery to a per-step CRD cursor.

Service Fabric separates these responsibilities. FM removes a replica from
configuration and later sends `RemoveReplica` to the current primary RA, while
the primary RA owns exact held-replica and replicator removal
(`/data/code/service-fabric/src/prod/src/Reliability/Failover/fm/ReconfigurationTask.cpp:334-430`,
`/data/code/service-fabric/src/prod/src/Reliability/Failover/fm/PendingTask.cpp:112-160`,
`/data/code/service-fabric/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp:2044-2140`).
SF separately sends `DeleteReplica` from FM to the target RA
(`/data/code/service-fabric/src/prod/src/Reliability/Failover/fm/SendMessageAction.cpp:174-216`).

## Decision

Use this production flow:

```
operator / CRD durable authority
  → one RemoveReplicaIntent v1 over correlated control v3
  → exact current primary ReplicaAgent
      → reduced CatchUp configuration
      → tracked write-quorum wait
      → reduced Current configuration
      → exact target-incarnation connection removal
      → ReplicaLifecyclePeer v2 Retire stage v1
           → exact target ReplicaAgent
           → PodRuntime ChangeRole(None) then Close
  → operator records commit, fences labels, deletes exact UID, publishes stable topology
```

The operator owns desired topology, mode authorization, epoch, durable intent,
attempt redrive, commit recognition, Kubernetes lifecycle, and stable
publication. The primary agent owns only transient membership-removal
coordination. The target agent owns target-local retirement admission.
`PodRuntime` owns ordered effects only.

CRD status is the sole durable global authority. Agent action and peer ledgers
are bounded, generation-qualified, and volatile. There is no durable
agent-local workflow or exactly-once guarantee.

## Protocols and Versions

- `ReplicatorControl` exposes only `GetStatus` and
  `ExecuteCorrelatedControlAction`.
- Required correlated control protocol: **3**.
- `RemoveReplicaIntent` protocol: **1**.
- `ReplicaLifecyclePeer` protocol: **2**.
- Remove `Retire` stage semantic version: **1**.
- Durable add operation version: **3**.
- Durable remove operation version: **2**.

The intent signs operation/action/attempt identity, mode, epoch, exact primary
and target identities, primary generation/control version/endpoints, optional
all-or-none target peer authority, previous and reduced structural
configurations, quorum/minimum constraints, pod UID, and deadlines.

The peer request signs operation/attempt/message identity, sender and parent
authority, exact target generation/control version, epoch, full configuration
fence, removal mode, commit timestamp, retirement expiry, and the exact
reduced Current projection.

There is no old peer alias, version fallback, mixed-version support, or
per-step removal compatibility path. Operators must quiesce add/rebuild
and removal work before deploying the coordinated operator/runtime clean
break.

## Commit, Compensation, and Publication

Exact observation of the frozen reduced Current configuration is the
irreversible membership commit. Before that point, the primary may restore the
previous Current configuration and report `Compensated`. After it, neither
agent nor operator may restore the removed member.

The primary marks current-install dispatch before the effect. Once that flag
is observable, only exact reduced Current proves commit and permits
roll-forward. Previous Current, reduced CatchUp, invalid, or unavailable
configuration remains ambiguous because a timed-out queued effect may still
commit later. None authorizes compensation or a previous-configuration effect
without explicit tracked-effect cancellation or quiescence proof; the
operator poisons the operation.

The operator first persists `removeCommitEvidence` and the exact reduced
`committedSnapshot`. That snapshot is scoped to the active workflow:
`stableSnapshot` remains the previous topology until exact connection absence,
a terminal retirement observation, exact-UID `role=retired` label fencing, and
exact-UID pod deletion are durable. Final publication then installs the
reduced stable snapshot. Missing `GetStatus` from an exact still-present
primary Pod/incarnation is never treated as connection absence. Cleanup waits
without a deadline until that exact primary reports the target connection
absent; only primary process absence or replacement proves that the old
process-local connection is gone. The retirement deadline can degrade target
peer cleanup, but cannot waive this connection barrier.

## ScaleDown and Force

Both modes use identical configuration, quorum, `minReplicas`, commit,
connection-cleanup, and publication safety.

- `ScaleDown` requires exact target reachability and lifecycle-peer v2 before
  the operation is persisted. Preadmission records that exact target agent
  generation, and intent freeze must observe the same generation; a same-UID
  process restart cannot cross the preadmission/freeze boundary. Later loss
  never silently changes the mode to `Force`; safe commit can continue, with
  retirement expiry producing `CommittedDegraded`.
- `Force` makes target retirement best effort. If exact peer authority was
  frozen, the primary retries observation-first until expiry. Without frozen
  peer authority, it may report degraded retirement after commit and exact
  connection cleanup.

`Force` is not a quorum bypass and does not weaken target admission.

## Deadlines and Attempts

Each frozen attempt carries a 600-second overall deadline, 10-second runtime
or peer call bounds, a 30-second compensation grace, a compensation cap at
overall deadline plus 30 seconds, and a 60-second target-retirement budget.
Retirement expires at `min(commit time + 60, overall deadline)`.

One durable removal permits at most three pre-commit attempts, including the
initial attempt. No new attempt is frozen after current-install dispatch is
observable. Before dispatch, a new attempt may be frozen when a terminated
same-generation attempt leaves exact reduced CatchUp installed, or when a new
primary generation exposes exact previous Current or reduced CatchUp.
The deadline and compensation cap are per attempt, not shared across the
operation: one pre-commit attempt can consume 600 + 30 = 630 seconds, so three
attempts can consume up to 3 × 630 = 1,890 seconds before a pre-commit terminal
disposition. Post-commit cleanup is not attempt-capped, and exact connection
cleanup remains unbounded if its positive absence evidence is unavailable.

## Restart and Poison Handling

The operator redrives from CRD intent plus exact runtime, connection, peer,
pod, and label postconditions. Missing bounded history is never proof that an
effect did not execute.

Three operator dispositions pin removal in `Poisoned`:

- `FailedPreCommitIncomplete`: commit is known not to be proven after the
  deadline or three attempts, while a known pre-commit state remains.
- `InvalidRemovalState`: live evidence is structurally impossible, or commit
  state is unobservable after reduced-current installation was dispatched.
- `AmbiguousPrimaryRestart`: a complete same-Pod primary process restart
  erased configuration and coordinator evidence before durable commit
  recognition.

The last two forbid rollback, assumed commit, retry, failover handoff,
cleanup, and publication. After durable commit evidence, restart recovery is
roll-forward only. Retained-member Pod identity churn is therefore not passed
through the pre-commit fail-closed identity gate; cleanup and publication
continue from the committed snapshot while later health reconciliation repairs
the churned retained member.

Target generation or incarnation changes make old peer completion stale.
Kubernetes label and delete operations use the frozen UID, so a same-name
replacement is untouched. Label updates also test the observed Kubernetes
resource version and replace a merged label map, preserving unrelated labels
without weakening the UID fence.

## Lifecycle Peer Scope

`ReplicaLifecyclePeer` shares strict identity, signature, duplicate replay,
status, and transport primitives between:

- add/build `Prepare`, `Activate`, and `Cleanup`; and
- removal `Retire`.

This does not merge the workflows and does not expose a generic remote runtime
action API. Add/build and removal retain independent intents, coordinators,
commit rules, and terminal results.

## Rejected Alternatives

- Keep the operator-side per-step removal cursor.
- Add a second durable workflow store inside `ReplicaAgent`.
- Treat missing action history as proof of non-execution.
- Roll back to previous membership after ambiguous reduced-current dispatch.
- Make `Force` bypass quorum or exact target epoch/authority checks.
- Expose arbitrary peer runtime actions.
- Preserve aliases, migrate old operations, or support mixed protocol
  versions.

## Service Fabric Alignment and Differences

Kuberic matches SF's responsibility split: manager-owned topology and redrive,
primary-RA-owned exact connection removal, target-RA-owned local retirement,
and runtime-proxy-owned ordered effects. SF also keeps message-stage flags
transient while persisting stable failover-unit metadata
(`/data/code/service-fabric/src/prod/src/Reliability/Failover/ra/FailoverUnit.h:566-578`,
`/data/code/service-fabric/src/prod/src/Reliability/Failover/ra/Replica.h:202-257`).

The main transport difference is deliberate: SF uses direct FM→target
`DeleteReplica`; Kuberic uses primary-agent→target-agent `Retire`, then leaves
physical pod deletion with the operator.

SF force deletion can use `Obliterate`, bypassing the normal target
configuration-epoch rejection
(`/data/code/service-fabric/src/prod/src/Reliability/Failover/ra/ReconfigurationAgent.cpp:780-834`).
Kuberic `Force` has no equivalent target epoch bypass. A reachable target must
still validate exact sender, parent, target, generation, epoch, committed
reduced configuration, and signed deadline.

Kuberic also has no durable local RA failover-unit store, general RA-to-RA
reconfiguration framework, or exactly-once ledger.

## Consequences

- Production removal dispatches one operator mutation to the primary and none
  directly to the target runtime.
- Status remains responsive during quorum and peer waits.
- Exact reduced Current cleanly separates compensation from roll-forward.
- Same-ID replacement connections and same-name replacement pods are
  protected by incarnation and UID fences.
- Poisoned ambiguous commit states require external administrative recovery;
  that recovery protocol is not implemented.
- Primary removal remains out of scope.

Coarse agent-owned switchover is the next candidate. Its local
reconfiguration sequence is still operator-sequenced and should move behind a
separate coarse intent rather than being folded into removal.
