# Level-Triggered Kuberic Operator

The level-triggered stack is an independent, experimental implementation of
Kuberic stateful replication. It combines Kubernetes-style reconciliation
with Service Fabric-shaped replica, reconfiguration-agent, and replicator
boundaries.

> **Status:** implemented for development and CI validation, not production
> use. The controller and `kvstore2` images are not published release
> artifacts.

The production v2 controller configuration enables sequential scale-up. This
means the normal controller binary runs the implemented path; it does not
change the stack's experimental release status.

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

Before creating a canonical scale-up PVC, the controller durably authorizes
scaffolding for one allocation operation. The PVC is created with the
`operator.kuberic.io/scale-up-allocation-operation` annotation set to that
exact operation ID. Exact-name observation must return the same creation
provenance before the evaluator freezes the PVC UID, requests Pod creation, or
grants exact cleanup authority. A same-name PVC with missing or mismatched
provenance is a name collision: it is never adopted or deleted, and a fresh
allocation attempt waits until the canonical name is available.

Raw application progress is repair evidence only. Protocol version 3
introduced a separate authority-bound `verifiedReplicationLsn`; protocol
version 4 additionally binds control commands to the exact observed target
process session. Version 5 added accepted-spec-generation fencing for preparation
and retirement. Protocol version 6 added secondary removal, independent
previous/reduced policies, and durable preparation, acceptance, and retirement
evidence. Protocol version 7 added sequential scale-up with exact allocation,
build, admission, completion, and cleanup authority. **Protocol version 8**
adds epoch-fenced carried-failover election and persists its exact final safe
LSN in the completion receipt. **Protocol version 9** adds the service-created
replicator's advertised endpoint to agent status; all
level-triggered components must use that exact version.
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

`spec.replicas` must be positive. Lowering it after initialization requests
[secondary scale-down](#secondary-scale-down); increasing it requests
[sequential scale-up](#sequential-scale-up). The desired count is both target
and minimum by Kuberic policy. `spec.image` selects the application image.
`spec.failoverDelaySeconds` defaults to 30.

Status is controller-owned durable authority. Users must not author or repair
it manually. Important projections include:

- `initialized` and `observedGeneration`;
- `effectivePolicy`, frozen while authority is active;
- the last quorum-attested `topology`;
- at most one active `transition` or fresh `provisioning` intent;
- `primaryFailure` and same-configuration `quorumLoss`;
- `transition.switchover` for a frozen active request and handoff;
- `lastSwitchover` for the latest terminal receipt (not a history);
- `transition.secondaryScaleDown` and `transition.secondaryRemovalEvidence`
  for one frozen removal and its authority evidence;
- `secondaryScaleDownCleanup` after reduced membership commits, then
  `lastSecondaryRemoval` for bounded retained-member convergence, not deletion;
- `scaleUpAllocation` before resource creation, then scale-up `provisioning`
  and `transition.scaleUp` during build/admission;
- `scaleUpCleanup` for one exact failed or cancelled unadmitted candidate,
  then `lastScaleUp` for bounded post-commit convergence, including the exact
  final fenced failover-safe LSN when primary failure carried admission;
- `pendingReplacementCleanup`, moved unchanged to `lastReplacement` at
  replacement acceptance, for one exact cleanup obligation;
- conditions describing waiting or unsafe observations.

The checked-in CRD is generated from the Rust type and verified byte-for-byte
by the `kuberic-controller` Rust tests.
At the final-review baseline it is **344,907 bytes**, leaving **5,093 bytes** before
the restored strict-below-350,000-byte regression guard fails. Representative
serialized scale-up status guards cover 2, 3, 4, 6, 10, and 18 members; the
largest current 18-member sample is carried failover at **26,353 bytes**.
These are schema/status growth guards, not a supported-cardinality or
Kubernetes object-limit claim. No validated maximum replica-count budget exists.

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
- deterministic secondary scale-down, including 3→2, 2→1, sequential 5→2,
  and unavailable-target removal with sufficient old-read/new-write evidence;
- sequential scale-up, including one-at-a-time multi-count increases,
  restoration of a missing ordinal with a fresh incarnation, writes during
  healthy copy/catch-up, cancellation, exact retry cleanup, and pre/post-PC/CC
  failover recovery;
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
- primary removal, explicitly selected removal targets, or
  cancellation/retargeting of an active removal;
- timed dropping of unavailable replicas;
- PVC authority rebinding to a new Pod UID;
- automatic rolling image/spec upgrades;
- mixed-version protocol negotiation;
- publication of the controller and `kvstore2` images as release artifacts.

The generated model validates authority observations, single-writer
admissibility, and switchover recovery traces. Runtime race and subprocess
crash tests cover the handoff's durable boundaries; the live matrix exercises
bounded writes and retained TCP clients through restart, partition, replacement,
failover, switchover, and scale-down. Scale-down also has a focused successful-write
oracle and generated interruption traces. A broader arbitrary concurrent
successful-write history generator remains deferred.

Quorum loss does not advance the data-loss number and does not call an
application data-loss callback. Unsupported evidence produces `Wait` or
`Unsafe`, never success-shaped recovery.

## Sequential Scale-Up

This is **Service Fabric-inspired replica-add semantics**, not general Service
Fabric placement or scaling parity. The normal v2 controller enables the
feature, while classic `kuberic.io/v1` reconciliation remains unchanged.

Increase only `spec.replicas` on a stable initialized set. For example, after a
completed 3→2 secondary scale-down:

```bash
kubectl --kubeconfig "$KUBECONFIG" --context "$KUBE_CONTEXT" \
  patch kubericsets.operator.kuberic.io kvstore2 --type=merge \
  -p '{"spec":{"replicas":3}}'
```

For a larger increase, patch directly to the final desired count. The
controller still admits one member at a time; a 1→3 request completes 1→2
before allocating 2→3. A successful patch is not admission or readiness.
Inspect desired count, accepted topology/policy, the exact candidate attempt,
conditions, cleanup, and `Ready` together.
There is no validated maximum replica-count budget.

The next candidate is the first missing positive logical ID through accepted
count plus one. Normal growth therefore uses the next ordinal; scaling down and
back up restores the missing ordinal. The logical ID and canonical Pod/PVC names
may recur, but every attempt has a fresh allocation operation, PVC UID, Pod
UID/replica instance, durable generation, process session, endpoint, build ID,
and admission identity.

### Provisioning, copy, and admission

The controller persists `scaleUpAllocation` before resource creation. It proves
the canonical name absent, creates the PVC with
`operator.kuberic.io/scale-up-allocation-operation`, freezes the exact PVC UID,
revalidates that live PVC, and only then creates the Pod bound to it. A
same-name PVC or Pod with missing or mismatched allocation provenance is never
adopted or deleted.

The exact accepted primary supplies the build. The candidate remains
`IdleSecondary`, outside accepted membership and without quorum credit while it
is merely allocated, Ready, copying, or catching up. Snapshot enumeration uses
an immutable snapshot boundary. When enumeration ends, the source freezes a
separate post-enumeration `catchUpBoundaryLsn`; source and target must agree on
it, and the target must durably apply the snapshot plus every contiguous
operation through that boundary.

Admission then installs expanded previous/current authority. PC uses the prior
policy and CC uses the majority policy for the expanded membership. The exact
primary is unchanged in the healthy path. Writes may continue during allocation,
copy, catch-up, and PC/CC only while the accepted primary remains authorized and
both configurations satisfy their independent write-quorum requirements. There
is no zero-downtime, completion-time, or outage guarantee.

Expanded current-only authority is irreversible. Stable completion additionally
requires exact local current-only acceptance by the primary and candidate, the
candidate's `ActiveSecondary` role, expanded current write quorum including the
primary, write grant when quorum is available, and exact write-Service routing.
If current-only membership commits before candidate-local acceptance, status is
`ScaleUpCommittedDegraded`: the new member is accepted, pre-admission cleanup is
forbidden, and local convergence or ordinary accepted-member repair continues.

### Cancellation, failover, and cleanup

If desired count returns to accepted count before the first PC/CC command, the
unadmitted candidate is cancelled and exactly cleaned. After the durable
admission fence, a newer reduction does not roll back scale-up; the accepted
transition finishes before the latest desired count is evaluated.

Accepted-primary failure before admission invalidates the old-primary build,
allows ordinary failover, then cleans the exact candidate before a fresh retry.
Failure after PC/CC begins preserves the typed scale-up authority and its
original previous/expanded recovery evidence. The controller first persists a
provisional newer-epoch primary, installs that epoch write-closed, and waits
for fresh deactivation/progress reports that independently satisfy both read
quorums. It then persists a still-newer final primary and exact safe LSN from
that fenced evidence. A replica holding a later acknowledged tail wins the
final election; the provisional primary cannot repair peers from a shorter
prefix and become writable. The failed original primary is not a required
participant; if it returns, bounded receipt correction moves it to final
authority afterward. Final PC/CC catch-up and activation must certify
the exact safe LSN before current-only grants writes. A failed or disappeared
unadmitted candidate is never resumed from its partial PVC.

A completed prior addition never interprets the exact pending build or
retirement of a newer cancellation-eligible attempt as historical work. The
newer build is durably cancelled/retired first; unrelated pending work remains
blocked, and any genuine prior-receipt correction remains available afterward.

Cleanup is peer endpoint → Pod → PVC. Deletes use frozen UIDs and fresh resource
versions; PVC deletion waits for authoritative exact Pod absence. List omission
is not absence, and same-name/different-UID resources survive. After membership
commit, candidate loss is degraded accepted membership and cannot be handled by
failed-candidate cleanup.

| Condition reason | Inspect or wait for |
|---|---|
| `ScaleUpBlockedByHigherPriorityWork`, `ScaleUpAwaitingStableAuthority` | Existing repair, cleanup, accepted-member convergence, or unstable authority |
| `ScaleUpAllocationPvcPending`, `ScaleUpAllocationNameCollision`, `ScaleUpAllocationPodBindingLost` | Exact allocation provenance, canonical-name availability, and PVC-before-Pod identity |
| `ScaleUpCopying`, `ScaleUpBoundaryPending`, `ScaleUpCatchUpPending` | Exact build reports and durable receiver progress through the frozen boundary |
| `ScaleUpDualQuorumUnavailable`, `ScaleUpDualQuorumPending` | Independent PC/CC quorum, caught-up candidate, and writable primary evidence |
| `ScaleUpCommittedDegraded`, `ScaleUpCurrentOnlyQuorumPending` | Accepted expanded authority and remaining exact local/current-only convergence |
| `ScaleUpEndpointCleanupPending`, `ScaleUpPodCleanupPending`, `ScaleUpPvcCleanupPending` | Ordered exact cleanup and authoritative absence |
| `ScaleUpFailoverArbitrationPending`, `ScaleUpDualQuorumEvidencePending` | Pre-admission ordinary failover or original post-admission carried recovery evidence |
| `ScaleUpFencedPreviousQuorumPending`, `ScaleUpFencedCurrentQuorumPending`, `ScaleUpFencedDualQuorumPending` | Fresh write-closed election-epoch evidence independently missing for PC, CC, or both |
| `ScaleUpFencedCandidatePending`, `ScaleUpFinalPrimaryCatchupPending` | Final primary selection or exact fenced safe-prefix catch-up remains incomplete |

Scale-up condition messages include accepted and desired counts, exact target,
operation attempt, phase, and blocking class. `ScaleUpStable` is the terminal
Ready reason. Status retains at most one allocation/active intent, one cleanup
obligation, and one latest receipt. A later addition waits if a retained member
still needs the prior receipt for exact local acceptance.

## Secondary Scale-Down

This is SF-inspired secondary scale-down using PC/CC quorum principles, with
Kuberic-specific target/minimum coupling, deterministic selection, write closure,
sequential cleanup, and Kubernetes resource deletion. It is not general SF
scaling parity.

Lower only `spec.replicas` on an initialized set. The desired count is both the
**target and minimum**, with a floor of **one**. This `spec.replicas` target=min
coupling is a **Kuberic policy choice**, not general SF semantics: SF target and
minimum are independently configurable. There is no independent `minReplicas` or quorum
setting. The controller selects the **highest logical-ID committed secondary**,
not the highest Pod name or the highest reachable member. The exact primary,
retained incarnations, and data-loss number stay unchanged.

For an existing three-member `kvstore2`, preserve its image and other settings:

```bash
# 3 -> 2: remove one committed secondary.
kubectl --kubeconfig "$KUBECONFIG" --context "$KUBE_CONTEXT" \
  patch kubericsets.operator.kuberic.io kvstore2 --type=merge \
  -p '{"spec":{"replicas":2}}'
```

The corresponding desired-state sample is:

```yaml
apiVersion: operator.kuberic.io/v1alpha1
kind: KubericSet
metadata:
  name: kvstore2
spec:
  replicas: 2
  image: localhost/kvstore2:level-triggered-v1
  failoverDelaySeconds: 10
```

Wait for accepted two-member topology, completed cleanup, and Ready before
requesting a singleton:

```bash
# 2 -> 1: deliberately give up redundancy.
kubectl --kubeconfig "$KUBECONFIG" --context "$KUBE_CONTEXT" \
  patch kubericsets.operator.kuberic.io kvstore2 --type=merge \
  -p '{"spec":{"replicas":1}}'
kubectl --kubeconfig "$KUBECONFIG" --context "$KUBE_CONTEXT" \
  get kubericsets.operator.kuberic.io kvstore2 -o yaml
```

A successful patch is not removal completion; an old Ready condition can remain
visible until reconciliation. Inspect accepted topology, `observedGeneration`,
active intent, cleanup, and conditions together.

| Request or condition | Behavior |
|---|---|
| Lower positive count | One frozen highest-ID secondary removal at a time |
| 5→2 | 5→4, 4→3, 3→2, each committing and cleaning up before the next |
| Count equals accepted membership | No new removal |
| Retained read quorum unavailable before admission | Preserve existing routing/access; wait without a transition, preparation, replacement, or alternate target |
| Unavailable selected secondary | Same target; proceed only with retained evidence and frozen or reconstructable exact cleanup identity |
| Missing primary or insufficient evidence | Wait fail-closed; no alternate target or automatic failover inside the frozen removal |
| Count below one | Rejected |
| Scale-up | Sequential one-at-a-time restoration/addition |
| Primary/explicit-target removal | Unsupported |
| Changed desired count during removal | Queued for fresh evaluation after cleanup; not cancellation or retargeting |

**Target=min risk:** lowering the desired count also lowers the minimum,
including permission to remove an unavailable secondary when safe evidence
exists. A two-member set needs both members for ordinary write quorum; a
singleton has no replica redundancy or alternate failover primary. Sequential
scale-up can restore redundancy after a completed reduction.

### Authority and cleanup

**Retained-quorum preflight:** before freezing intent, removing routing, or closing
writes, the exact accepted members excluding the deterministic highest-ID target
must currently provide the previous configuration's read quorum. Credited members
must report stable accepted current-only authority, completed local work, and
fresh exact sessions. Otherwise `ScaleDownRetainedReadQuorumUnavailable` preserves
existing writable service and re-observes with bounded requeues: no transition,
primary preparation, retained-member replacement, or alternate target selection.
For `{P1,S2-down,S3-up}` requesting 3→2, P1+S3 continue serving under accepted
authority; after S2 heals, admission still selects S3. For 2→1 the retained
primary alone supplies the previous read quorum of one. An unavailable selected
target does not block admission when retained quorum exists.

This preflight is availability screening, not durable admission evidence and
not a guarantee against a member failing immediately afterward. Removal then
freezes the desired generation/count, exact primary/target, full PC/CC,
independent policies, and endpoint/Pod/PVC identities. Routing is removed;
durable primary preparation closes client access and records a verified boundary
covering acknowledged writes. Retained members must supply a previous-configuration
read quorum under accepted authority. Write-closed PC/CC convergence then needs
the reduced configuration's write quorum, including the unchanged primary,
through that boundary. Fresh completed current-only write-quorum evidence
precedes atomic publication of reduced topology, policy, and cleanup.

This old-read/new-write rule permits 2→1 with the primary alone. It does not
weaken ordinary PC/CC replication's dual-write-quorum rule: removal permits no
PC/CC client writes. Local commit acceptance, write regrant, and exact-primary
routing follow separately and can restore service while cleanup remains pending.

Only after membership commit does cleanup delete the frozen peer endpoint,
retire the exact target, delete its Pod, and finally delete its PVC. Reachable
retirement proves role `None`, closed application, and fenced client/peer access.
An unreachable target is **not** proven retired: post-commit deletion of its
exact Pod supplies the final local fence. The same post-commit fence applies to
a reachable exact target whose positively observed older authority cannot admit
the exact retirement command; this does not fabricate a retirement receipt.
Authoritative exact-name observation
must prove that Pod UID absent before PVC deletion. Deletes use frozen UIDs and
fresh resource versions; label-list omission or RPC failure is not absence.
Same-name/different-UID resources are preserved, including after cleanup ends.

**Cleanup-provenance limitation:** exact original PVC provenance must be
reconstructable before intent admission. If the Pod and PVC already disappeared
before the request and their exact mapping/generation cannot be reconstructed,
scale-down waits/fails closed (`ScaleDownExactIdentityRequired`); list omission
is not absence. “Unavailable target supported” applies only once exact cleanup
identity is frozen or reconstructable, not to arbitrary pre-admission resource
loss. Durable per-member provenance is a [deferred follow-up](../../proposal/v1-retirement-plan.md#deferred-scale-down-follow-ups).

> **PVC object deletion:** there is no retention option, storage import, or
> recovery path for removed storage. This differs from classic v1 scale-down.
> Deleting the PVC object is not a claim of physical storage erasure; backend
> reclaim behavior is separate.

During convergence, `status.transition.kind` is `secondaryScaleDown`.
After commit, `status.topology` and `effectivePolicy` are reduced while
`secondaryScaleDownCleanup` retains the immutable evidence and retirement
obligation. Cleanup waits normally show `Ready=unknown`, `Progressing=true`;
routed writes can already be available. Completion replaces cleanup with
`lastSecondaryRemoval`, a single retained convergence proof, **not** deletion
authority or a history. Before superseding this receipt, **every retained
member** needs its original completed current-only witness in the receipt or
fresh proof of completed local acceptance. A late non-witness can delay the next
reduction even when quorum arithmetic alone would permit it.

| Condition reason | Inspect or wait for |
|---|---|
| `ScaleDownRetainedReadQuorumUnavailable` | Pre-admission retained read quorum; existing routing/access stay unchanged |
| `ScaleDownExactIdentityRequired` | Reconstructable exact original Pod/PVC/endpoint cleanup provenance |
| `ScaleDownPreparationPending` | Exact primary's durable write closure |
| `ScaleDownPreviousReadQuorumUnavailable` | Retained accepted-epoch read evidence |
| `ScaleDownReducedWriteQuorumUnavailable`, `ScaleDownReducedCatchUpPending` | Reduced quorum and verified prepared prefix |
| `ScaleDownCurrentOnlyQuorumUnavailable` | Completed current-only evidence |
| `ScaleDownPrimaryUnavailable` | Recovery of the frozen exact primary |
| `ScaleDownRetirementPending` | Target-local retirement after commit |
| `ScaleDownExactPodFencePending`, `ScaleDownExactPodAbsenceRequired` | Exact Pod fence or authoritative absence |
| `ScaleDownCleanupPending` | Endpoint/Pod/PVC reads, finalizers, and deletion retries |
| `ScaleDownRetainedMemberPending` | Late retained-member convergence before the next removal |
| `SpecDriftUnsupported` | Replica-count change combined with unsupported image/delay drift |

No next removal, provisioning, replacement, failover, or switchover starts
while cleanup remains. Existing authority work finishes first; stable primary
safety handling and a fresh explicit switchover take precedence over admitting
a removal. Replacement similarly freezes `pendingReplacementCleanup` before
provisioning and moves it to `lastReplacement` at commit; exact cleanup completes
before another operation can overwrite that obligation.

### Availability and restart

Writes may return HTTP 503 and connections may close. Reconnect through the write
Service with bounded backoff; a lost reply remains ambiguous and requires
application-level idempotence. There is no zero-downtime or maximum interruption
guarantee. RPC deadlines, requeue intervals, and `failoverDelaySeconds` are not
scale-down deadlines. A frozen-primary failure deliberately blocks progress
rather than changing this operation into failover or primary removal, including
after membership commit while cleanup is pending. This can cause an **indefinite
outage**; recovery requires that exact primary. Sequential cleanup also blocks
otherwise unrelated failover. These are Kuberic availability limitations, not
general SF behavior. Independent target/minimum policy, placement-aware selection,
overlapping recovery, multi-member removal, and durable primary-agent phase
scheduling are [deferred](../../proposal/v1-retirement-plan.md#deferred-scale-down-follow-ups).
Scale-up is sequential and restores the first missing positive logical ordinal
outside accepted authority before allocating a new highest ordinal.

Protocol 9 and agent store schema 5 require a **fresh coordinated deployment**;
protocol 8 and earlier are rejected, as are older schemas, with no migration or
mixed-version mode.
Schema 3 persists scale-up build and admission authority in addition to the
schema-2 initialization provenance and admitted policies.
Carried failover preserves the original PC/expanded-CC recovery witnesses and
the exact provisional write-closed configuration. Final activation additionally
requires a separate post-fence certificate with independently sufficient PC/CC
read quorums, exact report sessions and progress, the selected final primary,
final epoch, and safe prefix. The certificate stores one typed witness record per
replica and quorum membership by replica ID; exact incarnations, roles,
configuration IDs, resource/attempt/build bindings, and the deterministic fence
operation are validated against the frozen intent and provisional authority.
The final configuration and safe prefix are reconstructed canonically from that
authority and the selected witness. The bounded completion receipt retains the
original recovery proof plus only the provisional-primary reference needed to
reconstruct the provisional configuration, so an exact returning provisional
member is corrected to final PC/CC and then current-only; unrelated provisional
or stale authority remains fenced.
Retirement-started and terminal tombstone records both prevent application Open
on restart: a fresh process finishes interrupted retirement without reopening
the removed application, then reports the exact receipt. Frozen quorum proof
does not restore stale session credit; current-session verified progress is
required after retained-peer restart.
For an already committed reduction, that progress may come from a current-only
primary that has restarted and resumed writes, provided its live report retains
the exact commit proof. Pre-commit witnesses remain strictly write-closed.
After a later accepted failover or replacement, matching bounded historical
removal evidence permits stale-member correction, not new authority. If a retained
secondary installed current-only removal authority but missed local commit
acceptance, the controller first publishes the exact certificate locally using
`AcceptSecondaryRemovalCommit.localRecovery`. This clears only its durable removal
fence after verifying exact identity, authority and boundary, without old-session
quorum credit, access grants, cleanup, or changes to cluster status/receipt. A fresh
report proving acceptance is required before ordinary newer-authority correction.
Missing or mismatched bounded proof fails closed; unrelated pending work waits.
If ordinary acceptance was already pending when the member crashed, the same
operation and complete certificate can resume in local-recovery mode. SQLite
atomically changes only that pending effect's mode, preserving its sequence,
stage and retained results. The conversion is one-way and requires exact local
identity, write-closed current-only authority and a durably verified boundary;
stale process sessions and mutated or conflicting effects remain rejected.
Peer endpoint scaffolding and accepted-configuration catch-up run before a new
switchover or removal is admitted.

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
generation, initialization provenance (including its original policy), and schema identity.
Admitted policy may evolve through an authorized reduction without changing that
provenance. Missing metadata,
identity drift, corruption, or an incompatible schema fails closed.

SQLite runs in WAL mode with `synchronous=FULL`, foreign keys enabled,
automatic WAL checkpoints disabled, and a five-second busy timeout. The agent
is the single database writer. The PVC filesystem must preserve fsync and
directory-entry durability and support SQLite WAL locking/shared-memory
semantics. Filesystems that cannot provide those semantics, including
unsupported network-filesystem arrangements, are not valid production
storage.

The current schema is **5** and accepts only its exact version. The migration hook records
an idempotent current-version migration; it does not upgrade older schemas.
Older schemas are rejected without conversion. Schema 5 retains committed
snapshot boundaries for default-engine replica builds and adds durable
application-path binding and one-way initialization permission. The applied suffix
follows as retained catch-up. Use a fresh deployment for protocol 9 / schema 5;
no rolling upgrade or existing-data migration is provided.

Crash injection is test-only. `KUBERIC_CRASH_WRITER_PATH` and
`KUBERIC_CRASH_BOUNDARY` are consumed only by the
`kuberic-agent` crash-boundary test executable. Production binaries expose no
fault-injection switch.

## Local Deployment

The following deployment recipes exercise KVStore2. The existing
[`sqlite-replicated` example](../sqlite/design.md) is migrated in place to v2
and uses the same public service/provider and `ReplicaHost` boundary, but this
migration validates SQLite only with unit and in-process tests. It does not add
SQLite images, deployment manifests, or live selectors.

SQLite freezes its snapshot at durable committed progress and sends the applied
suffix through ordinary retained catch-up. Quorum completion precedes local WAL
publication. Reopen resolves exact agent reservations and rematerializes committed
SQL state, removing stale WAL/SHM companions; acknowledged-history loss remains
rebuild-fenced. Primary activation settles only an authority-verified prefix.
The SQL API has no request-level deduplication: an unknown outcome requires
verification before retry. See the [SQLite storage and failure contract](../sqlite/design.md#durable-storage-and-recovery).

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
just level-triggered-kind-test scale-down
just level-triggered-kind-test scale-down-adversarial
just level-triggered-kind-test scale-up
just level-triggered-kind-test scale-up-multi
just level-triggered-kind-test scale-up-adversarial
just level-triggered-kind-test scale-up-full
```

The bounded full matrix expands to replacement, quorum loss, the composed
adversarial restart/partition/failover scenario, both switchover scenarios, and
both scale-down scenarios, followed by healthy, multi-count, and adversarial
scale-up (ten tests, in that order):

```bash
just level-triggered-kind-test all
```

Start `all` from fresh bootstrap: replacement assumes the original primary.
Standalone `failover` is not included in `all`; run it on a separate fresh
cluster rather than prepending it to that matrix.

The [PR workflow](../../../.github/workflows/level-triggered-CI.yml) separately
runs bootstrap, replacement, failover, healthy switchover, and healthy scale-down
plus healthy sequential scale-up smoke tests. Targeted tests run serially and
retain classic v1 scale-up/write-during-copy regressions. Scheduled
and manually dispatched full CI runs the matrix twice on separate fresh
clusters.

`scale-down` covers healthy 3→2→1, singleton process restarts and new writes,
plus a separate five-member set reduced sequentially to two. It checks every
acknowledged value, exact retained identity, unchanged primary/data-loss number,
intermediate topology, Service writes, and endpoint/Pod/PVC cleanup.
`scale-down-adversarial` covers reachable retirement with retained direct
clients, controller/primary/target restarts, ambiguous replies and old-session
rejection, unavailable-target sequential 3→1 (including 2→1), exact post-commit
Pod fencing, ownership-label loss, already-absent Pods, and preservation of a
same-name/different-UID endpoint.

`scale-up` covers healthy sequential growth, acknowledged writes during a
deliberately held live copy, exact PC/CC/current-only admission, and stable
routing. `scale-up-multi` covers larger one-at-a-time convergence.
`scale-up-adversarial` covers cancellation, failed-build cleanup/fresh retry,
primary and participant restart, pre-admission failover, post-PC/CC carried
failover, accepted-member replacement/failover composition, and
scale-down→scale-up restoration with a fresh incarnation. These tests validate
their bounded scenarios; they do not establish a maximum replica count,
arbitrary-failure coverage, outage bound, or production SLO.

The live copy gate used by those tests is disabled by default. The checked-in
sample and Service do not enable or route it. `just level-triggered-install`
injects the test-only `testing.kuberic.io/live-copy-gate: enabled` annotation,
which causes replica Pods to bind hold/release controls only on direct
diagnostic port 18080. This annotation, environment variable, port, and its
endpoints are test harness controls, not a CRD field or supported user API.

Recorded secondary-scale-down validation used two original full seven-scenario
fresh-cluster lifecycles of **18m16s / 16m29s**. Healthy scale-down measured
**320.74s / 322.67s** and adversarial scale-down **427.95s / 433.17s**.
After retained-peer acceptance and replacement-cleanup fixes, the final full
matrix at `09bbc7e` passed in **927.942s command wall time / 959.565s lifecycle**
(including cluster setup and teardown):

| Scenario | Final test duration |
|---|---:|
| replacement | 56.95s |
| quorum-loss | 41.24s |
| adversarial | 22.83s |
| switchover | 4.45s |
| switchover-adversarial | 47.12s |
| scale-down | 323.06s |
| scale-down-adversarial | 431.01s |
| supplemental failover (separate fresh cluster) | 18.71s |

These are test measurements, **not outage guarantees** or production SLOs.
Earlier matrices do not certify the subsequent fixes; the final rerun does.
All owned clusters, kubeconfigs, and ownership receipts were removed.

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

For scale-down, preserve the full transition/evidence, cleanup receipt or
`lastSecondaryRemoval`, conditions, exact resource UIDs, deletion timestamps,
finalizers, and current/previous replica logs. `/status` is a compact runtime
view; authenticated agent reports additionally carry `preparedSecondaryRemoval`,
`acceptedSecondaryRemoval`, and `retiredReplica`.
For replacement cleanup also retain `pendingReplacementCleanup`/`lastReplacement`.
Compare exact-name objects with frozen UIDs; a label-filtered list cannot prove
absence. Do not remove finalizers or edit status merely to force progress.

For scale-up, preserve `scaleUpAllocation`, `provisioning`,
`transition.scaleUp` or carried failover evidence, `scaleUpAdmissionStarted`,
`scaleUpCleanup`/`lastScaleUp`, condition messages, exact allocation
annotations, Pod/PVC/Service UIDs, and source/target build diagnostics. Compare
the snapshot and catch-up boundaries and both independent policies. Do not
manually add the live copy annotation or call its diagnostic port outside the
owned test harness.

Each replica also exposes `GET /status`, including its exact identity, durable
generation, process session, role, epoch, PC/CC IDs, progress, committed LSN,
write status, pending operation, active builds, and an additive `retired` boolean.
`retired: true` reflects terminal durable/runtime retirement, not merely role
`None` or denied access. Older diagnostic responses can omit the field; managed
certificates remain confined to the authenticated control plane.
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

The Cargo workspace keeps the level-triggered and classic packages explicit.
Package manifests and the compiled dependency graph are the source of truth for
their dependencies.
The existing `examples/sqlite` path and `sqlite-replicated` package are now v2,
not protected classic source. This reclassification does not authorize edits
to other classic v1 paths.

The runtime Rust tests maintain a reviewed source-public API inventory:

- an exhaustive `syn`-parsed inventory of every source `pub` signature,
  including declarations under private paths and `#[doc(hidden)]` paths.

Run the API inventory and the generated-schema/status regressions with Cargo:

```bash
cargo test -p kuberic-runtime --test public_api_inventory
cargo test -p kuberic-controller --lib
cargo test -p kuberic-protocol --lib representative_scale_up_status_variants
```

Classic v1 remains the documented path for existing `kuberic.io/v1` resources
and the classic KVStore example. SQLite and PostgreSQL are migrated in place to v2;
existing deployed application data has no import path. PostgreSQL Workstream 4
is complete with unit/host-local subprocess tests, not KinD/live coverage; its
images and deployment assets remain Workstream 5. PostgreSQL owns native WAL and
recovery behind ordinary custom-replicator callbacks; the shared controller
does not carry database-native evidence. See the
[PostgreSQL contract](../postgres/design.md) for trust and supervisor-loss limits.
V2 supports explicit planned switchover,
secondary-only scale-down, and sequential scale-up; no v1 conversion, data import, or
classic-path removal is implied. The [retirement plan](../../proposal/v1-retirement-plan.md)
keeps direct primary removal deferred and treats
distribution, deprecation, and source removal as separate workstreams. To
remove the physical replica currently hosting primary authority, complete a
planned switchover first and then reduce membership after it becomes an eligible
secondary under the deterministic removal policy. There is no user-selected
removal target or automatic/atomic primary-removal request.
