# Kuberic Operator: Failure Scenarios and Recovery

How the operator reconciler detects and handles various failure modes.
Design informed by CNPG patterns (see `docs/background/cloudnative-pg-architecture.md`).

---

## Pod Lifecycle Signals

| Signal | K8s Source | Detection |
|--------|-----------|-----------|
| Pod missing | Not in `list_pods()` | `driver.replica_ids().len() > pods.len()` |
| Pod not ready | `pod.status.conditions[Ready] == False` | `is_pod_ready(pod) == false` |
| CrashLoopBackOff | `container_status.waiting.reason == "CrashLoopBackOff"` | Specific reason check |
| Pod failed | `pod.status.phase == "Failed"` | Terminal state, won't recover |
| Pod evicted | `pod.status.phase == "Failed"`, `reason == "Evicted"` | Node pressure |
| Pod IP changed | Pod IP differs from GrpcReplicaHandle's address | Pod was recreated |
| Pod pending | `pod.status.phase == "Pending"` | Scheduling, image pull |
| gRPC unreachable | gRPC calls return Unavailable/DeadlineExceeded | `grpc_failure_count >= threshold` |

---

## Requeue Strategy

**Design decision (from CNPG):** Different situations warrant different
requeue intervals. The reconciler should NOT use a single fixed interval.

| Situation | Requeue Interval | Reason |
|-----------|-----------------|--------|
| Failover/switchover in progress | 1s | Poll for completion |
| Pod creation/deletion | 1s | Wait for informer cache |
| Secondary not ready (under threshold) | 5s | Give time to recover |
| All replicas unreachable | 10s | Network issue recovery |
| Optimistic lock conflict on CRD | Immediate | Retry with fresh version |
| Normal reconciliation | No explicit requeue | Watches trigger next run |

---

## Cluster Status & Conditions

**Design decision (from CNPG):** Persist enough state in CRD status to
reconstruct the operator's in-memory state after restart.

### CRD Status Fields

Current fields:
- `epoch`, `currentPrimary`, `targetPrimary`, `phase`
- `stableSnapshot`: authoritative stable epoch, primary logical ID, complete
  logical/incarnation member identities and roles, and write quorum

`currentPrimary` and per-member status remain compatibility/observational
output. They are not used to reconstruct topology.

**Implemented and future fields:**

| Field | Type | Purpose |
|-------|------|---------|
| `primaryFailingSince` | `Option<String>` | Persisted Unix time when continuous primary failure began |
| `stableElectionMetadataRefresh` | `Option<Checkpoint>` | Healthy-path configuration/progress refresh cursor and pending action |
| `operation.failover` | `Option<Checkpoint>` | Phase-1 observations, waits, epoch intents, callback result, and attestations |
| `instanceNames` | `Vec<String>` | All instance pod names (for missing-pod detection) |
| `instanceStates` | `HashMap<String, InstanceState>` | Per-instance reported state |
| `grpcFailureCounts` | `HashMap<String, u32>` | Consecutive gRPC failures per replica |
| `conditions` | `Vec<Condition>` | Standard K8s conditions |

### Instance State (per-pod)

```rust
struct InstanceState {
    is_primary: bool,
    current_progress: Lsn,      // last known LSN from ReplicaInfo
    pod_ip: String,             // for stale-IP detection
    last_seen: DateTime,        // last successful gRPC contact
}
```

### K8s Conditions

| Condition | Meaning |
|-----------|---------|
| `Ready` | Cluster is fully operational (primary + all replicas healthy) |
| `QuorumAvailable` | Write quorum is met (primary + enough replicas) |
| `Degraded` | One or more replicas unhealthy but quorum maintained |

---

## Failure Scenarios

### 1. Primary Pod Crash / Not Ready

**Detection:** Healthy phase checks `is_pod_ready(primary_pod) == false`.

**Current behavior:** ✅ Implemented
- Persists a versioned `Failover` operation before runtime mutation
- Records and validates live incarnation, epoch, role/configuration,
  progress/retained range, health, and deactivation evidence
- Evaluates previous/current read quorum independently
- Persists `WaitingForBestCandidate` or `QuorumLoss` when missing evidence
  could change the safe result
- Durably confirms the deterministic best candidate and immutable target
- Advances configuration/data-loss epochs through separate write-ahead intent
- Invokes epoch-fenced, correlated `OnDataLoss` only after the candidate
  observes the advanced epoch
- Commits promotion, rolls configuration/labels forward, attests exact final
  state, and atomically publishes `Healthy`

**Design addition — failover delay (K8s adaptation):**

SF's FM failovers immediately on heartbeat loss (lease-based federation
detection is fast). On K8s, pod readiness probes can flap during transient
issues. An optional delay prevents unnecessary failovers.

This is a **K8s-specific adaptation**, not an SF pattern. Default should
be 0 (immediate, matching SF behavior). Configurable for environments
with unstable pod readiness.

```
Primary not ready detected:
  │
  ├─ If failoverDelay == 0: immediate failover (SF default)
  │
  ├─ If failoverDelay > 0:
  │    ├─ If primaryFailingSince is None:
  │    │    Set primaryFailingSince = now
  │    │    Requeue after 1s
  │    │
  │    ├─ If elapsed < failoverDelay:
  │    │    Requeue after 1s (keep checking)
  │    │
  │    ├─ If primary recovers:
  │    │    Clear primaryFailingSince
  │    │    Stay in Healthy
  │    │
  │    └─ If elapsed >= failoverDelay:
  │         Clear primaryFailingSince
  │         Transition to FailingOver
```

**Phase-1 quorum and data-loss handling:**

```
Durable failover:
  │
  ├─ Persist full previous/current membership denominators
  ├─ Collect exact-incarnation live evidence
  │
  ├─ Missing replica could outrank survivor:
  │    WaitForBestCandidate; rotate probes; no mutation
  │
  ├─ Previous/current read quorum could still recover:
  │    WaitForReadQuorum; rotate probes; no mutation
  │
  ├─ Read quorum satisfied:
  │    Persist best candidate → configuration epoch → promote/converge
  │
  └─ At least one read quorum conclusively unavailable, no restorer,
       and no possibly better candidate:
       Persist candidate → configuration epoch → data-loss epoch
       Apply epoch → durable OnDataLoss
         NoStateChange → converge compatible target
         StateChanged → refresh candidate → primary-only target
         Error/conflict → poison without promotion
```

Elapsed time never converts missing safety evidence into data loss. This is
deliberately fail closed: the callback path is reached from conclusive
observations, not from a timeout. `failoverDelay` only delays initial primary
failure detection and resets if the primary recovers before operation
persistence.

**Edge case — gRPC unreachable but pod Ready:** See scenario 5.

---

### 2. Secondary Pod Crash / Not Ready

**Detection:** Healthy phase checks ALL replicas for Ready status.

**Current behavior:** ✅ Implemented for stable secondaries. A ready
replacement incarnation uses durable rebuild. An unreachable old incarnation
uses durable force-removal when retained membership can satisfy quorum.

**Implemented flow:**

CNPG detects secondary failures immediately via pod watches and recreates
pods automatically. We should follow the same pattern, but with a grace
period since our pods hold in-memory replicator state.

```
Healthy phase — secondary health check:
  │
  For each secondary in driver.replica_ids():
    Find matching pod in pods list
    │
    ├─ Pod Ready → OK, reset failure tracking
    │
    ├─ Ready pod with new UID:
    │    Persist one AddReplicaIntent to the current primary agent
    │    Primary retires the old exact connection, drives target peer
    │    Prepare/copy/Activate, catches up, and commits current configuration
    │
    └─ Missing/unreachable old UID:
         Validate non-primary target, minReplicas, and retained quorum
         Persist one Force RemoveReplicaIntent to the current primary
         Primary drives reduced CatchUp → tracked quorum → reduced Current
         Persist commit evidence and workflow-scoped reduced snapshot
         Primary removes exact old connection and best-effort Retire
         Operator exact-UID labels/deletes old pod, then publishes topology
    │
    └─ Pod missing → scenario 3
```

**Impact of not handling:** With persisted-mode ACK (acknowledge-gated
quorum), writes may hang waiting for ACK from a dead secondary. With
3 replicas and quorum=2, one secondary failure doesn't block writes
(primary + 1 secondary = quorum). Two secondaries down → writes hang.

---

### 3. Pod Deleted (Missing from list_pods)

**Detection:** Driver has a handle for a replica ID, but no matching pod
exists in `list_pods()`.

**Current behavior:** ✅ Stable missing secondaries are handled by the same
durable force-removal protocol. Missing primaries still route to failover.

**Implemented flow:**

CNPG detects missing pods by comparing expected instances (from CRD
`instanceNames`) against actual pods. We do the same with `driver.replica_ids()`.

```
Healthy phase — missing pod check:
  │
  Compare stableSnapshot members with current pod logical IDs:
      │
      └─ Stable pod not found:
           ├─ Primary → FailingOver
           └─ Secondary → persist durable Force removal
                one primary-agent coarse intent
                → reduced CatchUp → retained quorum → reduced Current
                → exact sender cleanup → lifecycle-peer retirement
                → UID-fenced pod cleanup → reduced stable snapshot
                → later durable scale-up restores desired capacity
```

The operator does not call a force-remove driver method or sequence target
runtime actions. It persists durable global authority first and sends one
coarse intent to the exact current primary. Target lifecycle work is
primary-agent-owned and conditional after membership commit.

---

### 4. CrashLoopBackOff

**Detection:** `container_status.state.waiting.reason == "CrashLoopBackOff"`

**Current behavior:** Kubernetes readiness gates primary health. Live gRPC
status checks cover both primary and secondary replicas and drive secondary
staleness handling. The operator does not yet inspect the `CrashLoopBackOff`
reason separately or enforce a recreate-attempt cap.

**Design (informed by CNPG):**

CNPG excludes crash-looping pods from the healthy pool and election
candidates. They cannot be promoted. We should do the same.

```
CrashLoop handling:
  │
  ├─ Crash-looping pods are treated as "not ready" (scenario 2 flow)
  │
  ├─ Primary loss routes to durable Phase-1 failover
  │   Unavailable replicas remain in quorum denominators
  │   Unknown/possibly-best replicas cause an explicit wait
  │
  ├─ After replacementDelay: delete and recreate
  │
  └─ Retry cap:
       Track recreate_count in CRD status per instance
       If recreate_count >= maxRecreateAttempts (default 5):
         Mark instance as Failed
         Log error, set condition Degraded=True
         Don't attempt further replacement
         (app bug requires human intervention)
```

---

### 5. Network Partition (Pod Reachable by Kubelet, Not by Operator)

**Detection:** Pod shows `Ready=True` but gRPC calls from operator timeout.

**Current behavior:** gRPC timeouts surface as errors in reconciler →
controller requeues with backoff. No specific handling.

**Design (SF fencing + K8s-specific detection):**

SF's defense against network partitions is **epoch fencing** — secondaries
reject operations from stale epochs. This is already implemented. K8s adds
complexity because the kubelet and operator have different network views.

#### Level 1: Operator-Side gRPC Failure Tracking (K8s adaptation)

SF's FM uses lease-based heartbeats. On K8s, we use gRPC failure tracking
as an equivalent signal:

```
Per-replica gRPC failure tracking:
  │
  On gRPC error (Unavailable, DeadlineExceeded):
    grpc_failure_count[replica_id] += 1
  │
  On gRPC success:
    grpc_failure_count[replica_id] = 0
  │
  If grpc_failure_count >= grpcFailureThreshold (default 3):
    Treat replica as unreachable
    ├─ Primary → initiate failover (with failover delay if configured)
    └─ Secondary → initiate replacement (with replacement delay)
```

Persist `grpc_failure_count` in CRD status so it survives operator restart.

#### Level 2: Epoch Fencing (SF core mechanism — ✅ implemented)

Our primary defense. When a network partition occurs:
1. Operator detects primary unreachable → failover
2. All secondaries receive `update_epoch(new_epoch)` → reject old primary's ops
3. Old primary's `replicate()` calls fail with quorum errors → `NoWriteQuorum`
4. User code sees `write_status() == NoWriteQuorum` → stops accepting writes

This is the exact SF model. It works even without K8s-specific self-fencing.

#### Level 3: Primary Self-Fencing via Liveness Probe (K8s defense-in-depth)

**This is a K8s-specific addition, not an SF pattern.** SF relies on epoch
fencing + federation heartbeats. On K8s, there's a gap: if the operator
can't reach the primary to send epoch updates, the old primary doesn't
know it's been replaced. The liveness probe fills this gap.

```
Primary liveness probe:
  │
  ├─ Can reach K8s API server?
  │    YES → pass (OK)
  │
  │    NO → Check peer reachability
  │         ├─ Can reach ANY other instance via gRPC data port?
  │         │    YES → pass (OK) — not fully isolated
  │         │
  │         │    NO → FAIL (HTTP 500)
  │         │         Primary is isolated from BOTH API server and all peers
  │         │         → Kubelet restarts pod
  │         │         → Operator detects NotReady → failover
  │
  Replicas: always pass (no benefit to restarting isolated replica)
```

#### Level 4: Multi-Primary Detection

After partition heals, operator may see two pods reporting as primary.
Epoch comparison resolves this deterministically:

```
Multi-primary detection (Healthy phase):
  │
  GetStatus on all replicas
  If multiple report as primary:
    ├─ The one with the OLDER epoch is stale
    ├─ Call close() on the stale primary
    ├─ Delete and recreate the pod
    └─ add_replica to rejoin as secondary
```

---

### 6. Node Failure / Node Drain

**Detection:** Multiple pods become NotReady simultaneously, or node is
marked `Unschedulable`.

**Current behavior:** Primary failure detected → failover. Secondary
failures ignored.

**Design (from CNPG):**

#### Node Drain Handling

CNPG detects drain taints and proactively moves the primary off draining
nodes. We should add a similar mechanism:

```
Healthy phase — node drain detection:
  │
  For each pod:
    Get pod's node
    If node.spec.unschedulable == true or drain taint present:
      ├─ If pod is primary:
      │    Find secondary on a schedulable node
      │    Trigger switchover to that secondary
      │
      └─ If pod is secondary:
           Mark for replacement after drain completes
           (PDB protects against too-fast eviction)
```

#### Pod Anti-Affinity (Prevention)

Add to CRD spec and pod template generation:

```yaml
spec:
  affinity:
    podAntiAffinityType: preferred  # or "required"
```

The operator generates anti-affinity rules:

```yaml
affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:  # or required
    - weight: 100
      podAffinityTerm:
        labelSelector:
          matchLabels:
            kuberic.io/set: {set_name}
        topologyKey: kubernetes.io/hostname
```

**Default: `preferred`** — pods spread across nodes when possible but
can colocate if scheduling fails. `required` enforces strict separation
(may prevent scheduling if not enough nodes).

---

### 7. Quorum Loss (Majority of Replicas Down)

**Detection:** Primary's `write_status()` returns `NoWriteQuorum`, or
gRPC calls to majority of replicas fail.

**Current behavior:** Primary-dead Phase-1 quorum loss is implemented in the
durable failover operation. Primary-alive `NoWriteQuorum` condition reporting
remains future operator health work.

SF distinguishes **quorum loss** (runtime state: writes blocked, system
waits) from **data loss** (explicit FM decision: committed ops may be
irrecoverable). Kuberic does not infer data loss from elapsed time.

See `docs/background/service-fabric/failover.md` §Quorum Loss
and Data Loss for the full SF protocol.

#### Case 1: Primary alive, write quorum lost

The runtime reports `NoWriteQuorum` and blocks writes. Operator conditions and
an explicit primary-alive recovery policy remain future work; the operator does
not automatically advance the data-loss epoch from a timer.

#### Case 2: Primary dead, quorum lost (data loss scenario)

Covered by the failover protocol (§1). The operator evaluates previous/current
**read** quorum from exact live observations. It waits while any unavailable
member could restore quorum or outrank the surviving candidate. Once all
safety-relevant members are accounted for, it persists the data-loss epoch,
applies it to the candidate, and invokes correlated `OnDataLoss`.

#### Case 3: Quorum loss during reconfiguration

The pure evaluator and persisted failover schema support distinct previous and
current configurations and count overlap independently. Production handoff
currently starts from stable `Healthy` topology, so previous configuration is
absent. Mid-reconfiguration failover handoff remains future work.

**Key principle:** Missing evidence is a wait, not permission to discard it.
Data loss requires conclusive observations and a durably confirmed candidate.

**Prevention (matching SF):**
- Use `spec.minReplicas` ≥ 2 to require quorum for writes
- Use pod anti-affinity across nodes (fault domain spreading)
- Synchronous replication (our default via persisted-mode ACK)
- Monitor `WaitingForBestCandidate` and `QuorumLoss` durable conditions

---

### 8. Operator Crash / Restart

**Detection:** N/A — the operator itself restarts.

**Current behavior:** ✅ Stable `Healthy`, durable `Creating`, durable
`Switchover`, durable replica add/rebuild, and durable replica removal recovery
are implemented. CRD snapshots/checkpoints survive in etcd while current pod
metadata and runtime status attest the live topology and pending activity.

```
Operator restart — first reconcile per KubericSet:
  │
  ├─ Require status.stableSnapshot:
  │    stable epoch, primary ID, complete members/roles, write quorum
  │
  ├─ List pods with label kuberic.io/set={name}:
  │    For each pod:
  │      Derive logical ID from the required pod-index label
  │      Create GrpcReplicaHandle from current pod UID and addresses
  │
  ├─ PartitionDriver::recover(snapshot, handles):
  │    Call only GetStatus
  │    Validate logical/incarnation bijection, exact epoch and stable roles
  │    Validate one primary, complete membership, and majority write quorum
  │    Rebuild handles and current configuration without runtime mutation
  │
  ├─ Validate consistency:
  │    Any absent/duplicate/relabelled/reincarnated member → fail closed
  │    Any runtime epoch/role/incarnation mismatch → fail closed
  │    Runtime unhealthy but otherwise consistent → recover, then health logic
  │
  └─ Resume normal reconciliation
```

`GetStatus` requires replica-agent control protocol version 3,
lifecycle-peer protocol version 2, and reports a pod-local
`AgentGeneration`. It is distinct from the Pod UID and changes when the
container/process restarts in place. Missing, malformed, or unsupported agent
status fails closed. A new generation publishes no inherited correlated action
state. Pending dispatches are fenced to the observed generation and agent
control version; an old-generation request is rejected without effects. If stable secondary
role/epoch continuity cannot be reconstructed under the same Pod UID, the
operator persists the established durable force-remove/rebuild operation
before mutation. Missing old-generation state is never treated as proof that
an ambiguous activity did not run.

`currentPrimary` is refreshed from recovered driver state and is not trusted
as input. Legacy status without `stableSnapshot` is rejected. Stable topology
changes persist a fresh snapshot; multi-member add/remove loops patch after
each committed change. If runtime mutation succeeds but status persistence
does not, the live operator retries that exact pending status before another
action.

During `Creating`, `Switchover`, `AddingReplica`, `RemovingReplica`, and
`FailingOver`,
`status.operation` is the authoritative compact checkpoint. Creation records
explicit no previous topology and an optional committed bootstrap snapshot;
the other protocols record previous/target stable snapshots. Each operation
stores exact target incarnations, current phase, and one write-ahead correlated
action. Add/rebuild and removal additionally store structured frozen intents
with primary/target generations and endpoints, exact configuration
descriptors, deadlines, and commit evidence. Each reconcile reconstructs fresh handles, observes
role/epoch/incarnation/progress/write/configuration/activity state, and either
advances one checkpoint, dispatches one activity, waits, compensates, or
poisons. The operator sends only the appropriate `AddReplicaIntent` or
`RemoveReplicaIntent` to the primary; peer/runtime phases are transient.
Add/rebuild observes coarse phase and tracked copy,
restores previous current configuration before pre-commit cleanup when needed,
and requires connection absence before deleting the candidate. A target process
restart under the same Pod UID changes generation and invalidates prior build
proof. Current-configuration commit is roll-forward only; unattested committed
membership becomes `CommittedDegraded` without a serving label. Kubernetes
`resourceVersion` rejects stale concurrent advancement. Failover
additionally persists Phase-1 observations, unavailable-probe rotation,
separate configuration/data-loss epoch intents, callback result, promotion
commit, and final attestations.

Removal freezes `ScaleDown` or `Force`, exact primary/target authority,
previous/reduced descriptors, a 600-second overall deadline, 10-second call
bounds, 30-second compensation grace, 60-second retirement budget, and at
most three pre-commit attempts. The primary agent owns reduced
CatchUp/quorum/reduced Current, exact connection cleanup, and
`ReplicaLifecyclePeer` Retire. Exact reduced Current is irreversible.
The 600-second deadline plus 30-second compensation cap is per attempt, so
three attempts may consume up to 3 × 630 = 1,890 seconds before pre-commit
termination. Post-commit connection cleanup is deliberately unbounded:
temporary loss of exact primary status is not absence evidence, and retirement
expiry cannot waive the connection barrier.

Before commit, exact previous Current can finish compensation and exact
reduced CatchUp can redrive on a new primary generation within the attempt
limit. After current-install dispatch, an ambiguous observation cannot
authorize rollback. After commit, `operation.committedSnapshot` is the scoped
recovery authority while `stableSnapshot` stays previous until exact-UID
labels/deletion and publication complete.

A complete primary restart with no surviving configuration before durable
commit evidence produces `AmbiguousPrimaryRestart`. Structurally impossible
or post-current-dispatch ambiguous evidence produces `InvalidRemovalState`.
Known pre-commit exhaustion produces `FailedPreCommitIncomplete`. All are
durable `Poisoned` dispositions, but their evidence and recovery meaning are
different. No disposition permits inferred commit, rollback, cleanup,
failover handoff, or publication.

Target peer replies are accepted only for the exact frozen generation,
incarnation, parent, epoch, and reduced Current projection. A same-name Pod
replacement is protected by exact-UID label and delete operations. If commit
evidence already exists, loss of the coordinator or primary process cannot
restore removed membership; cleanup rolls forward.

Creation commits primary-only and expanded partial bootstrap snapshots before
starting later members. Pods remain on a non-serving bootstrap label until the
complete target satisfies `minReplicas`; routing publication is itself
checkpointed. Before the first commit, cleanup can restart from no topology.
After a commit, creation preserves committed members and retries only the
candidate. A changed committed-member incarnation fails closed.

**Boundary:** Stable failover from `Healthy`, including data loss, is
recoverable. Mid-reconfiguration handoff into failover and loss of an
uncommitted bootstrap primary remain separate recovery problems.

---

### 9. Stale/Zombie Primary (Split Brain)

**Detection:** After failover, the old primary pod may still be running
and accepting writes if it didn't receive the epoch fence.

**Defense layers (SF core + K8s additions):**

| Layer | Mechanism | Origin | Status |
|-------|-----------|--------|--------|
| 1. Epoch fencing | Candidate observes the new epoch before promotion; retained secondaries receive it during post-promotion convergence. | **SF core** | ✅ Implemented |
| 2. Role status | `set_status_for_role()` sets `NotPrimary` on demotion. `StateReplicatorHandle::replicate()` rejects. | **SF core** | ✅ Implemented |
| 3. Primary self-fencing | Liveness probe isolation check (see scenario 5, level 3). Pod self-kills if isolated. | **K8s addition** (from CNPG) | ❌ Not implemented |
| 4. Old primary routing fence | Durable label convergence removes the old primary from serving selection; physical cleanup follows topology reconciliation. | **K8s addition** | ✅ Label fence |
| 5. Multi-primary detection | Healthy phase detects two primaries via GetStatus. Closes stale one (epoch comparison). | **K8s addition** | ❌ Not implemented |

**SF's primary defense is epoch fencing (layers 1-2).** Layers 3-5 are
K8s-specific additions that cover gaps where the operator can't reach pods
to deliver epoch updates. SF doesn't need these because the federation
subsystem provides reliable failure detection via lease-based heartbeats.

**Layer 4 — old primary cleanup:**

```
After failover completes:
  │
  ├─ If old primary pod still exists:
  │    Try close() on old primary handle
  │    If gRPC fails (unreachable):
  │      Delete the pod (force kill)
  │    Create replacement pod
  │    add_replica as secondary
  │
  └─ If old primary pod missing:
       Already gone, create replacement if needed
```

---

## Implementation Priority

| Priority | Scenario | Effort | Origin |
|----------|----------|--------|--------|
| P0 | Secondary not ready → replace | Medium | SF (FM detects + replaces) |
| P0 | Pod deleted → detect + replace | Medium | SF (FM detects) + K8s pod mgmt |
| Done | Durable Phase-1/data loss failover | Implemented | **SF** (`OnDataLoss` protocol) |
| Done | Stable Healthy operator restart → recover driver | Implemented | SF (FM is stateless, uses persistent config) |
| P1 | gRPC failure tracking | Medium | K8s adaptation (replaces SF federation heartbeats) |
| P1 | Old primary cleanup after failover | Small | K8s addition |
| P1 | CRD conditions (Ready, Degraded, Quorum) | Small | K8s addition (from CNPG) |
| Done | Failover delay (optional) | Implemented | K8s adaptation (from CNPG) |
| P2 | Primary self-fencing liveness probe | Medium | K8s addition (from CNPG isolation check) |
| P2 | Node drain detection | Medium | K8s addition (from CNPG, analogous to SF PLB) |
| P2 | Multi-primary detection | Small | K8s addition (epoch comparison) |
| Done | Agent-owned scale-down + force-remove secondary | Implemented | SF (`RemoveReplica`, `RemoveFromCurrentConfiguration`, target `DeleteReplica`) |
| P3 | CrashLoop retry capping | Small | K8s addition |
| P3 | Pod anti-affinity in CRD | Small | K8s addition (analogous to SF fault domains) |

---

## Reconciler Phase Diagram (Updated)

```
                    ┌─────────┐
                    │ Pending  │
                    └────┬─────┘
                         │ create pods
                    ┌────▼─────┐
                    │ Creating │ ◄── wait for all pods ready
                    └────┬─────┘
                         │ create_partition via driver
                    ┌────▼─────┐
          ┌────────►│ Healthy  │◄──────────────────────┐
          │         └─┬──┬──┬──┘                        │
          │           │  │  │                           │
          │    primary│  │  │target_primary             │
          │    unhealthy │  │!= current_primary         │
          │    (after │  │  │                           │
          │    delay) │  │  │                           │
          │    ┌──────▼┐ │ ┌▼──────────┐               │
          │    │Failing │ │ │Switchover │               │
          │    │Over    │ │ │           │               │
          │    └───┬────┘ │ └─────┬─────┘               │
          │        │      │       │                     │
          │   failover    │  switchover                 │
          │   (+ data loss│       │                     │
          │    if quorum  │       │                     │
          │    lost)      │       │                     │
          │        │      │       │                     │
          └────────┘      │       └─────────────────────┘
                          │
                    scale-up: durable add
                    scale-down: coarse agent-owned config-first remove
                    secondary health: durable rebuild or force-remove
                    node drain: switchover primary off draining node
```

**SF-aligned failover:** The operator validates previous/current read quorum
and the complete best-candidate ordering. Missing quorum-restoring or possibly
better evidence remains in an explicit durable wait. Once observations
conclusively establish data loss, the operator advances the data-loss epoch
before correlated `OnDataLoss`; it never promotes merely because time elapsed.

**New in Healthy phase:**
- Secondary health monitoring (replace unhealthy replicas)
- Missing pod detection (replace deleted pods)
- Node drain detection (switchover off draining nodes)
- gRPC failure tracking (detect unreachable but "Ready" pods)
- Multi-primary detection (close stale primary)
