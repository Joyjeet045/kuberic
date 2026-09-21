# Primary placement and balancing

Primary balancing is optional. It complements [replica spreading](topology-placement.md);
it never substitutes for replication safety or moves Pods between nodes.
The score uses the number of Kuberic primaries in each topology domain and node
across all namespaces. CPU, memory, and external load metrics are not inputs.

## Node-level example

```yaml
apiVersion: kuberic.io/v1
kind: KubericSet
metadata:
  name: balanced-kv
spec:
  image: localhost/kvstore:latest
  replicas: 3
  minReplicas: 2
  scheduling:
    mode: Required
    topologyKey: kubernetes.io/hostname
  primaryBalancing:
    mode: Automatic
    topologyKey: kubernetes.io/hostname
    cooldownSeconds: 300
    stabilizationSeconds: 60
    minimumImprovement: 1
```

Required replica spreading needs three eligible hostname domains for this example.
Use `scheduling.mode: Preferred` to permit colocation when capacity is insufficient.
That improves availability but does not provide failure-domain isolation.

## Zone-level example

```yaml
apiVersion: kuberic.io/v1
kind: KubericSet
metadata:
  name: zone-balanced-kv
spec:
  image: localhost/kvstore:latest
  replicas: 3
  minReplicas: 2
  scheduling:
    mode: Preferred
    topologyKey: topology.kubernetes.io/zone
  primaryBalancing:
    mode: Automatic
    topologyKey: topology.kubernetes.io/zone
```

Nodes must carry the configured labels. Scheduling and primary-balancing keys
are independent: for example, require different hostnames for replicas but
balance primaries across zones. With a zone balancing key, proactive moves are
between zones, not between nodes in the same zone.

## Policy and selection order

Omitting `primaryBalancing` preserves legacy initial/failover ordering, apart
from excluding known active-maintenance targets. Configuring an empty policy
enables `TieBreakOnly`; proactive balancing requires `mode: Automatic`.

| Field | Default | Valid range |
| --- | --- | --- |
| `mode` | `TieBreakOnly` | `TieBreakOnly`, `Automatic` |
| `topologyKey` | `kubernetes.io/hostname` | nonempty label key |
| `cooldownSeconds` | 300 | 30-86400 |
| `stabilizationSeconds` | 60 | 1-3600 |
| `minimumImprovement` | 1 | positive integer |

Initial creation chooses an eligible replica by domain primary count, node
primary count, then replica ID. A committed creation prefix and its primary
always remain authoritative on recovery.

Failover retains the existing protocol's incarnation, epoch, configuration,
deactivation, health, quorum, catch-up capability, and progress checks. Density
only orders equivalently safe and fresh candidates; it cannot promote a stale
replica, bypass an outstanding-candidate wait, or remove a replica's quorum
evidence merely because that replica is an ineligible placement target.
Placement evidence is frozen in the durable failover record.

Automatic balancing is more conservative than failure recovery:

1. The set must be Healthy with exact configured membership, no primary failure,
   no degraded commit, and no active topology operation.
2. Every committed incarnation must attest healthy, compatible configuration,
   role, epoch, and election metadata; the primary must have write quorum.
3. The target must be on a Ready, schedulable, non-deleting node outside active
   maintenance, in a different selected domain.
4. The target must already contain the primary's sampled current log and cover
   its committed prefix. A secondary's own commit notification can lag even
   when those records are replicated.
5. Replication progress precedes domain density, node density, and replica ID.

Admission persists a normal durable switchover before dispatching any role
change. That workflow fences, catches up, commits, or compensates using the
existing replication protocol. Balancing never directly promotes a replica.

## Oscillation, recovery, and concurrency

For source-domain count `s` and target-domain count `t`, the improvement score
is `s - t - 1`: half the reduction in squared domain load after moving one
primary. With the default threshold, `2 -> 0` is useful, but `1 -> 0` is not:
the latter would merely reverse an equally balanced placement.

The same target Pod UID, node, domain, source primary, and configuration epoch
must remain suitable throughout stabilization. Unsafe or unavailable evidence,
loss of improvement, or a topology change resets the window. Candidate state,
operation identity, and cooldown timestamps are persisted, so operator restarts
do not restart a completed move or bypass hysteresis.

Cooldown starts at admission and is refreshed at completion or failure.
Committed and in-flight target primaries count as reservations when planning
other sets. Scoring and reservation writes are serialized within one controller.
The supplied Deployment uses one replica and `Recreate` to prevent overlapping
controllers during an update. Replicas continue serving while reconciliation is
paused. Multiple active controllers require leader coordination before use.

Failover, explicit/maintenance switchover, scaling, add/remove, and topology
recovery take precedence. Existing workloads are not rolled just to apply a
scheduling-policy edit; see the replica-spreading document for that tradeoff.

## Unavailable evidence

Unknown data is never treated as zero load.

| Evidence missing | Initial placement | Failover with policy | Automatic balancing |
| --- | --- | --- | --- |
| Inventory/API read | Wait, `ObservationUnavailable` | Wait; eligibility is unknown | Suppressed |
| Target node or eligibility | No selection | Exclude that target | Exclude that target |
| Density or topology label | Wait, `MissingTopology` | `DensityUnavailable`: no density scores; safety-only ordering among known eligible nodes | Suppressed |

Failover can therefore restore service without a density score while still
honoring maintenance and node eligibility. Active maintenance comes from
`NodeMaintenanceRequest`, consistent with the maintenance controller.

## Observability

`status.placement` reports replica-domain counts using the scheduling key,
the committed primary's node/domain using the balancing key, unschedulable or
missing-topology counts, scheduling diagnostics, candidate identity and score,
hysteresis, cooldown, and the durable operation ID/outcome.

- `ReplicaTopologyReady` reports scheduler failures and Required-domain
  violations, including label drift after placement. Incomplete inventory or
  ordinary scheduling progress is Unknown, not proof of a violation.
- `PrimaryBalanced` is Unknown when balancing is disabled, tie-break-only,
  not evaluated during initial/failover selection, or evidence is unavailable.
  It is True for `Completed` or `InsufficientImprovement`, and False for
  stabilization, cooldown, unsafe candidates, or unsuccessful moves.
- Kubernetes Events describe meaningful changes only after durable status
  persistence. Events are best effort; publication failures are logged.
- The operator's `/metrics` endpoint on port 8081 exports bounded-label
  Prometheus gauges for current persisted state, not historical outcome
  counters. A Kubernetes read failure returns HTTP 503 instead of empty
  success or invented zero values. See the [operator README](../../kuberic-operator/README.md).

Required anti-affinity is enforced at scheduling time. Node-label changes can
produce a later violation: the operator reports it but does not evict serving
replicas to repair it. Investigate capacity, labels, native affinity, taints,
and storage topology before changing Required to Preferred.

## Verification

Operator tests cover generated scheduling constraints, safety-first placement,
election ties, missing evidence, reservations, topology drift, Events, and
metrics. Runtime-backed tests verify initial placement, durable failover and
switchover, status-write failure, restart, data preservation, and cooldown.

The scheduler integration test requires an explicitly owned isolated KinD
fixture. CI uses two ready hostname domains to exercise real spreading and
insufficient-domain cases without changing shared node labels or taints.
