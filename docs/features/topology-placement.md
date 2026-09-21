# Topology-aware placement

Replica scheduling and primary balancing are separate policies. Kubernetes places
replica Pods; the operator's primary-placement policy chooses among eligible,
already running replicas. A primary transition does not move a Pod.

## Controller deployment scope

The current deployment uses **one active controller**, with `replicas: 1` and
`strategy: Recreate`. Unlike a rolling deployment that can surge a second
controller, Recreate prevents overlapping placement planners during normal
operator updates. Reconciliation pauses during the update; existing application
replicas continue serving.

Per-process serialization and durable target reservations prevent overlapping
same-process balancing moves from concentrating on the same destination. This
feature does **not** introduce HA leader election or cross-process coordination.
HA requires leader coordination first; do not scale the operator to multiple
active controllers and assume balancing is coordinated between them.

Application workload rolling-update or scaling stages block automatic balancing:
the exact committed snapshot and all-replica safety checks must agree before a
proactive primary move can begin.

## Replica scheduling defaults

Omitting `spec.scheduling` (or setting it to `{}`) adds **preferred** Pod
anti-affinity, weight 100, on `kubernetes.io/hostname`. The selector matches only
`kuberic.io/set: <this-set>` in this set's namespace. Different sets and identically
named sets in other namespaces do not compete under the generated rule.

This is a preference, not a guarantee: a single-node cluster can run all replicas.
Resource availability, taints, storage, user constraints and the scheduler's other
scores can outweigh soft spreading. Existing Pods are not evicted or recreated
when the policy changes. The new policy applies to subsequently created Pods;
recreate replicas only using the normal safe replica lifecycle.

## Required separation

```yaml
apiVersion: kuberic.io/v1
kind: KubericSet
metadata:
  name: orders
  namespace: xedio
spec:
  replicas: 3
  image: kvstore:latest
  scheduling:
    mode: Required
    topologyKey: kubernetes.io/hostname
```

`mode` accepts `Preferred` (default) or `Required`. Required adds hard self-set
anti-affinity and requires the topology label to exist on the node. This enforces
at most one replica per labeled domain at scheduling time. Fewer eligible domains
than replicas leaves excess replicas **Pending / Unschedulable**; it does not
silently relax protection or fail over/promote a Pending replica. Nodes missing
the topology label are ineligible in Required mode. Preferred mode permits such
nodes and therefore cannot promise failure-domain isolation.

Both affinity forms are `IgnoredDuringExecution`: relabeling nodes after placement
does not evict Pods or retroactively enforce domain uniqueness. Keep topology labels
stable. There is no `Disabled` mode: soft spreading preserves compact-cluster
compatibility while retaining the safe default.

The Required-placement audit detects current collisions and missing topology
labels after label drift. It reports `RequiredTopologyViolation`, with affected
domains and Pods; it does not automatically evict or relocate existing replicas.
`RequiredTopologyUnverified` means a bound node is absent from the inventory,
not that a collision has been proven. The audit considers only this set's live,
bound Pods in this namespace.

## Native scheduling controls and zones

```yaml
spec:
  scheduling:
    mode: Preferred
    topologyKey: topology.kubernetes.io/zone
    nodeSelector:
      pool: storage
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
            - matchExpressions:
                - key: disk
                  operator: In
                  values: [ssd]
      podAntiAffinity:
        preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 20
            podAffinityTerm:
              topologyKey: kubernetes.io/hostname
              labelSelector:
                matchLabels:
                  workload: batch
    tolerations:
      - key: dedicated
        operator: Equal
        value: storage
        effect: NoSchedule
    topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: ScheduleAnyway
        labelSelector:
          matchLabels:
            kuberic.io/set: orders
```

`nodeSelector`, `affinity`, `tolerations` and `topologySpreadConstraints` use standard
Kubernetes structures. The example is a `spec` fragment to merge into a set, not a
standalone manifest. Supply a selector matching the intended Pods in custom spread
constraints. Native Pod affinity, Pod anti-affinity, required/preferred node
affinity and user spread constraints are preserved. The generated anti-affinity is
appended, not substituted for user rules.

For Required mode, topology-label existence is ANDed into **each** nonempty
required node-affinity term, preserving the user's OR alternatives, match fields
and match expressions. An empty node selector or term still matches no nodes.
All hard user constraints must also be satisfied; tolerations permit placement
but do not force it.

To require zone separation, use `mode: Required` with
`topologyKey: topology.kubernetes.io/zone`. Some clusters enable the
`LimitPodHardAntiAffinityTopology` admission plugin, which permits hard Pod
anti-affinity only on `kubernetes.io/hostname`. Such clusters reject hard
zone anti-affinity; use an administrator-approved admission policy or a native
`topologySpreadConstraints` rule with `whenUnsatisfiable: DoNotSchedule` instead.
Ensure nodes are labeled consistently before enabling hard topology rules.

The CRD validates the policy enum and topology-key syntax/length. The policy
validator also checks label values, tolerations and common spread-constraint
errors. Kubernetes Pod admission remains authoritative for native scheduling
rules and version-specific feature combinations.

## Diagnosing placement

Inspect `PodScheduled` before treating Pending as a scheduling failure:

```sh
kubectl -n xedio get pods -l kuberic.io/set=orders -o wide
kubectl -n xedio describe pod orders-2
kubectl get nodes -L kubernetes.io/hostname,topology.kubernetes.io/zone
```

The scheduling diagnosis helper preserves the scheduler's `reason` and `message`,
including insufficient domains, resource shortages, taints and conflicting native
constraints. An unassigned Pending Pod without scheduler evidence reports
`SchedulingPending`. A Pod already assigned to a node is not reported as a
scheduling failure (for example, a container image pull may keep it Pending).
Neither Pending nor an unavailable node inventory is a reason to weaken Required
rules or bypass durable primary-election checks.

## Scheduler integration coverage

`kuberic-tests/src/topology_placement_k8s.rs` submits the same generated scheduling
fields to a real Kubernetes scheduler. It verifies single-domain preferred
fallback, preferred spreading when two eligible hostname domains are available,
required separation, an Unschedulable excess replica, and independence of another
set. Unit tests cover zone selectors, missing labels, native-rule preservation,
Pod construction, scheduler diagnosis and Required-policy violations after
node-label drift.

Use the isolated KinD workflow and its ownership receipt, never a default or
production kubeconfig. With `KIND_CLUSTER_NAME`, `KUBECONFIG` and `KUBE_CONTEXT`
set to the fixture created by `just create-kind-cluster`:

```sh
just verify-kind-context
cargo test -p kuberic-tests topology_placement_k8s::test_topology_placement_k8s_scheduler -- --exact --nocapture
```

The test owns a uniquely generated namespace and deletes it with a UID precondition
even after scenario failure. It does not change node labels, taints, existing
workloads, or cluster configuration. The single-node default KinD fixture exercises
soft fallback and hard insufficient-domain behavior. A fixture with two ready,
schedulable hostname-labeled nodes also exercises actual cross-node placement.
Only scheduler binding is required; container image readiness is not an assertion.

## Primary balancing

See [Primary placement and balancing](primary-balancing.md) for the optional
policy, node/zone examples, safety ordering, durable recovery, cooldown,
unavailable-evidence behavior, and observability.
