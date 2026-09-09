# kuberic-operator

Kubernetes operator for [Kuberic](../README.md). Manages `KubericSet` custom resources — creates pods, orchestrates lifecycle, and handles failover.

## What It Does

1. Watches `KubericSet` CRDs (desired replica count, image, ports)
2. Creates/deletes bare pods to match desired state
3. Persists durable topology intent and delegates ordered effects through each
   pod's `ReplicaAgent`
4. Detects pod failures and triggers automatic failover
5. Sends one coarse scale-up/rebuild or scale-down/force-remove intent to the
   primary agent while retaining durable topology and Kubernetes ownership

## Production Remove Replica

Remove-replica always uses the framework-native durable workflow; there is no
remove execution-mode selector or remove-specific build feature. The
operator's shared in-process runner persists compact exact-command and
observation boundaries, quarantines uncertain exposed effects, and reloads
conflict or unknown-write outcomes before another one-use dispatch permit can
exist. Existing correlated control v3, `RemoveReplicaIntent` v1, and
`ReplicaLifecyclePeer` v2 behavior is unchanged.

The current three-member no-fault path records three external effects, two
passive observations, five durable boundaries, and six accepted writes.
Terminal checkpoint state is accepted and reloaded before reduced topology is
published.

`status.removeReplicaExecution` stores immutable admission, checkpoint
identity, and incompatibility state. The referenced same-namespace ConfigMap
stores compact boundary history and terminal evidence with a non-controlling
owner reference to the exact `KubericSet` UID. It remains through terminal
reload and is garbage collected with the owner; separately authorized orphan
cleanup is not granted to the normal writer. Reconciliation remains the
scheduler, so there is no extra worker, queue, lease, watcher, or durable
service.

## CRD Example

```yaml
apiVersion: kuberic.io/v1
kind: KubericSet
metadata:
  name: my-app
spec:
  replicas: 3
  image: my-app:latest
  controlPort: 50051
  dataPort: 50052
  clientPort: 50053
```

## Framework-Native Switchover

Switchover has one production path: the direct deterministic async workflow.
Its history uses 20 operation-specific version-1 activity names:

```text
revoke-writes, capture-frozen-lsn, wait-target-caught-up,
demote-old-primary, promote-target, distribute-replica-epoch,
install-target-catch-up-configuration, wait-target-write-quorum,
install-target-current-configuration, publish-target-primary-label,
publish-old-primary-secondary-label, attest-target-topology,
restore-previous-current-configuration, compensate-promote-old-primary,
compensate-distribute-replica-epoch,
install-compensation-catch-up-configuration,
install-compensation-current-configuration, restore-old-primary-label,
restore-target-secondary-label, attest-compensated-topology
```

Each name is prefixed by `kuberic.switchover.`. The workflow source directly
expresses normal ordering, sorted replica loops, waits, the pre-promotion
restore path, and the post-promotion compensation path. The host adapter
collects authoritative observations, prepares exact commands, dispatches only
under a consumed one-use permit, resolves quarantine, and validates terminal
state; it does not choose protocol progression.

The durable inputs are genuinely operation-specific. Fixed old-primary,
target-primary, distribution, configuration, and label activities expose only
their own target and attempt fields; there is no persisted cross-operation
kind enum or shared replica/label request superset behind the named history.

The public `status.switchoverExecution` contract is version 4. This is an
intentional clean break: previous switchover contract versions and histories
are not migrated or resumed. The CRD exposes only the required current shape.
Strict Kubernetes field validation rejects removed or unknown fields, missing
required fields fail schema admission, and a resource in `Switchover` without
the current reference fails closed.

Format-3 checkpoints are stored in same-namespace ConfigMaps named
`kuberic-checkpoint-<execution-id>`. They have a non-controlling owner
reference to the exact `KubericSet` UID, remain through terminal reload, and
are garbage collected with that owner. The operator needs ConfigMap `get`,
`create`, and `update`; it does not need checkpoint delete permission.

Kuberic supports 1–9 replicas. A one-replica set is valid but cannot switch
primary because there is no distinct target; direct switchover accepts stable
topologies with 2–9 members and rejects an invalid or identical target before
any effect.

Fused checkpoint compare-and-swap persists an exact prepared command before a
permit exists and combines authoritative observation with the next exposure or
terminal state. Unknown replica effects remain observation-only quarantined
past their activity deadline. Only a matching terminal ledger record, the
exact live postcondition, or generation-change proof of non-admission resolves
an exposed replica command. Precondition, unavailable, and scheduled/in-progress
evidence continue waiting. A new agent generation can permit one redelivery of
the same action identity; a second proof stops. UID-fenced label effects are
never redelivered and resolve only from the exact UID-bound label
postcondition. Conflict or unknown checkpoint writes reload before any later
permit.

Terminal state is compacted, reloaded, and validated before stable
topology/status publication. The compact terminal records its immutable branch
(`target_success`, `revoke_safe_failure`,
`previous_configuration_restored`, or
`post_promotion_compensated`), and reload accepts only member-count-specific
reachable external/passive accounting for that branch. Error strings are
bounded to 512 UTF-8 bytes before activity or terminal persistence. Use
`status.switchoverExecution`, its referenced ConfigMap, and the
`FrameworkNativeSwitchover` condition to inspect admission, named history,
reloads, quarantine, incompatibility, completion, or safe compensation.

The canonical three-member no-fault sample records nine external effects,
three passive observations, 12 completed boundaries, 13 accepted writes,
20,857 maximum active-checkpoint bytes, a 3,961-byte terminal checkpoint, and
a 924-byte terminal payload. The nine-member maximum-fault production sample
records 33 boundaries, 28 external effects, five passive observations,
48 accepted writes, 64,061 maximum active bytes, and 4,921 maximum terminal
bytes. The contract limits are 33 records, 4,096 workflow-input bytes,
8,192 maximum activity-input/result bytes, 524,288 active bytes, 16,384
terminal bytes, 4,096 terminal-payload bytes, 512 error bytes, 64 workflow
transitions, and 32 runner outcomes per reconcile. The single 64-transition
budget covers normal calls, compensation, attestation, and proof-backed
redelivery. Measurements are validation snapshots, not compatibility
constants.

The runtime topology is unchanged: one Kubernetes operator process and the
existing Set/Pod watches schedule the in-process runner; replica mutations
still cross `ReplicaAgent`, and label mutations still cross the Kubernetes
API. No worker, queue, lease, watcher, service, or remote activity host was
added. Creation, add/build/rejoin, failover, and remove-replica keep their
existing contracts and behavior.

## Deployment

```bash
# Deploy the operator
kubectl apply -f kuberic-operator/deploy/deployment.yaml

# Create a replicated service
kubectl apply -f examples/kvstore/deploy/kubericset.yaml
```

## Architecture

| Module | Purpose |
|--------|---------|
| `crd.rs` | `KubericSet` CRD definition and status types |
| `reconciler.rs` | Main reconcile loop — pod management, lifecycle orchestration |
| `cluster_api.rs` | Kubernetes API helpers for pod/service operations |
| `node_maintenance/` | `NodeMaintenanceRequest` CRD, preflight decisions and affected-replica discovery |
