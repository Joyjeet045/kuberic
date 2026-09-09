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

Switchover always uses the framework-native durable workflow. There is no
execution-mode selector or optional operator build feature. The workflow
stores format-3 checkpoints in same-namespace ConfigMaps named
`kuberic-checkpoint-<execution-id>`. They have a non-controlling owner
reference to the `KubericSet`, remain through terminal reload, and are garbage
collected with that owner. The operator needs ConfigMap `get`, `create`, and
`update`; it does not need checkpoint delete permission. Reconciliation
remains the scheduler and all replica mutations continue through
`ReplicaAgent`.

Kuberic supports 1–9 replicas. The CRD and reconciler enforce the same product
range; framework-native switchover admits four-member through nine-member
stable snapshots and rejects a tenth member before any effect.

The workflow uses typed activity calls and compact effect/observation
records. Deterministic switchover transitions replay in memory; fused
checkpoint CAS operations durably expose an exact command before returning a
permit and combine authoritative observation with the next command or terminal
state. Unknown replica or UID-fenced label outcomes remain quarantined and are
not automatically retried. Set/Pod watches provide normal wakeups, with
bounded deadline requeues as a fallback.

Use `status.switchoverExecution` and the `FrameworkNativeSwitchover` condition
to inspect immutable admission, checkpoint identity, current-contract
validation, storage reloads, exposed/quarantined work, and completion.
The CRD defines only the required current native reference. API requests using
strict Kubernetes field validation reject removed or unknown fields, and
missing required fields fail schema admission.
Resources in the `Switchover` phase without a current native reference fail
closed and are not restarted automatically.
Creation, add/build, removal, and failover retain their existing execution
models.

The representative integration test reports checkpoint, status, effect,
label, and Pod-list measurements. The canonical three-member path is nine
external effects, three passive observations, 12 durable boundaries, and 13
accepted writes.

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
