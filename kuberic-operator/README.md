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

## CRD Example

```yaml
apiVersion: kuberic.io/v1
kind: KubericSet
metadata:
  name: my-app
spec:
  replicas: 3
  image: my-app:latest
  switchoverExecutionMode: explicit
  controlPort: 50051
  dataPort: 50052
  clientPort: 50053
```

## Durable Switchover Pilot

The existing explicit switchover state machine is the default. A `KubericSet`
with at most three stable members can opt into the comparison pilot only when
the operator binary is built with `--features durable-switchover-pilot` and
the resource sets:

```yaml
spec:
  switchoverExecutionMode: durablePilot
```

The pilot stores format-3 checkpoints in same-namespace ConfigMaps named
`kuberic-checkpoint-<execution-id>`. They have a non-controlling owner
reference to the `KubericSet`, remain through terminal reload, and are garbage
collected with that owner. The operator needs ConfigMap `get`, `create`, and
`update`; it does not need checkpoint delete permission. Reconciliation
remains the scheduler and all replica mutations continue through
`ReplicaAgent`.

The pilot workflow uses typed activity calls and compact effect/observation
records. Deterministic switchover transitions replay in memory; fused
checkpoint CAS operations durably expose an exact command before returning a
permit and combine authoritative observation with the next command or terminal
state. Unknown replica or UID-fenced label outcomes remain quarantined and are
not automatically retried. Set/Pod watches provide normal wakeups, with
bounded deadline requeues as a fallback.

Use `status.durableSwitchoverPilot` and the `DurableSwitchoverPilot` condition
to inspect execution identity, storage reloads, exposed/quarantined work, and
completion. The pilot does not apply to creation, add/build, removal, or
failover.

Run `python3 scripts/measure-switchover-complexity.py` from the repository root
for workflow/shared/total lexical accounting. The representative integration
test prints checkpoint, status, effect, label, and Pod-list measurements.

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
