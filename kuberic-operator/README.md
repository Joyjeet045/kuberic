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
  controlPort: 50051
  dataPort: 50052
  clientPort: 50053
```

## Deployment

For development and CI, follow the [shared Gateway KinD setup](../docs/features/envoy-gateway-kind.md).
`just kvstore-deploy` installs the operator, both KVStore applications, and
their Gateway in the owned cluster. Gateway installation remains outside the
operator; the operator continues to manage only Kuberic resources.

## Planned Node Maintenance

An external coordinator creates a cluster-scoped `NodeMaintenanceRequest` for
one node and event. Kuberic moves primaries through its durable switchover path,
attests surviving write quorum, and reports `KubericPrepared=True`. The coordinator
owns cordon, eviction, infrastructure execution, and the final provider acknowledgment.

Set `spec.desiredState` to `Complete` or `Cancel` to release the request. Placement
remains excluded through `Releasing` until the node and affected replica sets
have recovered. Deletion follows the same guarded cancellation path through a
finalizer. Failed and expired requests also require explicit release.

See [Node maintenance](../docs/features/node-maintenance.md) for the lifecycle,
replacement/reimage rules, Events, metrics, and the external AKS Scheduled Events
bridge contract. The operator exposes Prometheus metrics at `/metrics` on port
8081, configurable with `KUBERIC_METRICS_ADDR`; keep this endpoint cluster-private.

## Architecture

| Module | Purpose |
|--------|---------|
| `crd.rs` | `KubericSet` CRD definition and status types |
| `reconciler.rs` | Main reconcile loop — pod management, lifecycle orchestration |
| `cluster_api.rs` | Kubernetes API helpers for pod/service operations |
| `node_maintenance/` | Request lifecycle, discovery, safety attestation, guarded release, Events, and metrics |
