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

## Development

After editing the `KubericSet` API, regenerate the deployed CRD from the repository
root and verify exact equality with the generated schema:

```sh
cargo run -p kuberic-operator --example generate-crd
cargo test -p kuberic-operator --test deployed_schema
```

## Placement observability

The operator serves Prometheus text exposition at `/metrics` on
`0.0.0.0:8081`. Set `KUBERIC_METRICS_BIND` to another IP/port, or to `disabled`
to turn off the listener. This endpoint is unauthenticated; expose it only to
trusted monitoring clients using network policy.

Each scrape lists durable `KubericSet` status, including all list pages. Kubernetes
read failures/timeouts return HTTP 503, never an apparently successful empty
inventory. Metrics describe the **last persisted observation**, not live Pod
health. They are gauges, not cumulative Events or rebalance counters:

| Metric (prefix `kuberic_placement_`) | Meaning |
|---|---|
| `status_available` | 1 if placement status exists; otherwise 0 |
| `replicas` | Replica count per topology domain |
| `missing_topology_replicas`, `unschedulable_replicas` | Observed scheduling/topology problems |
| `primary_info`, `target_info` | Current/desired node and domain, value 1 |
| `decision`, `improvement` | Current decision reason and candidate score |
| `scheduling_decision` | Independent replica scheduling/topology diagnostic, value 1; omitted when absent |
| `cooldown_remaining_seconds` | Configured cooldown minus elapsed time since the durable timestamp, floored at 0 |
| `last_rebalance_timestamp_seconds` | Durable last rebalance Unix timestamp |
| `rebalance_outcome` | Latest retained matching switchover outcome, value 1 |
| `topology_ready`, `primary_balanced` | Durable conditions: True=1, False=0, Unknown=-1 |

Labels are limited to `namespace`, `set`, `node`, `domain`, and a fixed set of
`reason` values. Pod/operation IDs and free-form messages are not labels.
Scheduling reasons are limited to `RequiredTopologyViolation`,
`RequiredTopologyUnverified`, `Unschedulable`, `SchedulingPending`, and `Unknown`;
unrecognized scheduler reasons map to `Unknown`. Scheduling diagnostic transitions
and their clearance publish Events independently of the primary-balancing decision;
message-only changes do not. Use `topology_ready` for readiness, including Unknown
when required topology cannot be verified or scheduling is pending.
Missing optional values/conditions have no sample; an empty domain on a known
node means unknown topology. Deleted objects and superseded labels disappear
on the next successful scrape. Outcome gauges retain only the latest matching
operation; they are omitted when that evidence has been replaced, and cannot
be used as historical success/failure counts.

Placement and rebalance Events are published only after successful reconciliation
and a fresh read of the durable status. Message/timer/progress-only changes do not
publish Events. Publication failures log warnings without rolling back status;
Events are best-effort notifications, not an exactly-once audit log or guaranteed
replay after a crash.

The operator service account needs cluster-wide `get/list/watch` on
`kubericsets` and `create/patch` on `events` in API group `events.k8s.io`.
To scrape through a Service, expose container port 8081 named `metrics` and
route a Service port to it (adjust both when overriding the bind port).

## Planned Node Maintenance

An external coordinator creates a cluster-scoped `NodeMaintenanceRequest` for
one node and event. Kuberic moves primaries through its durable switchover path,
attests surviving write quorum, and reports `KubericPrepared=True`. The coordinator
owns cordon, eviction, infrastructure execution, and the final provider acknowledgment.

The immutable recovery policies are `spec.nodeRecovery` (`Return` or
`MayDisappear`) and `spec.replicaRecovery` (`Preserve` or `Rebuild`), defaulting to
`Return` and `Preserve`. Provider-specific operation names remain coordinator
metadata, not controller behavior.

Set `spec.desiredState` to `Complete` or `Cancel` to release the request. Placement
remains excluded through `Releasing` until the node and affected replica sets
have recovered. An absent Node can waive only Node readiness, never workload
recovery. Deletion follows the same guarded release path through a finalizer:
it cancels `Prepare` but preserves an existing `Complete` decision. Failed and
expired requests also require explicit release.

See [Node maintenance](../docs/features/node-maintenance.md) for the lifecycle,
replacement/reimage rules, Events, and the external AKS Scheduled Events bridge
contract. Metrics are deferred to a separate feature.

## Architecture

| Module | Purpose |
|--------|---------|
| `crd.rs` | `KubericSet` CRD definition and status types |
| `reconciler.rs` | Main reconcile loop — pod management, lifecycle orchestration |
| `cluster_api.rs` | Kubernetes API helpers for pod/service operations |
| `node_maintenance/` | Request lifecycle, discovery, safety attestation, guarded release, and Events |
| `placement_observability.rs` | Durable-status placement gauges, HTTP endpoint, and transition Events |
