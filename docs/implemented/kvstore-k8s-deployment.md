# KVStore Kubernetes Deployment

Deploy the kvstore example as a KubericSet in KinD, matching the
existing xdata-app deployment pattern.

## Current State

The xedio stack deploys to KinD via:

```
Just recipes → cargo build → docker build → kind load → kubectl apply
```

Components:
- **xdata-operator**: Deployment (1 replica) watching `XdataApp` CRD
- **xdata-app**: StatefulSet (3 replicas) managed by the operator
- **kuberic-operator**: Deployment (1 replica) watching `KubericSet` CRD
- **kvstore**: 3 bare Pods managed by kuberic-operator
- **kind-config.yaml**: NodePort mappings 30081–30083 (xdata), 30090 (kvstore)

The kuberic-operator creates bare Pods (not StatefulSets) and manages
three ports per pod: app (8080), control (9090), data (9091).

## Architecture

```
┌──────────────────────────────────────────────────────┐
│  KinD Cluster                                        │
│                                                      │
│  ┌──────────────────────┐                            │
│  │ kuberic-operator   │ watches KubericSet      │
│  │ (Deployment, 1 pod)  │ creates pods + services    │
│  └──────┬───────────────┘                            │
│         │ gRPC control (9090)                        │
│         ▼                                            │
│  ┌─────────────┐ ┌─────────────┐ ┌────────────┐     │
│  │ kvstore-0   │ │ kvstore-1   │ │ kvstore-2  │     │
│  │ Primary     │ │ Secondary   │ │ Secondary  │     │
│  │ :8080 app   │ │ :8080 app   │ │ :8080 app  │     │
│  │ :9090 ctrl  │ │ :9090 ctrl  │ │ :9090 ctrl │     │
│  │ :9091 data  │ │ :9091 data  │ │ :9091 data │     │
│  └─────────────┘ └─────────────┘ └────────────┘     │
│         ▲                 ▲              ▲           │
│  ┌──────┴─────────────────┴──────────────┴──────┐    │
│  │  Services (ClusterIP, label-driven)          │    │
│  │  kvstore-rw  → role=primary                  │    │
│  │  kvstore-ro  → role=secondary                │    │
│  │  kvstore-r   → all pods                      │    │
│  └──────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────┘
         │
    NodePort 30090 (test access via KinD extraPortMappings)
```

## Deployment Artifacts

### 1. Dockerfile — `examples/kvstore/deploy/Dockerfile`

Follow xdata-app pattern: ubuntu:24.04 base, copy pre-built debug
binary, non-root user (UID 10000), expose ports.

```dockerfile
FROM ubuntu:24.04
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
RUN useradd -m -u 10000 appuser
WORKDIR /app
COPY target/debug/kvstore /app/kvstore
RUN chown -R appuser:appuser /app
USER appuser
EXPOSE 8080 9090 9091
CMD ["/app/kvstore"]
```

### 2. KubericSet CRD manifests

The kuberic-operator already has a `KubericSet` CRD and reconciler
that creates pods with the right ports. We only need an example CR.

**`examples/kvstore/deploy/kubericset.yaml`**:

```yaml
apiVersion: kuberic.io/v1
kind: KubericSet
metadata:
  name: kvstore
  namespace: xedio
spec:
  replicas: 3
  image: localhost/kvstore:latest
  port: 8080
  controlPort: 9090
  dataPort: 9091
  failoverDelay: 5
```

### 3. KinD config — `deploy/kind-config.yaml` (repo root)

Single canonical config for the whole repo. Includes NodePort
`extraPortMappings` for xdata-app (30081–30083) and kvstore (30090).

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      # xdata-app pods
      - containerPort: 30081
        hostPort: 30081
      - containerPort: 30082
        hostPort: 30082
      - containerPort: 30083
        hostPort: 30083
      # kvstore-rw client port
      - containerPort: 30090
        hostPort: 30090
```

### 4. Just recipes — `justfile`

```just
kvstore-image: build-rust-bins
    docker build -t localhost/kvstore \
        -f examples/kvstore/deploy/Dockerfile .
    kind load docker-image localhost/kvstore:latest --name kind

kvstore-deploy:
    kubectl apply -f examples/kvstore/deploy/kubericset.yaml

kvstore-delete:
    kubectl delete -f examples/kvstore/deploy/kubericset.yaml
```

The root `justfile` also defines the Kind cluster and operator image recipes.

### 5. CI changes — `.github/workflows/CI.yml`

Update the kind config path (or keep shared). Add kvstore image build
alongside xdata images. The kuberic-tests crate can use the same
`ensure_*_deployed()` pattern for kvstore.

## kvstore Binary Configuration

The binary supports fixed port binding for container use via env vars.
Defaults remain `127.0.0.1:0` (random port, localhost-only) for backward
compatibility with in-process tests.

| Env Var | Container Value | Default | Purpose |
|---------|----------------|---------|---------|
| `KUBERIC_CONTROL_BIND` | `0.0.0.0:9090` | `127.0.0.1:0` | Operator reaches pod via pod IP |
| `KUBERIC_DATA_BIND` | `0.0.0.0:9091` | `127.0.0.1:0` | Peer pods reach each other |
| `KUBERIC_CLIENT_BIND` | `0.0.0.0:8080` | `127.0.0.1:0` | Clients reach via Service |
| `KUBERIC_REPLICA_ID` | `<pod_index + 1>` | `1` | 1-based replica ID |

The `build_pod()` function in the reconciler sets these env vars in the
container spec. The kvstore binary uses clap `env` attribute for seamless
env var / CLI arg integration.

**Replica ID convention**: `KUBERIC_REPLICA_ID` is **1-based** (matching
the driver's `ReplicaId = pod_index + 1`). The pod label
`kuberic.io/pod-index` is 0-based (for PVC/pod naming).

## Services (CNPG Pattern)

The operator creates CNPG-style label-driven ClusterIP services.
Pod labels are managed by the operator; Kubernetes endpoint controller
automatically re-routes traffic on failover.

### Pod Labels

```
kuberic.io/set: kvstore
kuberic.io/role: primary | secondary
kuberic.io/pod-index: "0" | "1" | "2"
```

On failover/switchover, labels are updated in a specific order:

1. **Promote first**: Set new primary's label to `primary`
2. **Demote second**: Set old primary's label to `secondary`

This ensures the `-rw` service always has at least one endpoint.

### Services (created by operator in Creating phase)

| Service | Selector | Routes To |
|---------|----------|-----------|
| `kvstore-rw` | `kuberic.io/set=kvstore, kuberic.io/role=primary` | Primary only |
| `kvstore-ro` | `kuberic.io/set=kvstore, kuberic.io/role=secondary` | Secondaries only |
| `kvstore-r` | `kuberic.io/set=kvstore` | All pods |

All are ClusterIP services (no NodePort). Port mappings:
- Port 8080 → client gRPC
- Port 9090 → control gRPC (operator use)
- Port 9091 → data gRPC (inter-pod replication)

### Dev/Test Access via NodePort

A separate `nodeport-svc.yaml` overlay converts `kvstore-rw` to a
NodePort service (port 30090) for dev/test access from the host.
This is applied by the integration tests automatically. The KinD
config includes `extraPortMappings` so NodePort 30090 is reachable
at `localhost:30090`.

```bash
# Apply NodePort overlay (done automatically by tests)
kubectl apply -f examples/kvstore/deploy/nodeport-svc.yaml

# Connect via gRPC
grpcurl -plaintext localhost:30090 kvstore.v1.KvStore/Get
```

> **Note**: `kubectl port-forward` is unreliable in WSL2/KinD due to
> container runtime network namespace issues. NodePort with
> `extraPortMappings` bypasses the kubelet forwarding path entirely
> and uses kube-proxy iptables rules instead.

## Data Address Registration

The operator reads `pod.Status.PodIP` from the Kubernetes API and
constructs `GrpcReplicaHandle` addresses in `create_replica_handle()`
and `build_member_status()`.

**Invariant**: Only read PodIP from pods that pass `is_pod_ready()`.
The reconciler enforces this via the `ready_pods` filter.

## Persistent Storage (PVC)

The operator manages PVCs directly (one per pod, CNPG pattern).
PVCs persist across pod restarts for data recovery.

### PVC Lifecycle

| Scenario | PVC State | Behavior |
|----------|-----------|----------|
| Pod crash/restart | PVC exists | Re-attach, kvstore reads existing data |
| Pod deleted | PVC exists | Operator creates new pod, attaches PVC |
| PVC deleted | Gone | Fresh PVC + pod, copy from primary rebuilds state |
| CR deleted | Depends on policy | `pvcRetentionPolicy: Delete` cleans up; `Retain` preserves |

### PVC Spec

PVCs use labels (no `ownerReferences`) to prevent cascade deletion:

```yaml
metadata:
  name: kvstore-0-data
  labels:
    kuberic.io/set: kvstore
    kuberic.io/pod-index: "0"
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 256Mi  # from KubericSetSpec.storage
```

### CRD Fields

```rust
pub storage: String,                         // e.g. "256Mi"
pub pvc_retention_policy: PvcRetentionPolicy, // Delete (default) | Retain
```

### Idempotent Creation

`ensure_pvc()` and `ensure_pod()` check existence before creating —
safe to re-run after operator crash mid-creation. Scale-up calls
`ensure_pvc` before `ensure_pod` for each new index.

### Readiness and Liveness Probes

TCP probes on the control port (9090):

- **Readiness**: initialDelay=5s, period=5s, failureThreshold=2
- **Liveness**: initialDelay=10s, period=10s, failureThreshold=3

### RBAC

```yaml
- apiGroups: [""]
  resources: ["persistentvolumeclaims"]
  verbs: ["get", "list", "watch", "create", "delete"]
- apiGroups: [""]
  resources: ["services"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

## Deployment Workflow

```
# One-time setup
just create-kind-cluster

# Build and load images
just images
  → builds rust binaries
  → builds kvstore docker image
  → builds kuberic-operator docker image
  → loads both into kind

# Deploy
just kuberic-operator-deploy
just kvstore-deploy

# Verify
kubectl get kubericsets -n xedio
kubectl get pods -n xedio
```

## Implementation Summary

Implemented in two PRs:

### PR 1: Operator Infrastructure (done)

- ClusterApi trait: 7 new PVC/Service methods + KubeClusterApi impls
- `build_pod()`: typed `k8s_openapi` builders with probes, env, labels
- `build_pvc()`, `ensure_pvc()`, `ensure_pod()`: idempotent creation
- `create_services()`: rw/ro/r ClusterIP services
- CRD: `storage` and `pvc_retention_policy` fields
- Mock KvClusterApi: HashMap PVC/Service storage
- Tests: PVC/Service assertions + idempotent creation test (38 tests pass)

### PR 2: KVStore K8s Deployment (done)

- kvstore Dockerfile + Just recipes
- kuberic-operator Dockerfile + manifests (CRD, RBAC, Deployment)
- Binary env var support via clap `env` attribute
- KinD config consolidated at `deploy/kind-config.yaml` with NodePort mappings
- NodePort service overlay for dev/test access (port 30090)
- K8s integration tests: status check + gRPC Put/Get round-trip

## Open Questions

1. **Operator CRD extensions**: Should `KubericSetSpec` grow fields
   for `env`/custom volume mounts, or keep it minimal?

2. ~~**RWO volume fencing**~~: Deferred to multi-node support.

3. ~~**PVC retention policy**~~: Resolved — `pvcRetentionPolicy` field
   (Delete default, Retain option).

4. ~~**Stale-epoch PVC reattach**~~: Resolved — copy protocol
   overwrites stale data.

5. ~~**`build_pod()` typed builders**~~: Resolved — implemented with
   `k8s_openapi` typed builders.

6. ~~**KinD config consolidation**~~: Resolved — single canonical
   config with NodePort mappings for all workloads.
