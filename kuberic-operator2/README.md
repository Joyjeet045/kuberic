# kuberic-operator2

Minimal Kubernetes operator that exercises the `KubericSet` reconciliation
boundary through Kuberic DEX.

The operator watches its own alpha API in one namespace, selected by
`KUBERIC_NAMESPACE` (default: `default`):

```yaml
apiVersion: dex.kuberic.io/v1alpha1
kind: KubericSet
metadata:
  name: demo
spec:
  replicas: 3
  image: example:latest
```

Its initial workflow performs one idempotent observation activity, persists
the activity and terminal result in a DEX checkpoint ConfigMap, and projects
the workflow identity and phase into `status.workflow`. It does not yet
create workloads, elect a primary, scale replicas, fail over, or delete
resources.

Run it against the current Kubernetes context:

```bash
cargo run -p kuberic-operator2
```

The minimal in-cluster RBAC and deployment are in
[`deploy/deployment.yaml`](deploy/deployment.yaml). The deployment assumes the
existing `KubericSet` CRD has already been installed.

The classic `kuberic.io/v1` CRD and operator are unchanged. The two operators
watch different API groups and cannot reconcile the same object.
