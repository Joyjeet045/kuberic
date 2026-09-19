# Shared Gateway Access in KinD

The development and CI deployment routes two independent KVStore applications through
`127.0.0.1:30090`. The gRPC HTTP/2 authority chooses the application:

```text
127.0.0.1:30090 -> Envoy NodePort 30090 -> Gateway listener 8080
                    | kvstore-a.kuberic.test -> kvstore-a-rw:8080
                    | kvstore-b.kuberic.test -> kvstore-b-rw:8080
```

Both application Services remain `ClusterIP`, selecting their own set's
`kuberic.io/role: primary` pods. The operator continues to manage only its normal
application resources. It does not install Envoy, manage routes, or depend on
Gateway implementation-specific APIs. Gateway and route lifecycle belongs to
the deployment owner.

## Pinned Dependencies

The installer uses **Envoy Gateway v1.9.1** and **Gateway API v1.6.1**, a pairing
from the [Envoy Gateway compatibility matrix](https://gateway.envoyproxy.io/news/releases/matrix/).
Envoy's pinned CRD chart supplies the standard Gateway API bundle and the
Envoy-specific extension CRDs. The controller chart's CRD installation is
disabled to avoid installing a second copy. The chart selects its matching
Envoy Proxy version; the data-plane image is not independently overridden.

`scripts/external-dependencies.json` records the source, version, local filename,
and SHA-256 of every external artifact. Run
`just prepare-external-dependencies` once to populate
`.kuberic-cache/external`; set `KUBERIC_EXTERNAL_DIR` to use another directory.
Preparation checks each file's pinned SHA-256, including files already in the
cache. The chart hashes come from their OCI chart layers. A corrupt cache entry
is downloaded again; failed downloads and checksum mismatches are never
published.

Installation and tests never download these artifacts. They verify that the
local bundle matches the current dependency manifest and fail with preparation
instructions when it is missing, stale, or corrupt. Helm renders and installs
the verified local chart archives without fetching them again. CI restores the
same directory with `actions/cache` and runs preparation as an explicit step.
Container images referenced by the charts remain normal container-runtime
dependencies and may be pulled when they are not already present.

CI uses KinD v0.30.0, Kubernetes v1.34.0 and Helm v3.19.0.
Envoy Gateway and these manifests are reference/test dependencies, not a
production requirement. Production deployments can use a conformant
`GatewayClass` of their choice.

## Run the Reference

Requirements: Linux or a Linux development environment, Docker, KinD, kubectl,
Helm, just, curl, jq, GNU timeout, sha256sum, and the repository Rust/protoc prerequisites.
Port 30090 must be free on the host. This example is plaintext and binds only
to loopback; it does not expose a public authenticated endpoint.

This is the single KinD setup for KVStore, replacing the old single-set
NodePort setup. Only Envoy owns NodePort 30090. Do not apply a direct
application NodePort overlay to this cluster; the installer refuses to take
the port from a different Service.

From the repository root:

```sh
just prepare-external-dependencies

export KIND_CLUSTER_NAME=kuberic-dev
export KUBECONFIG="$(mktemp -d)/kubeconfig"
export KUBE_CONTEXT="kind-${KIND_CLUSTER_NAME}"

just create-kind-cluster
just images
just kvstore-deploy
just gateway-test
```

The default cluster config is `deploy/kind-config.yaml`. `just kvstore-deploy`
runs `just gateway-install`, deploying the Gateway and both KVStore sets.
Run `cargo test --workspace --all-features` for the complete suite, including
the Gateway scenario. CI uses the same cluster and setup for all tests.

The existing isolated-cluster ownership checks apply to every Kubernetes
operation. Do not point these commands at a default or production kubeconfig.
The deployment uses a single `Gateway`, an `EnvoyProxy` that customizes the
generated proxy Service's NodePort, and one `GRPCRoute` per application.

The installer waits for the operator, CRDs, controller, GatewayClass and
Gateway. The test additionally waits for accepted/resolved routes, available
proxy pods, the generated NodePort, healthy application sets and the correct
primary EndpointSlices. Route conditions must refer to the resource's current
generation and the exact Gateway namespace/listener; stale or unrelated
`Accepted` conditions do not count as readiness. All waits are bounded:
installation has a 15-minute overall limit, reconnection has a 90-second
deadline, and diagnostic collection has a 180-second overall limit with
5-second Kubernetes requests. Installation and test failures collect GatewayClass, Gateway,
GRPCRoute, EnvoyProxy, Service, EndpointSlice, pod, deployment, event and
KubericSet diagnostics, along with controller, proxy, operator and application logs.

```sh
just gateway-diagnostics
just delete-kind-cluster
```

Deleting the dedicated cluster also removes the pinned CRDs and controller.
It does not affect another application's or another test's cluster.

## Clients Without Hosts-File Changes

The client must dial loopback while sending the appropriate authority. Tonic
uses an endpoint URI for the connection and an explicit origin for HTTP/2:

```rust
let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:30090")
    .origin("http://kvstore-a.kuberic.test".parse()?)
    .connect()
    .await?;
let client = kvstore::proto::kv_store_client::KvStoreClient::new(channel);
```

For grpcurl, pass the repository proto because KVStore does not require gRPC
server reflection:

```sh
grpcurl -plaintext -authority kvstore-a.kuberic.test \
  -import-path examples/kvstore/proto -proto kvstore.proto \
  -d '{"key":"demo","value":"application-a"}' \
  127.0.0.1:30090 kvstore.v1.KvStore/Put

grpcurl -plaintext -authority kvstore-b.kuberic.test \
  -import-path examples/kvstore/proto -proto kvstore.proto \
  -d '{"key":"demo"}' \
  127.0.0.1:30090 kvstore.v1.KvStore/Get
```

The second call must not see the first application's value. Developers may
optionally configure DNS or local aliases, but neither the test nor CI edits
`/etc/hosts`. Unknown authorities have no matching route and cannot reach a
default application backend.

## Primary Transitions and Validation

The Gateway keeps targeting the stable `<set>-rw` Service when the operator
changes primary labels. EndpointSlices move to the promoted pod. Connections
or requests already in flight can fail during this transition; clients should
reconnect and retry appropriate operations rather than expecting the same
transport connection to survive. Retrying arbitrary non-idempotent writes
requires application-level request identity, not a Gateway guarantee.

The test retries connection failures and gRPC `UNAVAILABLE`, `DEADLINE_EXCEEDED`
or `CANCELLED` responses during reconnection. Permanent errors and data-integrity
failures fail immediately; a later success cannot hide incorrect routing or
missing data. Only idempotent test puts are retried.

The normal workspace test suite runs the Gateway cluster test. The workflow:

1. Installs the pinned dependencies and both independent three-replica sets.
2. Verifies listener, NodePort ownership, route backend and Service selectors.
3. Writes the same key with different values plus set-specific keys, checking
   data isolation and rejection of unknown authorities.
4. Requests switchover for each application in turn, then deletes each set's
   current primary pod with a required UID precondition to exercise unplanned failover.
   For every transition it exercises the existing connection, waits for a
   different primary and the corresponding endpoints, and verifies fresh
   connections through the unchanged authority and host port.
5. Continuously writes and reads through the other application's route during
   every switchover and failover, then verifies both applications' shared and
   application-specific keys survive and remain isolated.

The former single-set NodePort tests are replaced by this two-application
scenario. Cluster-free tests verify authority routing
to a loopback server, exact-parent/current-generation route readiness, transient
retry recovery, permanent/integrity failure handling, and stalled-attempt deadlines:

```sh
cargo test -p kuberic-tests gateway_ -- --skip gateway_k8s::test_gateway_k8s_multi_application
bash scripts/external_dependencies_test.sh
```

## Protocol Scope

- `GRPCRoute` routes gRPC traffic by HTTP/2 authority and optionally service or
  method. This is the reference used here.
- `HTTPRoute` is appropriate for HTTP applications and can route by hostname
  and path.
- `TLSRoute` can multiplex TLS connections using SNI where the selected
  Gateway implementation supports it. Configure TLS and certificates according
  to that implementation; this plaintext example does not configure them.
- Arbitrary plaintext TCP carries no standard hostname or SNI. `TCPRoute`
  therefore cannot generally multiplex applications by hostname on one
  listener. Use separate ports/listeners or a protocol-specific handshake.

Direct NodePort/LoadBalancer exposure and a shared Gateway are complementary.
The managed additional-Service feature in issue #55 is not a prerequisite for
this reference: it uses Kuberic's existing internal read/write Services.