#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
readonly ENVOY_GATEWAY_VERSION=v1.9.1
readonly GATEWAY_API_VERSION=v1.6.1
readonly artifact_dir="${KUBERIC_DOWNLOAD_DIR:-${CARGO_TARGET_DIR:-target}/downloads}"
readonly gateway_api="$artifact_dir/gateway-api-${GATEWAY_API_VERSION}.yaml"
readonly gateway_crds_chart="$artifact_dir/gateway-crds-helm-${ENVOY_GATEWAY_VERSION}.tgz"
readonly gateway_chart="$artifact_dir/gateway-helm-${ENVOY_GATEWAY_VERSION}.tgz"

download_artifacts() {
    bash scripts/download.sh \
        "https://github.com/kubernetes-sigs/gateway-api/releases/download/${GATEWAY_API_VERSION}/experimental-install.yaml" \
        d7fa77650e4ef28fca0411536fcb5e237deb4d50301cfded3be49d9a1b7bbd02 "$gateway_api"
    bash scripts/download.sh oci://docker.io/envoyproxy/gateway-crds-helm \
        ad2e1215749249ff6c8a2d82fb820c70edbb18a7ffc7331355632fb67b944688 "$gateway_crds_chart" "$ENVOY_GATEWAY_VERSION"
    bash scripts/download.sh oci://docker.io/envoyproxy/gateway-helm \
        68ce74961eeb5fc5e395628d6bbed9305a85b34619bd7ee17b01187f101bb8c5 "$gateway_chart" "$ENVOY_GATEWAY_VERSION"
}

if [[ "${1:-}" == download ]]; then
    download_artifacts
    exit 0
fi

: "${KIND_CLUSTER_NAME:?Set an isolated KIND_CLUSTER_NAME}"
: "${KUBECONFIG:?Set an isolated KUBECONFIG}"
: "${KUBE_CONTEXT:?Set the isolated KUBE_CONTEXT}"
[[ "$KUBE_CONTEXT" == "kind-${KIND_CLUSTER_NAME}" ]]
just verify-kind-context
kubectl_cmd=(kubectl --kubeconfig "$KUBECONFIG" --context "$KUBE_CONTEXT" --request-timeout=30s)
diagnostic_cmd=(kubectl --kubeconfig "$KUBECONFIG" --context "$KUBE_CONTEXT" --request-timeout=5s)
helm_cmd=(helm --kubeconfig "$KUBECONFIG" --kube-context "$KUBE_CONTEXT")

diagnostics() {
    for namespace in xedio envoy-gateway-system; do
        for resource in gateways grpcroutes envoyproxies services endpointslices pods deployments events kubericsets; do
            "${diagnostic_cmd[@]}" get "$resource" -n "$namespace" -o yaml || true
        done
        "${diagnostic_cmd[@]}" logs -n "$namespace" --all-containers=true --prefix=true \
            -l app.kubernetes.io/name=envoy-gateway --tail=200 || true
        "${diagnostic_cmd[@]}" logs -n "$namespace" --all-containers=true --prefix=true \
            -l gateway.envoyproxy.io/owning-gateway-name=kuberic --tail=200 || true
    done
    "${diagnostic_cmd[@]}" get gatewayclasses -o yaml || true
    "${diagnostic_cmd[@]}" logs -n xedio deployment/kuberic-operator --all-containers=true --prefix=true --tail=200 || true
    for application in kvstore-a kvstore-b; do
        "${diagnostic_cmd[@]}" logs -n xedio --all-containers=true --prefix=true \
            -l "kuberic.io/set=${application}" --tail=200 || true
    done
}

if [[ "${1:-}" == diagnostics ]]; then
    diagnostics
    exit 0
fi
[[ "${1:-}" == install ]] || { echo 'Usage: gateway_kind.sh download|install|diagnostics' >&2; exit 2; }
trap diagnostics ERR

"${kubectl_cmd[@]}" rollout status -n xedio deployment/kuberic-operator --timeout=180s
"${kubectl_cmd[@]}" wait --for=create crd/kubericsets.kuberic.io --timeout=120s
"${kubectl_cmd[@]}" wait --for=condition=Established crd/kubericsets.kuberic.io --timeout=120s

[[ "$(docker port "${KIND_CLUSTER_NAME}-control-plane" 30090/tcp)" == '127.0.0.1:30090' ]] || {
    echo 'Use deploy/kind-config.yaml: loopback host port 30090 is required.' >&2
    exit 1
}
"${kubectl_cmd[@]}" get services -A -o json | jq -e '
    [.items[] | select(any(.spec.ports[]?; .nodePort == 30090)) |
      select(.metadata.labels["gateway.envoyproxy.io/owning-gateway-name"] != "kuberic" or
             .metadata.labels["gateway.envoyproxy.io/owning-gateway-namespace"] != "xedio")] | length == 0
' >/dev/null || { echo 'NodePort 30090 belongs to another Service; only Envoy may own it.' >&2; exit 1; }

download_artifacts
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT
"${kubectl_cmd[@]}" apply --server-side -f "$gateway_api"
"${kubectl_cmd[@]}" get crd gateways.gateway.networking.k8s.io -o json | \
    jq -e --arg version "$GATEWAY_API_VERSION" '.metadata.annotations["gateway.networking.k8s.io/bundle-version"] == $version' >/dev/null
timeout 180 "${helm_cmd[@]}" template eg-crds "$gateway_crds_chart" \
    --set crds.gatewayAPI.enabled=false \
    --set crds.envoyGateway.enabled=true > "$temporary/envoy-crds.yaml"
"${kubectl_cmd[@]}" apply --server-side -f "$temporary/envoy-crds.yaml"
"${kubectl_cmd[@]}" wait --for=condition=Established --timeout=120s \
    crd/gatewayclasses.gateway.networking.k8s.io crd/gateways.gateway.networking.k8s.io \
    crd/grpcroutes.gateway.networking.k8s.io crd/envoyproxies.gateway.envoyproxy.io
timeout 360 "${helm_cmd[@]}" upgrade --install eg "$gateway_chart" \
    --namespace envoy-gateway-system --create-namespace \
    --set crds.enabled=false --wait --timeout 5m
"${kubectl_cmd[@]}" wait -n envoy-gateway-system deployment/envoy-gateway \
    --for=condition=Available --timeout=120s
"${kubectl_cmd[@]}" apply -f deploy/gateway/applications.yaml
"${kubectl_cmd[@]}" apply -f deploy/gateway/resources.yaml
"${kubectl_cmd[@]}" wait gatewayclass/kuberic-envoy --for=condition=Accepted --timeout=120s
"${kubectl_cmd[@]}" wait -n xedio gateway/kuberic --for=condition=Programmed --timeout=180s