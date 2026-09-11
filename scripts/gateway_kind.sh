#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
readonly ENVOY_GATEWAY_VERSION=v1.9.1
readonly GATEWAY_API_VERSION=v1.6.1

: "${KIND_CLUSTER_NAME:?Set an isolated KIND_CLUSTER_NAME}"
: "${KUBECONFIG:?Set an isolated KUBECONFIG}"
: "${KUBE_CONTEXT:?Set the isolated KUBE_CONTEXT}"
[[ "$KUBE_CONTEXT" == "kind-${KIND_CLUSTER_NAME}" ]]
just verify-kind-context
kubectl_cmd=(kubectl --kubeconfig "$KUBECONFIG" --context "$KUBE_CONTEXT" --request-timeout=30s)
helm_cmd=(helm --kubeconfig "$KUBECONFIG" --kube-context "$KUBE_CONTEXT")

diagnostics() {
    for namespace in xedio envoy-gateway-system; do
        for resource in gateways grpcroutes envoyproxies services endpointslices pods deployments events kubericsets; do
            "${kubectl_cmd[@]}" get "$resource" -n "$namespace" -o yaml || true
        done
        "${kubectl_cmd[@]}" logs -n "$namespace" --all-containers=true --prefix=true \
            -l app.kubernetes.io/name=envoy-gateway --tail=200 || true
        "${kubectl_cmd[@]}" logs -n "$namespace" --all-containers=true --prefix=true \
            -l gateway.envoyproxy.io/owning-gateway-name=kuberic --tail=200 || true
    done
    "${kubectl_cmd[@]}" get gatewayclasses -o yaml || true
}

if [[ "${1:-}" == diagnostics ]]; then
    diagnostics
    exit 0
fi
[[ "${1:-}" == install ]] || { echo 'Usage: gateway_kind.sh install|diagnostics' >&2; exit 2; }
trap diagnostics ERR

"${kubectl_cmd[@]}" rollout status -n xedio deployment/kuberic-operator --timeout=180s
"${kubectl_cmd[@]}" wait --for=create crd/kubericsets.kuberic.io --timeout=120s
"${kubectl_cmd[@]}" wait --for=condition=Established crd/kubericsets.kuberic.io --timeout=120s

[[ "$(docker port "${KIND_CLUSTER_NAME}-control-plane" 30090/tcp)" == '127.0.0.1:30090' ]] || {
    echo 'Use deploy/gateway/kind-config.yaml: loopback host port 30090 is required.' >&2
    exit 1
}
"${kubectl_cmd[@]}" get services -A -o json | jq -e '
    [.items[] | select(any(.spec.ports[]?; .nodePort == 30090)) |
      select(.metadata.labels["gateway.envoyproxy.io/owning-gateway-name"] != "kuberic" or
             .metadata.labels["gateway.envoyproxy.io/owning-gateway-namespace"] != "xedio")] | length == 0
' >/dev/null || { echo 'NodePort 30090 belongs to another Service; use a separate cluster.' >&2; exit 1; }

temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT
curl --fail --location --retry 3 --connect-timeout 20 --max-time 180 \
    "https://github.com/kubernetes-sigs/gateway-api/releases/download/${GATEWAY_API_VERSION}/experimental-install.yaml" \
    -o "$temporary/gateway-api.yaml"
"${kubectl_cmd[@]}" apply --server-side -f "$temporary/gateway-api.yaml"
"${kubectl_cmd[@]}" get crd gateways.gateway.networking.k8s.io -o json | \
    jq -e --arg version "$GATEWAY_API_VERSION" '.metadata.annotations["gateway.networking.k8s.io/bundle-version"] == $version' >/dev/null
timeout 180 "${helm_cmd[@]}" template eg-crds oci://docker.io/envoyproxy/gateway-crds-helm \
    --version "$ENVOY_GATEWAY_VERSION" --set crds.gatewayAPI.enabled=false \
    --set crds.envoyGateway.enabled=true > "$temporary/envoy-crds.yaml"
"${kubectl_cmd[@]}" apply --server-side -f "$temporary/envoy-crds.yaml"
"${kubectl_cmd[@]}" wait --for=condition=Established --timeout=120s \
    crd/gatewayclasses.gateway.networking.k8s.io crd/gateways.gateway.networking.k8s.io \
    crd/grpcroutes.gateway.networking.k8s.io crd/envoyproxies.gateway.envoyproxy.io
timeout 360 "${helm_cmd[@]}" upgrade --install eg oci://docker.io/envoyproxy/gateway-helm \
    --version "$ENVOY_GATEWAY_VERSION" --namespace envoy-gateway-system --create-namespace \
    --set crds.enabled=false --wait --timeout 5m
"${kubectl_cmd[@]}" wait -n envoy-gateway-system deployment/envoy-gateway \
    --for=condition=Available --timeout=120s
"${kubectl_cmd[@]}" apply -f deploy/gateway/applications.yaml
"${kubectl_cmd[@]}" apply -f deploy/gateway/resources.yaml
"${kubectl_cmd[@]}" wait gatewayclass/kuberic-envoy --for=condition=Accepted --timeout=120s
"${kubectl_cmd[@]}" wait -n xedio gateway/kuberic --for=condition=Programmed --timeout=180s