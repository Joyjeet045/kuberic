cluster_name := env_var_or_default("KIND_CLUSTER_NAME", "")
kubeconfig := env_var_or_default("KUBECONFIG", "")
cluster_context := "kind-" + cluster_name
kind_config := env_var_or_default("KIND_CONFIG", "deploy/kind-config.yaml")
ownership_receipt := kubeconfig + ".kuberic-owner"

# Build and load all container images into Kind.
default: images

# Create the local Kind cluster and write its kubeconfig.
create-kind-cluster:
    test -n "{{ cluster_name }}"
    test -n "{{ kubeconfig }}"
    test "{{ cluster_name }}" != "kind"
    test "{{ kubeconfig }}" != "$HOME/.kube/config"
    test "$(printf %s "{{ cluster_name }}" | wc -c)" -le 40
    kind create cluster --name {{ cluster_name }} \
        --config "{{ kind_config }}" \
        --kubeconfig "{{ kubeconfig }}"
    kind export kubeconfig --name {{ cluster_name }} --kubeconfig "{{ kubeconfig }}"
    printf '%s\n' \
        "cluster={{ cluster_name }}" \
        "context={{ cluster_context }}" \
        "kubeconfig={{ kubeconfig }}" \
        | install -m 600 /dev/stdin "{{ ownership_receipt }}"
    just verify-kind-context

# Verify the exact cluster/kubeconfig pair was created by this workflow.
verify-kind-ownership:
    test -n "{{ cluster_name }}"
    test -n "{{ kubeconfig }}"
    test -f "{{ ownership_receipt }}"
    grep -Fx "cluster={{ cluster_name }}" "{{ ownership_receipt }}"
    grep -Fx "context={{ cluster_context }}" "{{ ownership_receipt }}"
    grep -Fx "kubeconfig={{ kubeconfig }}" "{{ ownership_receipt }}"

# Verify every Kubernetes mutation targets the dedicated Kind checkout.
verify-kind-context: verify-kind-ownership
    test "$(kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" config current-context)" = "{{ cluster_context }}"
    kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" cluster-info

# Delete the local Kind cluster.
delete-kind-cluster: verify-kind-context
    kind delete cluster --name {{ cluster_name }}
    rm -f "{{ kubeconfig }}" "{{ ownership_receipt }}"

# Build all workspace binaries used by the container images.
build-rust-bins:
    cargo build --bins --workspace

# Build and load all container images.
images: kuberic-operator-image kvstore-image

# Build and load the kuberic-operator image.
kuberic-operator-image: verify-kind-context build-rust-bins
    docker build -t localhost/kuberic-operator \
        -f kuberic-operator/deploy/Dockerfile .
    kind load docker-image localhost/kuberic-operator:latest --name {{ cluster_name }}

# Deploy kuberic-operator.
kuberic-operator-deploy: verify-kind-context
    kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" \
        apply -f kuberic-operator/deploy/deployment.yaml

# Delete kuberic-operator.
kuberic-operator-delete: verify-kind-context
    kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" \
        delete -f kuberic-operator/deploy/deployment.yaml

# Build and load the kvstore image.
kvstore-image: verify-kind-context build-rust-bins
    docker build -t localhost/kvstore \
        -f examples/kvstore/deploy/Dockerfile .
    kind load docker-image localhost/kvstore:latest --name {{ cluster_name }}

# Deploy the KVStore applications through the shared Gateway.
kvstore-deploy: gateway-install

# Download and verify the pinned external test dependencies.
prepare-external-dependencies:
    bash scripts/external_dependencies.sh prepare

# Verify the prepared dependency bundle without network access.
verify-external-dependencies:
    bash scripts/external_dependencies.sh verify

# Delete the KVStore applications and their Gateway routes.
kvstore-delete: verify-kind-context
    kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" \
        delete -f deploy/gateway/resources.yaml -f deploy/gateway/applications.yaml

# Install the pinned Gateway and both KVStore applications in the owned cluster.
gateway-install: verify-external-dependencies verify-kind-context kuberic-operator-deploy
    timeout --kill-after=15s 15m bash scripts/gateway_kind.sh install

# Run the KVStore Gateway integration scenario.
gateway-test: verify-kind-context
    cargo test -p kuberic-tests gateway_k8s::test_gateway_k8s_multi_application -- --exact --nocapture

# Collect Gateway and application diagnostics from the owned cluster.
gateway-diagnostics: verify-kind-context
    timeout --kill-after=5s 180s bash scripts/gateway_kind.sh diagnostics
