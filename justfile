cluster_name := env_var_or_default("KIND_CLUSTER_NAME", "")
kubeconfig := env_var_or_default("KUBECONFIG", "")
cluster_context := "kind-" + cluster_name
kind_config := env_var_or_default("KIND_CONFIG", "deploy/kind-config.yaml")
ownership_receipt := kubeconfig + ".kuberic-owner"
level_token := env_var_or_default("KUBERIC_AGENT_BEARER_TOKEN", "")

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

# Build the isolated level-triggered packages.
level-triggered-build:
    cargo build -p kuberic-controller -p kvstore2 -p kuberic-level-tests

# Build and load only the level-triggered controller and application images.
level-triggered-images: verify-kind-context
    docker build -t localhost/kuberic-controller:level-triggered-v1 \
        -f kuberic-controller/Dockerfile .
    docker build -t localhost/kvstore2:level-triggered-v1 \
        -f examples/kvstore2/deploy/Dockerfile .
    kind load docker-image localhost/kuberic-controller:level-triggered-v1 --name {{ cluster_name }}
    kind load docker-image localhost/kvstore2:level-triggered-v1 --name {{ cluster_name }}

# Install the level-triggered controller and sample without changing classic v1.
level-triggered-install: verify-kind-context
    test -n "{{ level_token }}"
    kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" \
        create namespace kuberic-system --dry-run=client -o yaml | \
        kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" apply -f -
    kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" \
        -n kuberic-system create secret generic kuberic-agent-credentials \
        --from-literal=bearer-token="{{ level_token }}" \
        --dry-run=client -o yaml | \
        kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" apply -f -
    kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" \
        apply -k kuberic-controller/deploy
    kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" \
        -n kuberic-system set image deployment/kuberic-controller \
        controller=localhost/kuberic-controller:level-triggered-v1
    kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" \
        -n kuberic-system rollout status deployment/kuberic-controller --timeout=180s
    kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" \
        apply -f examples/kvstore2/deploy/sample.yaml

# Run one or more explicit isolated level-triggered KinD scenarios.
level-triggered-kind-test *scenarios: verify-kind-context
    #!/usr/bin/env bash
    set -euo pipefail
    requested=({{ scenarios }})
    if [[ ${#requested[@]} -eq 0 ]]; then
      requested=(all)
    fi
    expanded=()
    for scenario in "${requested[@]}"; do
      if [[ "$scenario" == "all" ]]; then
        expanded+=(replacement quorum-loss adversarial)
      else
        expanded+=("$scenario")
      fi
    done
    for scenario in "${expanded[@]}"; do
      case "$scenario" in
        bootstrap) test_name="level_triggered_k8s::bootstrap_reaches_three_member_topology_and_quorum_write" ;;
        replacement) test_name="level_triggered_k8s::replacement_preserves_quorum_write_and_retires_old_incarnation" ;;
        failover) test_name="level_triggered_k8s::failover_fences_old_primary_and_preserves_committed_data" ;;
        quorum-loss) test_name="level_triggered_k8s::quorum_loss_closes_writes_and_recovers_without_data_loss_epoch_change" ;;
        adversarial) test_name="level_triggered_k8s::adversarial_restart_partition_and_healing_preserve_single_writer" ;;
        *) echo "unknown level-triggered scenario: $scenario" >&2; exit 2 ;;
      esac
      cargo test -p kuberic-level-tests "$test_name" -- --ignored --exact --nocapture
    done

# Collect level-triggered controller, resource, and replica diagnostics.
level-triggered-diagnostics: verify-kind-context
    kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" \
        -n kuberic-system logs deployment/kuberic-controller --all-containers --tail=-1 || true
    kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" \
        -n default get kubericset kvstore2 -o yaml || true
    kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" \
        -n default get pods,pvc,services -o wide || true
    kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" \
        -n default get events --sort-by=.lastTimestamp || true
    kubectl --kubeconfig "{{ kubeconfig }}" --context "{{ cluster_context }}" \
        -n default logs -l operator.kuberic.io/set-name=kvstore2 --all-containers --tail=-1 || true
