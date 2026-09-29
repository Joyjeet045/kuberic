Install kind on windows
```ps1
winget install Kubernetes.kind
```

Add alias for kind in bashrc
```sh
alias kind="<kind location>/kind.exe"
```

For multiple KVStore applications sharing one loopback host port, see the
[Envoy Gateway KinD setup](features/envoy-gateway-kind.md). Local development
and CI use this single setup, with two applications behind one Gateway. It
replaces the former single-set NodePort setup. Run
`just prepare-external-dependencies` once before creating or installing the
cluster; subsequent installation uses only the verified local manifests and
Helm charts.

For the independent `operator.kuberic.io/v1alpha1` stack, follow the
[level-triggered deployment and testing guide](features/kuberic/level-triggered-operator.md#local-deployment).
Use fresh protocol-8/schema-4 storage and an owned cluster. The live
`just level-triggered-kind-test scale-down` and `scale-down-adversarial`
selectors exercise secondary removal; `scale-up`, `scale-up-multi`, and
`scale-up-adversarial` exercise sequential membership increase and recovery.
`all` runs the ten-scenario matrix from fresh bootstrap. Standalone failover
needs a separate fresh cluster.
See [tests and diagnostics](features/kuberic/level-triggered-operator.md#tests-and-diagnostics)
for CI tiers, measurements, and cleanup.
