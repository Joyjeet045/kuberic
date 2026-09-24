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
