Install kind on windows
```ps1
winget install Kubernetes.kind
```

Add alias for kind in bashrc
```sh
alias kind="<kind location>/kind.exe"
```

For multiple KVStore applications sharing one loopback host port, see the
[Envoy Gateway KinD reference](features/envoy-gateway-kind.md). It uses a
dedicated cluster and leaves the direct NodePort example unchanged.