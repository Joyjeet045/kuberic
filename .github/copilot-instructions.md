# Copilot Instructions — Kuberic

## What This Is

A stateful Kubernetes system combining CloudNativePG (CNPG) and Service
Fabric (SF) concepts. It uses a Kubernetes-native, level-triggered operator
model with quorum-based replication, automatic failover, switchover,
copy-based replica building, and epoch-based fencing.

## API stability
This project is unstable, and breaking API changes are allowed.
Do not change the `Replicator`, `PrimaryReplicator`, or `StateProvider`
interfaces.

## PostgreSQL tests
Use `just nextest-postgres-smoke` for routine checks. Run the full
`just nextest-postgres` suite only when explicitly requested.
