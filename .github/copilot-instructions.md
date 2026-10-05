# Copilot Instructions — Kuberic

## What This Is

A Service Fabric-inspired stateful replication system for Kubernetes.
Provides quorum-based replication with automatic failover, switchover,
copy-based replica building, and epoch-based fencing.

## API stability
This project is unstable, and breaking API changes are allowed.

## PostgreSQL tests
Use `just nextest-postgres-smoke` for routine checks. Run the full
`just nextest-postgres` suite only when explicitly requested.
