# kvstore2

`kvstore2` is the conformance application for the independent level-triggered
Kuberic stack. It implements `StatefulServiceReplica`, `StateProvider`, and
the durable default-engine storage adapter, then delegates replica-process
assembly to `kuberic-agent::ReplicaHost`.

The HTTP API is:

- `PUT /kv/{key}` with a UTF-8 request body, returning the committed LSN;
- `GET /kv/{key}`;
- `GET /status` for exact replica, authority, progress, access, and build
  diagnostics.

Application state lives under `/var/lib/kuberic/application`; agent authority
lives separately under `/var/lib/kuberic/.kuberic/agent.sqlite3`.

This example is local/CI-only. See the
[level-triggered operator guide](../../docs/features/kuberic/level-triggered-operator.md)
for KinD deployment and test commands.
