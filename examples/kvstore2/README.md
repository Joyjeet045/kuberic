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

The controller supports explicit named-target planned switchover using a
request ID and a committed logical secondary ID. Writes and connections may
be briefly interrupted; reconnect through the write Service and retry ambiguous
writes only according to application idempotence. A retained former-primary
client cannot commit while its write authority is revoked.

After installing into an explicitly owned KinD cluster, run:

```bash
just level-triggered-kind-test switchover
just level-triggered-kind-test switchover-adversarial
```

This example is local/CI-only. See the
[level-triggered operator guide](../../docs/features/kuberic/level-triggered-operator.md)
for KinD deployment, the
[request example and outcomes](../../docs/features/kuberic/level-triggered-operator.md#planned-switchover),
and diagnostics.
