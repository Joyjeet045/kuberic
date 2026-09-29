# kvstore2

`kvstore2` is the conformance application for the independent level-triggered
Kuberic stack. It implements `StatefulServiceReplica`, `StateProvider`, and
the durable default-engine storage adapter, then delegates replica-process
assembly to `kuberic-agent::ReplicaHost`.

The HTTP API is:

- `PUT /kv/{key}` with a UTF-8 request body, returning the committed LSN;
- `GET /kv/{key}`;
- `GET /status` for exact replica, authority, progress, access, and build
  diagnostics. Its additive `retired` boolean distinguishes terminal retirement
  from an ordinary role-None/access-denied state without exposing authority
  certificates; older responses may omit this field.

Application state lives under `/var/lib/kuberic/application`; agent authority
lives separately under `/var/lib/kuberic/.kuberic/agent.sqlite3`.

The controller supports explicit named-target planned switchover using a
request ID and a committed logical secondary ID. Writes and connections may
be briefly interrupted; reconnect through the write Service and retry ambiguous
writes only according to application idempotence. A retained former-primary
client cannot commit while its write authority is revoked.

Lowering `spec.replicas` supports secondary-only 3→2 and 2→1, or sequential
larger reductions, preserving the primary and acknowledged values. Desired
count is target and minimum by Kuberic policy, not general SF semantics (SF
configures them independently); a singleton has no redundancy. This is
SF-inspired secondary scale-down, not general scaling parity. Retained read-quorum
preflight preserves existing routing/access if admission must wait, without an
alternate target or replacement. Exact original PVC provenance must be
reconstructable before admission; otherwise pre-request Pod/PVC disappearance
waits/fails closed. Unavailable-target support requires frozen or reconstructable
cleanup identity. Scale-down deletes the removed replica's PVC object, Pod, and peer endpoint after
authority commit. Sequential scale-up is supported; primary removal and active
removal cancellation are unsupported.
Increasing `spec.replicas` adds or restores one missing ordinal at a time with
fresh PVC/Pod/process/build authority. The accepted set can remain writable
during healthy provisioning, copy, catch-up, and PC/CC only while both
configuration quorums remain authorized. A failed or cancelled unadmitted
candidate is cleaned endpoint→Pod→PVC and retried fresh; no maximum completion
time or supported replica-count budget is promised.
There is no PVC retention or import path, nor a physical storage erasure promise.
Frozen-primary loss during removal/cleanup can block service indefinitely.
Expect HTTP 503/disconnects during convergence, with no interruption-duration
guarantee. Protocol 8 / store schema 4 require fresh deployment, not data migration.

After installing into an explicitly owned KinD cluster, run:

```bash
just level-triggered-kind-test switchover
just level-triggered-kind-test switchover-adversarial
just level-triggered-kind-test scale-down
just level-triggered-kind-test scale-down-adversarial
just level-triggered-kind-test scale-up
just level-triggered-kind-test scale-up-multi
just level-triggered-kind-test scale-up-adversarial
```

This example is local/CI-only. See the
[level-triggered operator guide](../../docs/features/kuberic/level-triggered-operator.md)
for KinD deployment, the
[request example and outcomes](../../docs/features/kuberic/level-triggered-operator.md#planned-switchover),
[scale-down examples and cleanup](../../docs/features/kuberic/level-triggered-operator.md#secondary-scale-down),
[scale-up examples and recovery](../../docs/features/kuberic/level-triggered-operator.md#sequential-scale-up),
and diagnostics.
