# kuberic-agent

Replica-local hosting and durable authority for the level-triggered Kuberic
stack.

`kuberic-agent` owns the Service Fabric Replica Agent/FUP-equivalent process
boundary:

- application lifetime, `Open` registration, role/close/abort ordering, and
  exact returned-replicator identity;
- one SQLite metadata database under `.kuberic/agent.sqlite3` on the replica
  PVC, separate from application data;
- exact resource, Pod, PVC, replica-incarnation, durable-generation,
  initialization, policy, and schema identity;
- intent-before-effect and terminal-result-before-reply ordering;
- restart recovery from pending intent or retained terminal evidence;
- ephemeral process sessions and session-scoped report sequences.

The database is created only by an authorized `InitializeAgentStore` path.
Missing established metadata, corruption, incompatible schema, or identity
mismatch fails closed instead of creating empty authority. SQLite uses WAL and
`synchronous=FULL`; the agent is the single writer.

Primary promotion follows the SF host sequence: replicator role, epoch and
state-provider update, then application role. Abort stops the returned control
before application teardown. The default replicator's managed capability is
transferred directly into agent registration and is not returned to
application code.

The agent now owns fenced `EnsureConfiguration` admission, durable private
Demote/GetLSN/Catchup/Deactivate/Activate stages, restart-safe runtime effect
sequencing, independent read/write access, partition/load/fault reports,
separate authenticated control and replication listeners, fresh process
sessions, stale-session rejection, protobuf conversion, and bounded reliable
send windows. Missing retained replication payloads explicitly require full
copy rather than overstating catch-up capability.

Authority replacement requires a newer epoch, except for the exact
Previous/Current-to-current-only completion of one admitted transition.
Changed primary authority remains access-closed until provider epoch and
catch-up postconditions complete. Catch-up waits permit ACK progress, and
matching commands are serialized and revalidated at each durable stage.
Failover commands update the selected primary epoch before GetLSN, retain
deactivation epoch/LSN evidence, durably authorize only the elected safe
prefix under the new fence, and can publish `NoWriteQuorum` independently from
role. Configured lagging members may receive an explicitly authorized
full-copy build before current-only completion.

Serving starts fail-closed listeners before reconstructing live hosting from
durable authority, role, access, pending effects, and retained stage evidence.
Session replacement holds a delivery lease through runtime mutation. Shutdown
revokes readiness and aborts the owned runtime and workers.

Build authorization is admitted by the agent before source copy execution.
Dropping a returned copy stream cancels blocked provider iteration and releases
the build. Reports retry until durable authority and live progress form one
compatible snapshot and carry deactivation epoch with its LSN.

The controller remains responsible for selecting configurations,
re-observing command postconditions, routing fences, and distributing
deployment authentication material. Concrete outbound peer dialing is wired
through the agent's `OutboundDispatcher` contract.

## Storage and recovery contract

`ReplicaHost` places metadata at
`<data-root>/.kuberic/agent.sqlite3`; application state remains under an
application-owned sibling directory. SQLite uses WAL, `synchronous=FULL`,
foreign keys, disabled automatic WAL checkpoints, and a five-second busy
timeout. The agent is the single writer.

The PVC filesystem must provide durable fsync and directory-entry semantics
and SQLite WAL locking/shared memory. Opening an established store validates
integrity, exact storage identity, and the exact schema version. The current
migration hook is idempotent only for that version; it is not an older-schema
upgrade path.

Crash-boundary environment variables exist only in the test executable.
Production agent and application binaries expose no fault-injection mode.

See the
[level-triggered operator guide](../docs/features/kuberic/level-triggered-operator.md)
for identity semantics, deployment, diagnostics, and unsupported recovery.
