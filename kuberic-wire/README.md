# kuberic-wire

Versioned protobuf and tonic contracts for the level-triggered Kuberic stack.

This crate owns the new control-plane, replica-peer, and replication schemas.
It validates wire messages before converting them into
`kuberic-protocol` authority types.

Control and peer messages are defined in `proto/kuberic.proto`; exact-target
replication items, acknowledgements, and streaming RPCs are defined separately
in `proto/replication.proto`.

The wire contract carries exact replica incarnation, durable agent generation,
epoch, configuration identity, protocol version, and replication progress.
Unknown versions, enum values, missing fields, and contradictory authority are
rejected rather than defaulted.

Protocol version 2 adds explicit primary access targets
(`ReconfigurationPending`, `NoWriteQuorum`, or `Granted`), multiple build
retirements for carried replacement/failover repair, the election-safe LSN,
and failover build authority.
Protocol version 3 adds the authority-bound `verifiedReplicationLsn`
certificate used to close failover catch-up without treating raw application
progress as quorum credit.

`kuberic-wire` contains transport definitions only; protocol decisions remain
in `kuberic-protocol`.

Control and peer commands carry exact resource, replica-incarnation,
generation, epoch, and PC/CC authority. Replication and copy streams also carry
sender and receiver process-session IDs; the agent rejects retired sessions
before runtime mutation. Agent reports include independent read/write access,
quorum/catch-up progress, deactivation evidence, load/fault reports, and
pending or retained command identity.
Deactivation evidence carries its own epoch with the LSN so later
configurations cannot relabel historical evidence.

## Integration boundary

This crate does not negotiate or downgrade versions. Protocol version 3 is an
exact coordinated-deployment boundary; incompatible controller, agent, or
replica peers are rejected. Authentication, DNS resolution, retry policy, and
session registration are agent-owned transport concerns around these schemas.

Raw `currentProgress` is application/repair evidence. Only applied
authority-bound acknowledgements, validated `verifiedReplicationLsn`
certificates, and completed build handoffs may become remote quorum credit.

See the [level-triggered operator guide](../docs/features/kuberic/level-triggered-operator.md)
for deployment and failure behavior.
