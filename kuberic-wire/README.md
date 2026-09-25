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
Protocol version 4 binds every control command dispatch to the exact observed
target process session and adds the planned-switchover command and handoff wire
contracts used by canonical request and receipt authority. These contracts now
have supported evaluator, agent, and runtime execution: `PrepareSwitchover`
returns durable write-closed handoff evidence, and configuration commands carry
the certificate and exact preparation-retirement IDs. They are no longer
reserved for a future execution phase.
Protocol version 5 adds the accepted spec's `preparationGeneration` to
preparation commands and handoff certificates (including reports). Retirement
IDs now pair the operation ID with that generation. Deterministic preparation
identity binds the generation and starting configuration as well as the exact
source and target. The agent durably retains an authority-bound retirement
high-water mark, rejecting every earlier generation across repeated restorations
and process restarts without an unbounded tombstone history.

`kuberic-wire` contains transport definitions only; protocol decisions remain
in `kuberic-protocol`.

Control and peer commands carry exact resource, replica-incarnation,
generation, epoch, and PC/CC authority. Replication and copy streams also carry
sender and receiver process-session IDs; the agent rejects retired sessions
before runtime mutation. Agent reports include independent read/write access,
quorum/catch-up progress, deactivation evidence, load/fault reports, and
pending or retained command identity.
Reports also expose retained preparation evidence. A durable operation can be
replayed after restart, but each dispatch must use the freshly observed target
process session; the session is an envelope fence, not part of its durable ID.
Deactivation evidence carries its own epoch with the LSN so later
configurations cannot relabel historical evidence.

## Integration boundary

This crate does not negotiate or downgrade versions. Protocol version 5 is an
exact coordinated-deployment boundary; incompatible controller, agent, or
replica peers are rejected. Authentication, DNS resolution, retry policy, and
session registration are agent-owned transport concerns around these schemas.
Version 4 switchover-bearing persistent records lack the generation proof
(including retired certificates). No in-place migration of those records is
provided; incompatible persisted authority fails closed rather than inventing
a generation. This remains a coordinated deployment, not a rolling-upgrade
contract.

Raw `currentProgress` is application/repair evidence. Only applied
authority-bound acknowledgements, validated `verifiedReplicationLsn`
certificates, and completed build handoffs may become remote quorum credit.

See the [level-triggered operator guide](../docs/features/kuberic/level-triggered-operator.md)
for deployment and failure behavior.
