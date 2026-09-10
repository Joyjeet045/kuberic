# ADR: Pod-Local RA-Lite Control Boundary

> **Status:** Implemented
> **Date:** 2026-08-31

## Problem

`PodRuntime` previously combined manager-protocol correlation with ordered
service and replicator effects. Process restart under the same Kubernetes Pod
UID could therefore erase correlation evidence without changing the replica
incarnation.

Multiple mutation RPCs also allowed callers to bypass the correlated,
generation-fenced admission path.

## Decision

Use one production control path:

```
operator → ExecuteCorrelatedControlAction → ReplicaAgent → PodRuntime
```

`GetStatus` is the only other public `ReplicatorControl` RPC. The removed
individual mutation methods and `ExecuteDurableAction` have no compatibility
shim. Their protobuf request shapes remain only where needed as action-oneof
payloads, and removed status tags and names are reserved.

`ReplicaAgent` exclusively owns:

- protocol-version, replica-incarnation, process-generation,
  control-version, runtime-epoch, action-ID, and signature admission;
- one active action and 16 retained terminal observations;
- duplicate replay, conflict detection, and local serialization;
- generation-qualified runtime completion tokens; and
- bounded local fault and typed, normalized error publication.

`PodRuntime` exclusively owns ordered open, role, epoch, configuration,
catch-up, build/remove, data-loss, write-revocation, and close effects. It has
no manager action ledger.

## Status and Fencing

Every supported pod reports required replica-agent status with numeric control
protocol version 3, `AgentGeneration`, `AgentControlVersion`,
`current_action`, and `retained_terminal_actions`. Missing, malformed, or
unsupported agent status is rejected; it is not interpreted as an old peer.

Before dispatch, the operator requires matching addressed, runtime, and
pending Pod incarnations. It persists the observed generation, control
version, and runtime epoch during a no-activity reconcile. Direct non-add
and non-remove actions also persist an exact encoded payload. Agent-owned
add/build and removal use their structured CRD intents directly and have no
duplicate payload projection. The next reconcile calls only
`ExecuteCorrelatedControlAction`.

This is a coordinated operator/runtime deployment boundary. Mixed control
versions do not interoperate. Deployments must quiesce durable topology work
and update the operator and replica runtimes together.

## Recovery and Replay

CRD status remains the only durable global store. The operator retains
write-ahead intent, deterministic signatures, resource-version fencing,
observation-first recovery, one transition or one activity per reconcile, and
all existing commit, compensation, and roll-forward boundaries.

The authoritative local ledger is `current_action` plus
`retained_terminal_actions`. Exact duplicates replay without another effect.
A retained signature conflict performs no effect. Eviction is deterministic
and bounded; an old control-version request for an unretained action fails as
continuity unavailable. There is no exactly-once claim or durable local
history.

A same-Pod process restart changes `AgentGeneration` and publishes no inherited
action state. Old-generation requests fail before effects. The operator
evaluates durable postconditions before any at-least-once redrive.

## Consequences

- There is one fenced mutation API and one local observation ledger.
- Old control-plane clients and runtimes require coordinated replacement.
- ReplicaAgent remains a local acceptance/replay and bounded transient
  coordinator boundary, not a durable distributed workflow engine or global
  topology authority.
- PodRuntime callback ordering and typed completion remain unchanged.
- Fault and terminal histories are bounded, volatile evidence.
- Replica add/build and removal compose this boundary with the internal
  `ReplicaLifecyclePeer` v2 service. Add/build uses
  Prepare/Activate/Cleanup; removal uses Retire. Shared peer primitives do not
  merge their workflows or expose arbitrary remote runtime actions.
- Primary-agent add/build and removal coordination is transient and bounded;
  it does not add a durable local workflow store.

## Service Fabric Mapping

The ownership split follows the useful SF RA/RAProxy distinction: the local RA
accepts and fences commands while the runtime proxy executes ordered effects.
Kuberic deliberately does not copy RA's local durable failover-unit store;
Kubernetes CRD status and the operator remain globally authoritative.
