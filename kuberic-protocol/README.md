# kuberic-protocol

Pure domain model for the level-triggered Kuberic stack.

The crate defines:

- replica identity, epoch, PC/CC, topology, provisioning, and transition types;
- normalized Kubernetes and replica-agent observations;
- fenced protocol commands and reconciliation plans;
- validation for quorum, incarnation, epoch, and transition invariants;
- the deterministic, side-effect-free evaluator.

It intentionally has no Kubernetes, gRPC, async runtime, or filesystem
dependencies. Controllers and agents exchange these canonical types through
transport adapters such as `kuberic-wire`.

The evaluator covers write-closed bootstrap, same-cardinality replacement,
ordinary failover, and quorum loss. Failover persists exact failure timing,
fences routing before newer authority, requires PC and outstanding-CC read
quorum, selects from epoch-fenced deactivation/progress evidence, authorizes
only the elected safe prefix under the new fence, performs retained-history or
full-copy repair, and accepts only current-only quorum evidence. Quorum loss
publishes `NoWriteQuorum` without changing the data-loss epoch and restores
access when the same configuration quorum returns.

Configuration JSON omits duplicated `primaryId`, derives it from the unique
`Primary` member, and flattens each member's exact identity fields beside its
role. Legacy nested-member and explicit-primary JSON remains readable for
durable metadata compatibility.

## Integration boundary

The controller is the only owner of desired-cluster evaluation. Agents and
runtimes may validate canonical protocol values, but they do not select a new
configuration or infer authority from Kubernetes readiness, routing, or raw
application progress.

The supported evaluator contract is fixed-cardinality bootstrap,
same-cardinality replacement, ordinary failover, and non-destructive quorum
loss/recovery. Scaling, planned switchover, timed replica dropping, destructive
data-loss recovery, and mixed-version negotiation remain fail-closed.

See the [level-triggered operator guide](../docs/features/kuberic/level-triggered-operator.md)
for the complete operational contract.
