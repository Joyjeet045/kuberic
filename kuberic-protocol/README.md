# kuberic-protocol

Pure domain model for the level-triggered Kuberic stack.

The crate defines:

- replica identity, epoch, PC/CC, topology, provisioning, and transition types;
- explicit switchover requests, frozen handoffs, and terminal receipts;
- normalized Kubernetes and replica-agent observations;
- fenced protocol commands and reconciliation plans;
- validation for quorum, incarnation, epoch, and transition invariants;
- the deterministic, side-effect-free evaluator.

It intentionally has no Kubernetes, gRPC, async runtime, or filesystem
dependencies. Controllers and agents exchange these canonical types through
transport adapters such as `kuberic-wire`.

The evaluator covers write-closed bootstrap, same-cardinality replacement,
ordinary failover, planned switchover, and quorum loss. Failover persists exact
failure timing, fences routing before newer authority, requires PC and
outstanding-CC read quorum, selects from epoch-fenced deactivation/progress evidence, authorizes
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
same-cardinality replacement, ordinary failover, planned switchover, and
non-destructive quorum loss/recovery. Scaling, timed replica dropping,
destructive data-loss recovery, and mixed-version negotiation remain fail-closed.

Switchover requires a nonempty request ID and a committed logical secondary
ID. Identical active or latest-receipted requests are idempotent; cancellation,
retargeting, and conflicting reuse of that receipt's ID are rejected through
conditions. Only one terminal receipt is retained, not an operation history.
Admission rejection uses `SwitchoverRejected`; the receipt enum's `rejected`
value is reserved and not currently emitted by the evaluator.

Planned switchover freezes an exact source, named target, membership, policy,
and handoff certificate. Definitive target loss before authority admission
retires preparation and restores service at the starting authority. After any
requested authority admission, recovery requires the source's whole retained
certificate and a read quorum, and allocates a strictly newer configuration
epoch without changing the data-loss number. Compensation installs write-closed
PC/CC and current-only authority on every surviving exact participant before
stable write grant and routing. An absent exact secondary is not rebound.
An extant permanently faulted non-primary is first fenced by exact Pod deletion
with its PVC preserved; absence must be re-observed before survivor convergence.
Accepted target outcomes always converge forward.

Restoration durably retires the accepted spec generation and exact preparation ID
even if preparation was never observed, without inventing a handoff boundary.
An authority-bound high-water mark rejects all older/equal retired preparations
across repeated same-authority restorations and restart; deterministic identity
also binds the starting configuration and generation.
The source retains one retired certificate for compensation after current-only
completion but before the cluster receipt is persisted.

Temporary observations wait with bounded requeues. An impossible operation
retains its frozen intent in safety convergence: routing is removed and every
extant possible writer must report closed access under accepted or superseding
authority, or its exact Pod must be observed absent. Safety deletion is Pod-only,
UID/resourceVersion-fenced, and preserves PVCs. The terminal unsafe receipt is
published only after that proof; it never starts ordinary failover or replacement.

See the [level-triggered operator guide](../docs/features/kuberic/level-triggered-operator.md)
for the complete operational contract.
