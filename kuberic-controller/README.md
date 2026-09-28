# kuberic-controller

Kubernetes Failover Manager-equivalent for the independent level-triggered
Kuberic stack.

The controller watches `operator.kuberic.io/v1alpha1` `KubericSet` resources
and their owned Pods, PVCs, Services, and Secrets. Each reconcile observes and
normalizes the complete state, calls the pure evaluator in
`kuberic-protocol`, applies independent resource convergence, and dispatches
at most one fenced authority-changing command. Stable, waiting, and unsafe
states all use bounded re-observation.

The controller owns configuration selection, failure timing, routing fences,
replacement provisioning, status projection, and optimistic-concurrency
acceptance. It does not own replica-local effect sequencing, replication
quorum credit, application durability, or a workflow phase journal.

Planned switchover uses `spec.switchover.requestId` and
`spec.switchover.targetReplicaId` (a committed logical secondary ID). Acceptance
freezes exact identities in `status.transition.switchover`; the active request
cannot be cancelled or retargeted. `status.lastSwitchover` retains the latest
terminal receipt; admission rejections use `SwitchoverRejected` conditions.
Authority acceptance precedes write grant and exact-Pod routing publication.
Restoration, newer-epoch compensation, and Unsafe closure remain evidence-gated.

Lowering `spec.replicas` enables SF-inspired secondary scale-down using PC/CC
quorum principles, with Kuberic-specific target/minimum coupling, deterministic
selection, write closure, sequential cleanup, and Kubernetes resource deletion.
Desired count is target and minimum (floor one): a Kuberic policy choice, not
general SF semantics; SF target and minimum are independently configurable.
One frozen highest-ID committed secondary is removed at a time. Before intent,
routing removal, or write closure, retained exact members must provide the
previous read quorum under stable accepted current-only authority and fresh
exact sessions. `ScaleDownRetainedReadQuorumUnavailable` preserves existing
routing/access with bounded re-observation, without preparation, replacement,
or another target; after heal the same highest-ID target is selected. For 2→1
the primary alone suffices. Post-freeze previous-read/reduced-write evidence precedes atomic reduced
topology/policy and `secondaryScaleDownCleanup` acceptance. Separate local commit,
write grant, and routing can restore service while cleanup is pending.
Exact-name GETs and frozen UID/fresh resource-version deletes enforce
endpoint→Pod→PVC cleanup. An unreachable target is fenced only after commit
by exact Pod deletion; PVC object deletion waits for observed Pod absence, with
no retention or import path and no physical storage erasure guarantee.
Exact original PVC provenance must be reconstructable before admission. If Pod
and PVC already disappeared and their mapping/generation cannot be reconstructed,
scale-down waits/fails closed; list omission is not absence. Unavailable-target
support requires frozen or reconstructable exact cleanup identity.
`lastSecondaryRemoval` retains convergence proof, never deletion authority.
Before replacing that bounded receipt, every retained member needs its original
completed current-only witness or fresh proof of completed local acceptance.

Replacement freezes `pendingReplacementCleanup` before provisioning and moves
it unchanged to `lastReplacement` at commit. Both replacement and scale-down
serialize subsequent operations until exact cleanup completes; label loss,
finalizers, and ambiguous replies cannot overwrite the obligation or authorize
deletion of same-name replacement UIDs.

Protocol 8 and agent store schema 3 require a fresh coordinated deployment.
The production controller configuration enables sequential scale-up: it
persists one allocation, creates/freeze the annotated canonical PVC before the
bound Pod, builds from the exact accepted primary through a post-enumeration
catch-up boundary, and admits one fresh ordinal through independent PC/CC
policies. Cancellation before admission and exact endpoint→Pod→PVC cleanup use
frozen provenance; after admission, accepted authority rolls forward. Classic
v1 remains unchanged. Primary/explicit-target removal, independent target and
minimum counts, a validated maximum replica-count budget, and active-removal
cancellation are unsupported. There is no maximum write-interruption guarantee.
Frozen-primary loss during removal or cleanup can cause indefinite outage;
there is no overlapping failover even after membership commit.
See the [deferred follow-ups](../docs/proposal/v1-retirement-plan.md#deferred-scale-down-follow-ups)
for provenance, status/API redesign, helper refactors and recovery/policy expansion.

Deployment assets are under `deploy/` and are intentionally isolated from the
classic `kuberic.io/v1` operator. The controller and sample images are
development/CI artifacts and are not currently published release targets.

See the [level-triggered operator guide](../docs/features/kuberic/level-triggered-operator.md)
for the CRD, deployment, supported operations, diagnostics, and limitations.
The guide includes the [request example and retry contract](../docs/features/kuberic/level-triggered-operator.md#planned-switchover).
See also [secondary scale-down usage and conditions](../docs/features/kuberic/level-triggered-operator.md#secondary-scale-down).
See [sequential scale-up usage and diagnostics](../docs/features/kuberic/level-triggered-operator.md#sequential-scale-up)
for fresh-incarnation restoration, write-availability boundaries, failover,
cleanup, and limitations.
