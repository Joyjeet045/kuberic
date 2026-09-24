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

Deployment assets are under `deploy/` and are intentionally isolated from the
classic `kuberic.io/v1` operator. The controller and sample images are
development/CI artifacts and are not currently published release targets.

See the [level-triggered operator guide](../docs/features/kuberic/level-triggered-operator.md)
for the CRD, deployment, supported operations, diagnostics, and limitations.
