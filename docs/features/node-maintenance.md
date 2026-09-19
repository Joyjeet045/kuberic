# Coordinated Node Maintenance

`NodeMaintenanceRequest` is the provider-neutral contract for planned node
reboots, OS upgrades, reimages, replacements, and shutdowns. It is cluster-scoped
because a node can host KubericSets in multiple namespaces. Kuberic is the sole
writer of request status; an administrator or external coordinator owns spec.
No per-KubericSet maintenance flags are required.

This completes the request lifecycle introduced by PRs #45 and #58 for issue #43.
The operator does not implement a production infrastructure bridge or use Service
Fabric's private Azure infrastructure-job protocol. That protocol is not a public
AKS operator integration point.

## Request and Preparation

Create one request per maintenance event and target node:

```yaml
apiVersion: kuberic.io/v1alpha1
kind: NodeMaintenanceRequest
metadata:
  name: worker-04-reboot-event-123
spec:
  nodeName: worker-04
  operation: Reboot
  desiredState: Prepare
  provider: Manual
  providerEventId: event-123
  deadline: "2026-10-01T21:00:00Z"
```

Operations are `Reboot`, `OsUpgrade`, `Reimage`, `Replace`, and `Shutdown`.
`Prepare` and `Reboot` are defaults. Optional `notBefore` is the earliest time to
**begin preparation**, not the time at which infrastructure may disrupt the node.
`deadline` bounds preparation; it does not expire an already prepared request.

The operator installs `kuberic.io/node-maintenance` before preparation, records
the Node UID and affected replicas, excludes the node from planned primary
placement, and uses the existing durable switchover workflow. Preparation needs
a healthy primary and attested write quorum outside the node. Missing or
contradictory evidence never counts as success. An empty node needs no switchover.

Initial partition creation, including resuming its durable checkpoint, pauses
while a target pod is on an excluded node. A failed maintenance lookup stops
planned placement. An already completed targetPrimary does not pin the primary
to a node that now needs maintenance.

A coordinator may use the acknowledgment only when all of the following match
its freshly read request and target node:

- The request UID is the one it created, and deletion has not started.
- `spec.desiredState` and `status.observedDesiredState` are both `Prepare`.
- `status.observedGeneration` equals `metadata.generation`.
- `status.nodeUid` equals the current Node UID.
- `status.phase` is `Prepared` and `KubericPrepared=True`.

Readiness can be retracted if topology or quorum changes. Recheck immediately
before drain and provider acknowledgment; never treat an old condition or Event
as a perpetual permit. `Blocked`, `Failed`, or `Expired` never authorizes drain.
Examples of reasons include `BlockedByQuorum`, `NoEligibleTarget`,
`NodeIncarnationChanged`, and `DeadlineExceeded`.

The request identity (`nodeName`, operation, provider, providerEventId, and
notBefore) is immutable. A release decision cannot revert to Prepare or change
between Complete and Cancel. Use a new request for a new event. The deadline and
release Node UID can be corrected without replacing the request.

## Completion and Cancellation

After infrastructure work and recovery, the coordinator changes only spec:

```sh
kubectl patch nmr worker-04-reboot-event-123 --type=merge \
  -p '{"spec":{"desiredState":"Complete"}}'
kubectl wait nmr worker-04-reboot-event-123 \
  --for=jsonpath='{.status.phase}'=Released --timeout=5m
```

Use `Cancel` instead if the event was withdrawn before execution. Production
coordinators should include the observed request UID and resourceVersion as
preconditions on spec updates, and verify observedGeneration after waiting.

1. Kuberic durably enters `Releasing`, clears preparedAt, and publishes
   `KubericPrepared=False` before restoring placement.
2. The target Node must be Ready and not deleting. It may remain cordoned:
   Kuberic does not own cordon, taints, or uncordon.
3. Affected and currently hosted replica sets must attest a settled primary and
   write quorum. Returning replicas must have joined the committed topology and
   be healthy. Concurrent scale, failover, or switchover delays release with
   `ConflictingOperation`; incomplete rebuilding reports `ReplicaRecoveryIncomplete`.
   Cancellation may resume initial creation paused by this request. Completion
   also permits a newly appearing, never-prepared set to resume creation after
   the node is Ready; previously prepared sets must pass recovery checks.
4. Kuberic persists `Released`, releasedAt, releasedNodeUid, and the observed
   desired state. Only then is this request's placement exclusion removed and
   its finalizer cleared. The original nodeUid remains an audit record.

The coordinator can then uncordon **only a cordon it owns**, after checking other
workload gates and all remaining maintenance requests. One released request does
not override another active request or ordinary node eligibility checks.

Deleting a Prepare request means cancellation, not force release. An existing
Complete or Cancel decision is preserved, so deletion cannot skip reimage recovery.
The finalizer holds deletion until the same checks pass. A stuck deletion can still
need replica recovery or an explicit replacement UID. Do not manually strip the
finalizer to bypass recovery. Failed and expired requests retain their placement
exclusion until Complete, Cancel, or guarded deletion releases them.

If the Node no longer exists, cancellation can finish without restoring any node.
Completion for Replace or Shutdown can also finish for a removed node. Reboot,
OsUpgrade, and Reimage completion wait for the node to return. Supplying a
releaseNodeUid always means that exact node incarnation must exist and be Ready.

## Replacement and Reimage

A same-name Node with a different UID never inherits preparation evidence.
Preparation blocks on `NodeIncarnationChanged`. During release, first confirm
infrastructure recovery and then explicitly acknowledge the new UID:

```yaml
apiVersion: kuberic.io/v1alpha1
kind: NodeMaintenanceRequest
metadata:
  name: worker-04-replace-event-456
spec:
  nodeName: worker-04
  operation: Replace
  desiredState: Complete
  provider: Manual
  providerEventId: event-456
  releaseNodeUid: 87b66909-8286-4aae-b4a5-73cbdb26c85d
```

This example shows the final desired spec of an existing Replace request, not a
substitute for creating and preparing it before maintenance. Read the UID from
the recovered Node; do not copy the example value. A second replacement invalidates
that confirmation and release waits again.

Reimage and Replace can lose local data even if a Node name, and sometimes its
UID, survives. On completion, recorded pre-maintenance Pod UIDs must no longer be
present. Replacement replicas must be rebuilt and attest healthy committed
membership. Kuberic does not delete PVCs, wipe storage, or invent data-loss epochs
as part of maintenance. Use the normal replica-recovery procedure; the request
stays Releasing if recovery cannot be proven. Cancellation does not require
rebuilding replicas for an operation that never happened.

The first Prepared transition freezes `status.preparedSets` for recovery checks.
Discovery retains previously affected sets and Pod UIDs in affectedSets while
adding new ones. Draining the node does not turn the request into an empty-node
success: surviving quorum is still checked and loss of it retracts Prepared.
The frozen preparedSets inventory prevents rebuilt replicas from being mistaken
for pre-maintenance replicas during release.

## Duplicate Delivery and Restarts

Use a deterministic DNS-safe request name derived from the provider event ID and
target Node identity. On an AlreadyExists response, verify the existing immutable
identity and resume it; never recreate or reset its status. Arbitrarily different
names are distinct requests even if providerEventId is identical.

The coordinator should serialize work per node, for example using a Kubernetes
Lease. Kuberic conservatively combines overlapping placement exclusions. It does
not cancel another coordinator's request when one completes.

All release progress is in status. Reconciliation after a restart repeats reads
and fenced status updates, not infrastructure actions. Writes use the observed
request UID, generation, spec, deletion state, and resourceVersion. A delayed
reconcile cannot overwrite a replacement request or a newer coordinator decision.
Released requests do not restart preparation.

## External AKS Scheduled Events Bridge

The bridge is a separate workload with separate permissions. Consult the current
[Azure Scheduled Events API](https://learn.microsoft.com/en-us/azure/virtual-machines/linux/scheduled-events)
and your AKS/node-pool support policy; not every disruption offers advance notice.

1. Poll node-local IMDS at
   `http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01`
   with `Metadata: true`, bypassing HTTP proxies. The documented polling guidance
   is once per second. Do not treat a failed poll as an empty event list.
2. Map every VM in Resources to its Kubernetes Node using verified providerID
   and VM identity, not a hostname guess. Events can be broadcast to unaffected
   VMs, and one EventId can cover several nodes. Persist this mapping and elect
   one acknowledgment owner for the whole event.
3. Create or resume one Prepare request per affected node. Convert Azure's
   RFC 1123 NotBefore timestamp to RFC 3339 as the preparation deadline, with a
   conservative drain margin. **Do not copy Azure NotBefore into spec.notBefore:**
   preparation must begin immediately, not when the warning window ends.
4. Cordon the affected nodes and wait for current Kuberic acknowledgments plus
   all other workloads' readiness. Coordinate requests that share a replica set;
   individual node acknowledgments are not proof that several nodes can be lost
   simultaneously. Serialize such disruptions and recheck the combined survivor
   quorum before proceeding.
5. Drain using the Kubernetes Eviction API, respect PDBs and graceful termination,
   and wait for application Close/flush and termination to finish. KubericPrepared
   attests topology, not completion of every application's termination hook.
6. Only after preparation and drain succeed for **all** Resources, the elected
   owner may POST `{"StartRequests":[{"EventId":"the-event-id"}]}` to IMDS with
   `Metadata: true`. This authorizes Azure to start the event for all listed VMs.
   It is not an indefinite veto or a per-node acknowledgment. A missed deadline
   or Started event uses the coordinator's explicit failure policy, not fabricated
   readiness or silent force eviction.
7. Track Started and eventual removal from successful IMDS responses. Removal
   before Started can mean cancellation; an ambiguous observation or a bridge
   restart needs infrastructure evidence. After successful execution, wait for
   Node and replica recovery and set Complete. For confirmed cancellation, set
   Cancel. Include releaseNodeUid when the node incarnation changed.
8. Wait for Released at the current generation, restore only coordinator-owned
   scheduling restrictions after checking other active requests, and retain the
   request as an audit record or delete it after release.

Typical intent mapping is Reboot to Reboot, Redeploy with local-state loss to
Reimage/Replace, and Preempt or Terminate to Shutdown/Replace according to the
actual infrastructure plan. Freeze requires a coordinator policy; it is not
automatically safe merely because a duration estimate is short. OS upgrade and
reimage notifications depend on the supported VM/node-pool configuration.

The bridge needs request create/get/list/watch/patch, Node read and scheduling
patch access, pod list/get, pod eviction, PDB read, and Lease access if used. Grant
those permissions to the bridge, not the Kuberic operator. The operator has no
Azure credentials, IMDS polling, Node patch, eviction, or whole-node acknowledgment
responsibility. Deploy and secure the bridge separately; this repository provides
the contract, not a production bridge implementation.

## Operations and Observability

Spread replicas across nodes and failure domains with topology spread or
anti-affinity. Keep enough healthy replicas outside every proposed disruption to
meet the configured write quorum. PDBs must reflect that quorum and use actual
readiness signals. Set terminationGracePeriodSeconds long enough for the
application's Close/flush, replica shutdown, and connection draining. A PDB or a
preStop hook alone is not a maintenance acknowledgment protocol.

```sh
kubectl get nmr
kubectl describe nmr worker-04-reboot-event-123
kubectl get nmr worker-04-reboot-event-123 -o yaml
kubectl -n xedio port-forward deployment/kuberic-operator 8081:8081
```

Scrape `http://127.0.0.1:8081/metrics` through that port-forward, or configure an
internal pod scrape on the named metrics port. KUBERIC_METRICS_ADDR defaults to
0.0.0.0:8081. Do not expose it through a public Gateway; metrics have no separate
authentication endpoint.

Metrics use only bounded operation, phase, reason, and stage labels:

- `kuberic_node_maintenance_status_transitions_total`
- `kuberic_node_maintenance_preparation_duration_seconds`
- `kuberic_node_maintenance_release_duration_seconds`
- `kuberic_node_maintenance_errors_total`

Latency uses persisted timestamps, so an operator restart during preparation or
release does not reset the measured interval. Counters and histograms are
process-local and reset on restart. A new Prepared transition after readiness
retraction records another observation. Status is the durable authority.

Normal Events report preparation, release, Completed, and Cancelled. Warning
Events report blocked, failed, expired, and recovery-blocked outcomes. Unchanged
reconciles do not emit repeated Events. Cluster-scoped request Events are stored
in the default namespace. Event publication failures are logged and counted but
do not roll back a persisted safety transition; Events are best-effort diagnostics.

Planned maintenance is not abrupt-failure recovery. Forced platform actions,
network partitions, and emergency failover still use ordinary quorum, epoch
fencing, and recovery. The placement exclusion covers planned primary-selection
paths; it does not rewrite the core durable emergency-election protocol. The
bridge must not interpret a previous acknowledgment as protection from a later
quorum loss or forced disruption.

## Validation

Focused mock tests run with `cargo test -p kuberic-operator --lib node_maintenance`.
They cover preparation, release retries, cancellation, restart, Node/Pod identity,
unsafe recovery, stale writes, finalizers, schema synchronization, Events, and metrics.

The non-ignored `test_node_maintenance_release_replacement_and_deletion` test runs
with the workspace suite in the [owned Gateway KinD environment](envoy-gateway-kind.md).
It creates uniquely named, unschedulable synthetic Node fixtures, never drains real
worker workloads, and checks the running controller, admission validation, status
fencing, overlapping requests, release, deletion, changed Node UIDs, and Events.

The canonical KinD configuration has one control-plane and two worker nodes.
CI runs the real-replica regression separately after the workspace suite because
it restarts the operator:

```sh
cargo test -p kuberic-tests \
   node_maintenance_k8s::test_real_replicas_survive_node_maintenance \
   -- --ignored --exact --nocapture
```

It pre-places only its fixture pods, one per node, without adding a production
placement policy. It writes through the shared Gateway, prepares the primary's
node, restarts the operator, verifies the durable primary move, cancels an
overlapping secondary request, cordons the node, evicts the old primary through
the Eviction API with a two-of-three PDB, waits for replica rebuild, completes
maintenance, and checks every acknowledged value after each transition. Mutations
and cleanup are limited to the owned cluster and fixture identities. Actual Azure
reboots or provider acknowledgments are not performed by these tests.

### Acceptance Coverage

| Issue #43 requirement | Verification |
| --- | --- |
| Provider-neutral API, durable state, status ownership | API/schema tests, Kubernetes admission and stale-write tests |
| Node-wide affected workload and replica discovery | Discovery tests, retained post-drain inventory regression |
| Durable primary move and surviving write quorum | Attestation tests and three-node real-replica Gateway writes |
| No planned primary placement during maintenance | Initial/resumed creation and explicit-target regressions, real primary relocation |
| Fail-closed readiness, insufficient quorum, no eligible target, deadlines | Preflight/safety tests and post-drain quorum-loss regression |
| Cancellation, completion, duplicate delivery, operator restart | Mock lifecycle tests, live API scenario, real-replica restart and overlap scenario |
| Node replacement/reimage and safe restoration | UID-confirmation API test, replica-incarnation and recovery-inventory tests |
| Events, metrics, operational guidance | Event/metrics tests, live controller Events, documented scrape and recovery procedures |
| AKS integration and distinction from abrupt failure | External bridge contract above; no private Service Fabric protocol dependency |

Emergency-election protocol changes, a production Azure bridge, and destructive
storage cleanup remain outside the maintainer-defined three-PR scope. Readiness
does not claim these behaviors are implemented or provide an indefinite platform
maintenance veto.