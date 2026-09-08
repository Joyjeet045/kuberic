# Kuberic: Test Strategy

How the Kuberic project is tested — test layers, infrastructure,
what each layer validates, and known gaps.

> Part of the [Kuberic Design](../kuberic-replicator-design.md).

---

## Test Layers

The project uses three testing layers, each with different scope and
fidelity. Higher layers exercise more integration but are slower and
harder to debug.

```
Layer 3: Reconciler E2E integration tests
         └─ KvClusterApi/KvPod → real PodRuntime + KV service per pod
            Full reconciler state machine, real gRPC, real replication

Layer 2: Stable-recovery tests
         └─ Read-only PartitionDriver + status-only ReplicaHandle
            Snapshot identity, epoch, role, and quorum validation

Layer 1: Component unit tests
         └─ QuorumTracker, NoopReplicator, KubericRuntime, ReplicaAgent, PodRuntime
            Individual component behavior in isolation
```

---

## Layer 1: Component Unit Tests

Test individual components in isolation. Fast, deterministic.

### QuorumTracker (`replicator/quorum.rs` — 16 tests)

| Test | What It Validates |
|------|-------------------|
| `test_single_replica_commits_immediately` | Primary alone satisfies quorum=1 |
| `test_three_replicas_quorum` | 3-replica set, quorum=2, commit on 2nd ACK |
| `test_dual_config_quorum` | During reconfig: must satisfy BOTH CC and PC quorum |
| `test_out_of_order_acks` | ACKs arriving for higher LSN before lower LSN |
| `test_fail_all` | Role change / close fails all pending operations |
| `test_must_catch_up_enforcement` | Write mode: specific replica must individually ACK |
| `test_wait_catch_up_all_mode` | All mode: every member must ACK |
| Timeout tests (3) | Operation expiration, ACK boundary, late ACK safety, independent later writes |
| Catch-up timeout tests (3) | Waiter expiration, active-attempt failure and retry baseline |
| Configuration safety tests (3) | Deadline preservation, duplicate catch-up safety, quorum-relaxation commit |

**Infrastructure:** Direct `QuorumTracker` construction, no actors or gRPC.

### NoopReplicator (`noop.rs` — 3 tests)

| Test | What It Validates |
|------|-------------------|
| `test_noop_lifecycle` | Open → ChangeRole → Close lifecycle |
| `test_noop_replicate_handle` | StateReplicatorHandle::replicate() works |
| `test_noop_replicate_not_primary` | replicate() before promotion returns NotPrimary |

**Infrastructure:** `KubericRuntime` with `NoopReplicator` (no quorum,
no gRPC). Tests the event loop and handle APIs.

### KubericRuntime (`runtime.rs` — 3 tests)

| Test | What It Validates |
|------|-------------------|
| `test_runtime_full_lifecycle` | Full lifecycle with real `WalReplicatorActor` |
| `test_runtime_replicate_before_promote` | replicate() blocked until Primary role |
| `test_runtime_abort` | Abort event stops the runtime |

**Infrastructure:** `KubericRuntime` with `WalReplicatorActor` (real
quorum tracking, no gRPC).

### PodRuntime (`pod.rs`)

| Test | What It Validates |
|------|-------------------|
| `correlated_control_preserves_runtime_lifecycle_ordering` | Correlated Open → role changes → write revocation → Close through the sole production mutation path |

**Infrastructure:** `PodRuntime::builder()` with real gRPC servers. Tests
the dual-channel event delivery (lifecycle + state_provider) and the
command routing from gRPC → ReplicaAgent → PodRuntime → replicator + user.
Tracked background copy/quorum completion, cancellation, and responsive status
are exercised indirectly by the high-fidelity reconciler add/rebuild tests and
directly by the quorum tracker cancellation test.

### ReplicaAgent (`replica_agent.rs`)

The agent suite uses effect-channel harnesses plus real gRPC coverage. It
checks:

- exact duplicate in-progress and terminal replay;
- retained action-ID/signature conflict;
- strict version, target incarnation, generation, control-version and runtime
  epoch fences;
- continuity-unavailable behavior after bounded eviction;
- 16-entry terminal/fault retention and 1,024-byte UTF-8 error bounds;
- late execution-token rejection and best-effort fault saturation;
- same-Pod new-process generation with no inherited action state; and
- lifecycle-peer duplicate/conflict/version/identity fencing;
- coarse removal admission, progress/result validation, pre/post-commit
  sequencing, exact connection cleanup, compensation, and responsive status;
- Retire ordering, sender/parent/target/epoch/configuration/deadline fences,
  exact duplicate replay, restart recovery, and bounded peer retention; and
- missing/malformed/unsupported protocol rejection and transport error
  classes.

---

## Layer 2: Stable-Recovery Unit Tests

`PartitionDriver` is read-only. Its tests prove stable snapshot recovery calls
only `GetStatus`, validates identity/epoch/role/quorum, and round-trips the
authoritative snapshot. Mutable driver workflow tests were removed with the
retired production bypass.

---

## Layer 3: Integration & E2E Tests

Test the full stack with `GrpcReplicaHandle`, real `PodRuntime` pods, real
copy/replication streams, real user state management, and the durable
reconciler state machines. The test handle exposes only
`execute_correlated_control_action`.

### Reconciler E2E Tests

**File:** `examples/kvstore/src/reconciler_tests.rs` — 4 tests

Test the full reconciler state machine driving real pods. `KvClusterApi`
implements `ClusterApi` by spawning real `PodRuntime` + KV service pods.
Also supports `mark_pod_not_ready()` for testing failure detection paths.

| Test | What It Validates |
|------|-------------------|
| `test_reconciler_creates_partition_and_serves_kv` | Full Pending→Creating→Healthy flow. Write KV data, read from another pod. |
| `test_reconciler_switchover` | Switchover via targetPrimary change. Verify old primary rejects writes. |
| `test_reconciler_creating_waits_for_ready` | Creating phase requeues when pods are not ready (no transition to Healthy). |
| `test_reconciler_detects_primary_failure_and_fails_over` | Healthy detects NotReady primary → FailingOver → failover completes → Healthy with new primary. Verifies pre-crash data survives and new primary accepts writes. |
| `test_durable_failover_recovers_lost_replies_and_restarts` | Replaces controller state at each step and loses replies for epoch, promotion, configuration, quorum, and election-configuration actions. |
| `test_durable_failover_negotiates_data_loss_after_accounted_quorum_loss` | Invalid live evidence makes read quorum conclusively unavailable; verifies epoch advance and `OnDataLoss`. |
| `test_durable_failover_data_loss_state_changed_and_failure` | Exercises real runtime callback no-change/state-changed/error handling and fail-closed rejection. |
| `test_durable_failover_observes_lost_data_loss_reply` | Loses the callback response after application and resolves typed completion from status. |
| `test_durable_failover_waits_for_unavailable_possible_best_replica` | Persists explicit wait and rotates probes across unavailable possible-best replicas. |
| `test_durable_failover_incarnation_drift_is_phase_fenced` | Rejects confirmed-candidate replacement and rolls forward after post-commit secondary replacement. |
| `test_durable_failover_final_status_lost_reply_reloads_applied_snapshot` | Applies final stable status then loses the API response; authoritative reload prevents duplicate work. |
| `test_stable_metadata_refresh_records_live_configuration` | Records runtime election configuration and exact epoch/incarnation progress into the stable snapshot. |
| `test_reconciler_scale_up` | Healthy phase: spec.replicas increased → creates pods → completes durable correlated add. |
| `test_reconciler_scale_down` | Healthy phase: spec.replicas decreased → completes config-first durable correlated removal. |

### KvPod Helper

Each test spins up `KvPod` instances — a real `PodRuntime` + KV service
event loop + client gRPC server:

```rust
let pod = KvPod::start(id).await;
let handle = pod.replica_handle(id).await;  // GrpcReplicaHandle
let client = connect_kv_client(&pod.client_address).await;
```

### KvClusterApi

Implements `ClusterApi` trait. Instead of creating K8s pods, it spawns
local `KvPod` instances. Also provides `mark_pod_not_ready()` for
testing failure detection paths with real pods:

```rust
impl KvClusterApi {
    fn mark_all_pods_ready(&self) { ... }
    fn mark_pod_not_ready(&self, pod_name: &str) { ... }
}

impl ClusterApi for KvClusterApi {
    async fn create_pod(&self, ...) -> Result<Pod> {
        // Spawns real PodRuntime + KV service
    }
    async fn create_replica_handle(&self, ...) -> Result<Box<dyn ReplicaHandle>> {
        // Returns GrpcReplicaHandle connected to the live pod
    }
}
```

---

## Test Infrastructure Summary

| Component | Purpose | Used By |
|-----------|---------|--------|
| `QuorumTracker` (direct) | Test quorum math in isolation | Layer 1 |
| `NoopReplicator` | Stub replicator for lifecycle tests | Layer 1 |
| `KubericRuntime` | Lower-level harness (no gRPC) | Layer 1 |
| `KvPod` | Real PodRuntime + KV service + client server | Layer 3 |
| `GrpcReplicaHandle` | Real gRPC transport to pods | Layer 3 |
| `KvClusterApi` | Mock ClusterApi backed by real KvPods + readiness control | Layer 3 (reconciler) |

---

## How to Run Tests

```bash
# Meaningful non-cluster suites
cargo test -p kuberic-core -p kuberic-operator -p kvstore -p sqlite-replicated

# Documentation tests
cargo test --doc --workspace

# Core crate only
cargo test -p kuberic-core

# High-fidelity reconciler
cargo test -p kvstore --test reconciler

# Specific durable workflow test
cargo test -p kvstore --test reconciler test_durable_remove_coarse_activation

# With logging (requires test-log crate)
RUST_LOG=info cargo test -p kvstore --test reconciler test_durable_remove_coarse_activation -- --nocapture
```

`kuberic-tests` requires an existing Kubernetes cluster and is not part of the
normal local documentation or workflow gate.

---

## What's Tested vs What's Not

### Well-Tested (Happy Paths)

- ✅ Full create → write → failover → write lifecycle
- ✅ Switchover with old-primary write rejection
- ✅ Scale-up with copy protocol (full state transfer)
- ✅ Scale-down with config-first removal
- ✅ Restart secondary with rebuild
- ✅ Dual-config quorum during reconfiguration
- ✅ must_catch_up enforcement
- ✅ Catch-up baseline (no false catches on historical ops)
- ✅ Reconciler state machine (Pending→Creating→Healthy→FailingOver→Switchover)
- ✅ Reconciler: Creating waits for pod readiness
- ✅ Reconciler: Healthy detects NotReady primary → full failover cycle

### Not Tested (Implemented but Untested Code Paths)

| Gap | What's Missing | Difficulty |
|-----|---------------|------------|
| `remove_replica` (cancel build) | No test cancels an in-progress `build_replica` via `remove_replica`. | Medium |
| Cross-kind sequential operations | Switchover→failover and scale-up→failover combinations beyond the covered double-failover case. | Medium |

### Not Tested (Requires Design Work First)

| Gap | Category | Design Gap Reference |
|-----|----------|---------------------|
| Partial update_epoch failure (some replicas fenced, others not) | Protocol safety | A1 |
| Promotion failure after fencing | Protocol safety | A3 |
| gRPC ordering violations | Protocol safety | A4 |
| Build/catch-up stall detection | Operational | A5 |
| gRPC handle reconnection after pod restart | Operational | B3 |
| Concurrent reconciliation outside durable switchover | Operational | B4 |
| QuorumTracker stale ACK cleanup | Correctness | C1 |
| Zombie primary write rejection (epoch fencing on data plane) | Protocol safety | A2 |
| Mid-reconfiguration handoff into failover | Stable failover and other durable operations are implemented; interruption handoff is future work | D |
| Network partition (pod Ready but gRPC unreachable) | Designed, not impl | D |

### Intentionally Not Tested

- **Real Kubernetes integration** — requires a cluster. Future work:
  kind/minikube-based integration tests.
- **mTLS** — deferred to post-MVP.
- **Large dataset copy** — in-memory state, no multi-GB test fixtures.
- **Performance/latency** — no benchmarks yet. The atomic status reads
  (`PartitionState`) are designed for ~1ns but not benchmarked.

---

## Testing Principles

1. **Layer 2 validates read-only recovery.** Stable snapshot
   identity/epoch/role/quorum invariants are tested without mutation.

2. **Layer 3 integration tests validate the full stack.** These tests
   catch integration issues (gRPC serialization, stream lifecycle,
   copy protocol end-to-end) that Layer 2 cannot. Durable reconciler tests
   drive the sole correlated mutation path.

3. **`KvClusterApi` is the topology integration harness.** It preserves
   deterministic status/activity fault injection while using real
   `GrpcReplicaHandle`, `ReplicaAgent`, `PodRuntime`, quorum tracking, and
   replication streams.

4. **No separate gRPC transport tests.** gRPC transport correctness is
   validated implicitly by Layer 3 tests which use real `GrpcReplicaHandle`
   + real `PodRuntime`. Dedicated gRPC-only tests were removed as they
   covered a strict subset of Layer 3.

5. **Error path testing is the main gap.** Happy paths are well-covered
   across all 3 layers. Error paths (partial failures, stream deaths,
   timeouts, concurrent operations) are almost entirely untested. This
   mirrors the design gaps — error handling design is needed before
   error tests can be written.

---

## Simulating Pod Crash and Restart

### Crash Simulation APIs

| Layer | API | Behavior |
|-------|-----|----------|
| **Driver-level** | `KvPod::crash()` / `SqlitePod::crash()` | Aborts PodRuntime + service owner tasks. Useful for lifecycle tests, but independently spawned replication/drain tasks can survive; do not use it to inject ACK-path loss. |
| **Driver-level** | `KvPod::restart(id)` / `SqlitePod::restart(id)` | Crash + start fresh pod on same `data_dir`. Returns new pod with new ports. |
| **Reconciler-level** | `KvClusterApi::crash_pod(name)` | Aborts tasks, marks Pod NotReady, preserves `data_dir` in `data_dirs` map (PVC simulation). |
| **Reconciler-level** | `KvClusterApi::restart_pod(name)` | Fresh PodRuntime on new ports, reuses saved `data_dir` (PVC re-attach), marks Ready. |
| **Reconciler-level** | `KvClusterApi::restart_process_same_pod_uid(name)` | Fresh agent/runtime process and ports while retaining the Kubernetes Pod UID. |
| **ACK-path failure** | `handle.close()` | Graceful shutdown, not a real crash, but deterministically stops persisted replication ACKs and is used for B0 quorum-loss coverage. |
| **Legacy (low fidelity)** | `mark_pod_not_ready(name)` | Flips readiness flag but LivePod keeps running. |

### Reconciler Health Check (E3 fix)

The reconciler's Healthy phase probes ALL replicas via `get_status()`
on every reconcile cycle. This detects:

- **Epoch mismatch** — pod restarted, reports `epoch = (0,0)` vs driver's current epoch
- **Role = Unknown** — virgin PodRuntime, never received `ChangeRole`
- **gRPC unreachable** — pod crashed, handle is dead

Agent generation is observed for command dispatch fencing, not used as a
Healthy-phase staleness signal. A same-Pod process restart is currently
detected by runtime epoch/role divergence; the distinct generation prevents a
pending old-process command from being accepted by the new process.

The health check runs before switchover processing. A stale primary triggers
FailingOver. A ready secondary with a new incarnation starts the durable
replica-rejoin operation, which retires the old exact primary connection and
rebuilds the replacement without changing the stable snapshot before current
configuration commits.

See `design-gaps.md` E3 for the full design and `get_status` trait
extension details.

### Test Patterns

**Pattern 1: Secondary crash → reconciler re-integration** ✅
`test_reconciler_secondary_crash_and_rejoin` in `reconciler.rs`:
`crash_pod()` → `restart_pod()` before reconciliation → durable
retire/build/reconfigure. If no ready replacement exists, the separate durable
force-removal path commits reduced membership before cleanup.

**Pattern 2: Same-Pod process restart + operator restart** ✅
`test_same_pod_process_restart_changes_agent_generation_not_incarnation`
proves that Pod UID remains stable, agent generation changes, local action
state resets, and a fresh operator persists durable fail-closed recovery intent
before mutation.

**Pattern 3: Primary crash → reconciler failover** ✅
`test_reconciler_detects_primary_failure_and_fails_over` and
`test_reconciler_double_failover` in `reconciler.rs`: both use
`crash_pod()` for high-fidelity simulation.

**Pattern 4: Bounded quorum and catch-up loss** ✅
`QuorumTracker::test_pending_operations_expire_with_no_write_quorum`,
`test_catch_up_waiter_expires`, and the actor's
`demotion_fails_pending_write_before_expiration` verify bounded failure and
error preservation. The high-fidelity
`test_simultaneous_secondary_loss_bounds_new_and_inflight_writes` removes
both ACK paths through correlated Close actions and bounds new/in-flight
writes.

**Pattern 5: Failure during switchover compensation** ✅
`test_durable_switchover_compensates_failed_target_promotion` exercises the
real correlated path and verifies old-primary restoration.

**Pattern 6: Operator process restart recovery** ✅
`test_operator_restart_recovers_read_only_then_switches_and_scales` replaces
only `ReconcilerState` while real pod runtimes and persisted status remain. It
audits all control operations to prove recovery issues only `GetStatus`, then
verifies continued writes, switchover, and scale-up. Companion tests cover
recovered unhealthy-primary failover, legacy/mismatched snapshot rejection,
post-recovery pod logical/incarnation drift, and unordered pod listing.

**Pattern 7: Durable switchover boundary and ambiguity recovery** ✅
`test_durable_switchover_survives_state_loss_at_every_boundary` discards
`ReconcilerState` after every checkpoint/activity window. Companion tests
inject a lost target-promotion reply, force target-promotion compensation,
reject a stale pod incarnation, and reject a status resource-version conflict
before mutation. Assertions cover deterministic single dispatch of unsafe role
changes, terminal stable snapshot recovery, and unsupported checkpoint
versions with no mutating RPC.

**Pattern 7a: Feature-gated durable-execution switchover pilot** ✅
`test_durable_execution_switchover_pilot_*` keeps the explicit path as the
default and drives the opt-in path through the format-3 checkpoint kernel.
The matrix covers every-turn operator restart, schedule unknown outcomes with
and without apply, fused terminal CAS conflict, failed status publication
followed by terminal reload without Pods, stale target incarnation, failed
target-promotion compensation, and lost replies for every replica mutation. It
asserts the ordered mutation sequence, one admitted unsafe effect per
correlation identity, and terminal-before-status recovery. Operator unit tests
separately prove unknown UID-fenced label exposure remains quarantined,
ordinary activity payloads exclude full operation snapshots, and activity
count plus deterministic transition fuel are independently bounded.

Run only the targeted pilot matrix:

```console
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 cargo test -p kvstore \
  --test reconciler test_durable_execution_switchover_pilot_ -- --nocapture
```

The authoritative happy-path gate expects exactly nine external effects plus
three passive observations, giving 12 completed durable boundaries and 13
accepted checkpoint writes including terminal persistence. Seven former
preparation-only records are now part of their corresponding effect exposure.
Six repeated remediation runs observed maximum active checkpoints from 31,597
to 31,605 bytes and terminal checkpoints from 4,077 to 4,081 bytes. These are
run-specific snapshots because runtime-generated values affect serialized
length; no exact byte value is a compatibility contract. The stable admission
contracts are the configured 770,048-byte (752 KiB) encoded-checkpoint ceiling
and 4,096-byte terminal-payload ceiling, and the executable gate checks its
authoritative measurements against those ceilings.

The terminal payload carries the external-effect and passive-observation
counts, so a fresh measurement store can recover the classification without
prior active-checkpoint cache state. A successful full pilot requires exactly
three passive observations and `9 + r` external boundaries, where `r` is the
number of proven-no-admission redeliveries and is limited to the seven
projected ReplicaAgent-effect slots; UID-fenced label effects have no
redelivery path. The no-redelivery target remains 9/3, 12
boundaries, and 13 accepted writes; fault paths may consume additional
boundaries and writes. Compensated completion is validated against the exact
reachable pairs for its restore or failed-promotion compensation transcript,
including only redelivery slots belonging to effects that were actually
exposed.
`external_effects` counts ReplicaAgent commands and
UID-fenced label patches; `passive_observations` counts evidence-only
activities; `durable_boundaries` is their completed total.
`checkpoint_write_attempts` counts checkpoint CAS calls, while
`checkpoint_accepted_writes` counts only responses with a confirmed
authoritative revision. Latest/maximum authoritative, active, and terminal
checkpoint byte fields identify the lifecycle state instead of conflating
their sizes. The output also reports persistence outcomes, status
attempts/outcomes, UID-label calls, Pod-list calls, and an explicit reason
requeues are unavailable in the direct-reconcile harness.

Run the exact operational measurement and projection gates with:

```console
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 cargo test -p kuberic-operator \
  --features durable-switchover-pilot success_and_rollback_transcripts_fit_with_redelivery_headroom
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 cargo test -p kuberic-operator \
  --features durable-switchover-pilot maximum_projected_history_fits_both_budgets
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 cargo test -p kuberic-operator \
  --features durable-switchover-pilot measurements_ -- --nocapture
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 RUST_MIN_STACK=4194304 cargo test \
  -p kvstore --test reconciler \
  test_durable_execution_switchover_pilot_happy_path -- --nocapture
```

The ambiguity matrix distinguishes exposure conflicts, definite storage
failures, unknown outcomes without application, and unknown outcomes after
application. None returns a permit on the uncertain reconcile. Reload after an
applied unknown finds the complete prepared exposure and quarantines it;
reload after an unapplied unknown finds the predecessor and may prepare a new
attempt. Lost replies remain quarantined until authoritative postcondition
evidence or proof of non-admission permits bounded redelivery. The durable
kernel's 45-scenario conformance matrix also covers fused schedule/exposure,
observation/next exposure, observation/terminal, exact permit/attempt identity,
and capacity reservation.

Pilot/operator tests run with a 4 MiB test-thread stack in CI-constrained
environments:

```console
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 RUST_MIN_STACK=4194304 cargo test \
  -p kvstore --test reconciler test_durable_execution_switchover_pilot_
```

The real Kubernetes checkpoint test also creates an owner-bound terminal
checkpoint and proves owner deletion triggers garbage collection. It is
conditional validation: run it only when an authorized Kubernetes endpoint is
available and its namespace/ConfigMap preflight succeeds:

```console
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 cargo test \
  -p kuberic-durable-execution --features kubernetes \
  --test kubernetes_checkpoint_real -- --nocapture
```

When no authorized cluster is available, the required local measurement,
fault, replay, and bounds gates above remain authoritative; absence of the
optional environment is not evidence of real-API coverage.

**Pattern 7b: Framework-native remove-replica** ✅
`test_framework_native_remove_replica_*` exercises the only production remove
path through the shared bounded runner, an exact prepared
`RemoveReplicaIntent`, exact-UID label fencing, and exact-UID deletion. No
remove execution-mode selector or remove-specific Cargo feature is required.
The matrix includes default routing, legacy and unsupported-contract
incompatibility, every-boundary restart, uncertain effect recovery, terminal
reload before publication, status-gap waits without churn, malformed agent
isolation, and live data preservation.

Run the durable remove-replica operator and reconciler gates with:

```console
cargo test -p kuberic-operator framework_native_remove_replica
cargo test -p kuberic-operator durable_runner_tests
cargo test -p kuberic-operator checkpoint_store
cargo test -p kvstore --test reconciler test_framework_native_remove_replica_
```

The no-fault measurement is exact: three external effects, two passive
observations, five completed durable boundaries, and six accepted writes. The
three final samples observed an active-record lifecycle range of
3,373–18,693 bytes, per-run maxima of 18,685, 18,685, and 18,693 bytes, a
4,245-byte terminal record, and a 683-byte terminal payload. These bytes are
run-specific measurements; the active acceptance gate is 49,152 bytes
(48 KiB).

```console
cargo test -p kvstore --test reconciler \
  test_framework_native_remove_replica_three_no_fault_measurement_samples -- --nocapture
```

The independent contract bounds are 16 records, 4,096-byte boundary input,
2,048-byte boundary result, 262,144-byte active record, 12,288-byte terminal
record, and 4,096-byte terminal payload. The maximum-fault projection measures
182,589 active bytes and 11,453 terminal bytes at the full 4,096-byte payload
ceiling. The one-byte-over matrix rejects 17, 4,097, 2,049, 262,145, 12,289,
and 4,097 respectively:

```console
cargo test -p kuberic-operator \
  remove_replica_execution_rejects_all_six_one_byte_over_bounds
cargo test -p kuberic-operator \
  remove_replica_execution_maximum_fault_history_and_terminal_fit_independent_bounds -- --nocapture
```

The deletion safety inventory is retained by named native workflow,
shared-runner/kernel, reconciler, Kubernetes-provider, authorization, and
live-cluster tests in the committed source tree. The six operation-adapter
responsibilities have dedicated `framework_native_remove_replica_fr019_*`
tests.

For isolated live validation, use only the dedicated Kuberic Kind kubeconfig:

```console
KUBECONFIG="$HOME/.kube/kuberic-kind-config" kubectl \
  --kubeconfig "$HOME/.kube/kuberic-kind-config" cluster-info
KUBECONFIG="$HOME/.kube/kuberic-kind-config" just images
KUBECONFIG="$HOME/.kube/kuberic-kind-config" cargo test \
  -p kuberic-durable-execution --features kubernetes \
  --test kubernetes_checkpoint_real -- --nocapture
KUBECONFIG="$HOME/.kube/kuberic-kind-config" cargo test -p kuberic-tests \
  test_kvstore_k8s_status_healthy -- --nocapture
KUBECONFIG="$HOME/.kube/kuberic-kind-config" cargo test -p kuberic-tests \
  test_kvstore_k8s_write_read -- --nocapture
KUBECONFIG="$HOME/.kube/kuberic-kind-config" cargo test -p kuberic-tests \
  test_kvstore_k8s_framework_native_remove_replica -- --nocapture
```

The live removal test verifies selector-free admission, the owner-bound
terminal checkpoint, exact removal of one pod, preservation of the two
admitted surviving UIDs, and restoration of the shared fixture to three
healthy replicas.

**Pattern 8: Durable add/rejoin boundary and ambiguity recovery** ✅
`test_durable_add_survives_state_loss_and_every_lost_runtime_reply` loses the
single coarse operator-to-primary reply, replaces controller state, and proves
one `AddReplicaIntent` with zero operator-to-target mutations. Companion tests
cover exact old-incarnation retirement, status conflict before intent,
primary-owned compensation, and roll-forward after current configuration
commits.
`test_scale_up_replays_writes_buffered_during_copy` additionally writes during
the real copy window and verifies all buffered operations on the new
secondary. `test_add_target_same_pod_process_restart_invalidates_build_proof`
keeps the Pod UID, changes target process generation, and verifies that the old
semantic build proof is not reused.

Core coverage verifies peer accepted/in-progress replay, conflicting message
IDs, target generation fences, configuration descriptor signatures, add
protocol conversion, and execution-qualified quorum-wait cancellation.
Schema tests assert that superseded per-step add phases/actions and
compatibility sentinels are absent.

**Adapter data-plane coverage** ✅
`examples/sqlite/tests/correlated_replication.rs` covers multi-page WAL
shipping, schema changes, switchover, and failover through correlated actions.

**Pattern 9: Durable removal boundary and fencing** ✅
`test_durable_remove_coarse_activation` proves production dispatches one
`RemoveReplicaIntent` to the primary and no per-step removal controls.
`test_durable_force_remove_unreachable_secondary_with_retained_quorum`,
`test_scale_down_preadmission_and_minimum_are_mutation_free`, and
`test_scale_down_target_loss_after_dispatch_never_changes_to_force` cover
healthy/force admission and identical global quorum safety.

`test_precommit_quorum_loss_compensates_without_reduced_publication`, the
three-attempt/invalid-state unit matrices, and
`test_primary_process_restart_matrix_never_restores_same_epoch_primary` cover
pre-commit compensation, exact current-install ambiguity, and all primary
restart phases. `test_primary_process_restart_poison_is_durable_and_operator_restart_is_a_no_op`
proves `AmbiguousPrimaryRestart` remains terminal across controller restart.
Post-commit restart rolls forward from the workflow-scoped committed snapshot
and never reintroduces the removed member.

Real lifecycle-peer tests lose stage replies, return a temporarily unavailable
target before expiry, stall retirement while status remains responsive, and
restart the target at role-none/close boundaries. Core tests additionally
cover explicitly unsupported older control/peer generations, exact duplicate
and signature conflict,
sender/parent/epoch/configuration/generation fences, same-ID replacement
protection, 10/30/60/600-second budgets, and bounded terminal retention.

Commit/publication resource-version conflicts are refetched without duplicate
mutation. Exact-UID label/delete tests prove a same-name replacement is not
relabelled or deleted. Schema and source searches require remove operation v2,
framework-native remove contract v3, control v3, lifecycle peer v2, add
operation v3, and no superseded removal cursor or peer alias.

**Pattern 10: Durable Phase-1 failover and data loss** ✅

`failover_election` unit matrices cover complete previous/current
denominators, overlap, unhealthy and unknown observations, stale deactivation,
catch-up capability, deterministic ties, possible-best waiting, and
data-loss-required outcomes. Reconciler tests replace controller state,
inject before/after runtime failures and a final status apply-then-error,
exercise no-change/state-changed/failed/lost `OnDataLoss`, verify explicit
quorum wait with rotating probes, fence incarnation drift on both sides of
promotion commit, and run consecutive failovers. Every persisted failover
phase also round-trips through serialization.
`test_slow_data_loss_callback_does_not_poison_failover` keeps a callback
in-progress beyond the normal 10-second action window and verifies the
data-loss-specific deadline permits safe completion.

**Pattern 11: Durable creation bootstrap and routing gate** ✅
`test_durable_create_survives_state_loss_and_every_lost_runtime_reply`
replaces controller state at every creation boundary, injects a lost response
for every correlated runtime activity instance, and verifies exact Open/build
counts. Companion tests cover one/two/three replicas, partial majority
snapshots, `minReplicas` routing gating, unordered pod lists, status conflict
before intent, candidate replacement during build, pre-commit cleanup,
post-commit roll-forward, invalid checkpoints, committed-member incarnation
fencing, unavailable fence targets before/after primary-only commit,
fence-intent UID replacement with a new operation identity, committed-target
compensation rejection, and exact final live topology.

### Remaining Work

- **WAL recovery tests** — blocked on Option C implementation
- **`restartCount` tracking** — add to `MemberStatus` CRD for observability
