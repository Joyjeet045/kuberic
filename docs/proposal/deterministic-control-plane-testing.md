# Deterministic Control-Plane Testing for Kuberic

> **Status:** Proposed
>
> **Scope:** Cluster-free testing of interactions among the Kuberic controller,
> Kubernetes resources, replica agents, time, and external events. This
> proposal supplements the existing protocol models, durable crash tests, and
> KinD scenarios; it does not replace them.

## Summary

Add a small, test-only deterministic execution explorer that runs Kuberic's
real reconciliation decisions against controlled observations and effect
outcomes.

The explorer will be part of Kuberic's existing test infrastructure. It will
reuse the pure evaluator, controller API boundary, in-memory state, model
invariants, and scenario fixtures already in the repository. It is not a
standalone framework, a reusable Kubernetes simulator, or a production
component.

The new tier will explore executions that are difficult to reproduce reliably
in a live Kubernetes cluster:

- observations assembled from different moments in cluster history;
- replica reports delayed independently from Kubernetes resources;
- controller interruption between externally visible effects;
- successful effects whose responses are lost;
- user, Kubernetes, and replica events interleaved with reconciliation;
- controller and replica restarts at transition boundaries;
- different valid orderings of the same pending work.

Every explored execution will be checked continuously against Kuberic safety
invariants and, after injected faults heal, against bounded convergence and
semantic final-state consistency.

## Motivation

Kuberic already has strong validation at several layers:

- pure protocol and model tests;
- controller tests with an in-memory cluster API;
- durable agent and application crash-boundary tests;
- seeded adversarial histories;
- host-local SQLite and PostgreSQL validation;
- live KinD scenarios with process failures and network partitions.

These tests cover many known boundaries, but most controller tests still
construct one chosen observation and execute one chosen continuation. They do
not systematically explore the execution space created when independently
changing Kubernetes resources, agent reports, time, user intent, and effect
completion are observed in different orders.

A production reconciliation does not observe one atomic cluster snapshot. It
combines:

1. the current KubericSet;
2. separately listed Pods, PVCs, Services, and Secrets;
3. independently collected reports from multiple replica agents;
4. exact resource lookups performed later in the observation;
5. physical time used for failure and recovery decisions.

Each individual observation may be valid while the combined view spans
multiple moments in the system's history. The controller must remain safe and
eventually recover under those combinations.

Likewise, one reconciliation may perform multiple externally visible effects.
The controller may stop after any completed prefix, or an effect may complete
while its response is lost. Existing targeted tests cover important examples,
but Kuberic should validate this property systematically across supported
lifecycle operations.

## Decision

Kuberic will extend its test infrastructure with a deterministic execution
explorer between the existing model/unit tier and the live KinD tier.

The explorer will orchestrate existing Kuberic test seams rather than introduce
a parallel controller or protocol model. It will execute real Kuberic
evaluation and reconciliation behavior while controlling observations,
external events, effect outcomes, restarts, and time. Exploration will be
bounded and focused on selected transition windows. It will not attempt to
simulate all Kubernetes behavior or exhaust the complete state space.

Findings from simulation will be treated as bug candidates. Findings that
depend on Kubernetes semantics or timing will be validated against a live
cluster before they are classified as product defects. Once confirmed, the
focused scenario and schedule will become a deterministic cluster-free
regression test.

## Goals

1. Test Kuberic against non-atomic observations assembled from independently
   advancing sources.
2. Test interruption and ambiguous completion after every externally visible
   effect boundary in supported controller transitions.
3. Explore external events at points that are normally difficult to force in a
   live cluster.
4. Check safety invariants after every transition, not only at the end of a
   scenario.
5. Check eventual convergence after temporary faults, delays, and partitions
   heal.
6. Detect materially different terminal outcomes produced by equivalent
   initial state and external events.
7. Make every failing execution deterministic, inspectable, and replayable.
8. Keep routine exploration fast enough for the cluster-free continuous
   integration tier.
9. Use live KinD scenarios to validate simulator fidelity and selected
   high-value schedules.
10. Reuse and consolidate invariants already expressed in protocol models,
    controller tests, and live scenarios.

## Non-Goals

- Building a complete Kubernetes API server, scheduler, kubelet, storage
  controller, or networking simulator.
- Exhaustively exploring every ordering and every historical observation.
- Proving the Kuberic protocol correct.
- Replacing protocol model tests, durable process crash tests, database tests,
  or KinD scenarios.
- Treating raw Kubernetes object equality as the definition of deterministic
  convergence.
- Modeling low-level network packet behavior.
- Testing multiple independent KubericSets for shared-resource coordination.
- Introducing a production API or runtime dependency for test exploration.
- Automatically accepting every simulator finding as a real Kubernetes bug.

## Testing Model

### Physical State and Observed State

The testing model will distinguish physical state from the state presented to
one reconciliation.

Physical state represents the latest known Kubernetes resources, accepted
authority, replica-agent state, and external intent. Observed state may select
different valid points from the recent history of each observation source.

The independently controlled sources are:

- KubericSet specification and status;
- Pods;
- PVCs;
- Services;
- Secrets;
- each replica agent and process session;
- exact resource lookups;
- physical time.

This distinction will allow tests to represent torn observations without
claiming that an individual Kubernetes API response is internally invalid.

### Execution Events

Scenarios will be expressed as an initial physical state followed by external
events. Relevant events include:

- desired replica-count changes;
- planned-switchover requests and conflicting request updates;
- Pod, PVC, Service, and Secret creation, update, replacement, or deletion;
- Pod readiness and scheduling changes;
- replica failure, recovery, or process-session restart;
- agent report advancement or delay;
- network unavailability and healing;
- finalizer addition and removal;
- controller restart;
- clock advancement across a failure deadline.

The explorer will choose where eligible events occur relative to observation,
effect completion, and subsequent reconciliation.

### Effect Outcomes

Every externally visible controller effect will be considered at three high
level outcomes:

- it did not occur;
- it occurred and was acknowledged;
- it occurred but the response was lost or the controller stopped before
  recording completion.

For a reconciliation with multiple effects, exploration will cover interruption
after each completed prefix. Recovery must derive the next safe action from
durable observations rather than depend on process-local attempt history.

### Focused Exploration

Exploration will be scoped to a small set of relevant observation sources,
events, and transition depths for each scenario.

The normal workflow will be:

1. Run one deterministic reference execution.
2. Identify the transition window and observation sources relevant to the
   hypothesis.
3. Explore bounded variations only in that window.
4. Check invariant and convergence results.
5. Reduce any failure to the smallest useful replay.
6. Validate simulator-sensitive findings in KinD.

Broad exhaustive exploration will be reserved for small, well-bounded
interaction points.

## Oracles

### Continuous Safety

The following properties will be checked after every simulated transition:

- no more than one replica has granted write access;
- write routing is absent or selects the exact accepted and attested primary
  incarnation;
- accepted epochs and durable authority evidence do not regress;
- stale process sessions and superseded incarnations cannot authorize new
  authority or cleanup;
- membership cleanup does not begin before the required durable commit;
- exact Pod and PVC identity requirements are preserved during cleanup;
- an old or same-name replacement resource is never adopted without the
  required provenance;
- acknowledged application writes remain represented by recoverable durable
  state;
- contradictory observations fail closed rather than inferring progress;
- unsupported desired-state changes do not mutate frozen transition intent.

### Eventual Convergence

After temporary faults heal and no new external intent is introduced, a fair
execution must reach one of the explicitly supported terminal outcomes within
a bounded number of reconciliations:

- stable and ready;
- stable but write-closed because a documented safety prerequisite remains
  absent;
- a documented terminal unsafe outcome with routing fenced.

Repeated reconciliations in a terminal state must not continue mutating
resources or status.

### Semantic Convergence

Executions that begin from equivalent state and receive the same external
events should converge to semantically equivalent outcomes.

Comparison will focus on domain state:

- accepted topology and policy;
- primary identity and write authority;
- epoch and data-loss authority;
- active or completed transition identity;
- cleanup obligations;
- logical resource ownership and routing;
- acknowledged-write durability.

Generated UIDs, resource versions, timestamps, ordering of equivalent
collections, and diagnostic wording will not by themselves constitute
different outcomes.

## Proposed Workstreams

### Workstream 1: Shared Invariant Catalog

Create one documented catalog of controller, agent, and application invariants.
Consolidate equivalent assertions currently distributed across protocol model
tests, controller tests, crash tests, and KinD scenarios.

The catalog will identify:

- properties that must hold continuously;
- properties that apply only after an effect is durably accepted;
- properties that apply after faults heal;
- properties that require live-cluster confirmation.

Completion criterion: each initial simulation scenario can use the same
authoritative safety vocabulary and does not invent scenario-specific
definitions of single-writer, fencing, authority, or cleanup safety.

### Workstream 2: Deterministic Scenario and Replay Contract

Define a stable scenario vocabulary for initial state, external events,
observation variation, interruption points, and expected invariants.

Every run must emit enough information to replay the exact execution,
including:

- scenario identity;
- deterministic seed or explicit schedule;
- chosen observation versions;
- external-event order;
- effect outcomes and interruption points;
- clock progression;
- reconciliation outcomes and physical mutations.

Completion criterion: a failing run can be reproduced directly and retained as
a focused regression without depending on probabilistic timing.

### Workstream 3: Torn-Observation Exploration

Add bounded exploration of independently advancing Kubernetes resources,
replica reports, exact lookups, and time.

Initial exploration will prefer one-step historical variation and a small
number of implicated sources. Deeper history will be introduced only when a
specific scenario requires it.

Completion criterion: each priority lifecycle scenario is exercised with
current and delayed views of its safety-critical sources.

### Workstream 4: Effect-Prefix and Ambiguous-Reply Exploration

Systematically interrupt reconciliation after each externally visible effect
prefix and explore both definite failure and successful-but-unacknowledged
completion.

This workstream covers controller-level Kubernetes and command effects. It
complements, rather than duplicates, the existing durable replica-agent effect
cut tests.

Completion criterion: every multi-effect controller plan used by a priority
lifecycle scenario has restart and recovery coverage at each effect boundary.

### Workstream 5: External-Event Scheduling

Interleave user, Kubernetes, replica, and clock events with transition
progress.

The first event classes will be:

- failure and healing;
- replica process restart;
- desired replica-count mutation;
- switchover request mutation;
- same-name resource replacement;
- finalizer delay;
- failure-deadline crossing.

Completion criterion: each priority scenario covers the external events most
likely to invalidate an earlier observation or frozen decision.

### Workstream 6: Convergence and Path-Consistency Checks

Add bounded fair-recovery runs and semantic terminal-state comparison across
explored schedules.

Non-convergence and path-dependent outcomes will produce traces showing the
first point at which executions materially diverged.

Completion criterion: priority scenarios demonstrate both continuous safety
and convergence after all injected temporary faults heal.

### Workstream 7: Live-Cluster Correlation

Select representative simulator schedules for reproduction in the existing
owned KinD environment.

Live validation will focus on behavior where simulator fidelity is most
important:

- Kubernetes deletion and finalizer semantics;
- resource replacement and UID preconditions;
- Pod readiness and process restart;
- Service selector publication;
- network and timeout effects.

Completion criterion: the simulator's assumptions are covered by a small set
of correlation scenarios, and confirmed bug schedules have corresponding
deterministic cluster-free regressions.

### Workstream 8: Continuous Integration Adoption

Divide exploration into:

- a small deterministic regression corpus run on every change;
- bounded focused exploration run in the ordinary cluster-free tier;
- broader scheduled exploration with retained failure artifacts;
- selected KinD correlation scenarios in the existing live tier.

Newly discovered schedules will enter the deterministic regression corpus.
Exploration budgets will be explicit so test growth does not silently make the
ordinary tier impractical.

Completion criterion: routine pull-request validation exercises the focused
simulator without requiring Kubernetes, Docker, or external databases.

## Priority Scenarios

### 1. Failover and Quorum Recovery

Vary Service routing, primary-agent availability, secondary reports, accepted
status, and the failure clock around the failover threshold.

Required outcomes:

- old-primary routing is fenced before replacement authority is exposed;
- a healing primary before the deadline does not cause unnecessary data-loss
  authority;
- failover selects only sufficiently proven replicas;
- healed executions converge to one writer and exact routing.

### 2. Planned Switchover

Interrupt preparation, authority movement, routing publication, terminal
status persistence, and restoration. Interleave source or target process
restart and request mutation.

Required outcomes:

- the accepted request remains immutable;
- routing never selects an unattested target session;
- ambiguous dispatch is resolved by re-observation;
- recovery completes the accepted request or restores safe prior authority;
- terminal receipts describe the actual outcome.

### 3. Sequential Scale-Up

Vary PVC, Pod, endpoint, allocation status, candidate reports, copy progress,
desired replica count, and same-name resource replacement.

Required outcomes:

- PVC provenance is frozen before dependent Pod adoption;
- cancelled or failed candidates are not reused under a new identity;
- stale candidate sessions cannot complete admission;
- only one ordinal is admitted at a time;
- cleanup completes before a fresh attempt;
- acknowledged writes survive build, catch-up, and admission schedules.

### 4. Secondary Scale-Down

Vary retained-member availability, target return, endpoint/Pod/PVC visibility,
exact lookups, finalizers, and lost deletion replies at every authority
boundary.

Required outcomes:

- retained quorum is proven before write closure or membership change;
- committed removal is not rolled back by a returning target;
- cleanup uses exact frozen identities;
- PVC cleanup does not outrun Pod fencing and confirmed absence;
- sequential reductions do not overlap;
- healing converges to the requested membership.

### 5. Replacement

Vary old-incarnation failure, new scaffolding visibility, build evidence,
routing state, and the return of superseded resources.

Required outcomes:

- the old incarnation cannot regain authority;
- the replacement cannot be admitted without exact copy and catch-up evidence;
- same-name resources do not substitute for frozen UIDs;
- ambiguous cleanup remains repeatable and safe;
- the final topology contains one accepted incarnation for the logical member.

### 6. Stable-State Drift and Missed Notifications

Mutate support resources, routing, or replica sessions without relying on a
watch notification.

Required outcomes:

- bounded re-observation detects and repairs drift;
- repair does not create a new authority transition unnecessarily;
- a stable polling loop becomes quiescent after convergence;
- repeated repair does not cycle on unconditional writes.

## Delivery Sequence

The proposed delivery order is:

1. establish the shared invariant and semantic-state definitions;
2. establish deterministic scenario replay and trace evidence;
3. cover effect-prefix interruption for failover and switchover;
4. add torn-observation exploration for failover and routing;
5. extend both capabilities to scale-up, scale-down, and replacement;
6. add bounded external-event scheduling and convergence comparison;
7. correlate representative schedules in KinD;
8. make focused exploration part of ordinary continuous integration;
9. retain every confirmed finding as a minimized deterministic regression.

This sequence prioritizes safety-critical boundaries and reuses current test
strengths before expanding the explored state space.

## Success Criteria

The proposal is complete when:

1. failover, quorum recovery, switchover, scale-up, scale-down, replacement,
   and stable-drift scenarios run without a Kubernetes cluster;
2. each scenario varies its critical observation sources independently;
3. multi-effect controller plans are tested at every effect prefix used by
   those scenarios;
4. safety invariants are checked after every simulated transition;
5. temporary-fault scenarios demonstrate bounded convergence after healing;
6. semantically divergent terminal states are reported as failures;
7. every failure includes deterministic replay evidence;
8. representative schedules have been correlated with KinD behavior;
9. confirmed defects are preserved as focused regressions;
10. the ordinary test tier remains bounded and explicitly budgeted.

## Risks

### Simulation Fidelity

A simplified environment may permit impossible behavior or omit behavior that
Kubernetes can produce. The mitigation is to model only observable contracts,
keep assumptions explicit, and correlate representative schedules in KinD.

### State-Space Growth

Independent observation histories and event orderings grow combinatorially.
The mitigation is focused exploration, shallow history, bounded transition
windows, semantic state comparison, and permanent regression cases for known
interactions.

### Incorrect Oracles

An overly strict final-state comparison may reject valid differences, while a
weak invariant may miss semantic corruption. The mitigation is a shared
domain-level invariant catalog and semantic projection reviewed alongside the
protocol contract.

### Duplicate Test Coverage

Simulation could repeat existing model or durable crash tests without adding
interaction coverage. The new tier must focus on controller-environment
ordering, torn observations, and cross-boundary recovery. Agent-local durable
effect sequencing remains owned by the existing crash suites.

### Long-Running Continuous Integration

Exploration can expand silently as scenarios grow. Each suite must have an
explicit schedule and depth budget, with broader searches separated from the
pull-request regression tier.

## Expected Outcome

Kuberic will retain its current layered validation strategy while adding a
fast, deterministic way to explore control-plane executions that are currently
covered only by selected hand-written schedules or expensive live-cluster
tests.

The result should improve confidence in the central level-triggered promise:
regardless of observation timing, retries, restarts, and temporary failures,
Kuberic fails closed, preserves single-writer authority and durable data, and
converges from current state rather than depending on remembered attempt
history.

## Appendix: Further Exploration Ideas

The following ideas may improve the explorer after the core proposal is
established. They are candidates for incremental adoption rather than
additional prerequisites for the first usable testing tier.

### Resource Dependency Map

Maintain a compact map of which reconciliation behavior reads, writes, or is
triggered by each Kubernetes resource, agent report, exact lookup, and time
input. Use the map to identify the smallest causal set of observation sources
and effects that a scenario needs to vary.

### Reference-Trace Coverage

Use deterministic reference traces as a coverage inventory for transition
decisions, commands, status writes, Kubernetes effects, and terminal outcomes.
Report important boundaries that no scenario reaches or perturbs.

### Causal-Window Exploration

Derive focused exploration windows from the reference trace. Begin at the
observation that introduces a relevant condition and end at the effect that
commits or resolves it, rather than perturbing an entire lifecycle execution.

### Semantic State Deduplication

Avoid revisiting executions that differ only in generated UIDs, resource
versions, timestamps, equivalent collection ordering, diagnostic wording, or
other non-semantic details. Where the protocol permits it, consider symmetric
secondary replicas equivalent for exploration purposes while retaining exact
identity checks at authority and cleanup boundaries.

### Explicit Fidelity Contracts

Document the production contract represented by every simulated behavior.
Examples include non-atomic cross-resource observations, later exact lookups,
ambiguous successful effects, UID and resource-version preconditions, and
agent reports bound to exact process sessions. Findings outside the documented
contracts require live-cluster reproduction before becoming regressions.

### First-Divergence Reporting

When two executions produce materially different outcomes, report the first
semantic divergence as well as the final-state difference. Relevant divergence
points include accepted epoch, frozen identity, routing authority, cleanup
authorization, and acknowledged-write durability.

### Unobserved Transient States

Allow multiple physical mutations to occur before the next reconciliation so
that intermediate states are never observed. Candidate cases include rapid
Pod replacement, repeated agent-session changes, routing removal followed by
republication, and desired-state reversal during cleanup.

### Discovery and Regression Separation

Keep broad schedule discovery separate from the deterministic pull-request
regression corpus. Reduce confirmed findings to explicit replayable schedules,
while running larger bounded searches only in scheduled or manually requested
validation.

### Deliberately Deferred Directions

The following directions should remain deferred unless the narrower explorer
demonstrates a concrete need:

- general multi-controller ordering across unrelated control planes;
- a reusable Kubernetes API server or informer-cache simulator;
- low-level packet, storage-driver, scheduler, or kubelet simulation;
- source rewriting solely to control time;
- agent-generated scenario selection as a correctness dependency;
- exhaustive exploration outside small, explicitly bounded transition
  windows.
