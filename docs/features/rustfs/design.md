# RustFS Native-Replication Example

> **Status:** Iteration 1 of [issue #81](https://github.com/youyuanwu/kuberic/issues/81).
> This is an experimental contract library, not a runnable or operator-managed
> RustFS deployment.

## Ownership

Follow the PostgreSQL example's separation of control and data planes, not its
primary/secondary implementation. RustFS owns object storage, erasure coding,
data repair, and native read/write quorum. Kuberic must not ship RustFS data
through `WalReplicator`, invent a scalar replication LSN, or treat its own
primary election as RustFS write authority.

RustFS nodes are peers. A failed health probe does not authorize membership
changes, data deletion, promotion, or a new cluster. Loss of native quorum must
remain a native availability failure rather than trigger destructive recovery.

## PR Sequence

Each iteration is intended to be independently reviewable and merged before
the next one. Only iteration 1 is part of this PR.

| PR | Features | Acceptance gate |
|---|---|---|
| **1. Native topology contract** | Add an independent RustFS example crate, validated fixed-topology configuration, deterministic native volume arguments, explicit rejection of topology drift, and this roadmap. | Pure contract tests cover accepted configuration, malformed input, duplicates, identity, ordering, and unsupported changes. No RustFS process, Docker, or cluster is required. |
| **2. Observe-only runtime** | Pin a tested RustFS image by digest; add bounded native health probes, separate local health from cluster read/write availability, preserve failed and stale evidence, and provide a read-only CLI. | HTTP fixture tests cover real endpoint semantics, timeouts, errors, freshness, and shutdown. Opt-in tests verify observations against the pinned engine. |
| **3. Local process lifecycle** | Add an instance manager for explicit start, graceful stop, abort, restart, readiness deadlines, and unexpected-exit reporting. Keep persistent data and fixed topology intact. Credentials come from mounted secrets, never command arguments or logs. | Process tests cover failed startup, cancellation, repeated operations, graceful shutdown, restart, and data preservation. Real-engine tests verify S3 round trips across restart. |
| **4. Kuberic native-engine integration** | Add the minimal explicit capability boundary needed to host peer engines; wire lifecycle and health into Kuberic without pretending RustFS supports primary/secondary transitions, scalar catch-up, or Kuberic quorum. Reject unsupported topology and data-loss operations. | Contract tests cover every relevant control operation; integration tests show quorum loss does not cause promotion, deletion, bootstrap, or unsafe replacement. |
| **5. Kubernetes example and resilience tests** | Add pinned container packaging, persistent volumes, stable peer discovery, Secret references, peer-wide S3 routing, and an isolated KinD scenario with documentation. | S3 put/get/list/delete, node loss, process restart, same-identity recovery, quorum loss, and recovery preserve acknowledged data. CI explicitly owns its cluster and cleanup. |

Pool expansion, shrinking, automatic disk replacement, destructive recovery,
cross-site replication, production hardening, and performance claims are not
implied by this sequence. They need separate native-engine evidence and designs.

## Iteration 1 Contract

The [example crate](../../../examples/rustfs/) accepts four ordered DNS peers,
four ordered drive paths per peer, a common transport, each peer's API port, and
one local peer identity. Private validated fields prevent callers from
bypassing the supported shape. It renders sixteen explicit native volume
arguments and rejects duplicate or malformed identities, duplicate or nested
paths, unsupported counts, topology drift, and changes to local identity.

The intended geometry is one pool containing one sixteen-drive erasure set,
exported as `ERASURE_SET_DRIVE_COUNT`. This is deliberately a restricted example
profile, not a statement that RustFS only supports this geometry. No parity or
read/write quorum is calculated by Kuberic.

This is a pure contract library. It neither persists the accepted configuration
nor proves DNS locality, backing-device independence, disk incarnation, native
health, or actual engine compatibility. Its restart comparison is not a storage
fence. There is no process supervisor, HTTP client, S3 client, operator adapter,
image, or deployable manifest in this PR.

## Native Evidence and Follow-up Requirements

Static source verification supports the contract:

- The official [distributed Compose example](https://github.com/rustfs/rustfs/blob/edcc81a8fdac1901f8674050af9af2430d34cdce/.docker/compose/docker-compose.cluster.yaml#L15)
  uses four peers with four drives each and gives every peer the complete volume
  list.
- The [native layout parser](https://github.com/rustfs/rustfs/blob/d47f54bfb2f39f48bd1adda334bd27e151fe85b8/crates/ecstore/src/layout/disks_layout.rs#L119)
  accepts all-literal volume arguments as one legacy pool. Its
  [literal endpoint path](https://github.com/rustfs/rustfs/blob/edcc81a8fdac1901f8674050af9af2430d34cdce/crates/ecstore/src/layout/disks_layout.rs#L201)
  preserves declaration order. The example does not mix literal URLs and
  ellipsis expressions or claim support for non-legacy multi-pool workflows.
- The [startup topology validator](https://github.com/rustfs/rustfs/blob/d47f54bfb2f39f48bd1adda334bd27e151fe85b8/crates/ecstore/src/store/init_format.rs#L671)
  rejects incompatible persisted pool geometry. The
  [lifecycle runbook](https://github.com/rustfs/rustfs/blob/edcc81a8fdac1901f8674050af9af2430d34cdce/docs/operations/cluster-lifecycle-operations.md#L8)
  also prohibits reordering volume slots or moving initialized directories
  between slots.

The CLI and topology-validator citations are pinned to RustFS 1.0.0
(`d47f54bfb2f39f48bd1adda334bd27e151fe85b8`). The additional Compose, parser,
and lifecycle evidence is pinned to a later source snapshot
(`edcc81a8fdac1901f8674050af9af2430d34cdce`). This is source inspection, not a
claim that this example has been run against either build. Iteration 2 must pin
and test an actual image before making runtime compatibility claims.

The subsequent runtime work must enforce several facts that string validation
cannot establish:

1. The [server CLI](https://github.com/rustfs/rustfs/blob/d47f54bfb2f39f48bd1adda334bd27e151fe85b8/rustfs/src/config/cli.rs#L1419)
   accepts `rustfs server <volumes...>`. Bind its API listener to the selected
   local peer's configured port; do not use the console port for peer identity.
2. Explicitly set `RUSTFS_ERASURE_SET_DRIVE_COUNT` to sixteen and reject
   conflicting inherited settings. The
   [set-width selection](https://github.com/rustfs/rustfs/blob/edcc81a8fdac1901f8674050af9af2430d34cdce/crates/ecstore/src/layout/disks_layout.rs#L333)
   otherwise allows an override to partition the ordered arguments differently.
   Width four, for example, would concentrate each set on one peer.
3. Verify native locality and independently backed persistent drives. The
   [native endpoint checks](https://github.com/rustfs/rustfs/blob/edcc81a8fdac1901f8674050af9af2430d34cdce/crates/ecstore/src/layout/endpoints.rs#L1708)
   can reject different path strings that share backing storage.
4. Start the complete initial membership before awaiting readiness. Native
   [fresh initialization](https://github.com/rustfs/rustfs/blob/d47f54bfb2f39f48bd1adda334bd27e151fe85b8/crates/ecstore/src/store/init_format.rs#L99)
   requires evidence from every configured disk; waiting for the first peer to
   become ready before starting the next can block bootstrap.
5. Preserve the distinction between liveness and readiness. The
   [health handler](https://github.com/rustfs/rustfs/blob/d47f54bfb2f39f48bd1adda334bd27e151fe85b8/rustfs/src/server/health.rs#L108)
   can keep liveness healthy while readiness is degraded. Quorum-related
   unavailability must not create automatic restart loops. Follow the
   [rolling-restart procedure](https://github.com/rustfs/rustfs/blob/d47f54bfb2f39f48bd1adda334bd27e151fe85b8/docs/operations/rolling-restart.md#L20)
   and verify the native failure budget rather than deriving it from peer count.

## Integration Boundary

The classic PostgreSQL adapter translates Kuberic events into native database
operations. Its architectural separation is reusable, but RustFS has no
corresponding native primary promotion or standby catch-up operation. The
independent level-triggered stack also has single-writer assumptions.

Iteration 4 must resolve that mismatch explicitly before either operator is
allowed to manage RustFS. Until then, the RustFS crate stays independent of
`kuberic-core`, `kuberic-runtime`, and both controllers. Later PRs must not
acknowledge unsupported events as successful no-ops or expose fake replication
progress to satisfy an existing interface.
