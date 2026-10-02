# Lazy Repair and Peer-Repair Replication

Survey of systems that write to a **subset of replicas** on the write
path and rely on **background repair** to propagate data to the
remaining replicas. This pattern trades immediate full redundancy for
lower write latency and reduced write amplification.

> Related: [Leaderless Quorum Replication](leaderless-quorum-replication.md)
> (Dynamo-family systems that send to ALL replicas but wait for W),
> [Peer-Repair Design](../archive/v1/future/peer-repair-replication.md)

---

## Table of Contents

1. [The Two Replication Strategies](#the-two-replication-strategies)
2. [Dynamo-Family: Send to All, Wait for W](#dynamo-family-send-to-all-wait-for-w)
3. [Lazy Repair: Send to Subset, Repair Later](#lazy-repair-send-to-subset-repair-later)
4. [Erasure Coding Systems](#erasure-coding-systems)
5. [Peer-to-Peer Reconciliation Systems](#peer-to-peer-reconciliation-systems)
6. [Anti-Entropy Mechanisms](#anti-entropy-mechanisms)
7. [Comparison Table](#comparison-table)
8. [Implications for Kuberic](#implications-for-kuberic)
9. [References](#references)

---

## The Two Replication Strategies

All replicated storage systems face the same question on every write:
**how many replicas should receive the data on the write path?**

Two distinct strategies exist:

```
Strategy A: "Send to all, wait for quorum"
─────────────────────────────────────────
Writer ──► Replica₁  (ACK)  ← counted toward W
       ──► Replica₂  (ACK)  ← counted toward W
       ──► Replica₃  (no ACK needed, but data sent)
  All N replicas receive the data immediately.
  Writer waits for W ACKs before returning success.
  Anti-entropy is a fallback for failures only.

Strategy B: "Send to subset, repair later"
──────────────────────────────────────────
Writer ──► Replica₁  (ACK)  ← counted toward W
       ──► Replica₂  (ACK)  ← counted toward W
       ╳   Replica₃  (not sent — will get data via repair)
  Only W replicas receive the data on write path.
  Remaining N-W replicas get data via background repair.
  Anti-entropy is the primary distribution path.
```

**Strategy A** is universally adopted in production replication
systems (Cassandra, Riak, DynamoDB, ScyllaDB). **Strategy B** is
used in erasure coding systems and is the model for kuberic's
peer-repair design.

---

## Dynamo-Family: Send to All, Wait for W

All Dynamo-derived systems use Strategy A:

| System | Write sends to | Waits for | Remaining replicas |
|---|---|---|---|
| **Cassandra** | All N | W ACKs (consistency level) | Already received |
| **Riak** | All N | W ACKs | Already received |
| **DynamoDB** | All N (internal) | W ACKs | Already received |
| **ScyllaDB** | All N | W ACKs | Already received |
| **Voldemort** | All N | W ACKs | Already received |

**Why they send to all:** Sending to all N is cheap compared to the
cost of background repair infrastructure. The write is already
serialized and in a network buffer — fanning out to N instead of W
adds minimal latency (the N-W sends are fire-and-forget, not
awaited). The benefit: all replicas are immediately up-to-date, and
anti-entropy (read repair, Merkle trees) only needs to handle
failure cases (node was down, network lost the message).

**Anti-entropy as fallback:** In these systems, read repair and
background Merkle-tree repair exist to catch **stragglers** — the
rare case where a replica missed a write due to a temporary failure.
Anti-entropy is not the primary data distribution mechanism.

### Who Coordinates the Fan-Out?

| System | Coordinator | Client role |
|---|---|---|
| **Cassandra** | Server-side (any node the client contacts) | Thin — 1 connection |
| **Riak** | Server-side (any node) | Thin |
| **DynamoDB** | Server-side (AWS internal) | Thin — HTTP API |
| **ScyllaDB** | Server-side (same as Cassandra) | Thin |
| **Voldemort** | **Either** — `routing=client` or `routing=server` | Fat or thin |

Only Voldemort offered client-side fan-out (`routing=client`), where
the client library knew the ring topology and sent to all N replicas
directly. This eliminated the coordinator hop but required a "fat"
Java client. See [leaderless-quorum-replication.md](leaderless-quorum-replication.md)
for Voldemort's client state model (fully stateless, no persistent
state, bootstrap from any node).

---

## Lazy Repair: Send to Subset, Repair Later

Strategy B — writing to fewer than N replicas and relying on
background processes to fill in the rest — appears primarily in
**erasure coding** systems, not traditional replication systems.

### Why Not in Replication Systems?

Production replication systems (Cassandra, Riak, etc.) avoid
Strategy B because:

1. **Marginal cost of sending to N vs W is low.** The write is
   already serialized. Sending to 3 vs 2 replicas adds one extra
   network send (fire-and-forget), not a round-trip.

2. **Background repair is complex.** Anti-entropy requires:
   reconciliation protocol, Merkle trees or LSN comparison, GC
   coordination, epoch-aware validation, failure detection between
   replicas. This infrastructure must be built and operated
   regardless of whether it's the primary path or fallback.

3. **Reduced durability window.** Between the write and repair
   completion, only W copies exist. If W-1 of those fail, the op
   is lost — even though it was quorum-committed.

4. **Convergence delay.** N-W replicas are behind by one repair
   interval. Reads from a lagging replica return stale data.

### Where Strategy B Makes Sense

Strategy B is attractive when:

- **Write amplification is expensive** (large payloads where sending
  to N vs W is a meaningful bandwidth difference)
- **Writer bandwidth is constrained** (Writer is outside the cluster,
  on a slow link, or embedded in a resource-constrained app)
- **Immediate full redundancy is not required** (the system can
  tolerate a brief window of reduced durability)
- **Replicas are capable peers** (can run reconciliation
  independently, unlike thin secondaries that only receive)

---

## Erasure Coding Systems

Erasure coding systems are the clearest production examples of
Strategy B — they write to a minimum number of nodes initially,
then background-encode remaining redundancy.

### Azure Storage (WAS)

Microsoft's Windows Azure Storage uses a two-phase approach:

1. **Hot phase:** New blob is written to a local fault domain
   (fewer nodes, fast). Full redundancy is not yet achieved.
2. **Cold phase:** Background jobs erasure-encode the data
   (Reed-Solomon), compute parity blocks, and distribute across a
   wider set of nodes in multiple fault domains.

The lazy encoding optimizes for write latency — the blob is durable
enough (local fault domain) while the full cross-domain redundancy
is achieved asynchronously.

### Facebook f4 (Warm Blob Storage)

Facebook's f4 stores large immutable blobs using erasure codes
(e.g., Reed-Solomon 10+4):

1. **Write:** Blob is written to a subset of nodes (data blocks).
2. **Background encode:** Parity blocks are computed and distributed
   asynchronously by background jobs.
3. **Full redundancy:** Achieved after encoding completes.

The lazy repair pattern minimizes write amplification for large blobs
(100s of MB). The trade-off: reduced durability until parity is
computed. For warm/cold storage, this is acceptable — the data is
immutable and the encoding delay is bounded.

### HDFS Erasure Coding (3.0+)

HDFS 3.0 added erasure coding as an alternative to 3x replication:

- Data is striped across k data nodes
- Parity is computed and stored on m additional nodes
- Some configurations defer parity computation to background

However, HDFS's standard replication mode (pipeline replication)
always writes to all 3 replicas on the write path — it does NOT
use lazy repair for replicated data.

### Key Pattern

In all erasure coding systems, the lazy repair pattern follows:

```
Write path:     data → k nodes (minimum for reconstruction)
Background:     compute parity → distribute to m additional nodes
Full redundancy: k + m nodes hold the encoded data
```

The replication analog (our peer-repair design):

```
Write path:     op → W replicas (minimum for quorum commit)
Background:     reconciliation → distribute to N-W additional replicas
Full redundancy: all N replicas hold the op
```

---

## Peer-to-Peer Reconciliation Systems

A few systems use true peer-to-peer reconciliation as a primary
(not fallback) distribution mechanism:

### CouchDB

CouchDB uses **multi-master replication** with bidirectional sync:

- Any node can accept writes independently
- Nodes sync changes to peers via a pull-based replication protocol
- Conflicts are detected via revision trees (similar to vector clocks)
- Application resolves conflicts

CouchDB's reconciliation is the primary distribution path — there is
no coordinator that sends to all nodes. Each node pushes/pulls changes
from peers it knows about. This is the closest production analog to
our peer-repair design, though CouchDB is multi-writer (conflicts
possible) while our design is single-writer (no conflicts).

### Bayou (Xerox PARC, 1995)

Bayou is an academic system designed for disconnected/mobile
environments:

- Writes to a local replica only (the mobile device)
- Peers propagate via epidemic (gossip) sync when connected
- Ordering achieved via tentative/committed write classification
- Application-defined conflict resolution

Bayou's "write to one, propagate via gossip" model is the purest
form of Strategy B. It was never a production system but influenced
the design of CouchDB, Dynamo, and eventually our reconciliation
approach.

### Git

Git's distribution model, while not real-time, follows Strategy B:

- Developer commits to local repo (1 copy)
- Pushes to one remote (2 copies)
- Other clones pull from remotes (N copies over time)

No coordinator sends to all clones. Each clone pulls from peers it
knows about. Convergence is eventually complete but not bounded by
time — a developer who doesn't pull stays behind indefinitely.

---

## Anti-Entropy Mechanisms

Systems using Strategy B require robust anti-entropy. Three
mechanisms exist, applicable to both strategies:

### Read Repair (Passive)

Triggered on read — if the coordinator reads from R replicas and
detects a stale one, it pushes the latest value.

- **Used by:** Cassandra, Riak, Dynamo
- **Pro:** No background overhead
- **Con:** Only repairs data that is actually read. Cold data stays
  inconsistent forever.

### Merkle Tree Exchange (Active)

Background process: replicas build hash trees over their data,
exchange tree roots, drill down to find divergent keys.

- **Used by:** Cassandra (`nodetool repair`), Riak (AAE), Dynamo
- **Pro:** Repairs all data, including cold data. Efficient — only
  transfers divergent keys.
- **Con:** CPU and I/O overhead for tree maintenance. Tree rebuild
  is expensive for large datasets.

Riak's implementation is notable: persistent on-disk Merkle trees
(survive restarts), updated in real-time as writes arrive,
periodically regenerated from disk to catch silent corruption.

### LSN-Based Reconciliation (Log-Structured)

For totally-ordered logs (like our design): replicas compare a single
integer (`received_lsn`) rather than building hash trees.

- **Used by:** Our reconciliation design, database replication logs
- **Pro:** Extremely efficient — O(1) gap detection vs O(n) Merkle
  tree. No tree maintenance overhead.
- **Con:** Only works for totally-ordered, append-only logs. Cannot
  detect corruption within committed data (wrong value at a known
  LSN). Merkle trees needed for corruption detection.

**Our design uses LSN comparison as the primary mechanism** and
leaves Merkle trees as an optional enhancement for corruption
detection (see reconciliation design OQ-6).

---

## Comparison Table

| Aspect | Dynamo-family (Strategy A) | Erasure coding (Strategy B) | Our Peer-Repair (Strategy B) |
|---|---|---|---|
| **Write targets** | All N replicas | k data nodes | W replicas |
| **Wait for** | W ACKs | k ACKs | W ACKs |
| **Background fill** | Fallback only (repair failures) | Parity encoding | Reconciliation (primary path) |
| **Fill mechanism** | Read repair + Merkle trees | Reed-Solomon encoding | LSN-based peer pull |
| **Convergence** | Immediate (all N got the write) | Delayed (encoding time) | Delayed (~100-300ms) |
| **Durability window** | N copies immediately | k copies until parity computed | W copies until reconciled |
| **Data type** | KV pairs, documents | Large immutable blobs | Totally-ordered ops (log) |
| **Production proven** | Yes (Cassandra, Riak, DynamoDB) | Yes (Azure, Facebook f4) | No (novel) |
| **Conflict resolution** | Vector clocks / LWW / CRDTs | N/A (immutable) | None (single writer) |

---

## Implications for Kuberic

Kuberic's three replication designs map to known patterns:

| Kuberic design | Pattern | Prior art |
|---|---|---|
| **Leader-based** (existing) | Primary fans out to secondaries | Service Fabric, CNPG |
| **Writer-based** | Client fans out to all N, waits for W | Voldemort `routing=client` |
| **Peer-Repair** | Client sends to W, peers repair each other | Erasure coding lazy repair (Azure, f4) + CouchDB peer sync |

The reconciliation design is novel in that it applies the **erasure
coding lazy repair pattern** to **full replication** (not erasure
coded data) with **LSN-based reconciliation** (not Merkle trees or
parity computation). No production system combines these exactly,
but each component is individually proven.

**Key risk:** The reconciliation design introduces complexity
(epoch-aware reconciliation, GC coordination, committed_lsn
propagation) that Dynamo-family systems avoid by simply sending to
all replicas. The bandwidth savings from sending to W instead of N
must justify this complexity for the target workload.

---

## References

- DeCandia, G. et al. (2007). *Dynamo: Amazon's Highly Available
  Key-value Store.* SOSP '07.
- Demers, A. et al. (1987). *Epidemic Algorithms for Replicated
  Database Maintenance.* PODC '87.
- Calder, B. et al. (2011). *Windows Azure Storage: A Highly
  Available Cloud Storage Service with Strong Consistency.* USENIX
  ATC '11.
- Muralidhar, S. et al. (2014). *f4: Facebook's Warm BLOB Storage
  System.* OSDI '14.
- Terry, D. et al. (1995). *Managing Update Conflicts in Bayou, a
  Weakly Connected Replicated Storage System.* SOSP '95.
- Apache Cassandra. *Dynamo Architecture.*
  [cassandra.apache.org](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html)
- Riak KV. *Active Anti-Entropy.*
  [docs.riak.com](https://docs.riak.com/riak/kv/latest/learn/concepts/active-anti-entropy/index.html)
- CouchDB. *Replication.*
  [docs.couchdb.org](https://docs.couchdb.org/en/stable/replication/intro.html)
