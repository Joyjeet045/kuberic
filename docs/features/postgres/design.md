# PostgreSQL: Replicated PostgreSQL on Kuberic

A replicated PostgreSQL database orchestrated by kuberic-core. Unlike the
kvstore and SQLite examples, PostgreSQL has its own battle-tested streaming
replication — kuberic's WalReplicator data plane is **not used**. Instead,
kuberic provides lifecycle management, failover orchestration, and
epoch-based fencing while PostgreSQL handles the data plane natively.

---

## Goals

1. PostgreSQL streaming replication managed by kuberic lifecycle
2. Automatic failover: kuberic detects failure, selects best replica
   by LSN, promotes via `pg_ctl promote`
3. Switchover: graceful primary swap with write revocation
4. Epoch fencing: prevent split-brain / zombie primary reads
5. Copy protocol: `pg_basebackup` for new replica builds
6. Demonstrate kuberic as an **orchestration framework**, not just a
   replication engine

## Non-Goals

- Re-implement PostgreSQL streaming replication via kuberic's data plane
- Logical replication (physical streaming only)
- Connection pooling (out of scope — use PgBouncer externally)
- Backup/restore to object storage (CNPG-I concern, not kuberic)
- Multi-primary / active-active writes

---

## Required Configuration

PostgreSQL must be initialized and configured with these settings for
correctness:

```
# Required at initdb time (cannot be changed after)
initdb --data-checksums

# Required runtime settings (postgresql.conf)
wal_log_hints = on              # Enables pg_rewind (belt-and-suspenders with checksums)
synchronous_commit = on         # Writes durable on sync standbys before client ACK
hot_standby = on                # Allows PgMonitor to query standbys via SQL
logging_collector = off         # Logs go to stderr, piped through tracing
```

**Rationale**:
- `--data-checksums` + `wal_log_hints`: Required for `pg_rewind`. Without
  these, a demoted primary cannot rejoin and must do a full `pg_basebackup`.
- `synchronous_commit = on`: Critical for split-brain safety. If the old
  primary is partitioned, its synchronous commits hang (no standby to ACK),
  providing PG-native write fencing.
- `hot_standby = on`: PgMonitor queries `pg_last_wal_receive_lsn()` on
  standbys via SQL. With `hot_standby = off`, PG rejects all connections
  on standbys and monitoring breaks. Clients don't connect to standbys —
  `hot_standby` is only for monitoring access.

---

## Why Not the WalReplicator?

The kvstore and SQLite examples use kuberic's `WalReplicator` because those
systems have no built-in replication. The replicator ships operations from
primary to secondaries, tracks quorum ACKs, and manages copy/catchup.

PostgreSQL already provides all of this:

| Concern | WalReplicator (kvstore/sqlite) | PostgreSQL Native |
|---------|-------------------------------|-------------------|
| Data shipping | gRPC OperationStream | WAL sender/receiver |
| Durability | User ACKs after fsync | `synchronous_commit` |
| Quorum | QuorumTracker counts ACKs | `synchronous_standby_names ANY N` |
| Copy/rebuild | GetCopyState → user snapshot | `pg_basebackup` (adapter-direct) |
| Catchup | ReplicationQueue replay | Replication slots + WAL retention |
| Rollback | UpdateEpoch → user truncates | No-op (secondaries reconnect via ReconfigureStandby) |

Using WalReplicator for PostgreSQL would mean:
1. Parsing PG's binary WAL format (complex, version-dependent)
2. Bypassing PG's native WAL sender (losing its maturity)
3. Managing WAL segments in two places (PG + kuberic queue)
4. No benefit — PG's streaming is faster and more reliable

**Decision**: Kuberic orchestrates, PostgreSQL replicates.

---

## Architecture

### Integration Pattern: External Replication

This introduces a new integration pattern for kuberic — **external
replication** — where the database handles its own data plane and kuberic
provides the control plane:

```
┌─────────────────────────────────────────────────────┐
│                   kuberic operator                   │
│  (durable failover, switchover, reconfiguration)    │
└──────────────┬───────────────────────┬──────────────┘
               │ gRPC control plane    │
        ┌──────▼──────┐         ┌──────▼──────┐
        │  Pod (P)    │         │  Pod (S)    │
        │             │         │             │
        │ReplicaAgent │         │ReplicaAgent │
        │ PodRuntime  │         │ PodRuntime  │
        │ PgService   │         │ PgService   │
        │ PgMonitor   │         │ PgMonitor   │
        │             │         │             │
        │ ┌─────────┐ │   WAL   │ ┌─────────┐ │
        │ │ postgres ├─┼────────┼─► postgres │ │
        │ │ (primary)│ │streaming│ │(standby) │ │
        │ └─────────┘ │  repl   │ └─────────┘ │
        └─────────────┘         └─────────────┘
```

Key difference from kvstore/sqlite:
- **No WalReplicator actor** — no gRPC data plane between pods
- **No ReplicationQueue** — PG manages WAL retention via replication slots
- **No QuorumTracker** — PG manages `synchronous_standby_names`
- **PgMonitor** replaces replicator — queries PG for LSN, updates
  PartitionState so the operator can make failover decisions

### Client Access & Write Fencing

Clients connect directly to PostgreSQL's TCP port (5432) — there is no
gRPC proxy intercepting queries. This means kuberic's
`PartitionState.write_status()` cannot gate client writes at the
application layer like kvstore/sqlite do.

**How kvstore/sqlite fence writes**: `revoke_write_status()` sets an
atomic (`AccessStatus::ReconfigurationPending`), and
`StateReplicatorHandle::replicate()` checks this atomic before accepting
data — returning `Err(ReconfigurationPending)` immediately. This works
because all writes go through kuberic's WalReplicator data plane.

**Why PG is different**: PG clients connect directly to PG's TCP port.
Writes go through PG's native SQL engine, never touching kuberic's
`replicate()` or `write_status()` atomic. The atomic is set but nobody
checks it.

Instead, write fencing uses **PostgreSQL's native mechanisms**:

| Kuberic Event | PG Fencing Action | Effect on Clients |
|---------------|-------------------|-------------------|
| ChangeRole(ActiveSecondary) — demotion | `ALTER SYSTEM SET default_transaction_read_only = on` + `pg_reload_conf()` | New transactions get `ERROR: cannot execute ... in a read-only transaction` |
| ReconfigureStandby — reconnect to new primary | Create `standby.signal` + restart as standby | PG is physically read-only (standby mode) |
| Epoch fence (zombie primary) | Shut down PG → rejoin as standby via BuildReplica | Connections dropped |
| ChangeRole(Primary) — promotion | `pg_ctl promote` + `ALTER SYSTEM SET default_transaction_read_only = off` + reload | Writable |

**Implementation hook**: The adapter's `ChangeRole(ActiveSecondary)`
handler is the demote signal. During switchover, the operator submits a
correlated `RevokeWriteStatus` action and then a correlated
`ChangeRole(new_epoch, ActiveSecondary)` action to the old primary.
The adapter executes the PG-level fencing there:

```rust
// adapter.rs — handle_change_role
Role::ActiveSecondary => {
    // If demoting from Primary → fence writes at PG level
    if current_role == Role::Primary {
        if let Ok((client, _conn)) = instance.connect().await {
            // Soft fence: new transactions default to read-only
            let _ = client
                .execute("ALTER SYSTEM SET default_transaction_read_only = on", &[])
                .await;
            let _ = client.execute("SELECT pg_reload_conf()", &[]).await;
            info!("write fencing applied: default_transaction_read_only = on");
        }
    }
    monitor.set_role(Role::ActiveSecondary);
    Ok(())
}
```

**Write fence lifecycle**:

```
Switchover timeline on old primary:

1. revoke_write_status()
   └─ atomic set to ReconfigurationPending (PG doesn't know yet)

2. ChangeRole(ActiveSecondary)                    ← adapter fences here
   └─ ALTER SYSTEM SET default_transaction_read_only = on
   └─ pg_reload_conf()
   └─ New transactions get read-only errors immediately

3. UpdateCatchUpConfiguration
   └─ ReconfigureStandby RPC arrives
   └─ Create standby.signal (KP-3 fix)
   └─ Rewrite primary_conninfo → new primary
   └─ Restart PG as physical standby              ← hard fence
```

**Removing the fence on promotion**: When a standby is promoted to
primary, `ChangeRole(Primary)` should also clear the fence:

```rust
Role::Primary => {
    // ... promote logic ...
    // Clear write fence if previously set
    if let Ok((client, _conn)) = instance.connect().await {
        let _ = client
            .execute("ALTER SYSTEM SET default_transaction_read_only = off", &[])
            .await;
        let _ = client.execute("SELECT pg_reload_conf()", &[]).await;
    }
    monitor.set_role(Role::Primary);
    Ok(())
}
```

**Limitations of `default_transaction_read_only`**:

- **Bypassable**: Clients can override with `SET SESSION
  default_transaction_read_only = off` or `BEGIN READ WRITE`. This is a
  known PG limitation — the setting is a default, not a restriction.
- **Not instant for existing transactions**: In-flight write transactions
  complete; only new transactions are affected.
- **Adequate for kuberic's switchover window**: The window between
  demotion and PG restart (ReconfigureStandby) is typically <2 seconds.
  Combined with `synchronous_commit = on` (writes hang when standbys
  disconnect), the risk of data divergence is minimal.
- **Hard fence follows**: PG is restarted as a standby (physically
  read-only) within seconds. The soft fence just buys time.

**Client routing** uses a Kubernetes Service with label selectors:
- **Read-write Service** (`-rw`): selects only the pod with
  `role=primary` label. Kuberic operator updates pod labels on
  role changes.
- **Read-only Service** (`-ro`, Phase 2): selects pods with
  `role=secondary` + `hot_standby=on`.

Clients connect to the Service DNS name (e.g.,
`mydb-rw.namespace.svc:5432`). On failover, the operator relabels pods
and the Service automatically routes to the new primary — no client-side
discovery needed.

**PartitionState still tracks access status** — PgMonitor updates
`read_status` and `write_status` atomics based on PG's actual state
(e.g., `pg_is_in_recovery()`, `default_transaction_read_only`). The
operator uses these for health/status reporting, but they don't gate
client access — PG does that itself.

### Component Overview

| Component | Responsibility |
|-----------|---------------|
| **PgService** | Lifecycle event handler. Starts/stops PG, handles ChangeRole |
| **PgMonitor** | Polls PG for replication status, updates PartitionState |
| **PgReplicatorAdapter** | Implements ReplicatorHandle contract; delegates to PgMonitor |
| **PgInstanceManager** | PG process management: start, stop, promote, configure |

---

## Detailed Design

### PgInstanceManager — PostgreSQL Process Lifecycle

Wraps `pg_ctl` and PG configuration. Runs PostgreSQL as a child process.

```rust
pub struct PgInstanceManager {
    data_dir: PathBuf,
    pg_bin: PathBuf,         // e.g. /usr/lib/postgresql/16/bin (CLI arg)
    child: Option<Child>,    // PostgreSQL server process
    port: u16,
    socket_dir: PathBuf,     // UDS directory (= data_dir for isolation)
}

impl PgInstanceManager {
    /// Initialize a new PG cluster (initdb --data-checksums)
    pub async fn init_db(&self) -> Result<()>;

    /// Start PostgreSQL with given config.
    /// Passes -c unix_socket_directories=<socket_dir> -c port=<port>.
    pub async fn start(&mut self, config: &PgConfig) -> Result<()>;

    /// Stop PostgreSQL (fast mode)
    pub async fn stop(&mut self) -> Result<()>;

    /// Promote standby to primary (pg_ctl promote)
    pub async fn promote(&self) -> Result<()>;

    /// Run pg_basebackup from a source to initialize this replica
    pub async fn base_backup(&self, source_addr: &str) -> Result<()>;

    /// Run pg_rewind to rejoin as standby after demotion
    pub async fn rewind(&self, target_addr: &str) -> Result<()>;

    /// Configure streaming replication (primary_conninfo, etc.)
    pub fn configure_standby(&self, primary_addr: &str) -> Result<()>;

    /// Configure synchronous standbys
    pub fn configure_sync_standbys(&self, standbys: &[String]) -> Result<()>;

    /// Connect to the local PG instance
    pub async fn connect(&self) -> Result<PgConnection>;
}
```

### PostgreSQL Log Handling

PostgreSQL writes all log output to **stderr** (`logging_collector = off`).
Stdout is silent during normal operation. The instance manager pipes both
through `tracing`:

```rust
// In PgInstanceManager::start()
let mut child = Command::new(self.pg_bin.join("postgres"))
    .args(["-D", &self.data_dir.to_string_lossy()])
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()?;

let stdout = BufReader::new(child.stdout.take().unwrap());
let stderr = BufReader::new(child.stderr.take().unwrap());
tokio::spawn(async move {
    let mut stdout_lines = stdout.lines();
    let mut stderr_lines = stderr.lines();
    loop {
        tokio::select! {
            Ok(Some(line)) = stderr_lines.next_line() => {
                tracing::info!(target: "postgres", "{}", line);
            }
            Ok(Some(line)) = stdout_lines.next_line() => {
                tracing::debug!(target: "postgres", "{}", line);
            }
            else => break,
        }
    }
});
```

This gives unified structured logging — PG messages appear alongside
kuberic's own traces, filterable by `target: "postgres"`. In K8s,
container stdout is collected by the log aggregator as usual.

**Note**: CNPG uses a more sophisticated approach — PG writes CSV to a
FIFO via `logging_collector = on`, and a LogPipe goroutine re-serializes
to JSON on stdout. That's overkill for this example.

### Child Process Monitoring

The PG child process must be monitored for unexpected exit (OOM kill,
segfault, disk corruption). A separate task awaits `child.wait()` and
reports fault on unexpected exit:

```rust
// In PgInstanceManager::start(), after spawning log forwarder:
let fault_tx = fault_tx.clone();
tokio::spawn(async move {
    let status = child.wait().await;
    match status {
        Ok(exit) if exit.success() => {
            tracing::info!(target: "postgres", "PostgreSQL exited normally");
        }
        Ok(exit) => {
            tracing::error!(target: "postgres", "PostgreSQL exited: {}", exit);
            let _ = fault_tx.send(FaultType::Permanent).await;
        }
        Err(e) => {
            tracing::error!(target: "postgres", "Failed to wait on PG: {}", e);
            let _ = fault_tx.send(FaultType::Permanent).await;
        }
    }
});
```

On unexpected exit, `fault_tx` notifies the operator, which triggers
failover. This is critical because PG dying inside a running pod does
not make the pod NotReady unless a liveness probe detects it.

### PgMonitor — Replication Status Polling

Periodically queries PostgreSQL for replication progress and updates
kuberic's `PartitionState` atomics. This is how the operator knows
each replica's LSN for failover decisions.

#### PostgreSQL WAL LSN Pipeline

The primary tracks every standby's progress via `pg_stat_replication`:

```
Primary WAL pipeline (per standby):

  pg_current_wal_lsn()  →  sent_lsn  →  write_lsn  →  flush_lsn  →  replay_lsn
  (WAL generated)          (sent to      (written to    (fsync'd on    (applied on
                            standby)      standby disk)  standby disk)  standby DB)
```

| Column | Meaning | Source |
|--------|---------|--------|
| `pg_current_wal_lsn()` | Latest WAL position on primary | Primary (SQL function) |
| `sent_lsn` | How far WAL sent to each standby | Primary (`pg_stat_replication`) |
| `flush_lsn` | How far standby has fsync'd | Primary (`pg_stat_replication`) |
| `replay_lsn` | How far standby has applied | Primary (`pg_stat_replication`) |

**Key insight**: The primary already knows every standby's progress.
No need to query standbys directly for catchup or quorum tracking.
`flush_lsn` (not `replay_lsn`) is the durability boundary for
`synchronous_commit = on`.

#### PgMonitor Implementation

```rust
pub struct PgMonitor {
    instance: Arc<PgInstanceManager>,
    state: Arc<PartitionState>,
    fault_tx: mpsc::Sender<FaultType>,
    role: Role,
    consecutive_failures: u32,
}

impl PgMonitor {
    /// Run the monitor loop. Polls PG every 1 second.
    pub async fn run(&self, token: CancellationToken) {
        let interval = Duration::from_secs(1);
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = tokio::time::sleep(interval) => {
                    self.poll_status().await;
                }
            }
        }
    }

    /// Force an immediate poll. Called by the operator before failover
    /// candidate selection to minimize LSN staleness.
    pub async fn poll_now(&self) {
        self.poll_status().await;
    }

    async fn poll_status(&self) {
        match self.role {
            Role::Primary => {
                // current_progress:
                //   SELECT pg_current_wal_lsn()
                //
                // committed_lsn (for ANY N quorum):
                //   SELECT flush_lsn FROM pg_stat_replication
                //   WHERE sync_state IN ('sync', 'quorum')
                //   ORDER BY flush_lsn DESC
                //   LIMIT 1 OFFSET (N-1)
                //   → Nth-highest flush_lsn = quorum durability boundary
                //
                // On query failure: retain last-known-good values, increment
                //   consecutive_failures. After 3 failures → fault_tx.
            }
            Role::ActiveSecondary | Role::IdleSecondary => {
                // current_progress:
                //   SELECT pg_last_wal_receive_lsn()
                //   → self-reported receive position (for operator failover selection)
                //   Returns NULL if never connected — retain previous value.
                //
                // On query failure: retain last-known-good values.
            }
            _ => {}
        }
    }
}
```

**Staleness note**: PgMonitor polls every 1 second. For failover candidate
selection, the operator should call `poll_now()` on all reachable replicas
before reading `PartitionState` to minimize staleness. Under synchronous
replication, replicas are typically within milliseconds of each other, so
1-second staleness rarely changes candidate ranking.

**LSN type mapping**: PostgreSQL's `XLogRecPtr` is a 64-bit byte offset
(text format `0/XXXXXXXX`). Parse as `u64`, cast to kuberic's `Lsn` (`i64`).
Comparison semantics (>, <, ==) are preserved within the same timeline.
Cross-timeline LSN comparisons are invalid — PgMonitor must invalidate
stale LSN values on epoch change (reset to 0 and re-query).

The PartitionState updates enable:
- Operator reads `current_progress` for failover candidate selection
- Operator reads `committed_lsn` for quorum health assessment
- `read_status` / `write_status` fencing works as normal

### PostgreSQL Connection Strategy

PgMonitor and PgInstanceManager connect to the local PG instance via
**Unix domain socket** (UDS) — same approach as CNPG. No TCP overhead,
no TLS, `peer` auth (no password):

```rust
// Connect via UDS — socket_dir is the data_dir for test isolation
let (client, conn) = tokio_postgres::connect(
    &format!("host={} port={} user=postgres dbname=postgres",
             self.socket_dir.display(), self.port),
    NoTls,
).await?;

// Spawn the connection task
tokio::spawn(conn);
```

**Dependency**: `tokio-postgres` (async PG client). Used for all SQL
queries (monitoring, fencing, health checks). Admin tools (`pg_ctl`,
`initdb`, `pg_basebackup`, `pg_rewind`) are spawned as child processes.

### Test Instance Isolation

Replication/failover tests run 2–3 PG instances simultaneously on the
same host. Each instance uses its **data_dir as the socket directory**
to avoid collisions:

```
Instance 1: port=15432, data_dir=/tmp/pg-test-1, socket_dir=/tmp/pg-test-1
Instance 2: port=15433, data_dir=/tmp/pg-test-2, socket_dir=/tmp/pg-test-2
Instance 3: port=15434, data_dir=/tmp/pg-test-3, socket_dir=/tmp/pg-test-3
```

PG is started with `-c unix_socket_directories=<data_dir>` so the socket
file (`.s.PGSQL.<port>`) lives inside the data directory. Cleanup is just
`rm -rf data_dir`. Tests use `#[serial]` to avoid port contention across
test cases (same as kvstore/sqlite).

### PgReplicatorAdapter — Framework Integration

kuberic-core's PodRuntime requires a `ReplicatorHandle` from the `Open`
lifecycle event. The adapter satisfies this contract without running a
WalReplicator. It directly receives `ReplicatorControlEvent` on the
control channel and handles each variant with PG-specific logic:

```rust
/// Spawn the PG control event loop. Returns a ReplicatorHandle
/// that the runtime uses to send lifecycle commands.
pub fn create_pg_replicator(
    instance: Arc<PgInstanceManager>,
    state: Arc<PartitionState>,
    monitor: Arc<PgMonitor>,
) -> ReplicatorHandle {
    let (control_tx, mut control_rx) = mpsc::channel::<ReplicatorControlEvent>(16);
    let shutdown = CancellationToken::new();
    let data_address = instance.replication_address(); // pod IP:port, not localhost

    let mut current_role = Role::None;

    tokio::spawn(async move {
        while let Some(event) = control_rx.recv().await {
            match event {
                ReplicatorControlEvent::Open { reply, .. } => {
                    // PG already started in LifecycleEvent::Open
                    let _ = reply.send(Ok(()));
                }
                ReplicatorControlEvent::ChangeRole { epoch, role, reply } => {
                    if role == current_role {
                        let _ = reply.send(Ok(())); // idempotent
                        continue;
                    }
                    // pg_ctl promote (if Primary), stop PG (if None),
                    // configure standbys, update monitor role
                    current_role = role;
                    let _ = reply.send(Ok(()));
                }
                ReplicatorControlEvent::UpdateEpoch { epoch, reply } => {
                    // pg_rewind directly if diverged (no state_provider needed)
                    let _ = reply.send(Ok(()));
                }
                ReplicatorControlEvent::BuildReplica { replica, reply } => {
                    // Coordinate pg_basebackup via state provider
                    let _ = reply.send(Ok(()));
                }
                ReplicatorControlEvent::UpdateCatchUpConfiguration { reply, .. } => {
                    // Map to PG: add replica to synchronous_standby_names
                    // ALTER SYSTEM SET synchronous_standby_names = '...'
                    // SELECT pg_reload_conf()
                    let _ = reply.send(Ok(()));
                }
                ReplicatorControlEvent::UpdateCurrentConfiguration { reply, .. } => {
                    // Finalize sync standby list after catchup complete
                    let _ = reply.send(Ok(()));
                }
                ReplicatorControlEvent::WaitForCatchUpQuorum { reply, .. } => {
                    // "Caught up" = standby flush_lsn >= target_lsn
                    // All data queried from PRIMARY's pg_stat_replication.
                    let inst = instance.clone();
                    tokio::spawn(async move {
                        let target_lsn = inst.query_current_wal_lsn().await;
                        loop {
                            // SELECT flush_lsn FROM pg_stat_replication
                            // WHERE application_name IN (must_catch_up replicas)
                            // All flush_lsn >= target_lsn? → done
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            if all_caught_up { break; }
                            // Timeout after 30s → reply Err
                        }
                        let _ = reply.send(Ok(()));
                    });
                }
                ReplicatorControlEvent::RemoveReplica { reply, .. } => {
                    // Remove from synchronous_standby_names + reload
                    let _ = reply.send(Ok(()));
                }
                ReplicatorControlEvent::OnDataLoss { reply } => {
                    let _ = reply.send(Ok(DataLossAction::default()));
                }
                ReplicatorControlEvent::Close { reply } => {
                    // pg_ctl stop -m fast
                    let _ = reply.send(Ok(()));
                }
                ReplicatorControlEvent::Abort => {
                    // pg_ctl stop -m immediate (best effort)
                }
            }
        }
    });

    ReplicatorHandle::new(control_tx, state, data_address, shutdown)
}
```

The adapter creates a `ReplicatorHandle` with:
- `control_tx` → receives `ReplicatorControlEvent` directly (no wrapper enum)
- `state` → shared PartitionState (updated by PgMonitor)
- `data_address` → PG's streaming replication address (pod IP:port from
  `OpenContext.data_bind`, not localhost — must be routable from peers)
- `shutdown` → cancellation token

**Key**: The adapter does NOT create a gRPC data server. The
`data_address` points to PostgreSQL's native replication port.

### Open Sequence

```
LifecycleEvent::Open { ctx }
  │
  ├─ Create PgInstanceManager
  │   └─ If OpenMode::New → initdb + configure
  │   └─ If OpenMode::Existing → verify data_dir
  │
  ├─ Start PostgreSQL
  │   └─ pg_ctl start -D data_dir
  │
  ├─ Create PgMonitor
  │   └─ Spawn polling loop (updates PartitionState)
  │
  ├─ Create PgReplicatorAdapter
  │   └─ Spawn control event handler (handles all lifecycle directly —
  │      no StateProvider channel needed, PG operations are inline)
  │   └─ Build ReplicatorHandle
  │
  └─ Reply with ReplicatorHandle
```

### ChangeRole Flows

#### → Primary

```
ChangeRole { role: Primary }
  │
  ├─ If was standby → pg_ctl promote -w -t 60
  │   └─ Wait for promotion: poll pg_is_in_recovery() = false
  │   └─ Timeout after 60s → reply error (triggers switchover rollback A3)
  │
  ├─ Configure synchronous standbys
  │   └─ ALTER SYSTEM SET synchronous_standby_names = 'ANY 1 (...)'
  │   └─ SELECT pg_reload_conf()
  │
  ├─ Update PgMonitor role → Primary
  │   └─ Monitor now queries pg_stat_replication
  │
  ├─ Start client gRPC server (idempotent — skip if already running)
  │   └─ Accept SQL queries from clients
  │
  └─ Reply with client address
```

#### → IdleSecondary (new replica join)

```
ChangeRole { role: IdleSecondary }
  │
  ├─ Ensure PG is stopped
  │
  ├─ Update PgMonitor role → IdleSecondary
  │
  └─ Reply immediately with empty address
      (BuildReplica arrives later as a separate control event —
       do NOT block waiting for it here)
```

#### → ActiveSecondary (after copy + catchup)

```
ChangeRole { role: ActiveSecondary }
  │
  ├─ Verify PG is running and streaming
  │   └─ Check pg_last_wal_receive_lsn() is advancing
  │
  ├─ Update PgMonitor role → ActiveSecondary
  │
  └─ Reply with empty address
```

#### → None (demotion)

```
ChangeRole { role: None }
  │
  ├─ Set write_status → NotPrimary
  ├─ PG keeps running (stopped on Close, not here)
  └─ Reply OK
```

**Data lifecycle**: ChangeRole(None) marks the role. Close does cleanup:
- **ChangeRole(None) → Close**: PG stopped. Data directory deleted.
- **Close (from any other role)**: PG stopped. Data directory preserved.
  The replica will be reopened with `OpenMode::Existing`.

### Copy Protocol — pg_basebackup

When the operator builds a new replica, `BuildReplica` is sent to the
**primary's** adapter (kuberic convention), with the secondary's
`replicator_address`. In WalReplicator, the primary connects to the
secondary's gRPC data server and pushes state. For PG, we use the same
app-to-app `data_address` channel but with a PG-specific gRPC service.

#### PgDataService — App-to-App Protocol

Each PG pod runs a `PgDataService` gRPC server on `data_bind` (the same
address that WalReplicator would use for `ReplicatorDataServer`). This
keeps the existing two-address model:

| Address | Owner | Purpose |
|---------|-------|---------|
| `control_bind` | ControlServer | Operator → pod lifecycle |
| `data_bind` | PgDataService | Pod → pod (BuildReplica coordination) |

```protobuf
service PgDataService {
    // Primary calls this on the secondary during BuildReplica.
    // Secondary runs pg_basebackup from the given primary address,
    // configures standby.signal, and starts PG.
    rpc CloneFrom(CloneFromRequest) returns (CloneFromResponse);
}

message CloneFromRequest {
    string primary_host = 1;
    uint32 primary_port = 2;
}

message CloneFromResponse {
    bool success = 1;
    string error = 2;
}
```

#### BuildReplica Flow

```
Operator: add_replica(secondary_handle)
  │
  ├─ 1. Open secondary → initdb + start PgDataService on data_bind
  │
  ├─ 2. ChangeRole(IdleSecondary) on secondary → reply immediately
  │
  ├─ 3. BuildReplica(secondary_info) on PRIMARY
  │      │
  │      ├─ Primary adapter receives BuildReplica
  │      │   └─ replica_info.replicator_address = secondary's data_bind
  │      │
  │      ├─ Primary connects to secondary's PgDataService
  │      │   └─ Calls CloneFrom { primary_host, primary_port }
  │      │
  │      ├─ Secondary handles CloneFrom:
  │      │   ├─ Stop PG, remove data_dir contents
  │      │   ├─ pg_basebackup -D data_dir -h primary_host -p primary_port -R
  │      │   │   (-R creates standby.signal + primary_conninfo)
  │      │   ├─ Write kuberic config (socket_dir, port, etc.)
  │      │   ├─ Start PG → auto-streams from primary
  │      │   └─ Reply CloneFromResponse { success: true }
  │      │
  │      └─ Primary replies Ok on BuildReplica reply channel
  │
  ├─ 4. ChangeRole(ActiveSecondary) on secondary
  │
  └─ 5. Reconfigure quorum
```

**Failure handling**: If `pg_basebackup` fails (network timeout, disk
full), the secondary cleans up the partial data directory and replies
`CloneFromResponse { success: false, error: "..." }`. The primary
translates this to an error on the `BuildReplica` reply channel. The
operator retries with backoff or marks the replica as failed.

### Epoch Handling

`UpdateEpoch` is sent to surviving secondaries after failover. For PG,
this is a **no-op** — no pg_rewind or divergence check needed.

**Why no-op is correct (contrast with kvstore/sqlite)**:

In the WalReplicator path, `UpdateEpoch` forwards to
`StateProviderEvent::UpdateEpoch { epoch, previous_epoch_last_lsn }`.
kvstore and sqlite use this to **rollback uncommitted ops** — operations
that were applied locally (`current_lsn > previous_epoch_last_lsn`) but
never quorum-committed by the old primary. Without rollback, the
secondary would have divergent state.

PG doesn't need this because:

1. **PG handles divergence natively.** When a secondary reconnects to a
   new primary (via `ReconfigureStandby`), PG's timeline following
   mechanism automatically replays WAL from the fork point. No manual
   rollback is possible or needed — PG's WAL receiver is the authority.

2. **No in-memory state to rollback.** kvstore/sqlite have in-memory
   ops applied by the user's `StateProvider`. PG's WAL replay is
   entirely internal to the postgres process — the adapter has no
   ops to undo.

3. **`previous_epoch_last_lsn` doesn't map.** The framework's
   `committed_lsn()` is observational (PgMonitor polls
   `pg_stat_replication`), not the authoritative PG WAL boundary.
   Rolling back to this value would be meaningless for PG.

**What happens to each replica type during failover**:

- **Zombie primaries** are excluded by the durable failover workflow — they
  never receive `UpdateEpoch`.
- **Surviving secondaries** get `ReconfigureStandby` (via
  `UpdateCatchUpConfiguration`) to reconnect to the new primary. PG
  handles timeline following automatically when reconnected.
- **Demoted primaries** (switchover) get `ReconfigureStandby` which
  detects `was_primary` and uses `pg_rewind` (fallback: `pg_basebackup`)
  to handle timeline divergence.

**Minor consideration**: PgMonitor's cached LSN values may be briefly
stale after epoch change (from old timeline). LSNs are monotonic across
PG timelines in practice (new timeline continues from fork point), so
this doesn't affect failover candidate selection. A future optimization
could reset `current_progress` to 0 in UpdateEpoch and let PgMonitor
re-query on the next poll cycle.

### Failover Sequence

```
1. Operator detects primary failure (gRPC health check fails)

2. Operator triggers poll_now() on all reachable replicas
   └─ PgMonitor does immediate LSN query (minimizes staleness)

3. Operator reads PartitionState from each replica
   └─ current_progress = pg_last_wal_receive_lsn (freshly polled)

4. Select best candidate: highest current_progress
   └─ Tie-break: replica ID (deterministic)

5. ChangeRole(Primary) on selected replica
   └─ pg_ctl promote -w -t 60
   └─ Configure sync standbys
   └─ Start client server

6. ChangeRole(None) on old primary (if reachable)
   └─ Stop PG entirely (pg_ctl stop -m fast)
   └─ Not just write revocation — PG must be fully stopped

7. UpdateEpoch on remaining secondaries (no-op — secondaries already
   reconnected via ReconfigureStandby in step 5's UpdateCatchUpConfig)

8. Old primary eventually gets BuildReplica
   └─ pg_basebackup → rejoin as standby
```

### Switchover Sequence

```
1. Operator initiates switchover to target replica

2. Revoke write status on old primary
   └─ PartitionState atomic set (PG-level fencing is KP-4 future work)

3. ChangeRole(ActiveSecondary) on old primary
   └─ Monitor role updated, PG still running

4. ChangeRole(Primary) on target
   └─ pg_ctl promote -w -t 60 (creates timeline N+1)
   └─ Configure sync standbys

5. UpdateCatchUpConfiguration on new primary
   └─ ReconfigureStandby on old primary:
      - Detects was_primary (no standby.signal)
      - Tries pg_rewind from new primary (fast — diverged pages only)
      - If pg_rewind fails (WAL recycled): full pg_basebackup fallback
      - Creates standby.signal, patches config, starts as standby
   └─ ReconfigureStandby on other standbys:
      - Rewrites primary_conninfo to new primary
      - Restarts PG (timeline following automatic)
   └─ Configures synchronous_standby_names (ANY {quorum})

6. WaitForCatchUpQuorum on new primary
   └─ Polls flush_lsn from pg_stat_replication until standbys caught up

7. UpdateCurrentConfiguration (finalize)
```

**Timeline divergence during switchover**: Between steps 3 and 5, the
old primary's PG is still running and its background processes
(checkpointer, stats) generate small WAL records on the old timeline.
Other standbys connected to the old primary receive these records,
pushing their recovery point past the fork point. When reconfigured to
follow the new primary's timeline, they can't follow without rewinding.

For the **old primary** (was_primary = true): `ReconfigureStandby`
detects this and uses pg_rewind (or pg_basebackup fallback).

For **other standbys**: The divergence is typically tiny (< 1 WAL
segment from background PG activity). PG can usually follow the new
timeline automatically because the divergence is within the same WAL
segment that the new primary also has. In rare cases where the standby
has truly diverged past the fork point, the restart will fail and the
operator will rebuild via BuildReplica/CloneFrom on the next reconcile
cycle.

**Contrast with failover**: In failover, the old primary is dead — all
standbys' WAL receivers disconnect immediately. Their recovery points
stay at or before the fork point, so timeline following works cleanly
without pg_rewind.

---

## Comparison with CNPG

| Aspect | CNPG | Kuberic PostgreSQL |
|--------|------|--------------------|
| **Operator** | Go, full K8s operator with CRD | Rust, CRD-backed durable workflows |
| **Instance manager** | Go binary (PID 1 in pod) | Rust PgInstanceManager (child process) |
| **Failover trigger** | HTTP health check failure | gRPC control plane failure |
| **Candidate selection** | LSN-based (received, then replayed) | LSN-based (PartitionState.current_progress) |
| **Promotion** | `pg_ctl promote` | `pg_ctl promote` |
| **Demoted primary** | `pg_rewind` (automatic) | `pg_rewind` first, `pg_basebackup` fallback (via ReconfigureStandby) |
| **Fencing** | Annotation-based + self-fencing | Epoch-based (kuberic protocol) |
| **Quorum** | PG `synchronous_standby_names` | PG `synchronous_standby_names` |
| **Split-brain** | Operator detects multi-primary | Epoch fencing + PG sync commit fencing |
| **Pod management** | Direct pod management (no StatefulSet) | kuberic-operator manages pods |
| **New replica** | `pg_basebackup` | `pg_basebackup` (via copy protocol) |
| **WAL archiving** | Plugin-based (CNPG-I) | Not implemented (future) |

Key differences:
1. **Fencing**: CNPG uses annotation-based fencing + liveness probe
   self-fencing. Kuberic uses epoch-based fencing (SF protocol) — simpler,
   distributed after promotion, prevents zombie reads atomically.
2. **Operator model**: CNPG manages pods directly. Kuberic persists durable
   topology workflows in CRD status and executes fenced actions through each
   pod's ReplicaAgent.
3. **Instance manager**: CNPG's is a full Go binary running as PID 1.
   Kuberic's is a Rust library called by PgService — lighter weight
   but requires the kuberic runtime as the process entry point.

---

## Framework Impact

This example introduces the **external replication** pattern. Changes
needed in kuberic-core:

### Required Changes

1. **`ReplicatorHandle` generalization**: Currently tightly coupled to
   WalReplicatorActor. The handle already uses a generic control channel
   (`mpsc::Sender<ReplicatorControlEvent>`) — no change needed, but the
   PgReplicatorAdapter must translate `ReplicatorControlEvent` to PG
   operations.

2. **Data address semantics**: Currently `data_address` is the gRPC data
   plane address. For PG, it's the streaming replication address
   (derived from `OpenContext.data_bind`). The operator stores this in
   `ReplicaInfo` — all operator-side consumers use it only for
   registration/status, not for direct gRPC connections, so the semantic
   change is safe.

3. **No StateReplicatorHandle usage**: The `replicate()` method is not
   called — PG commits go through PG directly. Verified: `replicate()`
   is only called by user code, never by framework code. The
   `ServiceContext` returned from the adapter will have a no-op
   `StateReplicatorHandle` with unused `copy_stream` and
   `replication_stream` (set to `None`).

### No Changes Needed

- `PartitionState` atomics — PgMonitor writes the same fields
- `LifecycleEvent` enum — Open/ChangeRole/Close/Abort are generic
- `StateProviderEvent` — not used for PG (adapter handles PG ops directly)
- Operator workflows use the transport-agnostic `ReplicaHandle` status and
  correlated-action surface; `PartitionDriver` is read-only recovery.

### Future: Trait-based Replicator

If more external-replication databases are added, extract a `Replicator`
trait:

```rust
pub trait Replicator: Send + Sync {
    async fn change_role(&self, epoch: Epoch, role: Role) -> Result<()>;
    async fn update_epoch(&self, epoch: Epoch) -> Result<()>;
    async fn build_replica(&self, replica: ReplicaInfo) -> Result<()>;
    fn state(&self) -> &Arc<PartitionState>;
    fn data_address(&self) -> &str;
    fn abort(&self);
}
```

This is a Phase 2 concern — for now, the PgReplicatorAdapter directly
constructs a `ReplicatorHandle` using the existing struct.

---

## Implementation Plan

### Phase 1: Core Infrastructure

- PgInstanceManager: initdb, start, stop, promote, configure
- PgMonitor: poll LSN, update PartitionState
- PgReplicatorAdapter: satisfy ReplicatorHandle contract
- PgService: lifecycle event handler (Open, ChangeRole, Close)
- Basic client gRPC: Execute SQL, return rows
- Integration test: 3-pod cluster, write on primary, read on primary

### Phase 2: Failover & Replication

- Streaming replication setup (primary_conninfo, standby.signal)
- pg_basebackup for new replica builds (copy protocol)
- Failover test: kill primary, verify promotion + data survival
- Switchover test: graceful primary swap with write fencing verification
- ~~pg_rewind for demoted primary rejoin~~ → Done: ReconfigureStandby
  detects demoted primary, tries pg_rewind first, falls back to
  pg_basebackup

### Phase 3: Robustness

- ~~Epoch fencing: UpdateEpoch triggers pg_rewind~~ → Simplified: no-op.
  Secondaries reconnect via ReconfigureStandby; demoted primaries rejoin
  via pg_rewind (fallback: pg_basebackup) in ReconfigureStandby.
- ~~Synchronous replication~~ → Done in Phase 2 (synchronous_standby_names)
- ~~Quorum health: committed_lsn~~ → Done in Phase 2 (PgMonitor flush_lsn)
- ~~Crash recovery~~ → PG handles natively (tested via failover test)

---

## Design Decisions

### ~~DD-1: UpdateEpoch~~ (Simplified)

Originally planned to detect WAL divergence and trigger pg_rewind in
`UpdateEpoch`. Abandoned because:
1. `ReplicatorControlEvent::UpdateEpoch` carries only `epoch`, not
   `previous_epoch_last_lsn` — no divergence threshold available.
2. Secondaries' `PartitionState.committed_lsn()` is always 0 (only
   primary's PgMonitor sets it) — comparison is meaningless.
3. Zombie primaries are removed by the operator — they never receive
   `UpdateEpoch`.
4. Surviving secondaries are reconnected via `ReconfigureStandby`.

`UpdateEpoch` is a no-op. Old primaries rejoin via `BuildReplica` →
`CloneFrom` (full pg_basebackup).

### DD-2: Replica naming for `synchronous_standby_names`

PG's `synchronous_standby_names` uses `application_name` strings to
identify standbys. kuberic uses `ReplicaId` (i64). We need a mapping.

**Resolution**: Each PG standby connects with
`application_name = 'kuberic_{replica_id}'` in its `primary_conninfo`.
This is set automatically during `pg_basebackup -R` by appending to the
generated `primary_conninfo`, or by writing `postgresql.auto.conf`
after cloning.

```
primary_conninfo = 'host=primary port=5432 application_name=kuberic_2'
```

The `UpdateCatchUpConfiguration` handler maps `ReplicaSetConfig.members`
to PG's format:

```rust
ReplicatorControlEvent::UpdateCatchUpConfiguration { current, reply, .. } => {
    // Build synchronous_standby_names from replica IDs
    let standby_names: Vec<String> = current.members.iter()
        .filter(|r| r.id != self_replica_id && r.role != Role::None)
        .map(|r| format!("kuberic_{}", r.id))
        .collect();

    let quorum = current.write_quorum.saturating_sub(1); // subtract self
    if !standby_names.is_empty() && quorum > 0 {
        let names = standby_names.join(", ");
        let sql = format!(
            "ALTER SYSTEM SET synchronous_standby_names = 'ANY {quorum} ({names})'"
        );
        client.execute(&sql, &[]).await?;
        client.execute("SELECT pg_reload_conf()", &[]).await?;
    }
    // Also store primary address from members for future pg_rewind
    let _ = reply.send(Ok(()));
}
```

**Convention**: `kuberic_{replica_id}` is the application_name for all
PG standbys. This is consistent, collision-free, and maps trivially
between kuberic's numeric IDs and PG's string names.

### DD-3: Epoch vs Timeline — Two-Layer Fencing

kuberic's epoch and PG's timeline are orthogonal fencing mechanisms at
different layers. They naturally align (each failover bumps both) but
neither needs to be injected into the other.

| Layer | Mechanism | What it fences |
|-------|-----------|---------------|
| kuberic (control plane) | Epoch `(dln, cn)` | Operator won't send commands to wrong-epoch replicas |
| PG (data plane) | Timeline ID | PG rejects WAL from a different timeline |
| PG (write plane) | `synchronous_commit` | Zombie primary's commits hang when standbys disconnect |

**Why no injection**: PG has no GUC or extension point for custom
fencing metadata. Timelines are incremented automatically on
`pg_ctl promote` — they track the same events as kuberic epoch bumps
but from the data layer's perspective.

**Observability (Phase 3, optional)**: Store kuberic epoch in a PG
metadata table for DBA debugging. Not required for correctness — the
operator tracks epoch/role/LSN in its own state (PartitionDriver +
PartitionState atomics + CRD status).

```sql
-- Optional: created in Phase 3 for debugging convenience
CREATE TABLE IF NOT EXISTS kuberic_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
```

## Open Questions

### ~~OQ-1: PG Binary Distribution~~ (Resolved)

The PG binary directory is a required CLI argument (`--pg-bin`).
Users pass their system-installed path, e.g.
`--pg-bin /usr/lib/postgresql/16/bin`. For container images, the path
is baked into the entrypoint.

### ~~OQ-2: Connection String Management~~ (Resolved)

Clients connect directly to PostgreSQL's TCP port. In K8s, the operator
maintains a read-write Service (`-rw`) with label selectors pointing to
the current primary pod. On failover/switchover, the operator relabels
pods and the Service routes automatically. For local testing, clients
connect to `localhost:<port>` — the test harness tracks which instance
is primary.

### ~~OQ-3: WAL Retention~~ (Resolved)

Use replication slots with `max_slot_wal_keep_size` (PG 13+). This
provides reliable WAL retention with bounded disk usage. If a replica
falls too far behind, the slot is invalidated and kuberic rebuilds the
replica via `pg_basebackup` (copy protocol).

### ~~OQ-4: Read Replicas~~ (Resolved)

No read replicas. Secondaries exist for failover only — they do not
serve client queries. However, `hot_standby = on` is required so that
PgMonitor can query standby LSN via SQL (`pg_last_wal_receive_lsn()`).
Clients don't connect to standbys — the K8s Service routes only to the
primary.

---

## Known Problems

### KP-1: Split-brain window during network partition

If the old primary is partitioned from the operator during failover,
`ChangeRole(None)` and `UpdateEpoch` never reach it. The old primary's
PG stays running and writable until:
- (a) `synchronous_commit = on` causes commits to hang (no standby to
  ACK — all standbys have moved to new primary's timeline), or
- (b) PG's `wal_sender_timeout` disconnects stale standbys, or
- (c) K8s restarts the pod (if a liveness probe detects isolation)

**Mitigation**: `synchronous_commit = on` is mandatory (Required
Configuration). This provides PG-native write fencing — the old primary
blocks on commits when standbys disconnect. Clients with stale connections
see commit timeouts, not silent data divergence. Accepted residual risk:
async commits during the partition window (if any) may be lost.

### KP-2: Promotion latency with large WAL backlog

`pg_ctl promote` may take minutes if the standby has a large WAL backlog
to replay (e.g., after a crash-recovery scenario with many un-replayed
WAL segments). The 60-second timeout in `ChangeRole(Primary)` may be
insufficient. If promotion times out, the operator invokes switchover
rollback (A3 pattern) and tries another candidate.

### ~~KP-3: Demoted primary lacks `standby.signal` — dual-primary risk~~ (Fixed)

**Severity**: ~~must-fix~~ → fixed

After switchover, the old primary is demoted to `ActiveSecondary`.
`ReconfigureStandby` rewrites `primary_conninfo` and restarts PG, but
previously never created `standby.signal`. PG 12+ requires this file to
start in standby mode — without it, the instance restarts as a
read-write primary, creating a dual-primary split-brain.

**Fix applied**: `ReconfigureStandby` now detects when the target was a
primary (`standby.signal` didn't exist) and handles timeline divergence:
1. Stop PG
2. Try `pg_rewind` from new primary (fast — copies only diverged pages)
3. If `pg_rewind` fails (WAL recycled, corruption): full `pg_basebackup`
4. Create `standby.signal`, patch config, start as standby

This matches CNPG's approach. In production with `wal_keep_size`
configured, `pg_rewind` typically succeeds. In tests with aggressive WAL
recycling, the fallback to `pg_basebackup` handles it correctly.

### KP-4: PG-level write fencing not implemented

**Severity**: ~~must-fix~~ should-fix (UX improvement, not correctness)

The design specifies `ALTER SYSTEM SET default_transaction_read_only = on`
+ `pg_reload_conf()` during demotion (§Client Access & Write Fencing),
but no implementation exists. The framework's `revoke_write_status()` only
sets an in-memory atomic that PG clients never check.

**Why not must-fix**: `synchronous_commit = on` (mandatory config) already
provides correctness fencing — writes on the old primary hang because no
standby ACKs them (standbys have disconnected or moved to the new primary's
timeline). The `ReconfigureStandby` restart follows within seconds as a
hard fence. Data never actually diverges; the worst case is a client
seeing a commit timeout instead of an immediate read-only error.

**Design** (see §Client Access & Write Fencing for full details):

The adapter's `ChangeRole(ActiveSecondary)` handler is the implementation
hook — it fires immediately after `revoke_write_status()` during
switchover. When demoting from Primary:
1. Connect to PG
2. `ALTER SYSTEM SET default_transaction_read_only = on`
3. `SELECT pg_reload_conf()`

This provides a soft fence (new transactions default to read-only).
The hard fence follows seconds later when `ReconfigureStandby` restarts
PG as a physical standby (KP-3 fix).

On promotion (`ChangeRole(Primary)`), clear the fence:
`ALTER SYSTEM SET default_transaction_read_only = off` + reload.

**Limitation**: `default_transaction_read_only` is bypassable by clients
(`SET SESSION` / `BEGIN READ WRITE`). Acceptable for kuberic's switchover
window (~2 seconds), combined with `synchronous_commit = on` which causes
writes to hang when standbys disconnect.

### KP-5: `start()` TOCTOU race — double PG launch possible

**Severity**: ~~must-fix~~ should-fix (defensive hardening — not reachable in practice)

`PgInstanceManager::start()` drops the `child` mutex guard after checking
`is_some()`, spawns PG without the lock, then reacquires to store the
child. Two concurrent callers can both pass the check and spawn two PG
processes on the same data directory and port.

**Practical risk**: Low. All callers (`CloneFrom`, `ReconfigureStandby`)
call `stop()` first, so `is_some()` always returns false. The driver
serializes all operations per-replica — concurrent RPCs to the same
pod's PgDataService don't happen under normal operation. This is part
of the broader KP-8 (unserialized concurrent mutation).

**Fix**: Hold the `MutexGuard` across the entire `start()` body including
spawn. `tokio::sync::Mutex` is designed to be held across `.await`.

### ~~KP-6: `synchronous_standby_names` hardcodes `ANY 1`~~ (Fixed)

**Severity**: ~~should-fix~~ → fixed

`configure_sync_standbys` always set `ANY 1` regardless of
`ReplicaSetConfig.write_quorum`. For a 5-replica set with `write_quorum=3`,
PG should require `ANY 2` standby ACKs but only required 1.

**Fix applied**: Derives quorum from `write_quorum`: `quorum =
write_quorum.saturating_sub(1).max(1)`, then uses `ANY {quorum}`.
Also added SAFETY comment for the `format!`-based SQL (SF-7).

### KP-7: PgMonitor missing `fault_tx` — SQL failures unreported

**Severity**: should-fix (low urgency — narrow edge case)

Design specifies PgMonitor should have `fault_tx` and report
`FaultType::Permanent` after 3 consecutive SQL failures. Implementation
has no `fault_tx` field — SQL failures are silently dropped. Two
overlapping health monitors exist: `PgInstanceManager` checks
`pg_isready` (process-level, reports faults), `PgMonitor` checks SQL
(application-level, silent).

**Gap**: PG running but unable to execute SQL (corrupt catalog,
max_connections) goes unreported. However, `pg_isready` catches most
real failures (process death, socket unavailable). The gap is narrow:
PG alive but SQL-broken is unusual — corrupt catalogs typically crash PG
(caught by health monitor), and max_connections is unlikely in an
example app. Stale `current_progress` would be noticed by the operator
when values stop advancing.

### KP-8: Concurrent PgDataService/adapter mutation unserialized

**Severity**: ~~should-fix~~ consider (theoretical — not reachable in practice)

`PgDataServiceImpl` and `PgReplicatorAdapter` both hold
`Arc<PgInstanceManager>`. Multi-step operations (`CloneFrom`:
stop→clear→backup→start; `ReconfigureStandby`: rewrite→stop→start) are
not serialized against adapter control events (`ChangeRole`, `promote`).
The `Mutex<Option<Child>>` serializes field access but not operational
sequences.

**Why low risk**: The RPCs are **cross-pod** — the primary's adapter
calls `CloneFrom`/`ReconfigureStandby` on the **secondary's**
PgDataService. The driver blocks on each operation (awaits reply before
sending the next event). So the secondary's adapter never receives a
control event while its PgDataService is mid-operation. Concurrent
mutation requires an external actor calling RPCs directly, which doesn't
happen under normal kuberic operation.

**Fix (if needed)**: Route all PG mutations through the adapter's control
channel, or add a top-level operation lock.

### ~~KP-9: Zombie processes after PG crash~~ (Fixed)

**Severity**: ~~should-fix~~ → fixed

Design specified `child.wait()` for immediate crash detection.
Implementation used `pg_isready` polling — 6+ second detection delay.
After crash: (1) child process was a zombie (never reaped), (2) `child`
field stayed `Some` so `start()` thought PG was running.

**Fix applied**: Added a **process exit monitor** task alongside the
existing `pg_isready` health monitor. The exit monitor polls
`child.try_wait()` every 500ms — on unexpected exit (shutdown token
not cancelled), it immediately reaps the child, clears the `child`
field to `None`, and reports `FaultType::Permanent`. The `pg_isready`
health monitor is retained as a complement (catches PG hung but process
alive). Both monitors now check `shutdown.is_cancelled()` before
reporting fault (also fixes KP-11).

### KP-10: `rewrite_primary_conninfo` discards connection parameters

**Severity**: should-fix

Rebuilds `primary_conninfo` from scratch: `host={h} port={p}
application_name={name}`. All other params from `pg_basebackup -R`
(user, sslmode, channel_binding, etc.) are silently dropped. Breaks in
any non-`trust` auth environment.

**Fix**: Parse existing conninfo key=value pairs, replace only host/port/
application_name, preserve the rest.

### ~~KP-11: Health monitor false fault during intentional stop~~ (Fixed)

**Severity**: ~~should-fix~~ → fixed (addressed as part of KP-9 fix)

Race: monitor's `pg_isready` runs between shutdown token cancellation and
pg_ctl stop completion. If already at 2 accumulated failures, the
stop-induced failure triggered spurious `FaultType::Permanent`.

**Fix applied**: Both monitors (exit monitor and pg_isready monitor) now
check `shutdown.is_cancelled()` before sending `FaultType::Permanent`.
