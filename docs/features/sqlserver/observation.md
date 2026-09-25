# SQL Server Observe-Only Runtime

The second SQL Server delivery slice connects to an **already provisioned**
SQL Server 2022 Linux x86-64 instance and reports native HA evidence. It does
not deploy a container, accept an EULA, start or restart `sqlservr`, create an
AG, change a role, renew a write lease, or execute any mutation command.

The library is compatible with the observation side of the level-triggered
contract. It has no dependency on `kuberic-core` and is not wired into either
the classic operator or operator2. This is not a complete Kubernetes HA
integration or an automatic failover implementation. The
[support and safety design](design.md) remains authoritative.

## Run the observer

Provision the instance separately, explicitly accepting the SQL Server EULA
and using a digest-pinned SQL Server 2022 image. Developer edition is for
non-production use only. Enable HADR and install a TLS server certificate whose
SAN matches the configured connection host. Connect directly to each instance,
not an AG listener, load balancer, or read-only routing endpoint.

Copy [the example configuration](../../../examples/sqlserver/observer.example.json)
and set the instance hostname, AG name, expected `@@SERVERNAME`, logical
replica ID, and current Pod UID (or a laboratory incarnation ID). The hostname
and SQL Server name are different concepts and are both checked.

Mount observation credentials as **separate username and password files**.
Use a dedicated observation principal, not `sa` or the future mutation
principal. Files must be nonempty UTF-8 text without a trailing newline or NUL;
password whitespace is preserved, not trimmed. Configure absolute paths,
restrict mount access, and do not embed credentials in JSON, shell arguments,
or connection strings. The observer rereads these files on every connection,
so a subsequent observation sees rotated credentials.

```bash
cargo run --locked -p sqlserver-replicated --bin sqlserver-observer -- \
  --config /absolute/path/to/observer.json

cargo run --locked -p sqlserver-replicated --bin sqlserver-observer -- \
  --config /absolute/path/to/observer.json --watch
```

There is no mutation flag, arbitrary SQL input, plaintext connection option, or
certificate-verification bypass. Unknown JSON configuration fields and modes
are rejected, including inline passwords and mutation credentials.

TLS is required for the complete TDS session. By default the client uses the
system trust store. With `ca_certificate_file`, the pinned TDS driver's Rustls
backend uses **that CA instead of the system roots**; supply one PEM/CRT
certificate or a DER certificate, not a PEM bundle. The certificate chain and
connection hostname are still verified. This TDS CA is not the AG endpoint
certificate: AG endpoint authentication and private-key management remain
separate provisioning concerns.

The driver's system-root loader also honors `SSL_CERT_FILE`. Missing,
unreadable, invalid, or empty system roots can make the pinned driver panic.
These panics, like malformed PRELOGIN panics, become sanitized `malformed`
failure records at the `TLS/TDS login` stage; the connection is discarded and
watch mode retries. Check both the peer protocol and TLS trust configuration
when that record reports a driver panic. No trust or encryption check is bypassed.

### Configuration

The example contains all configuration fields. Optional values default to:

| Field | Default |
|---|---|
| `mode` | `observe_only` (the only supported mode) |
| `port` | `1433` |
| `ca_certificate_file` | omitted; system trust store |
| `connect_timeout_ms` | `5000`, including Secret loading, TCP, TLS, and login |
| `query_timeout_ms` | `5000`, including reading the complete result |
| `sample_timeout_ms` | `30000`, for the entire multi-query attempt |
| `poll_interval_ms` | `1000`, delay after each completed attempt |
| `max_age_ms` | `60000`, maximum age of evidence |

Durations must be in `1..=300000` milliseconds. Connect and query timeouts must
not exceed the whole-sample timeout. The configuration is limited to 64 KiB,
individual Secret files to 4096 bytes, and query results to 4096 rows with
4096 bytes per text cell. Exceeding a bound produces an explicit error, never
a truncated successful observation. Hostnames and IPv4 addresses are supported;
named-instance discovery and IPv6 literals are not currently supported.

## Permissions and capability checks

Before interpreting catalog absence, every attempt checks the SQL Server
version and edition, HADR capability, platform, expected server name, and
required metadata permissions. Insufficient permissions are a failed attempt,
not evidence that an AG or database does not exist.

The observation principal needs server-level access to the selected HA,
database recovery, host, and seeding DMVs. In particular, SQL Server 2022
requires `VIEW SERVER PERFORMANCE STATE`; metadata visibility also requires
`VIEW ANY DEFINITION`, `VIEW ANY DATABASE`, and `VIEW SERVER STATE`. Grant
only the observation permissions listed by the capability query, not AG
management permissions or `sysadmin`. The runtime never grants itself access.

An absent requested AG is valid evidence on a supported HADR-enabled instance.
An unsupported engine/profile or a changing observation is not. A missing,
resolving, disconnected, suspended, or unhealthy replica is never synthesized
into a healthy secondary.

An EXTERNAL AG must report numeric cluster type `2` and an ASCII
case-insensitive `EXTERNAL` descriptor, including SQL Server's lowercase
`external` value. The original native descriptor is preserved in the snapshot.

## Evidence and output

One-shot mode emits one JSON document. Watch mode emits newline-delimited JSON
containing the latest complete snapshot; a slow consumer can miss intermediate
samples, so this is not an audit journal.

Each report has:

- `schema_version: 1`;
- `source`: endpoint, requested AG, expected SQL Server name, logical replica
  ID, and caller-supplied incarnation;
- `evaluated_at_unix_millis` and `max_age_millis`;
- `fresh`: validity **at that evaluation time**, not a permanent health flag;
- `observation`: a tagged `present`, `absent`, or `failed` value.

An instance snapshot includes the native AG, replicas, databases, local role,
sync/connection health and seeding facts. Native GUIDs identify catalog and
DMV relationships. Hardened-block, redone-record, and committed-record
positions remain separate, exact decimal strings in JSON, including values
larger than `i64` or JavaScript's safe integer range. They are not a generic
scalar election score. Local recovery lineage is not assigned to remote DMV
rows; reports about remote replicas are not that replica's self-observation.
Automatic-seeding history is scoped to the current native database and replica
GUIDs. Physical-seeding rows require an already-associated local database GUID;
pre-join destination processes may be unavailable. Empty seeding lists are not
proof that seeding never ran or that a replica has synchronized.

Each snapshot uses a single connection and brackets its queries with native
identity, role, and configuration evidence. If the anchors change, the whole
attempt fails as `inconsistent`. This detects observable changes; it does
**not** make DMVs transactionally atomic or prove cluster-wide consensus.

A timestamp is captured at the **start** of every attempt, before connecting.
Timeouts, login failures, permission failures, malformed data, inconsistent
snapshots, and unsupported states produce fresh failure records but never
fresh **evidence** (`fresh` is false). A failed attempt replaces the previous
snapshot; the monitor does not silently retain prior successful progress.
Every new attempt reconnects, and an interrupted connection is discarded.

Consumers must recompute freshness from the original attempt timestamp and
their current time, using `ObservationReport::is_fresh_at` in Rust. The exact
age boundary is inclusive; future timestamps and failed attempts are not
fresh. Slow samples can already be stale when published. None of this evidence
alone proves promotion eligibility, fencing, or authority to serve writes.
The source incarnation is caller-provided attribution, not an authenticated
or durable fencing attestation.

The CLI exits zero only after a fresh, successful one-shot observation. This
does not mean the AG is healthy: a valid snapshot can report absence or
unhealthy replication. Failed/stale attempts exit nonzero. Watch mode keeps
observing after failures and exits nonzero on shutdown if any report was failed
or stale at publication, even if a later report overwrote it or shutdown
prevented its output. The monitor retains this aggregate independently of the
latest-value output channel. Cancelling an in-flight attempt before it publishes
does not itself mark watch mode as failed. SIGINT and SIGTERM cancel
in-flight observation even if the stdout consumer stops reading. Normal writes
are acknowledged and flushed, but cancellation may leave a partial final JSON
line. An interrupted one-shot invocation exits with code 130. Output,
configuration, and monitor errors also exit nonzero.

Diagnostics contain adapter-owned text and, when applicable, a numeric SQL
Server error code. Driver/server messages, credentials, and SQL batches are
not echoed. The CLI does not enable driver tracing.

## Library boundary

`SqlExecutor` and `SqlSession` provide a replaceable transport for server-free
tests and future callers. `ReadQuery` exposes only the closed set of
parameterized observation queries. `TdsExecutor` implements that interface
with verified-TLS TDS connections, while `SqlServerInstanceManager` owns
connection/sample lifecycle and `SqlServerMonitor` publishes complete reports
through a watch channel. The monitor starts with no sample, supports explicit
cancellation, and reports subscriber loss rather than silently discarding
results. On cancellation, the monitor returns a completion summary recording
whether any failed/stale report was published. The CLI joins the monitor and
includes that summary in its exit status without draining blocked output.

The TDS boundary contains panics while polling connection and query futures,
never reuses a failed or interrupted session, and requires `panic = "unwind"`
for recovery (the workspace default). Abort builds and process-level aborts
cannot be recovered. A process-wide delegating panic hook suppresses payloads
only during a driver poll, so panic text and backtraces cannot bypass sanitized
failure reports. The previous hook still handles unrelated panics, including
other tasks between polls. Embedders that replace the panic hook after starting
TDS observation must preserve this delegation.

Operation-envelope serialization/decoding and the durable result journal
remain stage 3 work. The existing canonical signatures and approval/fence
bindings are unchanged. Observation JSON is an output format, not a new
authenticated command protocol.

## Testing

Ordinary tests need neither SQL Server nor Kubernetes:

```bash
cargo fmt -p sqlserver-replicated -- --check
cargo test --locked -p sqlserver-replicated --all-features
cargo clippy --locked -p sqlserver-replicated --all-targets --all-features -- -D warnings
```

The dedicated SQL Server observer CI workflow runs these independently of
the existing workspace/Kubernetes workflow. It does not claim live SQL
Server validation.

Live observation tests are explicitly ignored by default. They require
externally provisioned, isolated SQL Server fixtures, mounted credentials,
valid TLS configuration, and explicit test-environment/EULA acknowledgement.
Set the following environment variables (only references/acknowledgements,
never credential contents):

| Variable | Required fixture or acknowledgement |
|---|---|
| `SQLSERVER_TEST_EULA_ACCEPTED` | `true`, attesting the fixture owner accepted the EULA |
| `SQLSERVER_TEST_IMAGE` | The actual fixture engine image reference with `@sha256:` digest |
| `SQLSERVER_LIVE_ABSENT_CONFIG` | Absolute config path for a supported HADR-enabled instance without the requested AG |
| `SQLSERVER_LIVE_AG_CONFIG` | Absolute config path for a preconfigured supported EXTERNAL AG |
| `SQLSERVER_LIVE_DENIED_CONFIG` | Config with a valid login/TLS connection but missing observation permissions |
| `SQLSERVER_LIVE_BAD_TLS_CONFIG` | Config for a reachable instance with a mismatched CA or TLS hostname |

The image acknowledgement is fixture-owner metadata, not container attestation
through TDS. Image pinning and EULA acceptance are the provisioning owner's
responsibility. Once those fixtures are configured:

```bash
cargo test --locked -p sqlserver-replicated --test live_observation -- --ignored
```

An explicitly requested live test fails if any of its prerequisites are
missing; it never silently skips. Tests use observation principals and issue
no setup/mutation SQL. Fixture provisioning and AG management must be done by
the test environment owner. Local three-node failover, lease expiry, old-primary
fencing, and fault injection are stage 4, not claims of this PR.
