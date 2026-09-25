# RustFS Native-Replication Example

**Iteration 1: topology contract only.** This crate does not start RustFS, probe
health, access S3, or register with a Kuberic operator. It has no runtime or
third-party dependencies. RustFS retains ownership of data replication,
erasure coding, healing, and quorum.

The [design and five-PR roadmap](../../docs/features/rustfs/design.md) describe
the remaining observation, lifecycle, integration, and deployment iterations.

## Initial Profile

The example intentionally accepts one fixed profile, not every topology
RustFS supports:

| Input | Contract |
|---|---|
| Peers | Exactly four ordered, distinct DNS identities |
| Drives | Exactly four ordered, disjoint absolute Linux paths per peer |
| Erasure geometry | One pool with one sixteen-drive set; exported as `ERASURE_SET_DRIVE_COUNT` |
| Transport | Explicit HTTP or HTTPS, shared by all peers |
| Port | Nonzero native S3 port for each peer |
| Local identity | Exactly one of the configured peer DNS names |
| Changes | No membership, ordering, transport, port, or path changes on restart |

DNS names are normalized to lowercase. IP addresses, numeric final DNS labels
(including hexadecimal address spellings), trailing dots, URLs, credentials,
whitespace, and endpoint expansion syntax are rejected. A name has at most 253
bytes and each label has at most 63 bytes.

Drive paths are interpreted as Linux paths even when these contract tests run
on Windows. Path components accept ASCII letters, digits, `.`, `_`, and `-`.
Empty components, `.` and `..`, root paths, trailing separators, nested paths,
URL escapes, and shell syntax are rejected rather than rewritten. Path case is
preserved. The same path on different peers is expected and allowed.

`Topology::volume_arguments` produces all sixteen explicit native volume URLs
in peer-major, drive-minor order. Every peer receives the same list, including
its own volumes. It neither sorts endpoints nor uses shell range expansion.
Pass these strings as separate process arguments in a future instance manager,
not through a shell. RustFS accepts this all-literal form as a legacy single
pool. It is not a multi-pool expansion or decommissioning interface.

The sixteen-drive set is the intended geometry, not a quorum calculation. The
future instance manager must explicitly set `RUSTFS_ERASURE_SET_DRIVE_COUNT`
to `ERASURE_SET_DRIVE_COUNT` and reject conflicting ambient configuration.
For example, a width of four would partition this peer-major list into sets
concentrated on individual peers. This library does not launch a process or
control its environment, so it cannot enforce that runtime requirement yet.

## Constructing Configuration

```rust
use rustfs_native::{ConfigError, Peer, RustFsConfig, Topology, Transport};

fn main() -> Result<(), ConfigError> {
    let peers = (0..4)
        .map(|index| {
            let drives = (1..=4)
                .map(|drive| format!("/data/rustfs{drive}"))
                .collect();
            Peer::new(format!("rustfs-{index}.rustfs"), 9000, drives)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let topology = Topology::new(Transport::Https, peers)?;
    let config = RustFsConfig::new(topology, "rustfs-0.rustfs")?;
    let volumes = config.topology().volume_arguments();

    assert_eq!(volumes.len(), 16);
    assert_eq!(volumes[0], "https://rustfs-0.rustfs:9000/data/rustfs1");
    assert_eq!(config.local_peer().port(), 9000);
    config.ensure_restart_compatible(&config.clone())?;
    Ok(())
}
```

Configuration fields are private and constructors validate inputs. Errors
identify the rejected field or zero-based peer/drive positions without echoing
untrusted configuration. `Topology::ensure_unchanged` rejects topology drift;
`RustFsConfig::ensure_restart_compatible` additionally prevents switching the
local identity. These are comparison helpers, not persisted runtime guards.

No DNS lookup, mount inspection, certificate validation, or storage identity
attestation happens here. Different DNS names can still alias the same server;
different paths can still alias the same disk. A compatible configuration does
not prove that persistent data or a disk incarnation is unchanged. Future
lifecycle and deployment work must verify those facts and persist the accepted
configuration before using these guards.

The eventual native API listener must use `local_peer().port()`; selecting a
local DNS name here does not prove RustFS will recognize that endpoint as local.
The console listener is not the native API listener.

Selecting HTTPS only renders HTTPS URLs; it does not provision TLS. HTTP is for
isolated testing. There are no default credentials, image tags, quorum settings,
or mutation paths in this iteration. A runnable example and real-engine
compatibility claim require the pinned-image tests in subsequent PRs.

## Validation

```sh
cargo test --locked -p rustfs-native
cargo clippy --locked -p rustfs-native --all-targets --all-features -- -D warnings
cargo fmt -p rustfs-native -- --check
```

The workspace CI already discovers this crate through its existing formatting,
Clippy, and test commands. No RustFS binary, Docker daemon, or Kubernetes cluster
is needed for these tests.
