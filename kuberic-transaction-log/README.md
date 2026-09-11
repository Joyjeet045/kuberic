# Transaction Log

Framework-owned log for Reliable Collections, independent from example storage.
Records contain a versioned magic, bounded payload length, Kuberic LSN, header
checksum, payload and full-record checksum. Append returns only after
`sync_all`. Recovery drops an incomplete trailing record but rejects a complete
record with an invalid header/checksum without rewriting the source history.

Checkpoint installation writes a complete new generation containing the
checkpoint and an empty log, syncs both, then atomically publishes its name.
Log reclamation happens only after publication. A crash cannot combine a new
checkpoint with the previous generation's suffix, including during replica copy.
Rollback writes a replacement log before changing in-memory history.
Any ambiguous I/O failure fences further appends until reopen. One process owns
the log directory through an exclusive lock.

The format currently permits 64 MiB records and 64 MiB of retained log bytes.
The caller checks capacity before replication and checkpoints at a confirmed
boundary to reclaim history. The transactional coordinator imposes smaller
transaction and snapshot budgets. These are explicit MVP limits, not automatic
background retention policy. Old orphan generations after a crash may be
removed offline; they are never replayed through the active generation pointer.

`atomic_write` uses file sync plus parent-directory sync on Unix. Windows uses
the platform's atomic replacement support; directory sync is not available
through the same portable API. Storage hardware and filesystem guarantees still
apply. The log owns no quorum or transaction semantics; its caller supplies the
authoritative LSN and snapshot boundary.

```sh
cargo test -p kuberic-transaction-log
```