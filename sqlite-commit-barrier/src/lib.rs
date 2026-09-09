//! A SQLite VFS that withholds a WAL transaction until an external barrier
//! accepts it.
//!
//! SQLite publishes a WAL transaction by writing a commit frame: the frame whose
//! header carries a non-zero database page count. Recovery, checkpointing and
//! readers all stop at the last valid commit frame, so a transaction whose
//! commit frame never reaches the file does not exist as far as SQLite is
//! concerned.
//!
//! This VFS buffers the WAL bytes of the transaction in progress and releases
//! them only once [`CommitBarrier::publish`] has accepted the transaction. Reads
//! are served from the buffer while it is held, so SQLite observes a file that
//! behaves normally. If the barrier rejects the transaction the buffer is
//! dropped and the write fails, so SQLite rolls the transaction back and nothing
//! recoverable is left behind.
//!
//! The barrier runs as soon as the commit frame is complete rather than waiting
//! for a sync, because SQLite only syncs the WAL when `synchronous=FULL`.
//!
//! [`CommitBarrier::publish`] runs on the thread driving SQLite and is expected
//! to block until the transaction is durable elsewhere.

mod vfs;
mod wal;

use std::fmt;
use std::sync::Arc;

pub use wal::{WAL_HEADER_SIZE, WalLayout, commit_page_count, parse_wal_header};

/// One WAL transaction, complete and in the order SQLite wrote it.
pub struct Transaction<'a> {
    /// Offset in the WAL file where [`Transaction::frames`] begins.
    pub wal_offset: u64,
    /// Raw WAL bytes, ending with the commit frame.
    pub frames: &'a [u8],
    /// Database page size recorded in the WAL header.
    pub page_size: u32,
    /// Database size in pages after this transaction commits.
    pub database_pages: u32,
}

/// Decides whether a transaction may become visible to SQLite.
pub trait CommitBarrier: Send + Sync + 'static {
    /// Returns once the transaction is durable elsewhere, or an error to abort
    /// the commit. Called on the thread driving SQLite.
    fn publish(&self, transaction: &Transaction<'_>) -> Result<(), BarrierError>;

    /// Called when a published transaction could not be written locally.
    ///
    /// The transaction is durable elsewhere but absent here, so this replica is
    /// behind the rest of the cluster and must not keep serving.
    fn abandon(&self, _error: &str) {}
}

#[derive(Debug)]
pub struct BarrierError(String);

impl BarrierError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for BarrierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for BarrierError {}

#[derive(Debug)]
pub enum Error {
    InvalidName,
    AlreadyRegistered,
    NoDefaultVfs,
    UnknownParent(String),
    Register(i32),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName => formatter.write_str("vfs name contains an interior nul byte"),
            Self::AlreadyRegistered => formatter.write_str("vfs name is already registered"),
            Self::NoDefaultVfs => formatter.write_str("sqlite has no default vfs"),
            Self::UnknownParent(name) => write!(formatter, "no vfs named {name} is registered"),
            Self::Register(code) => {
                write!(
                    formatter,
                    "sqlite rejected the vfs: {}",
                    vfs::describe_error(*code)
                )
            }
        }
    }
}

impl std::error::Error for Error {}

/// Registers a VFS under `name`. The registration and the barrier live for the
/// remainder of the process, because SQLite keeps a pointer to both.
pub fn register(name: &str, barrier: Arc<dyn CommitBarrier>) -> Result<(), Error> {
    vfs::register(name, None, barrier)
}

/// Registers a VFS that delegates to `parent` rather than to the default VFS.
pub fn register_with_parent(
    name: &str,
    parent: &str,
    barrier: Arc<dyn CommitBarrier>,
) -> Result<(), Error> {
    vfs::register(name, Some(parent), barrier)
}

pub fn is_registered(name: &str) -> bool {
    vfs::is_registered(name)
}
