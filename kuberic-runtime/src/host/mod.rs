//! Hosts application replicas with durable metadata, recovery, and fenced RPC sessions.

#![deny(clippy::disallowed_types)]

mod command;
mod coordinator;
mod error;
#[allow(clippy::disallowed_types)]
pub(crate) mod hosting;
mod observation;
#[allow(clippy::disallowed_types)]
mod process;
mod provisioning;
mod recovery;
mod removal;
pub(crate) mod report;
#[allow(clippy::disallowed_types)]
pub(crate) mod runtime_adapter;
#[allow(clippy::disallowed_types)]
pub(crate) mod service;
pub(crate) mod session;
pub(crate) mod sqlite_store;
pub(crate) mod state;
pub(crate) mod store;
#[cfg(feature = "testing")]
#[allow(clippy::disallowed_types)]
pub(crate) mod testing;
pub(crate) mod transport;

#[cfg(test)]
#[allow(clippy::disallowed_types)]
mod tests;

pub use error::{HostError, Result};
pub use process::{
    ApplicationStorageState, ReplicaBuildDiagnostics, ReplicaDiagnostics, ReplicaHandle,
    ReplicaHost, ReplicaProcessConfig, RunningReplica,
};
pub use transport::{KubernetesDnsResolver, ReplicaEndpointResolver};
