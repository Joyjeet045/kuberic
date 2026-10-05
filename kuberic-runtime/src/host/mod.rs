//! Hosts application replicas with durable metadata, recovery, and fenced RPC sessions.

mod command;
mod coordinator;
mod error;
pub(crate) mod hosting;
mod process;
mod provisioning;
#[cfg(test)]
mod recovery;
mod removal;
pub(crate) mod report;
pub(crate) mod runtime_adapter;
pub(crate) mod service;
pub(crate) mod session;
pub(crate) mod sqlite_store;
pub(crate) mod state;
pub(crate) mod store;
#[cfg(feature = "testing")]
pub(crate) mod testing;
pub(crate) mod transport;

#[cfg(test)]
mod tests;

pub use error::{HostError, Result};
pub use process::{
    ApplicationStorageState, ReplicaBuildDiagnostics, ReplicaDiagnostics, ReplicaHandle,
    ReplicaHost, ReplicaProcessConfig, RunningReplica,
};
pub use transport::{KubernetesDnsResolver, ReplicaEndpointResolver};
