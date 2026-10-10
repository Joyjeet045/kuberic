//! Independent application and replication runtime for the level-triggered stack.

#![deny(private_bounds, private_interfaces)]
#![cfg_attr(not(test), deny(unreachable_pub))]
#![allow(clippy::disallowed_types)]

#[cfg(feature = "host")]
pub mod application;
#[cfg(feature = "host")]
mod authority;
#[cfg(feature = "host")]
mod capabilities;
pub mod control;
#[cfg(feature = "host")]
mod effects;
#[cfg(feature = "host")]
pub mod engine;
#[cfg(feature = "host")]
/// Replica process hosting and peer transport, enabled by the `host` feature.
pub mod host;
#[cfg(feature = "native")]
pub mod native;
pub mod protocol;
#[cfg(feature = "host")]
mod receipts;
#[cfg(feature = "host")]
pub mod replicator;
#[cfg(feature = "host")]
mod runtime;
#[cfg(feature = "host")]
mod transport;

#[cfg(feature = "testing")]
/// Isolated host fixtures and deterministic transports; no production capabilities.
pub mod testing;

#[cfg(feature = "host")]
mod error;

#[cfg(all(test, feature = "host"))]
#[allow(dead_code)]
#[path = "test_support/secondary_scale_down.rs"]
mod removal_fixture;

#[cfg(all(test, feature = "host"))]
mod durable_contract_tests;

#[cfg(all(test, feature = "host", kuberic_workspace_tests))]
#[path = "test_support/controller.rs"]
mod test_controller;

#[cfg(feature = "host")]
pub use application::{StateProvider, StatefulServiceReplica};
#[cfg(feature = "host")]
pub use error::{Result, RuntimeError};
#[cfg(feature = "host")]
pub use replicator::{
    DefaultReplicator, DefaultReplicatorFactory, PrimaryReplicator, Replicator, ReplicatorFactory,
    ReplicatorInterfaces, StateReplicator, StatefulServicePartition,
};
