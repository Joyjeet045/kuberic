//! Independent application and replication runtime for the level-triggered stack.

pub mod application;
mod authority;
mod effects;
#[doc(hidden)]
pub mod engine;
#[doc(hidden)]
#[path = "runtime.rs"]
pub mod internal;
pub mod replicator;

mod error;

pub use application::{StateProvider, StatefulServiceReplica};
pub use error::{Result, RuntimeError};
pub use replicator::{
    DefaultReplicator, DefaultReplicatorFactory, PrimaryReplicator, Replicator, ReplicatorFactory,
    ReplicatorInterfaces, StateReplicator, StatefulServicePartition,
};
