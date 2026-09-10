//! SQLite: Replicated SQLite on Kuberic
//!
//! A stateful SQLite database using kuberic-core's PodRuntime.
//! Demonstrates WAL frame shipping replication:
//! - Primary replicates each transaction before its commit becomes visible
//! - Secondary persists frames to frames.log, applies on commit
//! - StateProvider callbacks for copy/catchup/epoch management
//! - Client-facing gRPC API for Execute/Query/ExecuteBatch

pub mod barrier;
pub mod demo;
pub mod framelog;
pub mod frames;
pub mod server;
pub mod service;
pub mod state;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub mod proto {
    tonic::include_proto!("sqlitestore.v1");
}
