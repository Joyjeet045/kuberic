//! KVStore: Replicated Key-Value Store
//!
//! A stateful key-value store using kuberic-core's PodRuntime.
//! Demonstrates the full replication protocol:
//! - Primary replicates writes via `replicate()`
//! - Secondary drains copy_stream and replication_stream
//! - StateProvider callbacks (GetCopyContext, GetCopyState, etc.)
//! - Client-facing gRPC API for Get/Put/Delete

pub mod client;
pub mod demo;
pub mod persistence;
pub mod server;
pub mod service;
pub mod state;
mod transactions;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub mod proto {
    tonic::include_proto!("kvstore.v1");
}
