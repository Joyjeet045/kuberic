//! One replicated SQLite application on the public level-triggered v2 stack.

pub mod barrier;
mod connection;
pub mod framelog;
pub mod frames;
pub mod server;
pub mod service;
pub mod state;
pub use state::{RecoveryState, SqlitePersistence};

#[cfg(test)]
mod persistence_tests;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub mod proto {
    tonic::include_proto!("sqlitestore.v1");
}
