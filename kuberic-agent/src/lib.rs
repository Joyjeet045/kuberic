//! Replica-local hosting and durable authority for the level-triggered stack.

mod error;

pub mod command;
pub mod coordinator;
pub mod hosting;
pub mod process;
pub mod provisioning;
pub mod recovery;
pub mod report;
pub mod runtime_adapter;
pub mod service;
pub mod session;
pub mod sqlite_store;
pub mod state;
pub mod store;
pub mod transport;

pub use error::{AgentError, Result};
