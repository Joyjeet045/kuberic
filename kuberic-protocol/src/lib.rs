//! Pure authority model and evaluator for the level-triggered Kuberic stack.
//!
//! This crate owns canonical protocol state and decisions. It performs no I/O;
//! Kubernetes, storage, and RPC implementations consume its observations and
//! execute the returned plans.

pub mod command;
pub mod evaluator;
pub mod observation;
pub mod plan;
pub mod types;
pub mod validation;

/// Exact protocol version supported by the current minimum contract.
pub const PROTOCOL_VERSION: u32 = 3;
