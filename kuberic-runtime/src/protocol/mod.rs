//! Canonical control contracts shared by replica hosts and the controller.
//!
//! These types and their validation perform no I/O or reconciliation. Control
//! transport conversions use the same canonical types; policy belongs to the
//! controller.

pub mod command;
pub mod observation;
pub mod types;
pub mod validation;

/// Exact protocol version supported by the current minimum contract.
pub const PROTOCOL_VERSION: u32 = 9;
