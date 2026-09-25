#![doc = include_str!("../README.md")]

mod config;
mod error;
mod topology;

pub use config::RustFsConfig;
pub use error::ConfigError;
pub use topology::{
    DRIVES_PER_PEER, ERASURE_SET_DRIVE_COUNT, PEER_COUNT, Peer, Topology, Transport,
};
