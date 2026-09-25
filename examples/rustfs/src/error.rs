use std::error::Error;
use std::fmt;

use crate::{DRIVES_PER_PEER, PEER_COUNT};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    InvalidHost,
    InvalidPort,
    InvalidDrivePath { index: usize },
    UnsupportedDriveCount { actual: usize },
    DuplicateDrive { first: usize, second: usize },
    OverlappingDrives { first: usize, second: usize },
    UnsupportedPeerCount { actual: usize },
    DuplicatePeer { first: usize, second: usize },
    LocalPeerNotFound,
    TopologyChangeUnsupported,
    LocalPeerChangeUnsupported,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidHost => {
                write!(
                    f,
                    "peer host must be a stable ASCII DNS name, not an IP address"
                )
            }
            Self::InvalidPort => write!(f, "peer port must be nonzero"),
            Self::InvalidDrivePath { index } => write!(
                f,
                "drive {index} must be an absolute non-root Linux path with ASCII alphanumeric, '.', '_', or '-' components, without traversal or empty components"
            ),
            Self::UnsupportedDriveCount { actual } => write!(
                f,
                "the example requires {DRIVES_PER_PEER} drives per peer, got {actual}"
            ),
            Self::DuplicateDrive { first, second } => {
                write!(f, "drives {first} and {second} have the same path")
            }
            Self::OverlappingDrives { first, second } => {
                write!(f, "drives {first} and {second} have nested paths")
            }
            Self::UnsupportedPeerCount { actual } => {
                write!(f, "the example requires {PEER_COUNT} peers, got {actual}")
            }
            Self::DuplicatePeer { first, second } => {
                write!(f, "peers {first} and {second} have the same DNS identity")
            }
            Self::LocalPeerNotFound => {
                write!(f, "local peer must belong to the configured topology")
            }
            Self::TopologyChangeUnsupported => write!(
                f,
                "changing peer order, membership, transport, ports, or drive paths is unsupported"
            ),
            Self::LocalPeerChangeUnsupported => {
                write!(f, "restarting as a different local peer is unsupported")
            }
        }
    }
}

impl Error for ConfigError {}
