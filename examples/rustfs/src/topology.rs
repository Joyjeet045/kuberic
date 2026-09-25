use std::num::NonZeroU16;

use crate::ConfigError;

pub const PEER_COUNT: usize = 4;
pub const DRIVES_PER_PEER: usize = 4;
pub const ERASURE_SET_DRIVE_COUNT: usize = PEER_COUNT * DRIVES_PER_PEER;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Http,
    Https,
}

impl Transport {
    pub fn scheme(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    host: String,
    port: NonZeroU16,
    drives: Vec<String>,
}

impl Peer {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        drives: Vec<String>,
    ) -> Result<Self, ConfigError> {
        let host = normalize_host(host.into())?;
        let port = NonZeroU16::new(port).ok_or(ConfigError::InvalidPort)?;
        if drives.len() != DRIVES_PER_PEER {
            return Err(ConfigError::UnsupportedDriveCount {
                actual: drives.len(),
            });
        }
        for (index, drive) in drives.iter().enumerate() {
            validate_drive(drive, index)?;
            for (previous, other) in drives[..index].iter().enumerate() {
                if drive == other {
                    return Err(ConfigError::DuplicateDrive {
                        first: previous,
                        second: index,
                    });
                }
                if is_nested(drive, other) || is_nested(other, drive) {
                    return Err(ConfigError::OverlappingDrives {
                        first: previous,
                        second: index,
                    });
                }
            }
        }
        Ok(Self { host, port, drives })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port.get()
    }

    pub fn drives(&self) -> &[String] {
        &self.drives
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topology {
    transport: Transport,
    peers: Vec<Peer>,
}

impl Topology {
    pub fn new(transport: Transport, peers: Vec<Peer>) -> Result<Self, ConfigError> {
        if peers.len() != PEER_COUNT {
            return Err(ConfigError::UnsupportedPeerCount {
                actual: peers.len(),
            });
        }
        for (index, peer) in peers.iter().enumerate() {
            if let Some(previous) = peers[..index]
                .iter()
                .position(|other| other.host == peer.host)
            {
                return Err(ConfigError::DuplicatePeer {
                    first: previous,
                    second: index,
                });
            }
        }
        Ok(Self { transport, peers })
    }

    pub fn transport(&self) -> Transport {
        self.transport
    }

    pub fn peers(&self) -> &[Peer] {
        &self.peers
    }

    pub fn volume_arguments(&self) -> Vec<String> {
        self.peers
            .iter()
            .flat_map(|peer| {
                peer.drives.iter().map(|drive| {
                    format!(
                        "{}://{}:{}{drive}",
                        self.transport.scheme(),
                        peer.host,
                        peer.port
                    )
                })
            })
            .collect()
    }

    pub fn ensure_unchanged(&self, requested: &Self) -> Result<(), ConfigError> {
        if self != requested {
            return Err(ConfigError::TopologyChangeUnsupported);
        }
        Ok(())
    }
}

pub(crate) fn normalize_host(mut host: String) -> Result<String, ConfigError> {
    host.make_ascii_lowercase();
    let numeric_suffix = host.rsplit('.').next().is_some_and(|label| {
        label.bytes().all(|byte| byte.is_ascii_digit())
            || label
                .strip_prefix("0x")
                .is_some_and(|digits| digits.bytes().all(|byte| byte.is_ascii_hexdigit()))
    });
    if host.is_empty()
        || host.len() > 253
        || numeric_suffix
        || !host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        return Err(ConfigError::InvalidHost);
    }
    Ok(host)
}

fn validate_drive(path: &str, index: usize) -> Result<(), ConfigError> {
    let valid = path.strip_prefix('/').is_some_and(|relative| {
        relative.split('/').all(|part| {
            !part.is_empty()
                && part != "."
                && part != ".."
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        })
    });
    if !valid {
        return Err(ConfigError::InvalidDrivePath { index });
    }
    Ok(())
}

fn is_nested(path: &str, parent: &str) -> bool {
    path.strip_prefix(parent)
        .is_some_and(|suffix| suffix.starts_with('/'))
}
