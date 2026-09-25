use crate::topology::normalize_host;
use crate::{ConfigError, Peer, Topology};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RustFsConfig {
    topology: Topology,
    local_peer_index: usize,
}

impl RustFsConfig {
    pub fn new(topology: Topology, local_host: impl Into<String>) -> Result<Self, ConfigError> {
        let host = normalize_host(local_host.into())?;
        let local_peer_index = topology
            .peers()
            .iter()
            .position(|peer| peer.host() == host)
            .ok_or(ConfigError::LocalPeerNotFound)?;
        Ok(Self {
            topology,
            local_peer_index,
        })
    }

    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    pub fn local_peer(&self) -> &Peer {
        &self.topology.peers()[self.local_peer_index]
    }

    pub fn ensure_restart_compatible(&self, requested: &Self) -> Result<(), ConfigError> {
        self.topology.ensure_unchanged(&requested.topology)?;
        if self.local_peer_index != requested.local_peer_index {
            return Err(ConfigError::LocalPeerChangeUnsupported);
        }
        Ok(())
    }
}
