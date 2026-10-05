use std::collections::BTreeMap;
use std::time::Duration;

#[cfg(test)]
use crate::protocol::types::ReplicaRole;
use crate::protocol::types::{ProcessSessionId, ReplicaId, ReplicaIdentity};
use crate::transport::{CopyItem, OutboundOperation, ReplicaEndpoint, ReplicationItem};

use crate::{Result, RuntimeError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetainedMessage<T> {
    pub(crate) sequence: u64,
    pub(crate) payload: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg(test)]
pub(crate) enum ResumeWindow<T> {
    Retained(Vec<RetainedMessage<T>>),
    FullCopyRequired,
}

#[derive(Debug)]
pub(crate) struct ReliableWindow<T> {
    capacity: usize,
    next_sequence: u64,
    acknowledged_sequence: u64,
    retained: BTreeMap<u64, T>,
    cancelled: bool,
    #[cfg(test)]
    ever_enqueued: bool,
}

impl<T: Clone> ReliableWindow<T> {
    pub(crate) fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(RuntimeError::Application(
                "reliable send window capacity must be positive".into(),
            ));
        }
        Ok(Self {
            capacity,
            next_sequence: 1,
            acknowledged_sequence: 0,
            retained: BTreeMap::new(),
            cancelled: false,
            #[cfg(test)]
            ever_enqueued: false,
        })
    }

    pub(crate) fn enqueue(&mut self, payload: T) -> Result<RetainedMessage<T>> {
        if self.cancelled {
            return Err(RuntimeError::OperationCancelled);
        }
        if self.retained.len() >= self.capacity {
            return Err(RuntimeError::QueueFull);
        }
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        #[cfg(test)]
        {
            self.ever_enqueued = true;
        }
        self.retained.insert(sequence, payload.clone());
        Ok(RetainedMessage { sequence, payload })
    }

    pub(crate) fn acknowledge_through(&mut self, sequence: u64) -> Result<()> {
        if sequence < self.acknowledged_sequence || sequence >= self.next_sequence {
            return Err(RuntimeError::InvalidReplication(
                "acknowledgement is outside the retained send window".into(),
            ));
        }
        self.acknowledged_sequence = sequence;
        self.retained.retain(|retained, _| *retained > sequence);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn retained(&self) -> Vec<RetainedMessage<T>> {
        self.retained
            .iter()
            .map(|(sequence, payload)| RetainedMessage {
                sequence: *sequence,
                payload: payload.clone(),
            })
            .collect()
    }

    pub(crate) fn cancel(&mut self) {
        self.cancelled = true;
        self.retained.clear();
    }
}

#[cfg(test)]
impl ReliableWindow<ReplicationItem> {
    pub(crate) fn catch_up_capability(&self) -> Option<i64> {
        self.retained.values().map(|item| item.lsn).min()
    }

    pub(crate) fn reconnect_from_lsn(&self, lsn: i64) -> ResumeWindow<ReplicationItem> {
        if self.cancelled {
            return ResumeWindow::FullCopyRequired;
        }
        let Some(first_lsn) = self.catch_up_capability() else {
            return if self.ever_enqueued || lsn > 0 {
                ResumeWindow::FullCopyRequired
            } else {
                ResumeWindow::Retained(Vec::new())
            };
        };
        if lsn < first_lsn {
            return ResumeWindow::FullCopyRequired;
        }
        ResumeWindow::Retained(
            self.retained
                .iter()
                .filter(|(_, item)| item.lsn >= lsn)
                .map(|(sequence, payload)| RetainedMessage {
                    sequence: *sequence,
                    payload: payload.clone(),
                })
                .collect(),
        )
    }
}

#[derive(Debug)]
#[cfg(test)]
pub(crate) enum RoleTransportState {
    None,
    Primary {
        sessions: BTreeMap<ReplicaIdentity, ReliableWindow<ReplicationItem>>,
    },
    Secondary {
        source: ReplicaIdentity,
    },
}

#[cfg(test)]
impl RoleTransportState {
    pub(crate) fn transition(
        &mut self,
        role: ReplicaRole,
        source: Option<ReplicaIdentity>,
    ) -> Result<()> {
        for window in match self {
            Self::Primary { sessions } => Some(sessions.values_mut()),
            _ => None,
        }
        .into_iter()
        .flatten()
        {
            window.cancel();
        }
        *self = match role {
            ReplicaRole::Primary => Self::Primary {
                sessions: BTreeMap::new(),
            },
            ReplicaRole::ActiveSecondary | ReplicaRole::IdleSecondary => Self::Secondary {
                source: source.ok_or_else(|| {
                    RuntimeError::AuthorityMismatch(
                        "secondary sender requires an exact primary source".into(),
                    )
                })?,
            },
            ReplicaRole::None => Self::None,
        };
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SenderOutbound {
    Replication {
        receiver: ReplicaIdentity,
        sender_session: ProcessSessionId,
        receiver_session: ProcessSessionId,
        message: RetainedMessage<ReplicationItem>,
    },
    Copy {
        receiver: ReplicaIdentity,
        sender_session: ProcessSessionId,
        receiver_session: ProcessSessionId,
        message: RetainedMessage<CopyItem>,
    },
    Build(ReplicaEndpoint),
    Remove(ReplicaId),
    Evict(ReplicaIdentity),
}

struct PeerWindows {
    session: ProcessSessionId,
    replication: ReliableWindow<ReplicationItem>,
    copy: ReliableWindow<CopyItem>,
}

pub(crate) struct ReliableSender {
    local_session: ProcessSessionId,
    capacity: usize,
    peers: BTreeMap<ReplicaIdentity, PeerWindows>,
    evicted: std::collections::BTreeSet<ReplicaIdentity>,
}

impl ReliableSender {
    pub(crate) fn new(local_session: ProcessSessionId, capacity: usize) -> Result<Self> {
        ReliableWindow::<ReplicationItem>::new(capacity)?;
        Ok(Self {
            local_session,
            capacity,
            peers: BTreeMap::new(),
            evicted: std::collections::BTreeSet::new(),
        })
    }

    pub(crate) fn local_session(&self) -> &ProcessSessionId {
        &self.local_session
    }

    pub(crate) const fn retry_delay(&self) -> Duration {
        Duration::from_millis(100)
    }

    pub(crate) fn admit_peer(
        &mut self,
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    ) -> Result<()> {
        if self.evicted.contains(&identity) {
            return Err(RuntimeError::OperationCancelled);
        }
        if let Some(peer) = self.peers.get_mut(&identity) {
            peer.session = session;
            return Ok(());
        }
        self.peers.insert(
            identity,
            PeerWindows {
                session,
                replication: ReliableWindow::new(self.capacity)?,
                copy: ReliableWindow::new(self.capacity)?,
            },
        );
        Ok(())
    }

    pub(crate) fn queue(&mut self, outbound: OutboundOperation) -> Result<SenderOutbound> {
        match outbound {
            OutboundOperation::Replication(item) => {
                let receiver = item.receiver.clone();
                let peer = self
                    .peers
                    .get_mut(&receiver)
                    .ok_or(RuntimeError::ReconfigurationPending)?;
                Ok(SenderOutbound::Replication {
                    receiver,
                    sender_session: self.local_session.clone(),
                    receiver_session: peer.session.clone(),
                    message: peer.replication.enqueue(item)?,
                })
            }
            OutboundOperation::Copy(item) => {
                let receiver = item.receiver.clone();
                let peer = self
                    .peers
                    .get_mut(&receiver)
                    .ok_or(RuntimeError::ReconfigurationPending)?;
                Ok(SenderOutbound::Copy {
                    receiver,
                    sender_session: self.local_session.clone(),
                    receiver_session: peer.session.clone(),
                    message: peer.copy.enqueue(item)?,
                })
            }
            OutboundOperation::Build(endpoint) => Ok(SenderOutbound::Build(endpoint)),
            OutboundOperation::Remove(replica_id) => Ok(SenderOutbound::Remove(replica_id)),
            OutboundOperation::Evict(identity) => {
                self.evict_peer(&identity);
                Ok(SenderOutbound::Evict(identity))
            }
        }
    }

    pub(crate) fn acknowledge_replication(
        &mut self,
        receiver: &ReplicaIdentity,
        applied_lsn: i64,
    ) -> Result<()> {
        let peer = self
            .peers
            .get_mut(receiver)
            .ok_or(RuntimeError::ReconfigurationPending)?;
        if let Some(sequence) = peer
            .replication
            .retained
            .iter()
            .filter(|(_, item)| item.lsn <= applied_lsn)
            .map(|(sequence, _)| *sequence)
            .max()
        {
            peer.replication.acknowledge_through(sequence)?;
        }
        Ok(())
    }

    pub(crate) fn acknowledge_copy(
        &mut self,
        receiver: &ReplicaIdentity,
        item_sequence: u64,
    ) -> Result<()> {
        let peer = self
            .peers
            .get_mut(receiver)
            .ok_or(RuntimeError::ReconfigurationPending)?;
        if let Some(sequence) = peer
            .copy
            .retained
            .iter()
            .filter(|(_, item)| item.sequence <= item_sequence)
            .map(|(sequence, _)| *sequence)
            .max()
        {
            peer.copy.acknowledge_through(sequence)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn reconnect_replication(
        &self,
        receiver: &ReplicaIdentity,
        from_lsn: i64,
    ) -> Result<ResumeWindow<ReplicationItem>> {
        self.peers
            .get(receiver)
            .ok_or(RuntimeError::ReconfigurationPending)
            .map(|peer| peer.replication.reconnect_from_lsn(from_lsn))
    }

    pub(crate) fn peer_for_replica(&self, replica_id: ReplicaId) -> Option<ReplicaIdentity> {
        self.peers
            .keys()
            .find(|identity| identity.replica_id == replica_id)
            .cloned()
    }

    pub(crate) fn retire_peer(&mut self, receiver: &ReplicaIdentity) {
        if let Some(mut peer) = self.peers.remove(receiver) {
            peer.replication.cancel();
            peer.copy.cancel();
        }
    }

    pub(crate) fn evict_peer(&mut self, receiver: &ReplicaIdentity) {
        self.retire_peer(receiver);
        self.evicted.insert(receiver.clone());
    }
}
