use std::collections::BTreeMap;
use std::time::Duration;

use kuberic_protocol::types::{ProcessSessionId, ReplicaId, ReplicaIdentity, ReplicaRole};
use kuberic_runtime_internal::transport::{
    CopyItem, OutboundOperation, ReplicaEndpoint, ReplicationItem,
};

use crate::{Result, RuntimeError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedMessage<T> {
    pub sequence: u64,
    pub payload: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeWindow<T> {
    Retained(Vec<RetainedMessage<T>>),
    FullCopyRequired,
}

#[derive(Debug)]
pub struct ReliableWindow<T> {
    capacity: usize,
    next_sequence: u64,
    acknowledged_sequence: u64,
    retained: BTreeMap<u64, T>,
    cancelled: bool,
    ever_enqueued: bool,
}

impl<T: Clone> ReliableWindow<T> {
    pub fn new(capacity: usize) -> Result<Self> {
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
            ever_enqueued: false,
        })
    }

    pub fn enqueue(&mut self, payload: T) -> Result<RetainedMessage<T>> {
        if self.cancelled {
            return Err(RuntimeError::OperationCancelled);
        }
        if self.retained.len() >= self.capacity {
            return Err(RuntimeError::QueueFull);
        }
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.ever_enqueued = true;
        self.retained.insert(sequence, payload.clone());
        Ok(RetainedMessage { sequence, payload })
    }

    pub fn acknowledge_through(&mut self, sequence: u64) -> Result<()> {
        if sequence < self.acknowledged_sequence || sequence >= self.next_sequence {
            return Err(RuntimeError::InvalidReplication(
                "acknowledgement is outside the retained send window".into(),
            ));
        }
        self.acknowledged_sequence = sequence;
        self.retained.retain(|retained, _| *retained > sequence);
        Ok(())
    }

    pub fn reconnect_from(&self, sequence: u64) -> ResumeWindow<T> {
        if self.cancelled {
            return ResumeWindow::FullCopyRequired;
        }
        if sequence <= self.acknowledged_sequence {
            return ResumeWindow::Retained(self.retained());
        }
        let first = self
            .retained
            .first_key_value()
            .map(|(sequence, _)| *sequence);
        if first.is_some_and(|first| sequence < first) || sequence >= self.next_sequence {
            return ResumeWindow::FullCopyRequired;
        }
        ResumeWindow::Retained(
            self.retained
                .range(sequence..)
                .map(|(sequence, payload)| RetainedMessage {
                    sequence: *sequence,
                    payload: payload.clone(),
                })
                .collect(),
        )
    }

    pub fn retained(&self) -> Vec<RetainedMessage<T>> {
        self.retained
            .iter()
            .map(|(sequence, payload)| RetainedMessage {
                sequence: *sequence,
                payload: payload.clone(),
            })
            .collect()
    }

    pub fn cancel(&mut self) {
        self.cancelled = true;
        self.retained.clear();
    }
}

impl ReliableWindow<ReplicationItem> {
    pub fn catch_up_capability(&self) -> Option<i64> {
        self.retained.values().map(|item| item.lsn).min()
    }

    pub fn reconnect_from_lsn(&self, lsn: i64) -> ResumeWindow<ReplicationItem> {
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
pub enum RoleTransportState {
    None,
    Primary {
        sessions: BTreeMap<ReplicaIdentity, ReliableWindow<ReplicationItem>>,
    },
    Secondary {
        source: ReplicaIdentity,
    },
}

impl RoleTransportState {
    pub fn transition(&mut self, role: ReplicaRole, source: Option<ReplicaIdentity>) -> Result<()> {
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
pub enum SenderOutbound {
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
}

struct PeerWindows {
    session: ProcessSessionId,
    replication: ReliableWindow<ReplicationItem>,
    copy: ReliableWindow<CopyItem>,
}

pub struct ReliableSender {
    local_session: ProcessSessionId,
    capacity: usize,
    peers: BTreeMap<ReplicaIdentity, PeerWindows>,
}

impl ReliableSender {
    pub fn new(local_session: ProcessSessionId, capacity: usize) -> Result<Self> {
        ReliableWindow::<ReplicationItem>::new(capacity)?;
        Ok(Self {
            local_session,
            capacity,
            peers: BTreeMap::new(),
        })
    }

    pub fn local_session(&self) -> &ProcessSessionId {
        &self.local_session
    }

    pub const fn retry_delay(&self) -> Duration {
        Duration::from_millis(100)
    }

    pub fn admit_peer(
        &mut self,
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    ) -> Result<()> {
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

    pub fn queue(&mut self, outbound: OutboundOperation) -> Result<SenderOutbound> {
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
        }
    }

    pub fn acknowledge_replication(
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

    pub fn acknowledge_copy(
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

    pub fn reconnect_replication(
        &self,
        receiver: &ReplicaIdentity,
        from_lsn: i64,
    ) -> Result<ResumeWindow<ReplicationItem>> {
        self.peers
            .get(receiver)
            .ok_or(RuntimeError::ReconfigurationPending)
            .map(|peer| peer.replication.reconnect_from_lsn(from_lsn))
    }

    pub fn peer_for_replica(&self, replica_id: ReplicaId) -> Option<ReplicaIdentity> {
        self.peers
            .keys()
            .find(|identity| identity.replica_id == replica_id)
            .cloned()
    }

    pub fn retire_peer(&mut self, receiver: &ReplicaIdentity) {
        if let Some(mut peer) = self.peers.remove(receiver) {
            peer.replication.cancel();
            peer.copy.cancel();
        }
    }
}
