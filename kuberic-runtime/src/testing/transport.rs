use std::sync::Arc;
use std::time::Duration;

use super::hosting::PodRuntime;
use crate::control::proto;
use crate::host::transport::OutboundDispatcher;
use crate::protocol::types::{OperationId, ProcessSessionId, ReplicaId, ReplicaIdentity};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaEndpoint {
    pub build_id: OperationId,
    pub identity: ReplicaIdentity,
    pub replication_address: String,
}

pub async fn dispatch_build<R: crate::host::ReplicaEndpointResolver + 'static>(
    runtime: &PodRuntime,
    session: ProcessSessionId,
    resolver: Arc<R>,
    resource: &str,
    token: &str,
    deadline: Duration,
    endpoint: ReplicaEndpoint,
) -> crate::host::Result<()> {
    let transport = Arc::new(tokio::sync::Mutex::new(
        crate::host::transport::ReliableTransport::new(session, 16)?,
    ));
    let dispatcher = crate::host::transport::GrpcOutboundDispatcher::new(
        runtime.inner.clone(),
        transport,
        resolver,
        resource,
        token,
        deadline,
    )?;
    dispatcher
        .dispatch(crate::host::transport::QueuedOutbound::Build(
            crate::transport::ReplicaEndpoint {
                build_id: endpoint.build_id,
                identity: endpoint.identity,
                replication_address: endpoint.replication_address,
            },
        ))
        .await
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("no exact endpoint registered for {0:?}")]
    Unregistered(ReplicaIdentity),
    #[error("obsolete process session {session} for {identity:?}")]
    StaleSession {
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    },
    #[error("invalid transport registration: {0}")]
    Registration(&'static str),
    #[error("invalid transport message: {0}")]
    Message(&'static str),
    #[error(transparent)]
    Runtime(#[from] crate::RuntimeError),
}

impl TransportError {
    fn from_inner(value: crate::host::testing::TransportError) -> Self {
        match value {
            crate::host::testing::TransportError::Unregistered(identity) => {
                Self::Unregistered(identity)
            }
            crate::host::testing::TransportError::StaleSession { identity, session } => {
                Self::StaleSession { identity, session }
            }
            crate::host::testing::TransportError::Registration(message) => {
                Self::Registration(message)
            }
            crate::host::testing::TransportError::Message(message) => Self::Message(message),
            crate::host::testing::TransportError::Runtime(error) => Self::Runtime(error),
        }
    }
}

pub type DeliveryId = u64;

#[derive(Debug, Clone)]
pub enum Message {
    Replication(proto::ReplicationItem),
    Copy(proto::CopyItem),
}

impl Message {
    fn into_inner(self) -> crate::host::testing::Message {
        match self {
            Self::Replication(item) => crate::host::testing::Message::Replication(item),
            Self::Copy(item) => crate::host::testing::Message::Copy(item),
        }
    }
    fn from_inner(value: crate::host::testing::Message) -> Self {
        match value {
            crate::host::testing::Message::Replication(item) => Self::Replication(item),
            crate::host::testing::Message::Copy(item) => Self::Copy(item),
        }
    }
}

#[derive(Debug)]
pub enum ControlOutput {
    Build(ReplicaEndpoint),
    Remove(ReplicaId),
    Evict(ReplicaIdentity),
}

#[derive(Debug)]
pub enum TransportEvent {
    Received {
        delivery: DeliveryId,
        acknowledgement: proto::ReplicationAck,
    },
    Applied {
        delivery: DeliveryId,
        acknowledgement: proto::ReplicationAck,
    },
    Copied {
        delivery: DeliveryId,
        acknowledgement: proto::CopyAck,
    },
    Control {
        source: ReplicaIdentity,
        session: ProcessSessionId,
        output: ControlOutput,
    },
    Rejected {
        delivery: Option<DeliveryId>,
        source: ReplicaIdentity,
        receiver: Option<ReplicaIdentity>,
        error: TransportError,
    },
    SourceClosed {
        source: ReplicaIdentity,
        session: ProcessSessionId,
    },
}

impl TransportEvent {
    fn from_inner(value: crate::host::testing::TransportEvent) -> Self {
        match value {
            crate::host::testing::TransportEvent::Received {
                delivery,
                acknowledgement,
            } => Self::Received {
                delivery,
                acknowledgement,
            },
            crate::host::testing::TransportEvent::Applied {
                delivery,
                acknowledgement,
            } => Self::Applied {
                delivery,
                acknowledgement,
            },
            crate::host::testing::TransportEvent::Copied {
                delivery,
                acknowledgement,
            } => Self::Copied {
                delivery,
                acknowledgement,
            },
            crate::host::testing::TransportEvent::Control {
                source,
                session,
                output,
            } => {
                let output = match output {
                    crate::host::testing::ControlOutput::Build(endpoint) => {
                        ControlOutput::Build(ReplicaEndpoint {
                            build_id: endpoint.build_id,
                            identity: endpoint.identity,
                            replication_address: endpoint.replication_address,
                        })
                    }
                    crate::host::testing::ControlOutput::Remove(id) => ControlOutput::Remove(id),
                    crate::host::testing::ControlOutput::Evict(identity) => {
                        ControlOutput::Evict(identity)
                    }
                };
                Self::Control {
                    source,
                    session,
                    output,
                }
            }
            crate::host::testing::TransportEvent::Rejected {
                delivery,
                source,
                receiver,
                error,
            } => Self::Rejected {
                delivery,
                source,
                receiver,
                error: TransportError::from_inner(error),
            },
            crate::host::testing::TransportEvent::SourceClosed { source, session } => {
                Self::SourceClosed { source, session }
            }
        }
    }
}

#[derive(Debug)]
pub struct PumpReport {
    pub dispatched: usize,
    pub in_flight: usize,
    pub events: Vec<TransportEvent>,
    pub idle: bool,
}

impl PumpReport {
    fn from_inner(value: crate::host::testing::PumpReport) -> Self {
        Self {
            dispatched: value.dispatched,
            in_flight: value.in_flight,
            events: value
                .events
                .into_iter()
                .map(TransportEvent::from_inner)
                .collect(),
            idle: value.idle,
        }
    }
}

#[derive(Default)]
pub struct InProcessTransport(crate::host::testing::InProcessTransport);

impl InProcessTransport {
    pub fn new() -> Self {
        Self(crate::host::testing::InProcessTransport::new())
    }

    pub async fn register(
        &mut self,
        runtime: Arc<PodRuntime>,
        session: ProcessSessionId,
    ) -> Result<ReplicaIdentity, TransportError> {
        self.0
            .register(runtime.inner.clone(), session)
            .await
            .map_err(TransportError::from_inner)
    }

    pub fn unregister(
        &mut self,
        identity: &ReplicaIdentity,
        session: &ProcessSessionId,
    ) -> Result<(), TransportError> {
        self.0
            .unregister(identity, session)
            .map_err(TransportError::from_inner)
    }

    pub fn bind(&self, message: Message) -> Result<Message, TransportError> {
        self.0
            .bind(message.into_inner())
            .map(Message::from_inner)
            .map_err(TransportError::from_inner)
    }

    pub fn enqueue(&mut self, message: Message) -> Result<DeliveryId, TransportError> {
        self.0
            .enqueue(message.into_inner())
            .map_err(TransportError::from_inner)
    }

    pub fn pump(&mut self) -> PumpReport {
        PumpReport::from_inner(self.0.pump())
    }

    pub async fn next(&mut self) -> PumpReport {
        PumpReport::from_inner(self.0.next().await)
    }
}
