//! Opt-in deterministic transport for in-process application tests.
//!
//! This owns data-plane polling, not authority or application lifetime. Callers
//! admit configurations/builds themselves and supply a fresh session on restart.
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::RuntimeError;
use crate::control::proto;
use crate::protocol::types::{ProcessSessionId, ReplicaId, ReplicaIdentity};
use crate::transport::ReplicaEndpoint;
use futures::future::{BoxFuture, poll_fn};
use futures::task::noop_waker_ref;

use crate::host::hosting::{OutboundReplication, PendingReplication, PodRuntime};
use crate::host::transport::{copy_from_proto, replication_from_proto};

#[derive(Debug, thiserror::Error)]
pub(crate) enum TransportError {
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
    Runtime(#[from] RuntimeError),
}

type Result<T> = std::result::Result<T, TransportError>;
pub(crate) type DeliveryId = u64;

/// Public wire messages; `bind` captures sessions before queuing or delaying one.
#[derive(Debug, Clone)]
pub(crate) enum Message {
    Replication(proto::ReplicationItem),
    Copy(proto::CopyItem),
}

impl Message {
    fn identities(&self) -> Result<(ReplicaIdentity, ReplicaIdentity)> {
        match self {
            Self::Replication(item) => {
                let item = replication_from_proto(item.clone())?;
                Ok((item.sender, item.receiver))
            }
            Self::Copy(item) => {
                let item = copy_from_proto(item.clone())?;
                Ok((item.sender, item.receiver))
            }
        }
    }

    fn sessions(&self) -> (&str, &str) {
        match self {
            Self::Replication(item) => (&item.sender_session_id, &item.receiver_session_id),
            Self::Copy(item) => (&item.sender_session_id, &item.receiver_session_id),
        }
    }

    fn set_sessions(&mut self, source: &ProcessSessionId, target: &ProcessSessionId) {
        let (sender, receiver) = match self {
            Self::Replication(item) => (&mut item.sender_session_id, &mut item.receiver_session_id),
            Self::Copy(item) => (&mut item.sender_session_id, &mut item.receiver_session_id),
        };
        *sender = source.to_string();
        *receiver = target.to_string();
    }
}

/// Control outputs are returned without interpreting them or mutating authority.
#[derive(Debug)]
pub(crate) enum ControlOutput {
    Build(ReplicaEndpoint),
    Remove(ReplicaId),
    Evict(ReplicaIdentity),
}

#[derive(Debug)]
pub(crate) enum TransportEvent {
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

#[derive(Debug)]
pub(crate) struct PumpReport {
    pub(crate) dispatched: usize,
    pub(crate) in_flight: usize,
    pub(crate) events: Vec<TransportEvent>,
    /// Nothing ready at this poll and no pending delivery/ACK. This is not a
    /// claim of cluster convergence, nor of quiescence of future application work.
    pub(crate) idle: bool,
}

struct Endpoint {
    runtime: Arc<PodRuntime>,
    session: ProcessSessionId,
    outbound: Option<BoxFuture<'static, Option<OutboundReplication>>>,
}

#[derive(Clone)]
struct Route {
    source: ReplicaIdentity,
    receiver: ReplicaIdentity,
    source_session: ProcessSessionId,
    receiver_session: ProcessSessionId,
}

enum Stage {
    Receiving(BoxFuture<'static, Result<PendingReplication>>),
    Received {
        acknowledgement: Box<proto::ReplicationAck>,
        future: BoxFuture<'static, Result<PendingReplication>>,
    },
    Applying(BoxFuture<'static, Result<proto::ReplicationAck>>),
    Copying(BoxFuture<'static, Result<proto::CopyAck>>),
    CopyAck(BoxFuture<'static, Result<proto::CopyAck>>),
}

struct Delivery {
    route: Route,
    stage: Stage,
}

/// Sole outbound consumer for registered runtimes. Pumping never fabricates
/// applied progress: it awaits the receiver's explicitly acknowledged stream.
#[derive(Default)]
pub(crate) struct InProcessTransport {
    endpoints: BTreeMap<ReplicaIdentity, Endpoint>,
    retired: BTreeSet<(ReplicaIdentity, ProcessSessionId)>,
    deliveries: BTreeMap<DeliveryId, Delivery>,
    events: VecDeque<TransportEvent>,
    next_delivery: DeliveryId,
}

fn outbound(runtime: Arc<PodRuntime>) -> BoxFuture<'static, Option<OutboundReplication>> {
    Box::pin(async move { runtime.data_plane().next_outbound().await })
}

impl InProcessTransport {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Register an opened runtime under its own exact identity. Replacing its
    /// session discards old polling/delivery futures, never transfers journals.
    pub(crate) async fn register(
        &mut self,
        runtime: Arc<PodRuntime>,
        session: ProcessSessionId,
    ) -> Result<ReplicaIdentity> {
        let snapshot = runtime.snapshot().await;
        if !snapshot.open {
            return Err(RuntimeError::NotOpen.into());
        }
        let identity = snapshot.identity;
        if session.is_empty() {
            return Err(TransportError::Registration("empty process session"));
        }
        if self.retired.contains(&(identity.clone(), session.clone())) {
            return Err(TransportError::StaleSession { identity, session });
        }
        if let Some(existing) = self.endpoints.get(&identity) {
            if existing.session == session {
                if Arc::ptr_eq(&existing.runtime, &runtime) {
                    return Ok(identity);
                }
                return Err(TransportError::Registration(
                    "session reused for a different runtime",
                ));
            }
            self.unregister(&identity, &existing.session.clone())?;
        }
        for (peer_identity, endpoint) in &self.endpoints {
            if runtime
                .snapshot()
                .await
                .authority
                .as_ref()
                .is_some_and(|authority| {
                    authority
                        .current_configuration
                        .members
                        .iter()
                        .chain(
                            authority
                                .previous_configuration
                                .iter()
                                .flat_map(|configuration| &configuration.members),
                        )
                        .any(|member| &member.identity == peer_identity)
                })
            {
                runtime
                    .register_peer_session(peer_identity.clone(), endpoint.session.clone())
                    .await?;
            }
            if endpoint
                .runtime
                .snapshot()
                .await
                .authority
                .as_ref()
                .is_some_and(|authority| {
                    authority
                        .current_configuration
                        .members
                        .iter()
                        .chain(
                            authority
                                .previous_configuration
                                .iter()
                                .flat_map(|configuration| &configuration.members),
                        )
                        .any(|member| member.identity == identity)
                })
            {
                endpoint
                    .runtime
                    .register_peer_session(identity.clone(), session.clone())
                    .await?;
            }
        }
        self.endpoints.insert(
            identity.clone(),
            Endpoint {
                outbound: Some(outbound(runtime.clone())),
                runtime,
                session,
            },
        );
        Ok(identity)
    }

    /// Remove only the exact current session. Every invalidated delivery is
    /// surfaced as a rejection on the next pump; no late ACK can earn credit.
    pub(crate) fn unregister(
        &mut self,
        identity: &ReplicaIdentity,
        session: &ProcessSessionId,
    ) -> Result<()> {
        self.endpoint(identity, session)?;
        self.endpoints.remove(identity);
        self.retired.insert((identity.clone(), session.clone()));
        let invalid: Vec<_> = self
            .deliveries
            .iter()
            .filter(|(_, d)| &d.route.source == identity || &d.route.receiver == identity)
            .map(|(id, _)| *id)
            .collect();
        for id in invalid {
            let delivery = self.deliveries.remove(&id).expect("selected delivery");
            self.events.push_back(TransportEvent::Rejected {
                delivery: Some(id),
                source: delivery.route.source,
                receiver: Some(delivery.route.receiver),
                error: TransportError::StaleSession {
                    identity: identity.clone(),
                    session: session.clone(),
                },
            });
        }
        Ok(())
    }

    fn endpoint(
        &self,
        identity: &ReplicaIdentity,
        session: &ProcessSessionId,
    ) -> Result<&Endpoint> {
        let endpoint = self
            .endpoints
            .get(identity)
            .ok_or_else(|| TransportError::Unregistered(identity.clone()))?;
        if endpoint.session != *session {
            return Err(TransportError::StaleSession {
                identity: identity.clone(),
                session: session.clone(),
            });
        }
        Ok(endpoint)
    }

    /// Capture sessions now, before delaying a message. Existing session fields
    /// are validated, never overwritten to make a stale envelope look current.
    pub(crate) fn bind(&self, mut message: Message) -> Result<Message> {
        let (source, receiver) = message.identities()?;
        let sender = self
            .endpoints
            .get(&source)
            .ok_or_else(|| TransportError::Unregistered(source.clone()))?;
        let target = self
            .endpoints
            .get(&receiver)
            .ok_or_else(|| TransportError::Unregistered(receiver.clone()))?;
        let (source_session, target_session) = message.sessions();
        for (identity, observed, current) in [
            (&source, source_session, &sender.session),
            (&receiver, target_session, &target.session),
        ] {
            if !observed.is_empty() && observed != current.as_str() {
                return Err(TransportError::StaleSession {
                    identity: identity.clone(),
                    session: ProcessSessionId::new(observed),
                });
            }
        }
        message.set_sessions(&sender.session, &target.session);
        Ok(message)
    }

    /// Queue an already session-bound message. Use `bind` at emission time for
    /// raw messages from a prepared copy stream or a manually polled data plane.
    pub(crate) fn enqueue(&mut self, message: Message) -> Result<DeliveryId> {
        let (source, receiver) = message.identities()?;
        if source == receiver {
            return Err(TransportError::Message("self delivery"));
        }
        let (source_session, target_session) = message.sessions();
        let route = Route {
            source,
            receiver,
            source_session: ProcessSessionId::new(source_session),
            receiver_session: ProcessSessionId::new(target_session),
        };
        let sender = self
            .endpoint(&route.source, &route.source_session)?
            .runtime
            .clone();
        let target = self
            .endpoint(&route.receiver, &route.receiver_session)?
            .runtime
            .clone();
        let stage = match message {
            Message::Replication(item) => {
                let identity = route.receiver.clone();
                let session = route.receiver_session.clone();
                Stage::Receiving(Box::pin(async move {
                    sender.register_peer_session(identity, session).await?;
                    Ok(target.data_plane().receive_replication(item).await?)
                }))
            }
            Message::Copy(item) => Stage::Copying(Box::pin(async move {
                Ok(target.data_plane().receive_copy_item(item).await?)
            })),
        };
        self.next_delivery = self
            .next_delivery
            .checked_add(1)
            .ok_or(TransportError::Message("delivery IDs exhausted"))?;
        let id = self.next_delivery;
        self.deliveries.insert(id, Delivery { route, stage });
        Ok(id)
    }

    /// One bounded, nonblocking poll of each source and pending delivery, in
    /// identity/delivery order. No sleeps, spawned workers or timeout heuristics.
    pub(crate) fn pump(&mut self) -> PumpReport {
        self.poll_ready(&mut Context::from_waker(noop_waker_ref()))
    }

    /// Wait for real outbound/ACK activity using its wakers. Cancelling this
    /// wait retains transport-owned deliveries for a subsequent pump.
    pub(crate) async fn next(&mut self) -> PumpReport {
        poll_fn(|cx| {
            let report = self.poll_ready(cx);
            if report.dispatched > 0 || !report.events.is_empty() {
                Poll::Ready(report)
            } else {
                Poll::Pending
            }
        })
        .await
    }

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> PumpReport {
        let mut events: Vec<_> = self.events.drain(..).collect();
        let mut ready = Vec::new();
        for (identity, endpoint) in &mut self.endpoints {
            if let Some(future) = &mut endpoint.outbound
                && let Poll::Ready(item) = future.as_mut().poll(cx)
            {
                endpoint.outbound = item.as_ref().map(|_| outbound(endpoint.runtime.clone()));
                ready.push((identity.clone(), endpoint.session.clone(), item));
            }
        }
        let mut dispatched = 0;
        for (source, session, item) in ready {
            let message = match item {
                Some(OutboundReplication::Replication(item)) => Message::Replication(item),
                Some(OutboundReplication::Copy(item)) => Message::Copy(item),
                control => {
                    let output = match control {
                        Some(OutboundReplication::Build(target)) => ControlOutput::Build(target),
                        Some(OutboundReplication::Remove(target)) => ControlOutput::Remove(target),
                        Some(OutboundReplication::Evict(target)) => ControlOutput::Evict(target),
                        None => {
                            events.push(TransportEvent::SourceClosed { source, session });
                            continue;
                        }
                        _ => unreachable!("data messages handled above"),
                    };
                    events.push(TransportEvent::Control {
                        source,
                        session,
                        output,
                    });
                    continue;
                }
            };
            let identities = message.identities();
            let receiver = identities
                .as_ref()
                .ok()
                .map(|(_, receiver)| receiver.clone());
            let result = identities.and_then(|(sender, _)| {
                if sender != source {
                    return Err(TransportError::Message(
                        "outbound sender differs from registered source",
                    ));
                }
                self.enqueue(self.bind(message)?)
            });
            match result {
                Ok(_) => dispatched += 1,
                Err(error) => events.push(TransportEvent::Rejected {
                    delivery: None,
                    source,
                    receiver,
                    error,
                }),
            }
        }
        let deliveries = std::mem::take(&mut self.deliveries);
        for (id, mut delivery) in deliveries {
            match self.poll_delivery(id, &mut delivery, cx, &mut events) {
                Poll::Pending => {
                    self.deliveries.insert(id, delivery);
                }
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(error)) => events.push(TransportEvent::Rejected {
                    delivery: Some(id),
                    source: delivery.route.source,
                    receiver: Some(delivery.route.receiver),
                    error,
                }),
            }
        }
        let in_flight = self.deliveries.len();
        let idle = dispatched == 0 && in_flight == 0 && events.is_empty();
        PumpReport {
            dispatched,
            in_flight,
            events,
            idle,
        }
    }

    fn poll_delivery(
        &self,
        id: DeliveryId,
        delivery: &mut Delivery,
        cx: &mut Context<'_>,
        events: &mut Vec<TransportEvent>,
    ) -> Poll<Result<()>> {
        let route = &delivery.route;
        let sender = self
            .endpoint(&route.source, &route.source_session)?
            .runtime
            .clone();
        self.endpoint(&route.receiver, &route.receiver_session)?;
        loop {
            delivery.stage = match &mut delivery.stage {
                Stage::Receiving(future) => {
                    let pending = futures::ready!(future.as_mut().poll(cx))?;
                    let mut ack = pending.received.clone();
                    ack.sender_session_id = route.source_session.to_string();
                    ack.receiver_session_id = route.receiver_session.to_string();
                    let acknowledgement = Box::new(ack.clone());
                    let runtime = sender.clone();
                    Stage::Received {
                        acknowledgement,
                        future: Box::pin(async move {
                            runtime.data_plane().accept_acknowledgement(ack).await?;
                            Ok(pending)
                        }),
                    }
                }
                Stage::Received {
                    acknowledgement,
                    future,
                } => {
                    let pending = futures::ready!(future.as_mut().poll(cx))?;
                    events.push(TransportEvent::Received {
                        delivery: id,
                        acknowledgement: acknowledgement.as_ref().clone(),
                    });
                    let runtime = sender.clone();
                    let route = route.clone();
                    Stage::Applying(Box::pin(async move {
                        let mut ack = pending.applied().await?;
                        ack.sender_session_id = route.source_session.to_string();
                        ack.receiver_session_id = route.receiver_session.to_string();
                        runtime
                            .data_plane()
                            .accept_acknowledgement(ack.clone())
                            .await?;
                        Ok(ack)
                    }))
                }
                Stage::Applying(future) => {
                    let acknowledgement = futures::ready!(future.as_mut().poll(cx))?;
                    events.push(TransportEvent::Applied {
                        delivery: id,
                        acknowledgement,
                    });
                    return Poll::Ready(Ok(()));
                }
                Stage::Copying(future) => {
                    let mut ack = futures::ready!(future.as_mut().poll(cx))?;
                    ack.sender_session_id = route.source_session.to_string();
                    ack.receiver_session_id = route.receiver_session.to_string();
                    let runtime = sender.clone();
                    Stage::CopyAck(Box::pin(async move {
                        runtime
                            .data_plane()
                            .accept_copy_acknowledgement(ack.clone())
                            .await?;
                        Ok(ack)
                    }))
                }
                Stage::CopyAck(future) => {
                    let acknowledgement = futures::ready!(future.as_mut().poll(cx))?;
                    events.push(TransportEvent::Copied {
                        delivery: id,
                        acknowledgement,
                    });
                    return Poll::Ready(Ok(()));
                }
            };
        }
    }
}
pub(crate) async fn describe_peer(
    runtime: &crate::host::hosting::PodRuntime,
    replica: crate::replicator::ReplicaInformation,
) -> crate::Result<()> {
    runtime.describe_peer(replica).await
}

pub(crate) async fn execute_build(
    runtime: &crate::host::hosting::PodRuntime,
    replica: crate::replicator::ReplicaInformation,
) -> crate::Result<()> {
    runtime
        .execute_admitted_build(replica, || async {
            Err(crate::RuntimeError::Application(
                "testing custom build requested the built-in copy route".into(),
            ))
        })
        .await
}

#[cfg(test)]
pub(crate) async fn execute_build_with_copy<F, Fut>(
    runtime: &crate::host::hosting::PodRuntime,
    replica: crate::replicator::ReplicaInformation,
    managed_copy: F,
) -> crate::Result<()>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = crate::Result<()>>,
{
    runtime.execute_admitted_build(replica, managed_copy).await
}

#[cfg(test)]
pub(crate) async fn build_generation(
    runtime: &crate::host::hosting::PodRuntime,
    build_id: &crate::protocol::types::OperationId,
) -> crate::Result<u64> {
    runtime.build_runtime().generation(build_id).await
}

#[cfg(test)]
pub(crate) async fn cancel_build_attempt(
    runtime: &crate::host::hosting::PodRuntime,
    build_id: &crate::protocol::types::OperationId,
    generation: u64,
) -> crate::Result<()> {
    runtime
        .build_attempt_runtime()
        .cancel_attempt(build_id, generation, true)
        .await
}

#[cfg(test)]
pub(crate) async fn set_lifecycle_access(
    runtime: &crate::host::hosting::PodRuntime,
    read: crate::protocol::types::AccessStatus,
    write: crate::protocol::types::AccessStatus,
) -> crate::Result<()> {
    runtime.testing_set_access(read, write).await
}

#[cfg(test)]
pub(crate) async fn register_lifecycle_peer_session(
    runtime: &crate::host::hosting::PodRuntime,
    identity: crate::protocol::types::ReplicaIdentity,
    session: crate::protocol::types::ProcessSessionId,
) -> crate::Result<()> {
    runtime.register_peer_session(identity, session).await
}

#[cfg(test)]
pub(crate) async fn wait_for_lifecycle_catch_up(
    runtime: &crate::host::hosting::PodRuntime,
) -> crate::Result<()> {
    runtime.testing_wait_for_catch_up().await
}

#[cfg(test)]
pub(crate) async fn admit_lifecycle_authority(
    runtime: &crate::host::hosting::PodRuntime,
    authority: crate::authority::AdmittedAuthority,
) -> crate::Result<()> {
    runtime.testing_admit_authority(authority).await
}

#[cfg(test)]
pub(crate) async fn close_lifecycle(
    runtime: &crate::host::hosting::PodRuntime,
) -> crate::Result<()> {
    runtime.testing_close().await
}
