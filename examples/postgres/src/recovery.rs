use std::time::Duration;

use kuberic_runtime::protocol::types::{
    ConfigurationDescriptor, ProcessSessionId, ReplicaIdentity, ReplicaRole, ResourceUid,
};
use kuberic_runtime::replicator::ReplicaSetConfiguration;
use kuberic_runtime::{Result, RuntimeError};
use serde::{Deserialize, Serialize};

use super::{PgReplicator, application_error};
use crate::build::{PgLineage, decode, encode};
use crate::durable::{PgDurableError, PgDurableRole};
use crate::native::{AcknowledgementPolicy, PgReplicationEvidence};

#[cfg(all(test, feature = "testing"))]
#[path = "promotion_status_tests.rs"]
mod promotion_status_tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStage {
    PolicyInvalidated,
    PolicyApplied,
    PolicyAccepted,
    InitialRound,
    ReceiversDrained,
    FinalRound,
    Promoted,
    Ready,
    SourceFenceIntent,
    SourceStopped,
}

#[cfg(feature = "testing")]
pub struct RecoveryGate {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Peer {
    identity: ReplicaIdentity,
    session: ProcessSessionId,
    endpoint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Membership {
    configuration: ConfigurationDescriptor,
    peers: Vec<Peer>,
}

impl Membership {
    fn new(configuration: &ReplicaSetConfiguration, local: &ReplicaIdentity) -> Self {
        let mut peers = std::collections::BTreeMap::new();
        for r in &configuration.replicas {
            if &r.identity != local
                && !configuration
                    .configuration
                    .members
                    .iter()
                    .any(|m| m.identity == r.identity)
            {
                continue;
            }
            peers.entry(r.identity.clone()).or_insert_with(|| Peer {
                identity: r.identity.clone(),
                session: r.process_session_id.clone(),
                endpoint: r.replication_address.clone(),
            });
        }
        Self {
            configuration: configuration.configuration.clone(),
            peers: peers.into_values().collect(),
        }
    }

    fn peer(&self, identity: &ReplicaIdentity) -> Result<&Peer> {
        self.peers
            .iter()
            .find(|p| &p.identity == identity)
            .filter(|p| {
                !p.session.is_empty()
                    && p.endpoint.starts_with("http://")
                    && p.endpoint.len() <= 512
            })
            .ok_or(RuntimeError::AuthorityNotAdmitted)
    }

    fn primary(&self) -> Result<&Peer> {
        let identity = &self
            .configuration
            .members
            .iter()
            .find(|m| m.role == ReplicaRole::Primary)
            .ok_or(RuntimeError::AuthorityNotAdmitted)?
            .identity;
        self.peer(identity)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Policy {
    membership: Membership,
    previous: Option<ConfigurationDescriptor>,
    generation: u64,
    policy: AcknowledgementPolicy,
    lineage: PgLineage,
}

impl Policy {
    fn validate(&self) -> Result<()> {
        self.policy.validate().map_err(application_error)?;
        self.lineage.validate().map_err(application_error)?;
        if self.generation == 0
            || self.membership.peers.len() > 32
            || self.policy.configuration_id != self.membership.configuration.configuration_id
            || u64::try_from(self.membership.configuration.epoch.configuration_number).ok()
                != Some(self.policy.configuration_generation)
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        let identities = self
            .membership
            .peers
            .iter()
            .map(|peer| &peer.identity)
            .collect::<std::collections::BTreeSet<_>>();
        if identities.len() != self.membership.peers.len() {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        for replica in &self.policy.eligible_standbys {
            if self.membership.peer(&replica.identity)?.session != replica.process_session_id {
                return Err(RuntimeError::AuthorityNotAdmitted);
            }
        }
        if self.policy.valid {
            let sessions = self
                .membership
                .peers
                .iter()
                .map(|p| (p.identity.clone(), p.session.clone()))
                .collect();
            let expected = crate::native::compile_synchronous_configuration(
                self.previous.as_ref(),
                &self.membership.configuration,
                &self.membership.primary()?.identity,
                &sessions,
                true,
            )
            .map_err(application_error)?;
            if self.policy.write_acknowledgements < expected.write_acknowledgements
                || self
                    .policy
                    .eligible_standbys
                    .iter()
                    .any(|peer| !expected.eligible_standbys.contains(peer))
            {
                return Err(RuntimeError::AuthorityNotAdmitted);
            }
        }
        Ok(())
    }

    fn quorum(&self, count: usize) -> Result<()> {
        self.validate()?;
        if !self.policy.valid
            || self.policy.write_acknowledgements == 0
            || count + self.policy.write_acknowledgements as usize
                <= self.policy.eligible_standbys.len()
        {
            tracing::warn!(
                responders = count,
                required = self.policy.write_acknowledgements,
                eligible = self.policy.eligible_standbys.len(),
                "PostgreSQL recovery quorum unavailable"
            );
            return Err(RuntimeError::ReconfigurationPending);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Recovery {
    pub membership: Membership,
    pub previous: Option<ConfigurationDescriptor>,
    pub accepted_policy: Option<Policy>,
    pub pending: Option<Election>,
    pub source_fence: Option<SourceFence>,
    pub former_primary: bool,
    #[serde(default)]
    pub receiver_epoch: Option<kuberic_runtime::protocol::types::Epoch>,
    #[serde(default)]
    followed: Option<(kuberic_runtime::protocol::types::Epoch, PgLineage)>,
    #[serde(default)]
    preparation: Option<(kuberic_runtime::protocol::types::ConfigurationId, i64)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    connection: Option<Connection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Connection {
    source: Peer,
    receiver: ProcessSessionId,
    host: String,
    port: u16,
    lineage: PgLineage,
}

impl Recovery {
    pub(crate) fn rebuilt(&mut self) {
        self.former_primary = false;
        self.source_fence = None;
        self.pending = None;
        self.receiver_epoch = None;
        self.accepted_policy = None;
        self.followed = None;
        self.preparation = None;
        self.connection = None;
    }

    pub(crate) fn validate(&self, local: &ReplicaIdentity) -> std::result::Result<(), String> {
        kuberic_runtime::protocol::validation::validate_configuration(
            &self.membership.configuration,
            None,
        )
        .map_err(|error| error.to_string())?;
        let identities = self
            .membership
            .peers
            .iter()
            .map(|peer| &peer.identity)
            .collect::<std::collections::BTreeSet<_>>();
        if self.membership.peers.len() > 64
            || identities.len() != self.membership.peers.len()
            || !identities.contains(local)
            || self
                .membership
                .peers
                .iter()
                .any(|p| p.endpoint.len() > 512 || p.session.as_str().len() > 128)
            || self
                .receiver_epoch
                .is_some_and(|epoch| epoch > self.membership.configuration.epoch)
        {
            return Err("invalid exact PostgreSQL recovery membership".into());
        }
        if let Some(policy) = &self.accepted_policy {
            policy.validate().map_err(|error| error.to_string())?;
            if policy.membership.configuration.epoch > self.membership.configuration.epoch {
                return Err("future PostgreSQL accepted policy".into());
            }
        }
        if let Some(election) = &self.pending {
            election
                .policy
                .quorum(election.responders.len())
                .map_err(|error| error.to_string())?;
            if election.responders.len() > 16
                || election.final_observations.len() > election.responders.len()
                || election.configuration.epoch > self.membership.configuration.epoch
                || election.boundary.is_some_and(|boundary| boundary < 0)
                || election.ready && !election.promoted
                || election.promoted && election.boundary.is_none()
                || election.promotion.as_ref().is_some_and(|intent| {
                    intent.process_generation == 0
                        || election.boundary.is_none()
                        || !election.responders.contains(&intent.candidate)
                        || !election.configuration.members.iter().any(|member| {
                            member.role == ReplicaRole::Primary
                                && member.identity == intent.candidate.identity
                        })
                })
            {
                return Err("invalid PostgreSQL election journal".into());
            }
            let responders = election
                .responders
                .iter()
                .map(|peer| &peer.identity)
                .collect::<std::collections::BTreeSet<_>>();
            if responders.len() != election.responders.len() {
                return Err("duplicate PostgreSQL recovery responder".into());
            }
            let final_peers = election
                .final_observations
                .iter()
                .map(|o| &o.peer.identity)
                .collect::<std::collections::BTreeSet<_>>();
            if final_peers.len() != election.final_observations.len()
                || election.boundary.is_some_and(|boundary| {
                    election.final_observations.len() != election.responders.len()
                        || !election.final_observations.iter().any(|observation| {
                            &observation.peer.identity == local
                                && observation.evidence.received_lsn == Some(boundary)
                        })
                })
            {
                return Err("non-exact PostgreSQL final-round boundary".into());
            }
            for observation in &election.final_observations {
                validate_observation(&election.policy, observation, true)
                    .map_err(|error| error.to_string())?;
                if !election.responders.contains(&observation.peer) {
                    return Err("unrequested final recovery observation".into());
                }
            }
        }
        if let Some(fence) = &self.source_fence {
            fence.lineage.validate()?;
            if fence.boundary < 0 || fence.source.session.is_empty() {
                return Err("invalid PostgreSQL source fence".into());
            }
        }
        if self
            .preparation
            .as_ref()
            .is_some_and(|(configuration, boundary)| {
                configuration.is_empty() || configuration.as_str().len() > 128 || *boundary < 0
            })
        {
            return Err("invalid PostgreSQL handoff boundary".into());
        }
        if let Some((epoch, lineage)) = &self.followed {
            lineage.validate()?;
            if *epoch > self.membership.configuration.epoch {
                return Err("future PostgreSQL following receipt".into());
            }
        }
        if let Some(connection) = &self.connection {
            connection.lineage.validate()?;
            if connection.source.identity.replica_id.value() <= 0
                || invalid_connection_value(connection.source.identity.instance_id.as_str(), 128)
                || invalid_connection_value(
                    connection.source.identity.agent_generation.as_str(),
                    128,
                )
                || invalid_connection_value(connection.source.session.as_str(), 128)
                || !connection.source.endpoint.starts_with("http://")
                || invalid_connection_value(&connection.source.endpoint, 512)
                || connection.host.parse::<std::net::IpAddr>().is_err()
                || connection.port == 0
                || invalid_connection_value(connection.receiver.as_str(), 128)
            {
                return Err("invalid PostgreSQL streaming connection".into());
            }
        }
        Ok(())
    }
}

fn invalid_connection_value(value: &str, maximum: usize) -> bool {
    value.is_empty() || value.len() > maximum || value.chars().any(char::is_control)
}

#[cfg(all(test, feature = "testing"))]
mod connection_validation_tests {
    use super::*;
    use crate::build::PgLineage;
    use crate::durable::{PgDurableIdentity, PgDurableStore, StorageMode};
    use crate::testing::{TestDataDir, native_configuration, native_identity};

    #[derive(Clone, Copy)]
    enum MalformedConnection {
        Host,
        Port,
        Receiver,
        SourceSession,
        SourceInstance,
        SourceGeneration,
    }

    impl MalformedConnection {
        fn all() -> [Self; 6] {
            [
                Self::Host,
                Self::Port,
                Self::Receiver,
                Self::SourceSession,
                Self::SourceInstance,
                Self::SourceGeneration,
            ]
        }
    }

    fn corrupt(recovery: &mut Recovery, malformed: MalformedConnection) {
        let connection = recovery.connection.as_mut().unwrap();
        match malformed {
            MalformedConnection::Host => connection.host = "not-an-ip-address".into(),
            MalformedConnection::Port => connection.port = 0,
            MalformedConnection::Receiver => {
                connection.receiver = ProcessSessionId::new("bad\nreceiver")
            }
            MalformedConnection::SourceSession => {
                connection.source.session = ProcessSessionId::new("bad\nsession")
            }
            MalformedConnection::SourceInstance => {
                connection.source.identity.instance_id =
                    kuberic_runtime::protocol::types::ReplicaInstanceId::new("bad\ninstance")
            }
            MalformedConnection::SourceGeneration => {
                connection.source.identity.agent_generation =
                    kuberic_runtime::protocol::types::AgentGeneration::new("bad\ngeneration")
            }
        }
    }

    fn recovery(local: &ReplicaIdentity, followed: bool) -> Recovery {
        let source = native_identity(1, "source");
        let lineage = PgLineage {
            system_identifier: "123456789".into(),
            timeline: 1,
            history: Vec::new(),
            history_text: String::new(),
        };
        Recovery {
            membership: Membership {
                configuration: native_configuration(&[source.clone(), local.clone()], 0, 1),
                peers: vec![
                    Peer {
                        identity: source.clone(),
                        session: ProcessSessionId::new("source-session"),
                        endpoint: "http://127.0.0.1:41001".into(),
                    },
                    Peer {
                        identity: local.clone(),
                        session: ProcessSessionId::new("local-session"),
                        endpoint: "http://127.0.0.1:41002".into(),
                    },
                ],
            },
            previous: None,
            accepted_policy: None,
            pending: None,
            source_fence: None,
            former_primary: false,
            receiver_epoch: None,
            followed: followed.then(|| {
                (
                    kuberic_runtime::protocol::types::Epoch::new(0, 1),
                    lineage.clone(),
                )
            }),
            preparation: None,
            connection: Some(Connection {
                source: Peer {
                    identity: source,
                    session: ProcessSessionId::new("source-session"),
                    endpoint: "http://127.0.0.1:41001".into(),
                },
                receiver: ProcessSessionId::new("local-session"),
                host: "127.0.0.1".into(),
                port: 5432,
                lineage,
            }),
        }
    }

    #[tokio::test]
    async fn malformed_streaming_connection_is_rejected_with_or_without_follow_receipt() {
        for followed in [false, true] {
            for malformed in MalformedConnection::all() {
                let directory = TestDataDir::new("invalid-streaming-connection");
                let local = native_identity(2, "local");
                let identity = PgDurableIdentity {
                    resource_uid: ResourceUid::new("postgres-native-test"),
                    replica: local.clone(),
                };
                let store = PgDurableStore::open(
                    directory.path().join("application"),
                    identity.clone(),
                    StorageMode::Fresh,
                )
                .await
                .unwrap();
                store
                    .update(|state| {
                        state.recovery = Some(recovery(&local, followed));
                        Ok(())
                    })
                    .await
                    .unwrap();

                let invalid = store
                    .update(|state| {
                        corrupt(state.recovery.as_mut().unwrap(), malformed);
                        Ok(())
                    })
                    .await;
                assert!(
                    matches!(invalid, Err(PgDurableError::Invalid(message)) if message.contains("streaming connection"))
                );

                let mut invalid = store.snapshot().await;
                corrupt(invalid.recovery.as_mut().unwrap(), malformed);
                store.persist_unchecked_for_test(invalid).await.unwrap();
                drop(store);

                let reopened = PgDurableStore::open(
                    directory.path().join("application"),
                    identity,
                    StorageMode::Established,
                )
                .await;
                assert!(
                    matches!(reopened, Err(PgDurableError::Invalid(message)) if message.contains("streaming connection"))
                );
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceFence {
    configuration: ConfigurationDescriptor,
    source: Peer,
    lineage: PgLineage,
    boundary: i64,
    stopped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Election {
    configuration: ConfigurationDescriptor,
    policy: Policy,
    responders: Vec<Peer>,
    final_observations: Vec<Observation>,
    boundary: Option<i64>,
    promoted: bool,
    #[serde(default)]
    ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    promotion: Option<Promotion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Promotion {
    candidate: Peer,
    process_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Observation {
    peer: Peer,
    lineage: PgLineage,
    evidence: PgReplicationEvidence,
    source_fence: Option<SourceFence>,
    policy: Option<Policy>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    host: String,
    #[serde(default, skip_serializing_if = "zero_port")]
    port: u16,
    #[serde(default = "legacy_running", skip_serializing_if = "is_running")]
    running: bool,
}

fn legacy_running() -> bool {
    true
}
fn is_running(value: &bool) -> bool {
    *value
}
fn zero_port(value: &u16) -> bool {
    *value == 0
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    version: u32,
    resource: ResourceUid,
    configuration: ConfigurationDescriptor,
    sender: Peer,
    receiver: Peer,
    action: Action,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Action {
    Policy(Box<Policy>),
    Observe,
    Drain,
    Fence(Box<SourceFence>),
    Follow {
        lineage: PgLineage,
        host: String,
        port: u16,
        boundary: i64,
    },
}

impl PgReplicator {
    pub(super) async fn nonvoting_recovery_session(
        &self,
        configuration: &ConfigurationDescriptor,
        identity: &ReplicaIdentity,
    ) -> Result<ProcessSessionId> {
        let recovery = self
            .durable
            .snapshot()
            .await
            .recovery
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let election = recovery
            .pending
            .filter(|e| e.promoted && e.configuration == *configuration)
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if election
            .responders
            .iter()
            .any(|peer| &peer.identity == identity)
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        Ok(election.policy.membership.peer(identity)?.session.clone())
    }

    pub(super) async fn interrupted_promotion(
        &self,
        durable: &crate::durable::PgDurableState,
        generation: u64,
        reconcile_stopped: bool,
    ) -> Result<bool> {
        let lease = self.instance.generation_lease();
        if lease.id() != generation {
            return Err(RuntimeError::OperationCancelled);
        }
        let Some(recovery) = &durable.recovery else {
            return Ok(false);
        };
        let Some(election) = &recovery.pending else {
            return Ok(false);
        };
        let Some(intent) = &election.promotion else {
            return Ok(false);
        };
        let Some(boundary) = election.boundary else {
            return Ok(false);
        };
        let local = recovery.membership.peer(&durable.identity.replica)?;
        if !durable.has_accepted_authority
            || !durable.external_access_closed
            || recovery.former_primary
            || election.promoted
            || election.ready
            || election.configuration != recovery.membership.configuration
            || recovery.accepted_policy.as_ref() != Some(&election.policy)
            || recovery.receiver_epoch != Some(election.configuration.epoch)
            || &intent.candidate != local
            || recovery.membership.primary()? != local
            || election.policy.membership.peer(&local.identity)? != local
            || !election.policy.policy.eligible_standbys.iter().any(|peer| {
                peer.identity == local.identity && peer.process_session_id == local.session
            })
            || durable.system_identifier.as_ref()
                != Some(&election.policy.lineage.system_identifier)
            || durable.timeline_id != Some(election.policy.lineage.timeline)
        {
            return Ok(false);
        }
        for peer in &election.responders {
            if recovery.membership.peer(&peer.identity)? != peer {
                return Ok(false);
            }
        }
        if let Some(fence) = &recovery.source_fence
            && (fence.configuration != election.configuration
                || !fence.stopped
                || fence.source != *election.policy.membership.primary()?
                || fence.lineage != election.policy.lineage
                || boundary < fence.boundary)
        {
            return Ok(false);
        }
        let installed = self.configuration.read().await;
        if installed.as_ref().is_some_and(|current| {
            Membership::new(current, &durable.identity.replica) != recovery.membership
        }) {
            return Ok(false);
        }
        let running = self.instance.is_running().await;
        if running && (installed.is_none() || intent.process_generation != generation) {
            return Ok(false);
        }
        // Cold open may inspect the old journal, but cannot reuse its session to
        // activate. Live retries additionally own the exact promoting generation.
        if running {
            self.recovery_checkpoint_sql().await?;
        }
        let lineage = self.lineage().await?;
        let compatible = election.policy.lineage.can_rewind_from(&lineage)
            && lineage.history.last().is_some_and(|fork| {
                fork.timeline == election.policy.lineage.timeline && fork.end_lsn >= boundary
            });
        if !self.instance.generation_is_current(&lease) {
            return Err(RuntimeError::OperationCancelled);
        }
        if compatible && !running && reconcile_stopped {
            let mut evidence = election
                .final_observations
                .iter()
                .find(|observation| &observation.peer == local)
                .ok_or(RuntimeError::AuthorityNotAdmitted)?
                .evidence
                .clone();
            evidence.timeline_id = lineage.timeline;
            evidence.in_recovery = false;
            let digest =
                crate::native::timeline_history_digest(self.instance.data_dir(), &evidence)
                    .await
                    .map_err(application_error)?;
            self.pg_result(
                self.instance
                    .generation_step(&lease, async {
                        self.durable
                            .update(|state| {
                                if state != durable {
                                    return Err(PgDurableError::Invalid(
                                        "interrupted promotion was superseded".into(),
                                    ));
                                }
                                state.role = PgDurableRole::Primary;
                                state.timeline_id = Some(lineage.timeline);
                                state.timeline_history_digest = Some(digest);
                                state
                                    .recovery
                                    .as_mut()
                                    .unwrap()
                                    .pending
                                    .as_mut()
                                    .unwrap()
                                    .promoted = true;
                                Ok(())
                            })
                            .await
                            .map_err(|error| crate::instance::PgError::Process(error.to_string()))
                    })
                    .await,
            )
            .await?;
        }
        Ok(compatible)
    }

    pub(super) async fn recovery_progress(&self) -> Result<Option<i64>> {
        let _state = self.state.lock().await;
        self.recovery_progress_inner().await
    }

    pub(super) async fn recovery_progress_inner(&self) -> Result<Option<i64>> {
        let durable = self.durable.revalidate().await.map_err(application_error)?;
        let Some(recovery) = durable.recovery else {
            return Ok(None);
        };
        let Some(election) = recovery.pending.filter(|e| !e.ready) else {
            return Ok(None);
        };
        let local = recovery.membership.peer(&durable.identity.replica)?;
        let observed = self.recovery_observation(local).await?;
        let compatible = if observed.evidence.in_recovery {
            observed.lineage == election.policy.lineage
        } else {
            election
                .boundary
                .is_some_and(|boundary| observed.evidence.flush_lsn >= boundary)
                && election.policy.lineage.can_rewind_from(&observed.lineage)
        };
        if !compatible {
            return Err(self
                .permanent("interrupted PostgreSQL recovery has incompatible lineage")
                .await);
        }
        Ok(Some(
            observed
                .evidence
                .received_lsn
                .unwrap_or(observed.evidence.flush_lsn),
        ))
    }

    #[cfg(feature = "testing")]
    pub fn pause_recovery(&self, stage: RecoveryStage) -> std::sync::Arc<RecoveryGate> {
        let gate = std::sync::Arc::new(RecoveryGate {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        *self.recovery_gate.lock().unwrap() = Some((stage, gate.clone()));
        gate
    }

    async fn recovery_checkpoint(&self, _stage: RecoveryStage) {
        #[cfg(feature = "testing")]
        {
            let gate = {
                let mut gate = self.recovery_gate.lock().unwrap();
                if gate.as_ref().is_some_and(|(stage, _)| *stage == _stage) {
                    gate.take().map(|(_, gate)| gate)
                } else {
                    None
                }
            };
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
        }
    }

    pub(super) async fn finish_recovery(&self, result: Result<()>) -> Result<()> {
        match result {
            Ok(()) => Ok(()),
            Err(
                error @ (RuntimeError::OperationCancelled
                | RuntimeError::AuthorityNotAdmitted
                | RuntimeError::ReconfigurationPending),
            ) => Err(error),
            Err(error @ RuntimeError::AuthorityMismatch(_)) => Err(self.permanent(error).await),
            Err(error) => Err(self
                .report(
                    kuberic_runtime::protocol::types::FaultType::Transient,
                    error,
                )
                .await),
        }
    }

    pub(super) async fn recovery_incomplete(&self) -> bool {
        self.durable
            .snapshot()
            .await
            .recovery
            .and_then(|r| r.pending)
            .is_some_and(|e| !e.ready)
    }

    pub(super) async fn recovered_policy(
        &self,
        mut policy: AcknowledgementPolicy,
    ) -> Result<AcknowledgementPolicy> {
        if let Some(election) = self
            .durable
            .snapshot()
            .await
            .recovery
            .and_then(|r| r.pending)
            && election.promoted
            && election.configuration.configuration_id == policy.configuration_id
        {
            policy.eligible_standbys.retain(|peer| {
                election
                    .responders
                    .iter()
                    .any(|p| p.identity == peer.identity && p.session == peer.process_session_id)
            });
        }
        policy.validate().map_err(application_error)?;
        Ok(policy)
    }

    pub(super) async fn install_recovery_configuration(
        &self,
        current: &ReplicaSetConfiguration,
        previous: Option<ConfigurationDescriptor>,
    ) -> Result<()> {
        let local = self.durable.snapshot().await.identity.replica;
        let membership = Membership::new(current, &local);
        // Incomplete discovery is retained but can never be used as a recovery witness.
        if !membership.peers.iter().any(|p| p.identity == local) {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        self.durable
            .update(|state| {
                let old = state.recovery.take();
                let receiver_epoch = old.as_ref().and_then(|r| r.receiver_epoch);
                let followed = old.as_ref().and_then(|r| r.followed.clone());
                let preparation = old.as_ref().and_then(|r| r.preparation.clone());
                let connection = old.as_ref().and_then(|r| r.connection.clone());
                state.recovery = Some(Recovery {
                    membership,
                    previous,
                    accepted_policy: old.as_ref().and_then(|r| r.accepted_policy.clone()),
                    pending: old.as_ref().and_then(|r| r.pending.clone()),
                    source_fence: old.as_ref().and_then(|r| r.source_fence.clone()),
                    former_primary: old.is_some_and(|r| r.former_primary),
                    receiver_epoch,
                    followed,
                    preparation,
                    connection,
                });
                Ok(())
            })
            .await
            .map_err(application_error)?;
        Ok(())
    }

    async fn stopped_policy_ack(&self, receiver: &Peer) -> Result<Observation> {
        let state = self.durable.revalidate().await.map_err(application_error)?;
        let lineage = self.lineage().await?;
        Ok(Observation {
            peer: receiver.clone(),
            lineage: lineage.clone(),
            evidence: PgReplicationEvidence {
                engine: "postgres-physical".into(),
                system_identifier: lineage.system_identifier,
                timeline_id: lineage.timeline,
                in_recovery: state.role == PgDurableRole::Standby,
                flush_lsn: state.flush_lsn,
                received_lsn: state.received_lsn,
                replay_lsn: state.replay_lsn,
                metadata_generation: state.generation,
                synchronous: state.synchronous.clone(),
                wal_receiver_stopped: state.role == PgDurableRole::Standby,
            },
            source_fence: state.recovery.as_ref().and_then(|r| r.source_fence.clone()),
            policy: state.recovery.and_then(|r| r.accepted_policy),
            host: self.instance.listen_host().into(),
            port: self.instance.port(),
            running: false,
        })
    }

    async fn recovery_rpc(
        &self,
        peer: &Peer,
        action: Action,
        membership: &Membership,
    ) -> Result<Observation> {
        tracing::debug!(replica = ?peer.identity, ?action, "PostgreSQL recovery coordination");
        let deadline = match &action {
            Action::Follow { .. } => Duration::from_secs(25),
            Action::Drain => Duration::from_secs(15),
            _ => Duration::from_secs(5),
        };
        let local = self.durable.snapshot().await.identity;
        let request = Request {
            version: crate::build::BUILD_PROTOCOL_VERSION,
            resource: local.resource_uid,
            configuration: membership.configuration.clone(),
            sender: membership.peer(&local.replica)?.clone(),
            receiver: peer.clone(),
            action,
        };
        if self.peer_session(&local.replica).await? != request.sender.session {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        if peer.identity == local.replica {
            return tokio::time::timeout(deadline, self.recovery_request_inner(request))
                .await
                .map_err(|_| application_error("PostgreSQL local recovery deadline"))?;
        }
        tokio::time::timeout(deadline, async {
            let mut client = crate::proto::pg_data_service_client::PgDataServiceClient::connect(
                peer.endpoint.clone(),
            )
            .await
            .map_err(application_error)?;
            let mut wire = tonic::Request::new(crate::proto::RecoveryRequest {
                envelope_json: encode(&request).map_err(application_error)?,
            });
            wire.set_timeout(deadline);
            wire.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", self.coordination.bearer_token)
                    .parse()
                    .map_err(application_error)?,
            );
            let response = client
                .recover(wire)
                .await
                .map_err(|error| {
                    if error.code() == tonic::Code::FailedPrecondition {
                        tracing::warn!(%error, replica = ?peer.identity, "PostgreSQL peer rejected recovery admission");
                        RuntimeError::AuthorityNotAdmitted
                    } else {
                        application_error(error)
                    }
                })?
                .into_inner();
            let observation: Observation =
                decode(&response.envelope_json).map_err(application_error)?;
            if observation.peer != *peer {
                return Err(RuntimeError::AuthorityNotAdmitted);
            }
            observation.lineage.validate().map_err(application_error)?;
            observation.evidence.validate().map_err(application_error)?;
            Ok(observation)
        })
        .await
        .map_err(|_| application_error("PostgreSQL recovery peer deadline"))?
    }

    pub(crate) async fn recovery_request(&self, request: Request) -> Result<Observation> {
        if request.action == Action::Observe {
            // Read-only discovery must remain available while the primary waits
            // for the receiver whose fresh session is being reconstructed.
            let before = self.durable.revalidate().await.map_err(application_error)?;
            self.validate_recovery_request(&request, &before)?;
            let generation = self.instance.generation_id();
            let observation = self.recovery_observation(&request.receiver).await?;
            let after = self.durable.revalidate().await.map_err(application_error)?;
            self.validate_recovery_request(&request, &after)?;
            if generation != self.instance.generation_id()
                || observation.policy
                    != after
                        .recovery
                        .as_ref()
                        .and_then(|r| r.accepted_policy.clone())
                || observation.source_fence
                    != after.recovery.as_ref().and_then(|r| r.source_fence.clone())
            {
                return Err(RuntimeError::OperationCancelled);
            }
            return Ok(observation);
        }
        let _state = self.state.lock().await;
        let _publication = self.role_publication.write().await;
        let _access = self.instance.access_lock.lock().await;
        self.recovery_request_inner(request).await
    }

    async fn recovery_request_inner(&self, request: Request) -> Result<Observation> {
        let durable = self.durable.revalidate().await.map_err(application_error)?;
        self.validate_recovery_request(&request, &durable)?;
        let recovery = durable.recovery.ok_or(RuntimeError::AuthorityNotAdmitted)?;
        match request.action {
            Action::Policy(policy) => {
                policy.validate()?;
                if recovery.membership.primary()? != &request.sender
                    || policy.membership.configuration != recovery.membership.configuration
                    || policy.previous != recovery.previous
                    || recovery.accepted_policy.as_ref().is_some_and(|p| {
                        p.generation > policy.generation
                            && p.membership.configuration.epoch
                                >= policy.membership.configuration.epoch
                    })
                {
                    return Err(RuntimeError::AuthorityNotAdmitted);
                }
                if let Some(previous) = &recovery.accepted_policy
                    && previous.generation == policy.generation
                    && previous != policy.as_ref()
                    && (!previous.policy.valid
                        || policy.policy.valid
                        || previous.membership != policy.membership
                        || previous.lineage != policy.lineage
                        || previous.policy.eligible_standbys != policy.policy.eligible_standbys)
                {
                    return Err(RuntimeError::AuthorityNotAdmitted);
                }
                self.durable
                    .update(|state| {
                        state
                            .recovery
                            .as_mut()
                            .ok_or_else(|| {
                                PgDurableError::Invalid("missing recovery configuration".into())
                            })?
                            .accepted_policy = Some(*policy);
                        Ok(())
                    })
                    .await
                    .map_err(application_error)?;
                if !self.instance.is_running().await {
                    return self.stopped_policy_ack(&request.receiver).await;
                }
            }
            Action::Drain => {
                if recovery.membership.primary()? != &request.sender
                    || recovery.previous.is_none()
                    || recovery
                        .followed
                        .as_ref()
                        .is_some_and(|(epoch, _)| *epoch >= request.configuration.epoch)
                    || recovery.pending.as_ref().is_some_and(|e| e.promoted)
                {
                    return Err(RuntimeError::AuthorityNotAdmitted);
                }
                self.drain_receiver().await?;
            }
            Action::Observe => {}
            Action::Fence(fence) => {
                if recovery.membership.primary()? != &request.sender
                    || fence.source != request.sender
                    || !fence.stopped
                    || fence.configuration.epoch <= request.configuration.epoch
                    || fence.boundary < 0
                    || recovery
                        .source_fence
                        .as_ref()
                        .is_some_and(|old| old.configuration.epoch > fence.configuration.epoch)
                {
                    return Err(RuntimeError::AuthorityNotAdmitted);
                }
                fence.lineage.validate().map_err(application_error)?;
                self.durable
                    .update(|state| {
                        state.recovery.as_mut().unwrap().source_fence = Some(*fence);
                        Ok(())
                    })
                    .await
                    .map_err(application_error)?;
            }
            Action::Follow {
                lineage,
                host,
                port,
                boundary,
            } => {
                if recovery.membership.primary()? != &request.sender
                    || recovery.former_primary
                    || durable.role != PgDurableRole::Standby
                    || durable.recovery_state != crate::durable::PgRecoveryState::Ready
                    || durable
                        .native_build
                        .as_ref()
                        .is_some_and(|build| build.stage != crate::build::PgBuildStage::Complete)
                {
                    return Err(RuntimeError::AuthorityNotAdmitted);
                }
                if !self
                    .recovery_observation(&request.receiver)
                    .await?
                    .evidence
                    .in_recovery
                {
                    return Err(RuntimeError::AuthorityNotAdmitted);
                }
                if recovery.followed.as_ref()
                    == Some(&(request.configuration.epoch, lineage.clone()))
                    && self.instance.is_running().await
                {
                    let observation = self.recovery_observation(&request.receiver).await?;
                    if observation.lineage == lineage
                        && observation
                            .evidence
                            .replay_lsn
                            .is_some_and(|p| p >= boundary)
                    {
                        return Ok(observation);
                    }
                }
                let local = self.lineage().await?;
                if !local.can_rewind_from(&lineage) {
                    return Err(application_error("unsafe PostgreSQL follow lineage"));
                }
                self.instance.stop().await.map_err(application_error)?;
                self.instance
                    .config()
                    .configure_standby(
                        self.instance.data_dir(),
                        &host,
                        port,
                        &crate::native::replication_application_name(
                            &request.receiver.identity,
                            &request.receiver.session,
                        ),
                        &crate::native::replication_slot_name(&request.receiver.identity),
                        &lineage,
                    )
                    .await
                    .map_err(application_error)?;
                self.pg_result(
                    self.instance
                        .start_native_with_cancellation(
                            self.fault_tx.clone(),
                            self.cancellation.clone(),
                        )
                        .await,
                )
                .await?;
                let evidence = tokio::time::timeout(Duration::from_secs(15), async {
                    loop {
                        let evidence = self
                            .observer
                            .snapshot()
                            .await
                            .map_err(application_error)?
                            .evidence
                            .ok_or(RuntimeError::ReconfigurationPending)?;
                        if evidence.system_identifier != lineage.system_identifier {
                            return Err(RuntimeError::AuthorityNotAdmitted);
                        }
                        if evidence.timeline_id == lineage.timeline
                            && evidence.replay_lsn.is_some_and(|p| p >= boundary)
                        {
                            break Ok(evidence);
                        }
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                })
                .await
                .map_err(|_| application_error("PostgreSQL follow replay deadline"))??;
                let (client, connection) =
                    self.instance.connect().await.map_err(application_error)?;
                tokio::time::timeout(Duration::from_secs(5), client.simple_query("CHECKPOINT"))
                    .await
                    .map_err(|_| application_error("PostgreSQL follow checkpoint deadline"))?
                    .map_err(application_error)?;
                drop(client);
                connection.await.map_err(application_error)?;
                let digest =
                    crate::native::timeline_history_digest(self.instance.data_dir(), &evidence)
                        .await
                        .map_err(application_error)?;
                self.durable
                    .update(|state| {
                        state.native_build = None;
                        state.accepted_build = None;
                        state.system_identifier = Some(evidence.system_identifier);
                        state.timeline_id = Some(evidence.timeline_id);
                        state.timeline_history_digest = Some(digest);
                        state.role = PgDurableRole::Standby;
                        state.postgres_stopped = false;
                        let recovery = state.recovery.as_mut().unwrap();
                        recovery.connection = Some(Connection {
                            source: request.sender.clone(),
                            receiver: request.receiver.session.clone(),
                            host,
                            port,
                            lineage: lineage.clone(),
                        });
                        recovery.followed = Some((request.configuration.epoch, lineage));
                        recovery.pending = None;
                        recovery.source_fence = None;
                        recovery.receiver_epoch = None;
                        recovery.preparation = None;
                        Ok(())
                    })
                    .await
                    .map_err(application_error)?;
            }
        }
        self.recovery_observation(&request.receiver).await
    }

    async fn recovery_observation(&self, peer: &Peer) -> Result<Observation> {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.recovery_observation_inner(peer),
        )
        .await
        .map_err(|_| application_error("PostgreSQL observation deadline"))?
    }

    async fn recovery_observation_inner(&self, peer: &Peer) -> Result<Observation> {
        let durable = self.durable.snapshot().await;
        let fence = durable
            .recovery
            .as_ref()
            .and_then(|r| r.source_fence.clone());
        if !self.instance.is_running().await {
            let source = fence
                .clone()
                .filter(|f| f.stopped)
                .ok_or(RuntimeError::ReconfigurationPending)?;
            return Ok(Observation {
                host: self.instance.listen_host().into(),
                port: self.instance.port(),
                running: false,
                peer: peer.clone(),
                lineage: source.lineage.clone(),
                source_fence: fence,
                policy: durable
                    .recovery
                    .as_ref()
                    .and_then(|r| r.accepted_policy.clone()),
                evidence: PgReplicationEvidence {
                    engine: "postgres-physical".into(),
                    system_identifier: source.lineage.system_identifier,
                    timeline_id: source.lineage.timeline,
                    in_recovery: false,
                    flush_lsn: source.boundary,
                    received_lsn: None,
                    replay_lsn: None,
                    metadata_generation: durable.generation,
                    synchronous: None,
                    wal_receiver_stopped: false,
                },
            });
        }
        let snapshot = self.pg_result(self.observer.snapshot().await).await?;
        let mut evidence = snapshot
            .evidence
            .ok_or(RuntimeError::ReconfigurationPending)?;
        if evidence.in_recovery
            && durable.system_identifier.as_ref() == Some(&evidence.system_identifier)
            && durable.timeline_id == Some(evidence.timeline_id)
        {
            evidence.received_lsn = evidence
                .received_lsn
                .into_iter()
                .chain(durable.received_lsn)
                .max();
            evidence.flush_lsn = evidence
                .flush_lsn
                .max(evidence.received_lsn.unwrap_or_default());
        }
        let lineage = self
            .pg_result(
                self.instance
                    .generation_operation(PgLineage::read(
                        self.instance.data_dir(),
                        evidence.system_identifier.clone(),
                        evidence.timeline_id,
                    ))
                    .await,
            )
            .await?;
        Ok(Observation {
            host: self.instance.listen_host().into(),
            port: self.instance.port(),
            running: true,
            peer: peer.clone(),
            lineage,
            evidence,
            source_fence: fence,
            policy: durable.recovery.and_then(|r| r.accepted_policy),
        })
    }

    fn validate_recovery_request(
        &self,
        request: &Request,
        durable: &crate::durable::PgDurableState,
    ) -> Result<()> {
        let recovery = durable
            .recovery
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if self.cancellation.is_cancelled()
            || request.version != crate::build::BUILD_PROTOCOL_VERSION
            || request.resource != durable.identity.resource_uid
            || request.configuration != recovery.membership.configuration
            || recovery.membership.peer(&durable.identity.replica)? != &request.receiver
            || recovery.membership.peer(&request.sender.identity)? != &request.sender
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        Ok(())
    }

    pub(super) async fn reconnect_receiver(&self) -> Result<bool> {
        let _access = self.instance.access_lock.lock().await;
        let durable = self.durable.revalidate().await.map_err(application_error)?;
        let recovery = durable
            .recovery
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if recovery.former_primary
            || durable.role != PgDurableRole::Standby
            || recovery.pending.as_ref().is_some_and(|e| !e.ready)
        {
            return Ok(false);
        }
        let source = match recovery.membership.primary() {
            Ok(source) => source,
            Err(RuntimeError::AuthorityNotAdmitted) => return Ok(false),
            Err(error) => return Err(error),
        };
        if source.identity == durable.identity.replica {
            return Ok(false);
        }
        let receiver = recovery.membership.peer(&durable.identity.replica)?;
        if recovery
            .connection
            .as_ref()
            .is_some_and(|connection| connection.source.identity != source.identity)
            || (recovery.connection.is_none()
                && durable
                    .native_build
                    .as_ref()
                    .is_some_and(|build| build.request.authority.source != source.identity))
        {
            return Ok(false);
        }
        if self.instance.is_running().await
            && recovery.connection.is_none()
            && durable.native_build.as_ref().is_some_and(|build| {
                build.stage == crate::build::PgBuildStage::Complete
                    && build.request.source_session == source.session
                    && build.request.target_session == receiver.session
            })
        {
            return Ok(true);
        }
        if self.instance.is_running().await
            && recovery
                .connection
                .as_ref()
                .is_some_and(|c| c.source == *source && c.receiver == receiver.session)
        {
            return Ok(true);
        }
        let cached = recovery
            .connection
            .as_ref()
            .filter(|connection| connection.source == *source)
            .cloned()
            .or_else(|| {
                let build = durable.native_build.as_ref().filter(|build| {
                    build.stage == crate::build::PgBuildStage::Complete
                        && build.request.authority.source == source.identity
                        && build.request.source_session == source.session
                        && build.request.source_endpoint == source.endpoint
                })?;
                Some(Connection {
                    source: source.clone(),
                    receiver: receiver.session.clone(),
                    host: build.request.source_host.clone(),
                    port: build.request.source_port,
                    lineage: build.request.lineage.clone(),
                })
            });
        let mut connection = if let Some(cached) = cached {
            cached
        } else {
            let observed = match self
                .recovery_rpc(source, Action::Observe, &recovery.membership)
                .await
            {
                Ok(observed) => observed,
                Err(
                    error @ (RuntimeError::AuthorityNotAdmitted
                    | RuntimeError::ReconfigurationPending),
                ) => {
                    tracing::warn!(%error, "receiver reconnect awaits exact source observation");
                    return Ok(false);
                }
                Err(RuntimeError::OperationCancelled) => {
                    return Err(RuntimeError::OperationCancelled);
                }
                Err(error) => {
                    return Err(self
                        .report(
                            kuberic_runtime::protocol::types::FaultType::Transient,
                            error,
                        )
                        .await);
                }
            };
            if !observed.running || observed.evidence.in_recovery {
                return Ok(false);
            }
            if observed.host.parse::<std::net::IpAddr>().is_err() || observed.port == 0 {
                tracing::warn!("receiver reconnect source omitted a valid SQL endpoint");
                return Err(RuntimeError::AuthorityNotAdmitted);
            }
            Connection {
                source: source.clone(),
                receiver: receiver.session.clone(),
                host: observed.host,
                port: observed.port,
                lineage: observed.lineage,
            }
        };
        connection.receiver = receiver.session.clone();
        let local_lineage = self.lineage().await?;
        if local_lineage != connection.lineage {
            return Err(self
                .permanent("receiver restart requires matching admitted lineage or a new build")
                .await);
        }
        self.pg_result(self.instance.stop().await).await?;
        self.pg_result(
            self.instance
                .config()
                .configure_standby(
                    self.instance.data_dir(),
                    &connection.host,
                    connection.port,
                    &crate::native::replication_application_name(
                        &durable.identity.replica,
                        &connection.receiver,
                    ),
                    &crate::native::replication_slot_name(&durable.identity.replica),
                    &connection.lineage,
                )
                .await,
        )
        .await?;
        self.pg_result(
            self.instance
                .start_native_with_cancellation(self.fault_tx.clone(), self.cancellation.clone())
                .await,
        )
        .await?;
        self.durable
            .update(|state| {
                state.recovery.as_mut().unwrap().connection = Some(connection);
                state.postgres_stopped = false;
                state.external_access_closed = true;
                Ok(())
            })
            .await
            .map_err(application_error)?;
        self.observe_pg().await?;
        Ok(true)
    }

    async fn drain_receiver(&self) -> Result<()> {
        let epoch = self
            .durable
            .snapshot()
            .await
            .recovery
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?
            .membership
            .configuration
            .epoch;
        self.durable
            .update(|state| {
                let recovery = state.recovery.as_mut().ok_or_else(|| {
                    PgDurableError::Invalid("missing receiver recovery intent".into())
                })?;
                if recovery.former_primary {
                    return Err(PgDurableError::Invalid(
                        "former primary cannot become a receiver without rebuild".into(),
                    ));
                }
                state.external_access_closed = true;
                recovery.receiver_epoch = Some(epoch);
                Ok(())
            })
            .await
            .map_err(application_error)?;
        self.instance
            .config()
            .disconnect_receiver(self.instance.data_dir())
            .await
            .map_err(application_error)?;
        let (client, connection) = self.instance.connect().await.map_err(application_error)?;
        client
            .query_one("SELECT pg_reload_conf()", &[])
            .await
            .map_err(application_error)?;
        // The startup process handles connection-setting changes only after it
        // leaves a replay pause. Resume before waiting for the receiver to exit;
        // any already accepted transaction still has an unknown client outcome.
        client
            .query_one("SELECT pg_wal_replay_resume()", &[])
            .await
            .map_err(application_error)?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let count: i64 = client
                    .query_one("SELECT count(*) FROM pg_stat_wal_receiver", &[])
                    .await
                    .map_err(application_error)?
                    .get(0);
                if count == 0 {
                    return Ok::<_, RuntimeError>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| application_error("PostgreSQL WAL receiver drain deadline"))??;
        let evidence = self
            .observer
            .snapshot()
            .await
            .map_err(application_error)?
            .evidence
            .ok_or(RuntimeError::ReconfigurationPending)?;
        self.durable
            .update(|state| {
                state.received_lsn = state
                    .received_lsn
                    .into_iter()
                    .chain(evidence.received_lsn)
                    .max();
                state.replay_lsn = state
                    .replay_lsn
                    .into_iter()
                    .chain(evidence.replay_lsn)
                    .max();
                state.current_lsn = state
                    .current_lsn
                    .max(evidence.received_lsn.unwrap_or_default());
                state.flush_lsn = state.flush_lsn.max(evidence.flush_lsn);
                Ok(())
            })
            .await
            .map_err(application_error)?;
        drop(client);
        connection.await.map_err(application_error)?;
        self.pg_result(self.instance.stop().await).await?;
        self.pg_result(
            self.instance
                .start_native_with_cancellation(self.fault_tx.clone(), self.cancellation.clone())
                .await,
        )
        .await?;
        let evidence = self
            .observer
            .snapshot()
            .await
            .map_err(application_error)?
            .evidence
            .ok_or(RuntimeError::ReconfigurationPending)?;
        if !evidence.in_recovery || !evidence.wal_receiver_stopped {
            return Err(application_error("PostgreSQL receiver-down proof failed"));
        }
        Ok(())
    }

    pub(super) async fn publish_recovery_policy(&self) -> Result<()> {
        let _access = self.instance.access_lock.lock().await;
        self.publish_recovery_policy_inner().await
    }

    async fn publish_recovery_policy_inner(&self) -> Result<()> {
        let durable = self.durable.revalidate().await.map_err(application_error)?;
        let recovery = durable.recovery.ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let policy = durable
            .synchronous
            .clone()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let mut membership = recovery.membership.clone();
        let sessions = self.policy_sessions(&membership.configuration).await?;
        for peer in &mut membership.peers {
            if peer.session.is_empty()
                && let Some(session) = sessions.get(&peer.identity)
            {
                peer.session = session.clone();
            }
        }
        let mut record = Policy {
            membership,
            previous: recovery.previous,
            generation: durable.synchronous_generation,
            policy,
            lineage: self.lineage().await?,
        };
        record.validate()?;
        if recovery.accepted_policy.as_ref() == Some(&record) {
            return Ok(());
        }
        let mut invalid = record.clone();
        invalid.policy.valid = false;
        invalid.policy.write_acknowledgements = 0;
        for standby in &record.policy.eligible_standbys {
            let peer = record.membership.peer(&standby.identity)?;
            self.recovery_rpc(
                peer,
                Action::Policy(Box::new(invalid.clone())),
                &record.membership,
            )
            .await?;
        }
        self.durable
            .update(|state| {
                state.recovery.as_mut().unwrap().accepted_policy = Some(invalid);
                Ok(())
            })
            .await
            .map_err(application_error)?;
        self.recovery_checkpoint(RecoveryStage::PolicyInvalidated)
            .await;
        self.observer
            .apply_synchronous(record.policy.clone())
            .await
            .map_err(application_error)?;
        self.recovery_checkpoint(RecoveryStage::PolicyApplied).await;
        record.generation = self.durable.snapshot().await.synchronous_generation;
        for standby in &record.policy.eligible_standbys {
            let peer = record.membership.peer(&standby.identity)?;
            self.recovery_rpc(
                peer,
                Action::Policy(Box::new(record.clone())),
                &record.membership,
            )
            .await?;
        }
        self.durable
            .update(|state| {
                state
                    .recovery
                    .as_mut()
                    .ok_or_else(|| PgDurableError::Invalid("missing policy configuration".into()))?
                    .accepted_policy = Some(record);
                Ok(())
            })
            .await
            .map_err(application_error)?;
        self.recovery_checkpoint(RecoveryStage::PolicyAccepted)
            .await;
        Ok(())
    }

    pub(super) async fn demote_postgres(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(30), self.demote_postgres_inner())
            .await
            .map_err(|_| application_error("PostgreSQL source fence deadline"))?
    }

    async fn demote_postgres_inner(&self) -> Result<()> {
        let durable = self.durable.revalidate().await.map_err(application_error)?;
        let recovery = durable
            .recovery
            .clone()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if let Some(mut fence) = recovery
            .source_fence
            .clone()
            .filter(|f| f.configuration == recovery.membership.configuration)
        {
            self.pg_result(self.instance.stop().await).await?;
            fence.stopped = true;
            self.durable
                .update(|state| {
                    state.recovery.as_mut().unwrap().source_fence = Some(fence.clone());
                    state.postgres_stopped = true;
                    state.external_access_closed = true;
                    Ok(())
                })
                .await
                .map_err(application_error)?;
            return self.distribute_source_fence(&recovery, fence).await;
        }
        if !self.instance.is_running().await {
            self.pg_result(self.instance.stop().await).await?;
            return Err(self
                .permanent("source stopped before durable PostgreSQL handoff capture")
                .await);
        }
        self.close_access().await?;
        let _access = self.instance.access_lock.lock().await;
        self.recovery_checkpoint_sql().await?;
        let snapshot = self.observer.snapshot().await.map_err(application_error)?;
        let evidence = snapshot
            .evidence
            .ok_or(RuntimeError::ReconfigurationPending)?;
        let fence = SourceFence {
            configuration: recovery.membership.configuration.clone(),
            source: recovery.membership.peer(&durable.identity.replica)?.clone(),
            lineage: self.lineage().await?,
            boundary: recovery
                .preparation
                .as_ref()
                .filter(|(configuration, _)| {
                    recovery.accepted_policy.as_ref().is_some_and(|policy| {
                        &policy.membership.configuration.configuration_id == configuration
                    })
                })
                .map_or(evidence.flush_lsn, |(_, boundary)| *boundary),
            stopped: false,
        };
        self.durable
            .update(|state| {
                let recovery = state
                    .recovery
                    .as_mut()
                    .ok_or_else(|| PgDurableError::Invalid("missing demotion intent".into()))?;
                recovery.source_fence = Some(fence.clone());
                recovery.former_primary = true;
                state.external_access_closed = true;
                Ok(())
            })
            .await
            .map_err(application_error)?;
        self.recovery_checkpoint(RecoveryStage::SourceFenceIntent)
            .await;
        self.pg_result(self.instance.stop().await).await?;
        self.durable
            .update(|state| {
                state
                    .recovery
                    .as_mut()
                    .unwrap()
                    .source_fence
                    .as_mut()
                    .unwrap()
                    .stopped = true;
                state.postgres_stopped = true;
                Ok(())
            })
            .await
            .map_err(application_error)?;
        self.recovery_checkpoint(RecoveryStage::SourceStopped).await;
        let mut receipt = fence;
        receipt.stopped = true;
        self.distribute_source_fence(&recovery, receipt).await
    }

    async fn distribute_source_fence(
        &self,
        recovery: &Recovery,
        receipt: SourceFence,
    ) -> Result<()> {
        let local = self.durable.snapshot().await.identity.replica;
        if let Some(policy) = &recovery.accepted_policy {
            if recovery
                .preparation
                .as_ref()
                .is_none_or(|(configuration, _)| {
                    configuration != &policy.membership.configuration.configuration_id
                })
            {
                return Ok(());
            }
            for peer in &policy.membership.peers {
                if peer.identity == local {
                    continue;
                }
                self.recovery_rpc(
                    peer,
                    Action::Fence(Box::new(receipt.clone())),
                    &policy.membership,
                )
                .await?;
            }
        }
        Ok(())
    }

    pub(super) async fn record_handoff_preparation(
        &self,
        configuration: kuberic_runtime::protocol::types::ConfigurationId,
        boundary: i64,
    ) -> Result<()> {
        self.durable
            .update(|state| {
                let recovery = state.recovery.as_mut().ok_or_else(|| {
                    PgDurableError::Invalid("missing handoff configuration".into())
                })?;
                if recovery.membership.configuration.configuration_id != configuration
                    || !state.external_access_closed
                {
                    return Err(PgDurableError::Invalid(
                        "handoff preparation was superseded".into(),
                    ));
                }
                recovery.preparation = Some((configuration, boundary));
                Ok(())
            })
            .await
            .map_err(application_error)?;
        Ok(())
    }

    pub(super) async fn recover_primary(&self) -> Result<()> {
        let _access = self.instance.access_lock.lock().await;
        let durable = self.durable.revalidate().await.map_err(application_error)?;
        let recovery = durable.recovery.ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if recovery.former_primary {
            return Err(application_error(
                "former PostgreSQL primary requires authorized rewind/rebuild",
            ));
        }
        if !self.instance.is_running().await {
            if recovery.receiver_epoch.is_some() {
                self.instance
                    .config()
                    .disconnect_receiver(self.instance.data_dir())
                    .await
                    .map_err(application_error)?;
            }
            self.pg_result(
                self.instance
                    .start_native_with_cancellation(
                        self.fault_tx.clone(),
                        self.cancellation.clone(),
                    )
                    .await,
            )
            .await?;
        }
        let retained = recovery
            .pending
            .as_ref()
            .filter(|e| e.configuration == recovery.membership.configuration);
        let policy = retained
            .map(|e| e.policy.clone())
            .or(recovery.accepted_policy.clone())
            .ok_or_else(|| application_error("missing accepted PostgreSQL synchronous policy"))?;
        if retained.is_none() {
            let previous = recovery
                .previous
                .as_ref()
                .ok_or(RuntimeError::AuthorityNotAdmitted)?;
            let accepted = &policy.membership.configuration;
            let same_members = |a: &ConfigurationDescriptor, b: &ConfigurationDescriptor| {
                a.members
                    .iter()
                    .map(|m| &m.identity)
                    .collect::<std::collections::BTreeSet<_>>()
                    == b.members
                        .iter()
                        .map(|m| &m.identity)
                        .collect::<std::collections::BTreeSet<_>>()
            };
            // A refused provisional candidate may be replaced at a newer epoch.
            // Only the unchanged exact membership and the same policy on every
            // final responder may carry its last accepted acknowledgement basis.
            // That policy can belong to an outstanding CC newer than the
            // controller's PC, but must predate this promotion epoch.
            if accepted.epoch >= recovery.membership.configuration.epoch
                || accepted.epoch.data_loss_number
                    != recovery.membership.configuration.epoch.data_loss_number
                || !same_members(accepted, previous)
                || !same_members(accepted, &recovery.membership.configuration)
            {
                return Err(application_error("stale PostgreSQL synchronous policy"));
            }
        }
        let local = recovery.membership.peer(&durable.identity.replica)?.clone();
        if policy.membership.peer(&local.identity)? != &local
            || !policy.policy.eligible_standbys.iter().any(|standby| {
                standby.identity == local.identity && standby.process_session_id == local.session
            })
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        let mut election = match recovery.pending {
            Some(election)
                if election.configuration == recovery.membership.configuration
                    && election.policy == policy =>
            {
                election
            }
            _ => {
                policy.quorum(policy.policy.eligible_standbys.len())?;
                let mut responders = Vec::new();
                for standby in &policy.policy.eligible_standbys {
                    let Ok(peer) = recovery.membership.peer(&standby.identity) else {
                        continue;
                    };
                    if peer.session != standby.process_session_id
                        || policy.membership.peer(&standby.identity)? != peer
                    {
                        continue;
                    }
                    let observation = self
                        .recovery_rpc(peer, Action::Observe, &recovery.membership)
                        .await;
                    let accepted = observation
                        .and_then(|observation| validate_observation(&policy, &observation, false));
                    match accepted {
                        Ok(()) => responders.push(peer.clone()),
                        Err(error) if peer == &local => return Err(error),
                        Err(error) => {
                            tracing::warn!(%error, replica = ?peer.identity, "recovery initial responder unavailable or incompatible")
                        }
                    }
                }
                policy.quorum(responders.len())?;
                let election = Election {
                    configuration: recovery.membership.configuration.clone(),
                    policy: policy.clone(),
                    responders,
                    final_observations: Vec::new(),
                    boundary: None,
                    promoted: false,
                    ready: false,
                    promotion: None,
                };
                self.persist_election(election.clone()).await?;
                self.recovery_checkpoint(RecoveryStage::InitialRound).await;
                election
            }
        };
        let already_primary = !self
            .recovery_observation(&local)
            .await?
            .evidence
            .in_recovery;
        if already_primary && election.boundary.is_none() {
            return Err(application_error(
                "promotion lacks durable election boundary",
            ));
        }
        for peer in &election.responders {
            if recovery.membership.peer(&peer.identity)? != peer {
                return Err(RuntimeError::AuthorityNotAdmitted);
            }
        }
        if election.boundary.is_none() {
            for peer in &election.responders {
                let observation = self
                    .recovery_rpc(peer, Action::Drain, &recovery.membership)
                    .await?;
                validate_observation(&policy, &observation, true)?;
            }
            self.recovery_checkpoint(RecoveryStage::ReceiversDrained)
                .await;
            let mut final_observations = Vec::new();
            for peer in &election.responders {
                let observation = self
                    .recovery_rpc(peer, Action::Observe, &recovery.membership)
                    .await?;
                validate_observation(&policy, &observation, true)?;
                final_observations.push(observation);
            }
            policy.quorum(final_observations.len())?;
            let safest = final_observations
                .iter()
                .max_by(|a, b| {
                    a.evidence
                        .received_lsn
                        .cmp(&b.evidence.received_lsn)
                        .then_with(|| a.evidence.replay_lsn.cmp(&b.evidence.replay_lsn))
                        .then_with(|| b.peer.identity.cmp(&a.peer.identity))
                })
                .ok_or(RuntimeError::ReconfigurationPending)?;
            if safest.peer != local && recovery.source_fence.is_none() {
                tracing::warn!(candidate = ?safest.peer.identity, selected = ?local.identity, "PostgreSQL requires a different safe candidate");
                return Err(RuntimeError::ReconfigurationPending);
            }
            election.boundary = if let Some(fence) = &recovery.source_fence {
                final_observations
                    .iter()
                    .find(|o| o.peer == local)
                    .and_then(|o| o.evidence.received_lsn)
                    .filter(|p| *p >= fence.boundary)
            } else {
                safest.evidence.received_lsn
            };
            election.final_observations = final_observations;
            self.persist_election(election.clone()).await?;
            self.recovery_checkpoint(RecoveryStage::FinalRound).await;
        }
        let boundary = election
            .boundary
            .ok_or(RuntimeError::ReconfigurationPending)?;
        if let Some(fence) = &recovery.source_fence
            && (fence.configuration != recovery.membership.configuration
                || !fence.stopped
                || fence.source != *policy.membership.primary()?
                || fence.lineage != policy.lineage
                || boundary < fence.boundary)
        {
            return Err(application_error(
                "stale or incomplete PostgreSQL source-fenced handoff",
            ));
        }
        if !already_primary {
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    let observation = self.recovery_observation(&local).await?;
                    validate_observation(&policy, &observation, true)?;
                    if observation
                        .evidence
                        .replay_lsn
                        .is_some_and(|replay| replay >= boundary)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Ok::<_, RuntimeError>(())
            })
            .await
            .map_err(|_| application_error("PostgreSQL candidate replay deadline"))??;
            let lease = self.instance.generation_lease();
            election.promotion = Some(Promotion {
                candidate: local.clone(),
                process_generation: lease.id(),
            });
            self.persist_election(election.clone()).await?;
            self.pg_result(
                self.instance
                    .generation_step(&lease, self.instance.promote())
                    .await,
            )
            .await?;
            self.recovery_checkpoint(RecoveryStage::Promoted).await;
        }
        self.recovery_checkpoint_sql().await?;
        let observation = self.recovery_observation(&local).await?;
        if observation.evidence.in_recovery
            || !policy.lineage.can_rewind_from(&observation.lineage)
            || observation.evidence.flush_lsn < boundary
        {
            return Err(application_error(
                "PostgreSQL post-promotion lineage/replay verification failed",
            ));
        }
        election.promoted = true;
        let digest =
            crate::native::timeline_history_digest(self.instance.data_dir(), &observation.evidence)
                .await
                .map_err(application_error)?;
        self.durable
            .update(|state| {
                state.native_build = None;
                state.accepted_build = None;
                state.system_identifier = Some(observation.lineage.system_identifier.clone());
                state.timeline_id = Some(observation.lineage.timeline);
                state.timeline_history_digest = Some(digest);
                state.role = PgDurableRole::Primary;
                state.recovery.as_mut().unwrap().pending = Some(election.clone());
                Ok(())
            })
            .await
            .map_err(application_error)?;
        let current = self
            .configuration
            .read()
            .await
            .clone()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        self.observer
            .reconcile_replication_slots(&durable.identity.replica, &current)
            .await
            .map_err(application_error)?;
        for peer in &election.responders {
            if peer == &local {
                continue;
            }
            self.recovery_rpc(
                peer,
                Action::Follow {
                    lineage: observation.lineage.clone(),
                    host: self.instance.listen_host().into(),
                    port: self.instance.port(),
                    boundary: observation.evidence.flush_lsn,
                },
                &recovery.membership,
            )
            .await?;
        }
        let sessions = self.policy_sessions(&current.configuration).await?;
        let policy = crate::native::compile_synchronous_configuration(
            recovery.previous.as_ref(),
            &current.configuration,
            &durable.identity.replica,
            &sessions,
            true,
        )
        .map_err(application_error)?;
        let policy = self.recovered_policy(policy).await?;
        self.observer
            .apply_synchronous(policy)
            .await
            .map_err(application_error)?;
        self.publish_recovery_policy_inner().await?;
        election.ready = true;
        self.persist_election(election).await?;
        self.recovery_checkpoint(RecoveryStage::Ready).await;
        Ok(())
    }

    async fn persist_election(&self, election: Election) -> Result<()> {
        self.durable
            .update(|state| {
                state
                    .recovery
                    .as_mut()
                    .ok_or_else(|| {
                        PgDurableError::Invalid("missing election configuration".into())
                    })?
                    .pending = Some(election);
                Ok(())
            })
            .await
            .map_err(application_error)?;
        Ok(())
    }

    pub(super) async fn recovery_checkpoint_sql(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (client, connection) = self.instance.connect().await.map_err(application_error)?;
            client
                .simple_query("CHECKPOINT")
                .await
                .map_err(application_error)?;
            drop(client);
            connection.await.map_err(application_error)?;
            Ok(())
        })
        .await
        .map_err(|_| application_error("PostgreSQL checkpoint deadline"))?
    }
}

fn validate_observation(policy: &Policy, observation: &Observation, drained: bool) -> Result<()> {
    observation.lineage.validate().map_err(application_error)?;
    observation.evidence.validate().map_err(application_error)?;
    if !observation.running
        || !observation.evidence.in_recovery
        || observation.policy.as_ref() != Some(policy)
        || policy.membership.peer(&observation.peer.identity)? != &observation.peer
        || observation.lineage != policy.lineage
        || observation.evidence.system_identifier != observation.lineage.system_identifier
        || observation.evidence.timeline_id != observation.lineage.timeline
        || observation
            .evidence
            .received_lsn
            .is_some_and(|received| received > observation.evidence.flush_lsn)
        || observation.evidence.received_lsn.is_none()
        || observation.evidence.replay_lsn.is_none()
        || drained && !observation.evidence.wal_receiver_stopped
    {
        return Err(RuntimeError::AuthorityMismatch(
            "incompatible PostgreSQL recovery observation".into(),
        ));
    }

    Ok(())
}

#[cfg(all(test, feature = "testing"))]
#[path = "recovery_subset_tests.rs"]
mod subset_tests;

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use crate::native::compile_synchronous_configuration;
    use crate::testing::{native_configuration, native_identity};

    fn policy() -> Policy {
        let identities = (1..=3)
            .map(|id| native_identity(id, &format!("peer-{id}")))
            .collect::<Vec<_>>();
        let configuration = native_configuration(&identities, 0, 2);
        let peers = identities
            .iter()
            .map(|identity| Peer {
                identity: identity.clone(),
                session: ProcessSessionId::new(format!("session-{}", identity.replica_id.value())),
                endpoint: format!("http://127.0.0.1:{}", 10000 + identity.replica_id.value()),
            })
            .collect::<Vec<_>>();
        let sessions = peers
            .iter()
            .map(|peer| (peer.identity.clone(), peer.session.clone()))
            .collect();
        Policy {
            policy: compile_synchronous_configuration(
                None,
                &configuration,
                &identities[0],
                &sessions,
                true,
            )
            .unwrap(),
            membership: Membership {
                configuration,
                peers,
            },
            previous: None,
            generation: 1,
            lineage: PgLineage {
                system_identifier: "123".into(),
                timeline: 1,
                history: Vec::new(),
                history_text: String::new(),
            },
        }
    }

    fn observation(policy: &Policy) -> Observation {
        Observation {
            host: "127.0.0.1".into(),
            port: 5432,
            running: true,
            peer: policy.membership.peers[1].clone(),
            lineage: policy.lineage.clone(),
            source_fence: None,
            policy: Some(policy.clone()),
            evidence: PgReplicationEvidence {
                engine: "postgres-physical".into(),
                system_identifier: "123".into(),
                timeline_id: 1,
                in_recovery: true,
                flush_lsn: 100,
                received_lsn: Some(100),
                replay_lsn: Some(50),
                metadata_generation: 1,
                synchronous: None,
                wal_receiver_stopped: true,
            },
        }
    }

    #[test]
    fn exact_policy_quorum_requires_strict_intersection_and_valid_configuration_generation() {
        let policy = policy();
        assert!(policy.quorum(1).is_err());
        policy.quorum(2).unwrap();
        for mutation in 0..5 {
            let mut bad = policy.clone();
            match mutation {
                0 => {
                    bad.policy.valid = false;
                    bad.policy.write_acknowledgements = 0;
                }
                1 => bad.policy.configuration_generation += 1,
                2 => {
                    bad.policy.configuration_id =
                        kuberic_runtime::protocol::types::ConfigurationId::new("stale")
                }
                3 => {
                    bad.policy.eligible_standbys[0].process_session_id =
                        ProcessSessionId::new("retired")
                }
                _ => bad.generation = 0,
            }
            assert!(
                bad.quorum(2).is_err(),
                "accepted policy mutation {mutation}"
            );
        }
    }

    #[test]
    fn final_round_rejects_stale_sessions_policy_lineage_and_receiver_state() {
        let policy = policy();
        let good = observation(&policy);
        validate_observation(&policy, &good, true).unwrap();
        assert!(good.evidence.replay_lsn.unwrap() < good.evidence.received_lsn.unwrap());
        for mutation in 0..8 {
            let mut bad = good.clone();
            match mutation {
                0 => bad.peer.session = ProcessSessionId::new("replacement"),
                1 => bad.lineage.system_identifier = "999".into(),
                2 => bad.lineage.timeline = 2,
                3 => bad.evidence.wal_receiver_stopped = false,
                4 => bad.policy = None,
                5 => bad.policy.as_mut().unwrap().generation += 1,
                6 => bad.evidence.replay_lsn = Some(101),
                _ => bad.running = false,
            }
            assert!(
                validate_observation(&policy, &bad, true).is_err(),
                "accepted observation mutation {mutation}"
            );
        }
    }

    #[test]
    fn legacy_running_receipts_keep_their_serialized_shape_and_stopped_acks_are_explicit() {
        let policy = policy();
        let mut legacy = serde_json::to_value(observation(&policy)).unwrap();
        let object = legacy.as_object_mut().unwrap();
        object.remove("host");
        object.remove("port");
        object.remove("running");
        let mut receipt: Observation = serde_json::from_value(legacy.clone()).unwrap();
        assert!(receipt.running);
        assert_eq!(serde_json::to_value(&receipt).unwrap(), legacy);
        validate_observation(&policy, &receipt, true).unwrap();
        receipt.running = false;
        assert_eq!(serde_json::to_value(&receipt).unwrap()["running"], false);
        assert!(validate_observation(&policy, &receipt, true).is_err());
    }
}
