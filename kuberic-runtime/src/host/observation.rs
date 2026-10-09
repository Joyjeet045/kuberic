//! Private owner observations and narrow host-consumer projections.
//!
//! Ownership and report composition:
//! - durable agent: identity, desired epoch/configuration/role/access,
//!   topology evidence, retained work, fallback build progress, load and fault;
//! - host proxy: actual open/address/role/access, admitted-authority projection,
//!   configuration/access generations, peer sessions and pending restoration;
//! - replication engine: executable configuration fence, replication/quorum
//!   progress, live builds, secondary removal and retirement.
//!
//! A report takes durable fields from the durable aggregate, actual lifecycle
//! fields from [`HostProxyObservation`], and progress/topology observations
//! from [`ReplicationEngineObservation`]. Build output is selected from live
//! engine progress plus durable fallback under host receipt/retirement policy.

use crate::authority::{AdmittedAuthority, RetiredAuthority};
use crate::effects::{
    AccessCompletion, AccessCompletionKind, AuthorityCompletion, BuildCompletion, BuildEffectState,
    BuildPostcondition, CatchUpCompletion, EpochCompletion, HistoricalSecondaryRemovalCompletion,
    ProcessCompletion, RetirementCompletion, RoleCompletion, RoleTransition, RuntimeEffectAction,
    RuntimeEffectOutcome, RuntimeSnapshot, SecondaryRemovalPreparationCompletion,
    SwitchoverCompletion,
};
use crate::host::state::AgentState;
use crate::protocol::types::{
    AccessStatus, ProcessSessionId, ReplicaIdentity, ReplicaRole, SecondaryRemovalPreparation,
    SecondaryScaleDownCleanup,
};
use crate::receipts::{NativeProgressStatus, TopologyReceipt};
use crate::replicator::ManagedOperationFence;

/// Durable-agent facts used by reporting and recovery decisions.
///
/// This wrapper makes the durable aggregate's role explicit without exposing
/// process-local host or engine observations through the durable owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DurableAgentObservation(pub(crate) AgentState);

impl From<AgentState> for DurableAgentObservation {
    fn from(state: AgentState) -> Self {
        Self(state)
    }
}

impl std::ops::Deref for DurableAgentObservation {
    type Target = AgentState;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DurableAgentObservation {
    pub(crate) fn into_state(self) -> AgentState {
        self.0
    }
}

/// Adapter-local state used to construct host and engine owner observations.
///
/// Physical co-location here does not transfer field ownership: lifecycle
/// fields are projected through [`HostProxyObservation`], while progress and
/// topology fields are projected through [`ReplicationEngineObservation`].
/// This type is never serialized or transported through capability views. A
/// complete snapshot is produced only for effect evidence and testing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicaRuntimeState {
    pub(crate) identity: ReplicaIdentity,
    pub(crate) open: bool,
    pub(crate) replication_address: Option<String>,
    pub(crate) role: ReplicaRole,
    pub(crate) role_transition: Option<RoleTransition>,
    pub(crate) read_status: AccessStatus,
    pub(crate) write_status: AccessStatus,
    pub(crate) authority: Option<AdmittedAuthority>,
    pub(crate) prepared_secondary_removal: Option<SecondaryRemovalPreparation>,
    pub(crate) retired_authority: Option<RetiredAuthority>,
    pub(crate) accepted_secondary_removal: Option<SecondaryScaleDownCleanup>,
    pub(crate) current_progress: i64,
    pub(crate) verified_replication_lsn: Option<i64>,
    pub(crate) live_builds_only: bool,
    pub(crate) committed_lsn: i64,
    pub(crate) current_configuration_quorum_progress: i64,
    pub(crate) catch_up_boundary: Option<i64>,
    pub(crate) catch_up_complete: bool,
    pub(crate) builds: Vec<BuildPostcondition>,
}

impl ReplicaRuntimeState {
    pub(crate) fn empty(identity: ReplicaIdentity) -> Self {
        Self {
            identity,
            open: false,
            replication_address: None,
            role: ReplicaRole::None,
            role_transition: None,
            read_status: AccessStatus::NotPrimary,
            write_status: AccessStatus::NotPrimary,
            authority: None,
            prepared_secondary_removal: None,
            retired_authority: None,
            accepted_secondary_removal: None,
            current_progress: 0,
            verified_replication_lsn: None,
            live_builds_only: false,
            committed_lsn: 0,
            current_configuration_quorum_progress: 0,
            catch_up_boundary: None,
            catch_up_complete: false,
            builds: Vec::new(),
        }
    }

    pub(crate) fn effect_outcome(
        &self,
        action: &RuntimeEffectAction,
        progress: Option<&NativeProgressStatus>,
        receipt: Option<&TopologyReceipt>,
    ) -> crate::Result<RuntimeEffectOutcome> {
        let progress = progress.cloned().unwrap_or(NativeProgressStatus {
            current_progress: self.current_progress,
            verified_replication_lsn: self.verified_replication_lsn,
            committed_lsn: self.committed_lsn,
            current_configuration_quorum_progress: self.current_configuration_quorum_progress,
            catch_up_boundary: self.catch_up_boundary,
            catch_up_complete: self.catch_up_complete,
        });
        let unexpected_receipt = || {
            crate::RuntimeError::InvalidReplication(
                "topology receipt does not match runtime effect".into(),
            )
        };
        let secondary_receipt = || match receipt {
            Some(TopologyReceipt::SecondaryRemoval(receipt)) => Ok(Some(receipt.clone())),
            None => Ok(None),
            Some(_) => Err(unexpected_receipt()),
        };
        let certified_receipt = || match receipt {
            Some(TopologyReceipt::CertifiedPrefix(receipt)) => Ok(Some(receipt.clone())),
            None => Ok(None),
            Some(_) => Err(unexpected_receipt()),
        };
        let switchover_receipt = || match receipt {
            Some(TopologyReceipt::Switchover(receipt)) => Ok(Some(receipt.clone())),
            None => Ok(None),
            Some(_) => Err(unexpected_receipt()),
        };
        let retirement_receipt = || match receipt {
            Some(TopologyReceipt::Retirement(receipt)) => Ok(Some(receipt.clone())),
            None => Ok(None),
            Some(_) => Err(unexpected_receipt()),
        };
        let process = || ProcessCompletion {
            open: self.open,
            role: self.role,
            read_status: self.read_status,
            write_status: self.write_status,
            authority: self.authority.clone(),
        };
        let retirement = |retired: &RetiredAuthority| -> crate::Result<RetirementCompletion> {
            Ok(RetirementCompletion {
                retired: retired.clone(),
                open: self.open,
                role: self.role,
                read_status: self.read_status,
                write_status: self.write_status,
                authority: self.authority.clone(),
                role_transition_clear: self.role_transition.is_none(),
                active_builds: !self.builds.is_empty(),
                receipt: retirement_receipt()?,
            })
        };
        Ok(match action {
            RuntimeEffectAction::Open(_) => {
                if !self.open {
                    return Err(crate::RuntimeError::NotOpen);
                }
                RuntimeEffectOutcome::Opened
            }
            RuntimeEffectAction::AdmitAuthority(_) => {
                RuntimeEffectOutcome::AuthorityAdmitted(AuthorityCompletion {
                    authority: self.authority.clone().ok_or_else(|| {
                        crate::RuntimeError::AuthorityMismatch(
                            "authority admission produced no authority".into(),
                        )
                    })?,
                    read_status: self.read_status,
                    write_status: self.write_status,
                    accepted_secondary_removal: self.accepted_secondary_removal.clone(),
                })
            }
            RuntimeEffectAction::PrepareSecondaryRemoval { .. } => {
                RuntimeEffectOutcome::SecondaryRemovalPrepared(
                    SecondaryRemovalPreparationCompletion {
                        prepared_secondary_removal: self.prepared_secondary_removal.clone(),
                        authority: self.authority.clone(),
                        role: self.role,
                        read_status: self.read_status,
                        write_status: self.write_status,
                        current_progress: progress.current_progress,
                        verified_replication_lsn: progress.verified_replication_lsn,
                        committed_lsn: progress.committed_lsn,
                        receipt: secondary_receipt()?,
                    },
                )
            }
            RuntimeEffectAction::RegisterPeerSession { identity, session } => {
                RuntimeEffectOutcome::PeerSessionRegistered {
                    identity: identity.clone(),
                    session: session.clone(),
                }
            }
            RuntimeEffectAction::ObserveSecondaryRemovalWitness(witness) => {
                RuntimeEffectOutcome::SecondaryRemovalWitnessObserved {
                    witness: witness.clone(),
                    receipt: secondary_receipt()?,
                }
            }
            RuntimeEffectAction::ObserveSecondaryRemovalProgress { witness, committed } => {
                RuntimeEffectOutcome::SecondaryRemovalProgressObserved {
                    witness: witness.clone(),
                    committed: committed.clone(),
                    receipt: secondary_receipt()?,
                }
            }
            RuntimeEffectAction::ObserveReplicationAck {
                acknowledgement,
                session,
            } => RuntimeEffectOutcome::ReplicationAckObserved {
                acknowledgement: acknowledgement.clone(),
                session: session.clone(),
            },
            RuntimeEffectAction::AcceptSecondaryRemovalCommit(committed) => {
                RuntimeEffectOutcome::SecondaryRemovalAccepted {
                    committed: committed.clone(),
                    receipt: secondary_receipt()?,
                }
            }
            RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(_) => {
                RuntimeEffectOutcome::HistoricalSecondaryRemovalAccepted(
                    HistoricalSecondaryRemovalCompletion {
                        accepted_secondary_removal: self
                            .accepted_secondary_removal
                            .clone()
                            .ok_or_else(|| {
                                crate::RuntimeError::InvalidReplication(
                                    "historical removal acceptance was not observed".into(),
                                )
                            })?,
                        authority: self.authority.clone(),
                        role: self.role,
                        write_status: self.write_status,
                        role_transition_clear: self.role_transition.is_none(),
                        verified_replication_lsn: progress.verified_replication_lsn,
                        receipt: secondary_receipt()?,
                    },
                )
            }
            RuntimeEffectAction::RetireReplica(retired) => {
                RuntimeEffectOutcome::ReplicaRetired(retirement(retired)?)
            }
            RuntimeEffectAction::FenceRetirement(retired) => {
                RuntimeEffectOutcome::RetirementFenced(retirement(retired)?)
            }
            RuntimeEffectAction::CompleteRetirement(retired) => {
                RuntimeEffectOutcome::RetirementCompleted(retirement(retired)?)
            }
            RuntimeEffectAction::AuthorizeFailoverPrefix(boundary_lsn) => {
                RuntimeEffectOutcome::FailoverPrefixAuthorized {
                    boundary_lsn: *boundary_lsn,
                    receipt: certified_receipt()?,
                }
            }
            RuntimeEffectAction::AdmitBuildAuthority(authority) => {
                RuntimeEffectOutcome::BuildAuthorityAdmitted {
                    authority: authority.clone(),
                }
            }
            RuntimeEffectAction::ChangeRole(_) => {
                RuntimeEffectOutcome::RoleChanged(RoleCompletion {
                    role: self.role,
                    role_transition: self.role_transition.clone(),
                })
            }
            RuntimeEffectAction::ChangeReplicatorRole(_) => {
                RuntimeEffectOutcome::ReplicatorRoleChanged(RoleCompletion {
                    role: self.role,
                    role_transition: self.role_transition.clone(),
                })
            }
            RuntimeEffectAction::UpdateEpoch => {
                RuntimeEffectOutcome::EpochUpdated(EpochCompletion {
                    epoch: self
                        .authority
                        .as_ref()
                        .map(|authority| authority.current_configuration.epoch)
                        .ok_or_else(|| {
                            crate::RuntimeError::AuthorityMismatch(
                                "epoch completion produced no authority".into(),
                            )
                        })?,
                    role_transition: self.role_transition.clone(),
                })
            }
            RuntimeEffectAction::ChangeApplicationRole(_) => {
                RuntimeEffectOutcome::ApplicationRoleChanged {
                    completion: RoleCompletion {
                        role: self.role,
                        role_transition: self.role_transition.clone(),
                    },
                    receipt: certified_receipt()?,
                }
            }
            RuntimeEffectAction::WaitForCatchup => {
                if !progress.catch_up_complete {
                    return Err(crate::RuntimeError::ReconfigurationPending);
                }
                RuntimeEffectOutcome::CatchUpCompleted(CatchUpCompletion {
                    authority: self.authority.clone(),
                    boundary_lsn: progress
                        .catch_up_boundary
                        .unwrap_or(progress.current_progress),
                })
            }
            RuntimeEffectAction::SetAccessStatus { .. }
            | RuntimeEffectAction::SetReadStatus(_)
            | RuntimeEffectAction::SetWriteStatus(_) => {
                let kind = match action {
                    RuntimeEffectAction::SetAccessStatus { .. } => AccessCompletionKind::Combined,
                    RuntimeEffectAction::SetReadStatus(_) => AccessCompletionKind::Read,
                    RuntimeEffectAction::SetWriteStatus(_) => AccessCompletionKind::Write,
                    _ => unreachable!(),
                };
                RuntimeEffectOutcome::AccessChanged(AccessCompletion {
                    kind,
                    read_status: self.read_status,
                    write_status: self.write_status,
                    authority: self.authority.clone(),
                    role: self.role,
                })
            }
            RuntimeEffectAction::PrepareSwitchover { .. } => {
                RuntimeEffectOutcome::SwitchoverPrepared(SwitchoverCompletion {
                    authority: self.authority.clone(),
                    role: self.role,
                    write_status: self.write_status,
                    current_progress: progress.current_progress,
                    committed_lsn: progress.committed_lsn,
                    receipt: switchover_receipt()?,
                })
            }
            RuntimeEffectAction::RefreshApplicationProgress => {
                RuntimeEffectOutcome::ApplicationProgressRefreshed {
                    current_progress: progress.current_progress,
                }
            }
            RuntimeEffectAction::BuildReplica {
                build_id, target, ..
            } => {
                let build = self
                    .builds
                    .iter()
                    .find(|build| {
                        build.authority.build_id == *build_id && build.authority.target == *target
                    })
                    .cloned();
                let state = match build {
                    Some(build) if build.completed => BuildEffectState::Completed(Box::new(build)),
                    _ => BuildEffectState::Dispatched,
                };
                RuntimeEffectOutcome::BuildReplica(BuildCompletion {
                    build_id: build_id.clone(),
                    target: target.clone(),
                    state,
                })
            }
            RuntimeEffectAction::RetireBuild(build_id) => RuntimeEffectOutcome::BuildRetired {
                build_id: build_id.clone(),
                active: self
                    .builds
                    .iter()
                    .any(|build| build.authority.build_id == *build_id),
            },
            RuntimeEffectAction::Close => RuntimeEffectOutcome::Closed(process()),
            RuntimeEffectAction::Abort => RuntimeEffectOutcome::Aborted(process()),
        })
    }
}

impl From<ReplicaRuntimeState> for RuntimeSnapshot {
    fn from(state: ReplicaRuntimeState) -> Self {
        Self {
            identity: state.identity,
            open: state.open,
            replication_address: state.replication_address,
            role: state.role,
            role_transition: state.role_transition,
            read_status: state.read_status,
            write_status: state.write_status,
            authority: state.authority,
            prepared_secondary_removal: state.prepared_secondary_removal,
            retired_authority: state.retired_authority,
            accepted_secondary_removal: state.accepted_secondary_removal,
            current_progress: state.current_progress,
            verified_replication_lsn: state.verified_replication_lsn,
            live_builds_only: state.live_builds_only,
            committed_lsn: state.committed_lsn,
            current_configuration_quorum_progress: state.current_configuration_quorum_progress,
            catch_up_boundary: state.catch_up_boundary,
            catch_up_complete: state.catch_up_complete,
            builds: state.builds,
        }
    }
}

/// Host-proxy facts used by reporting.
///
/// These fields describe the process-local application/proxy projection. The
/// durable agent remains authoritative for desired authority, role, access,
/// and topology; the replication engine remains authoritative for progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HostProxyObservation {
    pub(crate) identity: ReplicaIdentity,
    pub(crate) open: bool,
    pub(crate) replication_address: Option<String>,
    pub(crate) role: ReplicaRole,
    pub(crate) role_transition: Option<RoleTransition>,
    pub(crate) read_status: AccessStatus,
    pub(crate) write_status: AccessStatus,
    pub(crate) authority: Option<AdmittedAuthority>,
    pub(crate) live_builds_only: bool,
    pub(crate) engine_required: bool,
    pub(crate) configuration_generation: u64,
    pub(crate) engine_host_generation: Option<u64>,
    pub(crate) access_generation: u64,
    pub(crate) peer_sessions: Vec<(ReplicaIdentity, ProcessSessionId)>,
    pub(crate) pending_access: Option<PendingAccessObservation>,
}

/// Exact owner context for one deferred access restoration observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingAccessObservation {
    pub(crate) desired: (AccessStatus, AccessStatus),
    pub(crate) authority: Option<AdmittedAuthority>,
    pub(crate) configuration_generation: u64,
    pub(crate) access_generation: u64,
    pub(crate) active_access_generation: Option<u64>,
    pub(crate) peer_sessions: Vec<(ReplicaIdentity, ProcessSessionId)>,
    pub(crate) engine_fence: Option<ManagedOperationFence>,
}

/// Replication-engine facts used by reporting.
///
/// Engine-only fields are process-local and deliberately do not implement
/// durable serialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationEngineObservation {
    pub(crate) fence: Option<ManagedOperationFence>,
    pub(crate) host_generation: Option<u64>,
    pub(crate) prepared_secondary_removal: Option<SecondaryRemovalPreparation>,
    pub(crate) retired_authority: Option<RetiredAuthority>,
    pub(crate) accepted_secondary_removal: Option<SecondaryScaleDownCleanup>,
    pub(crate) current_progress: i64,
    pub(crate) verified_replication_lsn: Option<i64>,
    pub(crate) committed_lsn: i64,
    pub(crate) current_configuration_quorum_progress: i64,
    pub(crate) catch_up_boundary: Option<i64>,
    pub(crate) catch_up_complete: bool,
    pub(crate) catch_up_capability: Option<i64>,
    pub(crate) builds: Vec<BuildPostcondition>,
}

/// The explicit read-only report input assembled from the owning host and
/// replication-engine views.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReportObservation {
    pub(crate) host: HostProxyObservation,
    pub(crate) engine: ReplicationEngineObservation,
}

#[cfg(test)]
impl From<RuntimeSnapshot> for ReportObservation {
    fn from(snapshot: RuntimeSnapshot) -> Self {
        Self {
            host: HostProxyObservation {
                identity: snapshot.identity,
                open: snapshot.open,
                replication_address: snapshot.replication_address,
                role: snapshot.role,
                role_transition: snapshot.role_transition,
                read_status: snapshot.read_status,
                write_status: snapshot.write_status,
                authority: snapshot.authority,
                live_builds_only: snapshot.live_builds_only,
                engine_required: false,
                configuration_generation: 0,
                engine_host_generation: None,
                access_generation: 0,
                peer_sessions: Vec::new(),
                pending_access: None,
            },
            engine: ReplicationEngineObservation {
                fence: None,
                host_generation: None,
                prepared_secondary_removal: snapshot.prepared_secondary_removal,
                retired_authority: snapshot.retired_authority,
                accepted_secondary_removal: snapshot.accepted_secondary_removal,
                current_progress: snapshot.current_progress,
                verified_replication_lsn: snapshot.verified_replication_lsn,
                committed_lsn: snapshot.committed_lsn,
                current_configuration_quorum_progress: snapshot
                    .current_configuration_quorum_progress,
                catch_up_boundary: snapshot.catch_up_boundary,
                catch_up_complete: snapshot.catch_up_complete,
                catch_up_capability: None,
                builds: snapshot.builds,
            },
        }
    }
}

/// Build dispatch reads only live builds and current engine progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BuildObservation {
    pub(crate) authority: Option<AdmittedAuthority>,
    pub(crate) builds: Vec<BuildPostcondition>,
    pub(crate) current_progress: i64,
    pub(crate) committed_lsn: i64,
}

/// Peer discovery reads only live authority and retirement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeerObservation {
    pub(crate) authority: Option<AdmittedAuthority>,
    pub(crate) retired_authority: Option<RetiredAuthority>,
}

/// Outbound validation reads only host liveness and live authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutboundObservation {
    pub(crate) open: bool,
    pub(crate) authority: Option<AdmittedAuthority>,
}

/// Restart recovery inspection needs only the actual write projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveryObservation {
    pub(crate) read_status: AccessStatus,
    pub(crate) write_status: AccessStatus,
    pub(crate) authority: Option<AdmittedAuthority>,
}

impl RecoveryObservation {
    pub(crate) fn new(
        read_status: AccessStatus,
        write_status: AccessStatus,
        authority: Option<AdmittedAuthority>,
    ) -> Self {
        Self {
            read_status,
            write_status,
            authority,
        }
    }
}

#[cfg(test)]
impl From<RuntimeSnapshot> for RecoveryObservation {
    fn from(snapshot: RuntimeSnapshot) -> Self {
        Self::new(
            snapshot.read_status,
            snapshot.write_status,
            snapshot.authority,
        )
    }
}

impl ReportObservation {
    pub(crate) fn same_fence(&self, other: &Self) -> bool {
        self.host == other.host
            && self.engine.fence == other.engine.fence
            && self.engine.host_generation == other.engine.host_generation
            && self.engine.prepared_secondary_removal == other.engine.prepared_secondary_removal
            && self.engine.retired_authority == other.engine.retired_authority
            && self.engine.accepted_secondary_removal == other.engine.accepted_secondary_removal
    }

    #[cfg(test)]
    pub(crate) fn outbound(&self) -> OutboundObservation {
        OutboundObservation {
            open: self.host.open,
            authority: self.host.authority.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::{BuildAuthority, BuildAuthorityKind};
    use crate::effects::{BuildPostcondition, RoleTransition};
    use crate::host::state::{SCHEMA_VERSION, StorageIdentity};
    use crate::protocol::types::{
        AgentGeneration, ConfigurationDescriptor, ConfigurationMember, EffectivePolicy, Epoch,
        InitializationId, OperationId, PodUid, PvcUid, ReplicaId, ReplicaInstanceId, ReplicaRole,
        ResourceUid,
    };

    fn identity() -> ReplicaIdentity {
        ReplicaIdentity {
            replica_id: ReplicaId::new(1),
            instance_id: ReplicaInstanceId::new("instance"),
            agent_generation: AgentGeneration::new("generation"),
        }
    }

    fn authority() -> AdmittedAuthority {
        let local = identity();
        AdmittedAuthority {
            local_identity: local.clone(),
            transition_kind: None,
            previous_configuration: None,
            current_configuration: ConfigurationDescriptor::new(
                Epoch::new(1, 2),
                local.replica_id,
                vec![ConfigurationMember {
                    identity: local,
                    role: ReplicaRole::Primary,
                }],
                1,
            ),
            switchover_handoff: None,
            scale_up: None,
            secondary_removal: None,
        }
    }

    #[test]
    fn engine_only_fence_does_not_change_durable_serialization_or_unrelated_projection() {
        let state = AgentState::new(StorageIdentity {
            schema_version: SCHEMA_VERSION,
            resource_uid: ResourceUid::new("resource"),
            local_identity: identity(),
            pod_uid: PodUid::new("pod"),
            pvc_uid: PvcUid::new("pvc"),
            initialization_id: InitializationId::new("init"),
            effective_policy: EffectivePolicy::fixed(1, 30).unwrap(),
        });
        let durable_before = serde_json::to_vec(&state).unwrap();
        let effect_snapshot: RuntimeSnapshot = ReplicaRuntimeState::empty(identity()).into();
        let retained = crate::effects::RuntimeEffectResult {
            operation_id: OperationId::new("effect"),
            sequence: 1,
            outcome: crate::effects::RuntimeEffectOutcome::ApplicationProgressRefreshed {
                current_progress: 0,
            },
        };
        let retained_before = serde_json::to_vec(&retained).unwrap();
        let mut before: ReportObservation = effect_snapshot.into();
        let outbound = before.outbound();
        before.engine.fence = Some(ManagedOperationFence {
            configuration: None,
            engine_session_id: "engine-session".into(),
            engine_generation: 7,
        });
        assert_eq!(serde_json::to_vec(&state).unwrap(), durable_before);
        assert_eq!(serde_json::to_vec(&retained).unwrap(), retained_before);
        assert_eq!(before.outbound(), outbound);
        assert_eq!(state.identity.schema_version, SCHEMA_VERSION);
    }

    #[test]
    fn required_effect_outcomes_ignore_unrelated_runtime_observations() {
        let authority = authority();
        let mut base = ReplicaRuntimeState::empty(identity());
        base.open = true;
        base.role = ReplicaRole::Primary;
        base.read_status = AccessStatus::Granted;
        base.write_status = AccessStatus::Granted;
        base.authority = Some(authority.clone());
        base.role_transition = Some(RoleTransition {
            completed_role: ReplicaRole::IdleSecondary,
            target_role: ReplicaRole::Primary,
            replicator_completed: true,
            epoch_completed: true,
            application_completed: false,
        });
        base.catch_up_boundary = Some(9);
        base.catch_up_complete = true;
        let build = BuildPostcondition {
            authority: BuildAuthority {
                build_id: OperationId::new("build"),
                kind: BuildAuthorityKind::Provisioning,
                source: identity(),
                target: ReplicaIdentity {
                    replica_id: ReplicaId::new(2),
                    instance_id: ReplicaInstanceId::new("target"),
                    agent_generation: AgentGeneration::new("target-generation"),
                },
                current_configuration: authority.current_configuration.clone(),
                replication_boundary_lsn: 7,
            },
            last_sequence: 1,
            durable_lsn: 9,
            completed: true,
            catch_up_boundary_lsn: Some(7),
        };
        base.builds.push(build.clone());

        let role_action = RuntimeEffectAction::ChangeApplicationRole(ReplicaRole::Primary);
        let role_before = base.effect_outcome(&role_action, None, None).unwrap();
        let mut role_after_state = base.clone();
        role_after_state.current_progress = 100;
        role_after_state.committed_lsn = 90;
        role_after_state.builds.clear();
        assert_eq!(
            role_after_state
                .effect_outcome(&role_action, None, None)
                .unwrap(),
            role_before
        );

        let epoch_action = RuntimeEffectAction::UpdateEpoch;
        let epoch_before = base.effect_outcome(&epoch_action, None, None).unwrap();
        let mut epoch_after_state = base.clone();
        epoch_after_state.read_status = AccessStatus::NotPrimary;
        epoch_after_state.write_status = AccessStatus::NotPrimary;
        epoch_after_state.current_progress = 101;
        assert_eq!(
            epoch_after_state
                .effect_outcome(&epoch_action, None, None)
                .unwrap(),
            epoch_before
        );

        let access_action = RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::Granted,
        };
        let access_before = base.effect_outcome(&access_action, None, None).unwrap();
        let mut access_after_state = base.clone();
        access_after_state.current_progress = 102;
        access_after_state.committed_lsn = 91;
        access_after_state.builds.clear();
        assert_eq!(
            access_after_state
                .effect_outcome(&access_action, None, None)
                .unwrap(),
            access_before
        );

        let catch_up_action = RuntimeEffectAction::WaitForCatchup;
        let catch_up_before = base.effect_outcome(&catch_up_action, None, None).unwrap();
        let mut catch_up_after_state = base.clone();
        catch_up_after_state.role = ReplicaRole::IdleSecondary;
        catch_up_after_state.read_status = AccessStatus::NotPrimary;
        catch_up_after_state.write_status = AccessStatus::NotPrimary;
        catch_up_after_state.current_progress = 103;
        catch_up_after_state.committed_lsn = 92;
        catch_up_after_state.builds.clear();
        assert_eq!(
            catch_up_after_state
                .effect_outcome(&catch_up_action, None, None)
                .unwrap(),
            catch_up_before
        );

        let build_action = RuntimeEffectAction::BuildReplica {
            build_id: build.authority.build_id.clone(),
            target: build.authority.target.clone(),
            replication_address: "in-process://target".into(),
        };
        let build_before = base.effect_outcome(&build_action, None, None).unwrap();
        let mut build_after_state = base.clone();
        build_after_state.role = ReplicaRole::IdleSecondary;
        build_after_state.read_status = AccessStatus::NotPrimary;
        build_after_state.write_status = AccessStatus::NotPrimary;
        build_after_state.current_progress = 104;
        assert_eq!(
            build_after_state
                .effect_outcome(&build_action, None, None)
                .unwrap(),
            build_before
        );
    }
}
