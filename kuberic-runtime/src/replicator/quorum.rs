use std::collections::{BTreeMap, BTreeSet};

use crate::protocol::types::{
    AccessStatus, ConfigurationDescriptor, OperationId, ProcessSessionId, ReplicaIdentity,
    ReplicaRole, SecondaryRemovalStage, SecondaryRemovalWitness, SecondaryScaleDownCleanup,
};
use crate::transport::ReplicationAck;
use tokio::sync::oneshot;

use crate::application::Lsn;
use crate::replicator::configuration::ManagedReplicaConfiguration;
use crate::{Result, RuntimeError};

#[derive(Debug, Default)]
pub(crate) struct QuorumTracker {
    authority: Option<ManagedReplicaConfiguration>,
    progress: BTreeMap<ReplicaIdentity, Lsn>,
    pending: BTreeMap<Lsn, Vec<oneshot::Sender<Result<Lsn>>>>,
    highest_lsn: Lsn,
    committed_lsn: Lsn,
    catch_up_boundary: Option<Lsn>,
    must_catch_up: BTreeSet<ReplicaIdentity>,
    sessions: BTreeMap<ReplicaIdentity, ProcessSessionId>,
    obsolete_sessions: BTreeSet<(ReplicaIdentity, ProcessSessionId)>,
    verified: BTreeMap<ReplicaIdentity, (u64, Lsn)>,
    witnesses: BTreeMap<ReplicaIdentity, SecondaryRemovalWitness>,
    live_commits: BTreeMap<ReplicaIdentity, SecondaryScaleDownCleanup>,
}

impl QuorumTracker {
    #[cfg(test)]
    pub(crate) fn configure(
        &mut self,
        authority: ManagedReplicaConfiguration,
        local_progress: Lsn,
    ) -> Result<()> {
        authority.validate()?;
        self.configure_prevalidated(authority, local_progress);
        Ok(())
    }

    pub(crate) fn configure_prevalidated(
        &mut self,
        authority: ManagedReplicaConfiguration,
        local_progress: Lsn,
    ) {
        let same_fence = self.authority.as_ref() == Some(&authority);
        let current_only_completion = self.authority.as_ref().is_some_and(|existing| {
            authority.scale_up.is_some() && authority.is_current_only_completion_of(existing)
        });
        if self.authority.is_some() && !same_fence {
            for (_, senders) in std::mem::take(&mut self.pending) {
                for sender in senders {
                    let _ = sender.send(Err(RuntimeError::AuthorityMismatch(
                        "authority changed before the write committed".to_string(),
                    )));
                }
            }
            if !current_only_completion {
                self.progress.clear();
                self.verified.clear();
                self.witnesses.clear();
                self.live_commits.clear();
            }
        }
        let members = authority_members(&authority);
        self.sessions
            .retain(|identity, _| members.contains(identity));
        self.progress
            .retain(|identity, _| members.contains(identity));
        self.progress
            .entry(authority.local_identity.clone())
            .and_modify(|progress| *progress = (*progress).max(local_progress))
            .or_insert(local_progress);
        self.highest_lsn = self.highest_lsn.max(local_progress);
        if !same_fence {
            self.catch_up_boundary = authority
                .scale_up
                .as_deref()
                .map(|evidence| evidence.intent().catch_up_boundary_lsn)
                .or_else(|| {
                    authority
                        .secondary_removal
                        .as_ref()
                        .map(|e| e.preparation.boundary_lsn)
                })
                .or_else(|| {
                    authority.previous_configuration.as_ref().map(|_| {
                        authority
                            .switchover_handoff
                            .as_ref()
                            .map_or(self.highest_lsn, |handoff| handoff.handoff_lsn)
                    })
                });
            self.must_catch_up = derive_must_catch_up(&authority);
        }
        self.authority = Some(authority);
    }

    pub(crate) fn register_peer_session(
        &mut self,
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    ) -> Result<()> {
        let authority = self
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if session.is_empty()
            || identity == authority.local_identity
            || !authority.contains_member(&identity)
            || self
                .obsolete_sessions
                .contains(&(identity.clone(), session.clone()))
        {
            return Err(RuntimeError::AuthorityMismatch(
                "invalid peer session identity".into(),
            ));
        }
        if self.sessions.get(&identity) != Some(&session) {
            if let Some(evidence) = &authority.secondary_removal {
                for historical in evidence
                    .previous_read_quorum
                    .iter()
                    .chain(&evidence.reduced_write_quorum)
                {
                    if historical.identity == identity && historical.process_session_id != session {
                        self.obsolete_sessions
                            .insert((identity.clone(), historical.process_session_id.clone()));
                    }
                }
            }
            if let Some(previous) = self.sessions.get(&identity) {
                self.obsolete_sessions
                    .insert((identity.clone(), previous.clone()));
            }
            self.progress.remove(&identity);
            self.verified.remove(&identity);
            self.witnesses.remove(&identity);
            self.live_commits.remove(&identity);
            self.sessions.insert(identity, session);
        }
        Ok(())
    }

    pub(crate) fn record_verified_local_progress(&mut self, lsn: Lsn) {
        if let Some(authority) = &self.authority {
            self.verified
                .insert(authority.local_identity.clone(), (0, lsn));
        }
    }

    pub(crate) fn observe_secondary_removal(
        &mut self,
        witness: &SecondaryRemovalWitness,
    ) -> Result<()> {
        if self.witnesses.get(&witness.identity) == Some(witness)
            && !self.live_commits.contains_key(&witness.identity)
        {
            return Ok(());
        }
        let authority = self
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let evidence = authority
            .secondary_removal
            .as_ref()
            .ok_or(RuntimeError::ReconfigurationPending)?;
        let intent = &evidence.preparation.intent;
        let stage = if authority.previous_configuration.is_some() {
            SecondaryRemovalStage::PreviousCurrent
        } else {
            SecondaryRemovalStage::CurrentOnly
        };
        if witness.identity == authority.local_identity
            || self.sessions.get(&witness.identity) != Some(&witness.process_session_id)
            || witness.resource_uid != intent.resource_uid
            || !intent
                .current_configuration
                .members
                .iter()
                .any(|m| m.identity == witness.identity && m.role == witness.role)
            || witness.epoch != authority.current_configuration.epoch
            || witness.current_configuration_id != authority.current_configuration.configuration_id
            || witness.previous_configuration_id != authority.fence().previous_configuration_id
            || witness.report_sequence == 0
            || witness.verified_replication_lsn < evidence.preparation.boundary_lsn
            || witness.pending_operation_id.is_some()
            || witness.write_status == crate::protocol::types::AccessStatus::Granted
            || witness.retained_operation_id.as_ref()
                != Some(&intent.command_operation_id(stage, &witness.identity))
            || self
                .verified
                .get(&witness.identity)
                .is_some_and(|(seq, _)| *seq >= witness.report_sequence)
            || evidence
                .previous_read_quorum
                .iter()
                .chain(if authority.previous_configuration.is_none() {
                    evidence.reduced_write_quorum.as_slice()
                } else {
                    &[]
                })
                .any(|old| {
                    old.identity == witness.identity
                        && old.process_session_id == witness.process_session_id
                        && old.report_sequence >= witness.report_sequence
                })
        {
            return Err(RuntimeError::AuthorityMismatch(
                "stale or unverified reduced-quorum witness".into(),
            ));
        }
        self.verified.insert(
            witness.identity.clone(),
            (witness.report_sequence, witness.verified_replication_lsn),
        );
        self.witnesses
            .insert(witness.identity.clone(), witness.clone());
        Ok(())
    }

    /// Fresh progress for an already committed reduction is not a transition witness.
    pub(crate) fn observe_secondary_removal_progress(
        &mut self,
        witness: &SecondaryRemovalWitness,
        committed: &SecondaryScaleDownCleanup,
    ) -> Result<()> {
        crate::protocol::validation::validate_secondary_scale_down_cleanup(committed)
            .map_err(|e| RuntimeError::AuthorityMismatch(e.to_string()))?;
        let authority = self
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let intent = &committed.evidence.preparation.intent;
        let operation = witness.retained_operation_id.as_ref();
        let retained = operation
            == Some(
                &intent.command_operation_id(SecondaryRemovalStage::CurrentOnly, &witness.identity),
            )
            || operation
                == Some(
                    &intent.command_operation_id(
                        SecondaryRemovalStage::AcceptCommit,
                        &witness.identity,
                    ),
                )
            || (witness.identity == intent.primary
                && operation
                    == Some(&OperationId::new(format!(
                        "availability:{}:{}",
                        intent.current_configuration.configuration_id,
                        if witness.write_status == AccessStatus::Granted {
                            "grant-write"
                        } else {
                            "no-write-quorum"
                        }
                    ))));
        if authority.previous_configuration.is_some()
            || authority.secondary_removal.as_ref() != Some(&committed.evidence)
            || authority.current_configuration != intent.current_configuration
            || witness.identity == authority.local_identity
            || self.sessions.get(&witness.identity) != Some(&witness.process_session_id)
            || witness.resource_uid != intent.resource_uid
            || witness.epoch != intent.current_configuration.epoch
            || witness.previous_configuration_id.is_some()
            || witness.current_configuration_id != intent.current_configuration.configuration_id
            || !intent
                .current_configuration
                .members
                .iter()
                .any(|member| member.identity == witness.identity && member.role == witness.role)
            || witness.report_sequence == 0
            || witness.verified_replication_lsn < committed.evidence.preparation.boundary_lsn
            || witness.pending_operation_id.is_some()
            || (witness.write_status == AccessStatus::Granted && witness.identity != intent.primary)
            || !retained
            || self.live_commits.values().any(|old| {
                old.evidence != committed.evidence
                    || old.current_only_write_quorum != committed.current_only_write_quorum
            })
        {
            return Err(RuntimeError::AuthorityMismatch(
                "invalid committed-removal live progress".into(),
            ));
        }
        if self.witnesses.get(&witness.identity) == Some(witness)
            && self.live_commits.contains_key(&witness.identity)
        {
            return Ok(());
        }
        if self
            .verified
            .get(&witness.identity)
            .is_some_and(|(sequence, _)| *sequence >= witness.report_sequence)
            || committed.current_only_write_quorum.iter().any(|old| {
                old.identity == witness.identity
                    && old.process_session_id == witness.process_session_id
                    && old.report_sequence >= witness.report_sequence
            })
        {
            return Err(RuntimeError::AuthorityMismatch(
                "stale committed-removal live progress".into(),
            ));
        }
        self.verified.insert(
            witness.identity.clone(),
            (witness.report_sequence, witness.verified_replication_lsn),
        );
        self.witnesses
            .insert(witness.identity.clone(), witness.clone());
        self.live_commits
            .insert(witness.identity.clone(), committed.clone());
        Ok(())
    }

    pub(crate) fn validate_secondary_removal_commit(
        &self,
        committed: &SecondaryScaleDownCleanup,
    ) -> Result<()> {
        if self.live_commits.values().any(|old| {
            old.evidence != committed.evidence
                || old.current_only_write_quorum != committed.current_only_write_quorum
        }) {
            return Err(RuntimeError::AuthorityMismatch(
                "commit differs from live progress proof".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn observe_committed_secondary_removal(
        &mut self,
        witness: &SecondaryRemovalWitness,
    ) -> Result<()> {
        if self.sessions.get(&witness.identity) != Some(&witness.process_session_id) {
            // The certificate authorizes the configuration, not obsolete
            // session credit. Session registration erased that credit;
            // catch-up now requires freshly verified current-session reports.
            return Ok(());
        }
        // A frozen commit certificate can arrive after fresher peer discovery.
        // Retain the newer verified credit, without replaying an older sequence.
        if self
            .witnesses
            .get(&witness.identity)
            .is_some_and(|current| {
                current.process_session_id == witness.process_session_id
                    && current.resource_uid == witness.resource_uid
                    && current.role == witness.role
                    && current.epoch == witness.epoch
                    && current.current_configuration_id == witness.current_configuration_id
                    && current.previous_configuration_id == witness.previous_configuration_id
                    && current.report_sequence >= witness.report_sequence
                    && current.verified_replication_lsn >= witness.verified_replication_lsn
            })
        {
            return Ok(());
        }
        self.observe_secondary_removal(witness)
    }

    pub(crate) fn restore_committed_secondary_removal(
        &mut self,
        witness: &SecondaryRemovalWitness,
    ) -> Result<()> {
        if !self.sessions.contains_key(&witness.identity) {
            self.register_peer_session(
                witness.identity.clone(),
                witness.process_session_id.clone(),
            )?;
        }
        self.observe_committed_secondary_removal(witness)
    }

    pub(crate) fn register_write(&mut self, lsn: Lsn) -> Result<oneshot::Receiver<Result<Lsn>>> {
        if self.authority.is_none() {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        self.highest_lsn = self.highest_lsn.max(lsn);
        let (sender, receiver) = oneshot::channel();
        self.pending.entry(lsn).or_default().push(sender);
        Ok(receiver)
    }

    pub(crate) fn record_local_progress(&mut self, lsn: Lsn) -> Result<()> {
        let authority = self
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        self.progress
            .entry(authority.local_identity.clone())
            .and_modify(|progress| *progress = (*progress).max(lsn))
            .or_insert(lsn);
        self.highest_lsn = self.highest_lsn.max(lsn);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn record_build_handoff_progress(
        &mut self,
        identity: ReplicaIdentity,
        lsn: Lsn,
    ) -> Result<()> {
        let authority = self
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if !authority.contains_member(&identity) {
            return Err(RuntimeError::AuthorityMismatch(
                "durable progress belongs to a replica outside authority".to_string(),
            ));
        }
        self.progress
            .entry(identity)
            .and_modify(|progress| *progress = (*progress).max(lsn))
            .or_insert(lsn);
        self.highest_lsn = self.highest_lsn.max(lsn);
        Ok(())
    }

    pub(crate) fn record_prepared_build_handoff_progress(
        &mut self,
        identity: ReplicaIdentity,
        lsn: Lsn,
    ) {
        self.progress
            .entry(identity)
            .and_modify(|progress| *progress = (*progress).max(lsn))
            .or_insert(lsn);
        self.highest_lsn = self.highest_lsn.max(lsn);
    }

    pub(crate) fn acknowledge(&mut self, acknowledgement: &ReplicationAck) -> Result<()> {
        let authority = self
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        authority.validate_acknowledgement(acknowledgement)?;
        self.progress
            .entry(acknowledgement.receiver.clone())
            .and_modify(|progress| {
                *progress = (*progress).max(acknowledgement.applied_lsn);
            })
            .or_insert(acknowledgement.applied_lsn);
        Ok(())
    }

    pub(crate) fn acknowledge_in_session(
        &mut self,
        acknowledgement: &ReplicationAck,
        session: &ProcessSessionId,
    ) -> Result<()> {
        if self.sessions.get(&acknowledgement.receiver) != Some(session) {
            return Err(RuntimeError::AuthorityMismatch(
                "acknowledgement belongs to an obsolete peer session".into(),
            ));
        }
        self.acknowledge(acknowledgement)
    }

    pub(crate) fn current_configuration_quorum_progress(&self) -> Lsn {
        self.authority.as_ref().map_or(0, |authority| {
            quorum_progress(&authority.current_configuration, &self.progress)
        })
    }

    pub(crate) fn committed_lsn(&self) -> Lsn {
        self.committed_lsn
    }

    pub(crate) fn highest_lsn(&self) -> Lsn {
        self.highest_lsn
    }

    pub(crate) fn catch_up_boundary(&self) -> Option<Lsn> {
        self.catch_up_boundary
    }

    pub(crate) fn fail_pending(&mut self) {
        for (_, senders) in std::mem::take(&mut self.pending) {
            for sender in senders {
                let _ = sender.send(Err(RuntimeError::WriteClosed(
                    crate::protocol::types::AccessStatus::ReconfigurationPending,
                )));
            }
        }
    }

    pub(crate) fn catch_up_complete(&self) -> bool {
        if let Some(authority) = &self.authority
            && let Some(evidence) = &authority.secondary_removal
        {
            let boundary = evidence.preparation.boundary_lsn;
            return self
                .verified
                .get(authority.primary_identity())
                .is_some_and(|(_, lsn)| *lsn >= boundary)
                && authority
                    .current_configuration
                    .members
                    .iter()
                    .filter(|m| {
                        self.verified
                            .get(&m.identity)
                            .is_some_and(|(_, lsn)| *lsn >= boundary)
                    })
                    .count()
                    >= authority.current_configuration.write_quorum as usize;
        }
        let Some(boundary) = self.catch_up_boundary else {
            return true;
        };
        let quorum_progress = self.current_configuration_quorum_progress();
        quorum_progress >= boundary
            && self.must_catch_up.iter().all(|identity| {
                self.progress
                    .get(identity)
                    .is_some_and(|progress| *progress >= quorum_progress)
            })
    }

    pub(crate) fn all_caught_up(&self, lsn: Lsn) -> bool {
        self.authority.as_ref().is_some_and(|authority| {
            authority
                .current_configuration
                .members
                .iter()
                .all(|member| self.progress.get(&member.identity).copied().unwrap_or(0) >= lsn)
        })
    }

    pub(crate) fn ready_commit_lsn(&self) -> Option<Lsn> {
        let authority = self.authority.as_ref()?;
        self.pending
            .keys()
            .copied()
            .filter(|lsn| client_commit_ready(authority, &self.progress, *lsn))
            .max()
    }

    pub(crate) fn finalize_commit(&mut self, committed_lsn: Lsn) -> Result<()> {
        let ready = self.ready_commit_lsn().ok_or_else(|| {
            RuntimeError::Application("no quorum-ready write can be finalized".to_string())
        })?;
        if committed_lsn > ready {
            return Err(RuntimeError::Application(
                "application commit exceeds quorum-ready progress".to_string(),
            ));
        }
        let completed = self
            .pending
            .keys()
            .copied()
            .take_while(|lsn| *lsn <= committed_lsn)
            .collect::<Vec<_>>();
        for lsn in completed {
            if let Some(senders) = self.pending.remove(&lsn) {
                for sender in senders {
                    let _ = sender.send(Ok(lsn));
                }
            }
        }
        self.committed_lsn = self.committed_lsn.max(committed_lsn);
        Ok(())
    }

    pub(crate) fn restore_committed_lsn(&mut self, committed_lsn: Lsn) {
        let completed = self
            .pending
            .keys()
            .copied()
            .take_while(|lsn| *lsn <= committed_lsn)
            .collect::<Vec<_>>();
        for lsn in completed {
            if let Some(senders) = self.pending.remove(&lsn) {
                for sender in senders {
                    let _ = sender.send(Ok(lsn));
                }
            }
        }
        self.highest_lsn = self.highest_lsn.max(committed_lsn);
        self.committed_lsn = self.committed_lsn.max(committed_lsn);
    }

    pub(crate) fn reset_progress_after_data_loss(
        &mut self,
        local_identity: ReplicaIdentity,
        current_progress: Lsn,
        committed_lsn: Lsn,
    ) {
        self.fail_pending();
        self.progress.clear();
        self.progress.insert(local_identity, current_progress);
        self.highest_lsn = current_progress;
        self.committed_lsn = committed_lsn;
        self.catch_up_boundary = None;
        self.must_catch_up.clear();
        self.sessions.clear();
        self.verified.clear();
        self.witnesses.clear();
    }
}

fn authority_members(authority: &ManagedReplicaConfiguration) -> BTreeSet<ReplicaIdentity> {
    authority
        .current_configuration
        .members
        .iter()
        .chain(
            authority
                .previous_configuration
                .iter()
                .flat_map(|configuration| configuration.members.iter()),
        )
        .map(|member| member.identity.clone())
        .collect()
}

fn derive_must_catch_up(authority: &ManagedReplicaConfiguration) -> BTreeSet<ReplicaIdentity> {
    let mut required = authority
        .scale_up
        .as_deref()
        .map(|evidence| BTreeSet::from([evidence.intent().target.clone()]))
        .unwrap_or_default();
    let Some(previous) = authority.previous_configuration.as_ref() else {
        return required;
    };
    let current_primary = authority
        .current_configuration
        .members
        .iter()
        .find(|member| {
            member.identity.replica_id == authority.current_configuration.primary_id
                && member.role == ReplicaRole::Primary
        })
        .expect("validated configuration has one primary");
    let was_same_primary = previous.members.iter().any(|member| {
        member.identity == current_primary.identity && member.role == ReplicaRole::Primary
    });
    if was_same_primary {
        required
    } else {
        required.insert(current_primary.identity.clone());
        required
    }
}

fn quorum_progress(
    configuration: &ConfigurationDescriptor,
    progress: &BTreeMap<ReplicaIdentity, Lsn>,
) -> Lsn {
    let mut values = configuration
        .members
        .iter()
        .map(|member| progress.get(&member.identity).copied().unwrap_or(0))
        .collect::<Vec<_>>();
    values.sort_unstable_by(|left, right| right.cmp(left));
    values
        .get(configuration.write_quorum.saturating_sub(1) as usize)
        .copied()
        .unwrap_or(0)
}

fn client_commit_ready(
    authority: &ManagedReplicaConfiguration,
    progress: &BTreeMap<ReplicaIdentity, Lsn>,
    lsn: Lsn,
) -> bool {
    has_quorum(&authority.current_configuration, progress, lsn)
        && authority
            .previous_configuration
            .as_ref()
            .is_none_or(|previous| has_quorum(previous, progress, lsn))
}

fn has_quorum(
    configuration: &ConfigurationDescriptor,
    progress: &BTreeMap<ReplicaIdentity, Lsn>,
    lsn: Lsn,
) -> bool {
    configuration
        .members
        .iter()
        .filter(|member| progress.get(&member.identity).copied().unwrap_or(0) >= lsn)
        .count()
        >= configuration.write_quorum as usize
}

#[cfg(all(test, kuberic_workspace_tests))]
#[path = "quorum_tests.rs"]
mod scenario_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::{AdmittedAuthority, BuildAuthorityKind};
    use crate::protocol::types::{
        EffectivePolicy, Epoch, ReplicaId, ScaleUpConfigurationEvidence, ScaleUpFailoverEvidence,
        ScaleUpIntent, ScaleUpStage, ScaleUpWitness, TransitionKind,
    };
    #[cfg(kuberic_workspace_tests)]
    use crate::removal_fixture;

    fn managed(authority: AdmittedAuthority) -> ManagedReplicaConfiguration {
        ManagedReplicaConfiguration {
            local_identity: authority.local_identity,
            previous_configuration: authority.previous_configuration,
            current_configuration: authority.current_configuration,
            switchover_handoff: authority.switchover_handoff,
            secondary_removal: authority.secondary_removal,
            scale_up: authority.scale_up,
            build_kind: if authority.transition_kind == Some(TransitionKind::Failover) {
                BuildAuthorityKind::Failover
            } else {
                BuildAuthorityKind::Provisioning
            },
        }
    }

    fn scale_up_authority() -> (AdmittedAuthority, ReplicaIdentity) {
        let primary = ReplicaIdentity {
            replica_id: ReplicaId::new(1),
            instance_id: crate::protocol::types::ReplicaInstanceId::new("primary"),
            agent_generation: crate::protocol::types::AgentGeneration::new("primary-gen"),
        };
        let target = ReplicaIdentity {
            replica_id: ReplicaId::new(2),
            instance_id: crate::protocol::types::ReplicaInstanceId::new("target"),
            agent_generation: crate::protocol::types::AgentGeneration::new("target-gen"),
        };
        let previous_policy = EffectivePolicy::fixed(1, 30).unwrap();
        let current_policy = EffectivePolicy::fixed(2, 30).unwrap();
        let previous = ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            primary.replica_id,
            vec![crate::protocol::types::ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            }],
            previous_policy.write_quorum,
        );
        let current = ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            primary.replica_id,
            vec![
                crate::protocol::types::ConfigurationMember {
                    identity: primary.clone(),
                    role: ReplicaRole::Primary,
                },
                crate::protocol::types::ConfigurationMember {
                    identity: target.clone(),
                    role: ReplicaRole::ActiveSecondary,
                },
            ],
            current_policy.write_quorum,
        );
        let mut intent = ScaleUpIntent {
            operation_id: OperationId::default(),
            resource_uid: crate::protocol::types::ResourceUid::new("set"),
            spec_generation: 2,
            desired_replicas: 2,
            previous_configuration: previous.clone(),
            current_configuration: current.clone(),
            previous_policy,
            current_policy,
            primary: primary.clone(),
            target: target.clone(),
            build_id: OperationId::new("build"),
            snapshot_boundary_lsn: 0,
            catch_up_boundary_lsn: 2,
        };
        intent.operation_id = intent.expected_operation_id();
        (
            AdmittedAuthority {
                local_identity: primary,
                transition_kind: Some(TransitionKind::ScaleUp),
                previous_configuration: Some(previous),
                current_configuration: current,
                switchover_handoff: None,
                secondary_removal: None,
                scale_up: Some(Box::new(ScaleUpConfigurationEvidence::Admission { intent })),
            },
            target,
        )
    }

    #[test]
    fn scale_up_requires_the_exact_candidate_through_the_frozen_boundary() {
        let (authority, target) = scale_up_authority();
        let mut tracker = QuorumTracker::default();
        tracker.configure(managed(authority), 2).unwrap();
        assert!(!tracker.catch_up_complete());
        tracker.record_build_handoff_progress(target, 2).unwrap();
        assert!(tracker.catch_up_complete());
    }

    #[test]
    fn zero_boundary_requires_explicit_candidate_evidence() {
        let (mut authority, target) = scale_up_authority();
        let Some(ScaleUpConfigurationEvidence::Admission { intent }) =
            authority.scale_up.as_deref_mut()
        else {
            unreachable!()
        };
        intent.catch_up_boundary_lsn = 0;
        intent.operation_id = intent.expected_operation_id();
        let mut tracker = QuorumTracker::default();
        tracker.configure(managed(authority), 0).unwrap();
        assert!(!tracker.catch_up_complete());
        tracker.record_build_handoff_progress(target, 0).unwrap();
        assert!(tracker.catch_up_complete());
    }

    #[test]
    fn carried_scale_up_failover_requires_candidate_and_new_primary() {
        let identities = (1..=5)
            .map(|id| ReplicaIdentity {
                replica_id: ReplicaId::new(id),
                instance_id: crate::protocol::types::ReplicaInstanceId::new(format!("pod-{id}")),
                agent_generation: crate::protocol::types::AgentGeneration::new(format!("gen-{id}")),
            })
            .collect::<Vec<_>>();
        let previous_policy = EffectivePolicy::fixed(4, 30).unwrap();
        let current_policy = EffectivePolicy::fixed(5, 30).unwrap();
        let previous = ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            ReplicaId::new(1),
            identities[..4]
                .iter()
                .enumerate()
                .map(
                    |(index, identity)| crate::protocol::types::ConfigurationMember {
                        identity: identity.clone(),
                        role: if index == 0 {
                            ReplicaRole::Primary
                        } else {
                            ReplicaRole::ActiveSecondary
                        },
                    },
                )
                .collect(),
            previous_policy.write_quorum,
        );
        let expanded = ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            ReplicaId::new(1),
            identities
                .iter()
                .enumerate()
                .map(
                    |(index, identity)| crate::protocol::types::ConfigurationMember {
                        identity: identity.clone(),
                        role: if index == 0 {
                            ReplicaRole::Primary
                        } else {
                            ReplicaRole::ActiveSecondary
                        },
                    },
                )
                .collect(),
            current_policy.write_quorum,
        );
        let mut intent = ScaleUpIntent {
            operation_id: OperationId::default(),
            resource_uid: crate::protocol::types::ResourceUid::new("set"),
            spec_generation: 2,
            desired_replicas: 5,
            previous_configuration: previous.clone(),
            current_configuration: expanded.clone(),
            previous_policy,
            current_policy: current_policy.clone(),
            primary: identities[0].clone(),
            target: identities[4].clone(),
            build_id: OperationId::new("build"),
            snapshot_boundary_lsn: 5,
            catch_up_boundary_lsn: 10,
        };
        intent.operation_id = intent.expected_operation_id();
        let witness = |identity: ReplicaIdentity, sequence: u64| ScaleUpWitness {
            resource_uid: intent.resource_uid.clone(),
            role: expanded
                .members
                .iter()
                .find(|member| member.identity == identity)
                .unwrap()
                .role,
            retained_operation_id: Some(intent.command_operation_id(
                ScaleUpStage::PreviousCurrent,
                &identity,
                &expanded,
            )),
            identity,
            process_session_id: ProcessSessionId::new(format!("session-{sequence}")),
            report_sequence: sequence,
            epoch: expanded.epoch,
            previous_configuration_id: Some(previous.configuration_id.clone()),
            current_configuration_id: expanded.configuration_id.clone(),
            verified_replication_lsn: 10,
            write_status: AccessStatus::ReconfigurationPending,
            pending_operation_id: None,
        };
        let failover = ConfigurationDescriptor::new(
            Epoch::new(0, 3),
            ReplicaId::new(2),
            identities
                .iter()
                .enumerate()
                .map(
                    |(index, identity)| crate::protocol::types::ConfigurationMember {
                        identity: identity.clone(),
                        role: if index == 1 {
                            ReplicaRole::Primary
                        } else {
                            ReplicaRole::ActiveSecondary
                        },
                    },
                )
                .collect(),
            current_policy.write_quorum,
        );
        let evidence = ScaleUpFailoverEvidence {
            provisional_configuration: failover.clone(),
            previous_read_quorum: vec![
                witness(identities[2].clone(), 1),
                witness(identities[3].clone(), 2),
            ],
            current_read_quorum: vec![
                witness(identities[2].clone(), 3),
                witness(identities[3].clone(), 4),
                witness(identities[4].clone(), 5),
            ],
            final_election: None,
            intent: intent.clone(),
        };
        let authority = AdmittedAuthority {
            local_identity: identities[1].clone(),
            transition_kind: Some(TransitionKind::Failover),
            previous_configuration: Some(previous),
            current_configuration: failover,
            switchover_handoff: None,
            secondary_removal: None,
            scale_up: Some(Box::new(ScaleUpConfigurationEvidence::Failover {
                evidence,
            })),
        };
        let mut tracker = QuorumTracker::default();
        tracker.configure(managed(authority), 5).unwrap();
        tracker
            .record_build_handoff_progress(identities[4].clone(), 10)
            .unwrap();
        tracker
            .record_build_handoff_progress(identities[2].clone(), 10)
            .unwrap();
        tracker
            .record_build_handoff_progress(identities[3].clone(), 10)
            .unwrap();
        assert!(!tracker.catch_up_complete());
        tracker.record_local_progress(10).unwrap();
        assert!(tracker.catch_up_complete());
    }

    #[test]
    #[cfg(kuberic_workspace_tests)]
    fn late_member_uses_live_committed_primary_progress_not_a_transition_certificate() {
        let intent = removal_fixture::intent(&[1, 2, 3, 4], 1);
        let committed = removal_fixture::cleanup(&intent);
        let local = intent.current_configuration.members[2].identity.clone();
        let authority = AdmittedAuthority {
            local_identity: local,
            transition_kind: None,
            previous_configuration: None,
            current_configuration: intent.current_configuration.clone(),
            switchover_handoff: None,
            secondary_removal: Some(committed.evidence.clone()),
            scale_up: None,
        };
        let mut fresh = committed.current_only_write_quorum[0].clone();
        fresh.process_session_id = ProcessSessionId::new("primary-restarted");
        fresh.report_sequence = 1;
        fresh.verified_replication_lsn = 12;
        fresh.write_status = AccessStatus::Granted;
        fresh.retained_operation_id = Some(OperationId::new(format!(
            "availability:{}:grant-write",
            intent.current_configuration.configuration_id
        )));
        let mut tracker = QuorumTracker::default();
        tracker.configure(managed(authority.clone()), 10).unwrap();
        tracker.record_verified_local_progress(10);
        tracker
            .register_peer_session(
                fresh.identity.clone(),
                committed.current_only_write_quorum[0]
                    .process_session_id
                    .clone(),
            )
            .unwrap();
        tracker
            .register_peer_session(fresh.identity.clone(), fresh.process_session_id.clone())
            .unwrap();
        assert!(tracker.observe_secondary_removal(&fresh).is_err());
        for mutation in 0..10 {
            let mut invalid = fresh.clone();
            let mut proof = committed.clone();
            match mutation {
                0 => {
                    invalid.process_session_id = committed.current_only_write_quorum[0]
                        .process_session_id
                        .clone()
                }
                1 => invalid.resource_uid = crate::protocol::types::ResourceUid::new("other"),
                2 => invalid.identity = intent.target.clone(),
                3 => invalid.epoch.configuration_number += 1,
                4 => {
                    invalid.previous_configuration_id =
                        Some(intent.previous_configuration.configuration_id.clone())
                }
                5 => invalid.verified_replication_lsn = 9,
                6 => invalid.pending_operation_id = Some(OperationId::new("pending")),
                7 => invalid.retained_operation_id = Some(OperationId::new("unrelated")),
                8 => proof.evidence.preparation.boundary_lsn -= 1,
                _ => invalid.role = ReplicaRole::ActiveSecondary,
            }
            assert!(
                tracker
                    .observe_secondary_removal_progress(&invalid, &proof)
                    .is_err(),
                "mutation {mutation}"
            );
            assert!(!tracker.catch_up_complete());
        }
        let mut joint = authority;
        joint.previous_configuration = Some(intent.previous_configuration.clone());
        joint.transition_kind = Some(crate::protocol::types::TransitionKind::SecondaryScaleDown);
        let mut precommit = QuorumTracker::default();
        precommit.configure(managed(joint), 10).unwrap();
        precommit
            .register_peer_session(fresh.identity.clone(), fresh.process_session_id.clone())
            .unwrap();
        assert!(
            precommit
                .observe_secondary_removal_progress(&fresh, &committed)
                .is_err()
        );
        tracker
            .observe_secondary_removal_progress(&fresh, &committed)
            .unwrap();
        tracker
            .observe_secondary_removal_progress(&fresh, &committed)
            .unwrap();
        assert!(tracker.catch_up_complete());
        assert!(
            tracker.observe_secondary_removal(&fresh).is_err(),
            "live progress never becomes a transition witness"
        );
        tracker
            .observe_committed_secondary_removal(&committed.current_only_write_quorum[0])
            .unwrap();
        assert!(tracker.catch_up_complete());
        let mut conflicting = committed.clone();
        conflicting.current_only_write_quorum[0].report_sequence += 1;
        assert!(
            tracker
                .validate_secondary_removal_commit(&conflicting)
                .is_err()
        );
        assert!(
            tracker
                .register_peer_session(
                    fresh.identity,
                    committed.current_only_write_quorum[0]
                        .process_session_id
                        .clone()
                )
                .is_err()
        );
    }

    #[test]
    #[cfg(kuberic_workspace_tests)]
    fn committed_witness_cannot_revive_obsolete_credit_after_session_change() {
        let intent = removal_fixture::intent(&[1, 2, 3], 1);
        let authority = AdmittedAuthority {
            local_identity: intent.primary.clone(),
            transition_kind: None,
            previous_configuration: None,
            current_configuration: intent.current_configuration.clone(),
            switchover_handoff: None,
            secondary_removal: Some(removal_fixture::evidence(&intent)),
            scale_up: None,
        };
        let mut tracker = QuorumTracker::default();
        tracker.configure(managed(authority), 10).unwrap();
        tracker.record_verified_local_progress(10);
        let mut frozen =
            removal_fixture::witnesses(&intent, SecondaryRemovalStage::CurrentOnly)[1].clone();
        frozen.verified_replication_lsn = 11;
        tracker
            .observe_committed_secondary_removal(&frozen)
            .unwrap();
        assert!(!tracker.catch_up_complete());
        tracker
            .restore_committed_secondary_removal(&frozen)
            .unwrap();
        assert!(tracker.catch_up_complete());
        let session = ProcessSessionId::new("restarted");
        tracker
            .register_peer_session(frozen.identity.clone(), session.clone())
            .unwrap();
        tracker
            .observe_committed_secondary_removal(&frozen)
            .unwrap();
        tracker
            .restore_committed_secondary_removal(&frozen)
            .unwrap();
        assert!(
            !tracker.catch_up_complete(),
            "frozen authorization is not current-session credit"
        );
        let mut fresh = frozen.clone();
        fresh.process_session_id = session;
        fresh.report_sequence = 1;
        fresh.verified_replication_lsn = 10;
        for mutation in 0..6 {
            let mut wrong = fresh.clone();
            match mutation {
                0 => {
                    wrong.identity.agent_generation =
                        crate::protocol::types::AgentGeneration::new("substitute")
                }
                1 => {
                    wrong.current_configuration_id =
                        crate::protocol::types::ConfigurationId::new("other")
                }
                2 => {
                    wrong.previous_configuration_id =
                        Some(intent.previous_configuration.configuration_id.clone())
                }
                3 => wrong.verified_replication_lsn = 9,
                4 => wrong.resource_uid = crate::protocol::types::ResourceUid::new("other"),
                _ => wrong.epoch.configuration_number += 1,
            }
            assert!(tracker.observe_secondary_removal(&wrong).is_err());
            tracker
                .observe_committed_secondary_removal(&frozen)
                .unwrap();
            assert!(!tracker.catch_up_complete());
        }
        tracker.observe_secondary_removal(&fresh).unwrap();
        tracker
            .observe_committed_secondary_removal(&frozen)
            .unwrap();
        assert!(tracker.catch_up_complete());
        assert_eq!(tracker.witnesses.get(&fresh.identity), Some(&fresh));
        assert!(
            tracker
                .register_peer_session(frozen.identity.clone(), frozen.process_session_id.clone())
                .is_err()
        );
        assert!(tracker.observe_secondary_removal(&frozen).is_err());
        assert_eq!(tracker.witnesses.get(&fresh.identity), Some(&fresh));
    }
}
