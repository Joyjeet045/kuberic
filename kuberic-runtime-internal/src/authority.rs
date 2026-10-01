use async_trait::async_trait;
use bytes::Bytes;
pub use kuberic_protocol::types::{BuildAuthority, BuildAuthorityKind};
use kuberic_protocol::types::{
    ConfigurationDescriptor, ConfigurationId, EffectivePolicy, Epoch, OperationId, ReplicaIdentity,
    ReplicaRole, ScaleUpConfigurationEvidence, SwitchoverHandoff, TransitionKind,
};
use kuberic_protocol::types::{
    ReplicaRetirementReport, SecondaryRemovalEvidence, SecondaryRemovalPreparation,
    SecondaryScaleDownCleanup,
};
use kuberic_protocol::validation::{validate_configuration, validate_transition_relationship};
use kuberic_protocol::validation::{
    validate_replica_retirement, validate_scale_up, validate_scale_up_failover_evidence,
    validate_scale_up_failover_transition, validate_secondary_removal_evidence,
    validate_secondary_removal_preparation, validate_secondary_scale_down_cleanup,
};
use serde::{Deserialize, Serialize};

use crate::transport::{CopyItem, ReplicationAck, ReplicationItem};
use crate::{ContractError, Result};

pub fn validate_build_envelope(authority: &BuildAuthority, envelope: &CopyItem) -> Result<()> {
    authority
        .validate()
        .map_err(|error| ContractError::AuthorityMismatch(error.to_string()))?;
    if envelope.build_id != authority.build_id
        || envelope.sender != authority.source
        || envelope.receiver != authority.target
        || envelope.epoch != authority.current_configuration.epoch
        || envelope.current_configuration_id != authority.current_configuration.configuration_id
        || envelope.replication_boundary_lsn != authority.replication_boundary_lsn
    {
        return Err(ContractError::AuthorityMismatch(
            "copy item does not match durable build authority".to_string(),
        ));
    }
    if (envelope.final_item != envelope.catch_up_boundary_lsn.is_some())
        || (envelope.final_item && envelope.committed_lsn != authority.replication_boundary_lsn)
        || envelope
            .catch_up_boundary_lsn
            .is_some_and(|boundary| boundary < authority.replication_boundary_lsn)
    {
        return Err(ContractError::AuthorityMismatch(
            "copy item has invalid post-enumeration catch-up authority".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AuthorityFence {
    pub epoch: Epoch,
    pub previous_configuration_id: Option<ConfigurationId>,
    pub current_configuration_id: ConfigurationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationProgress {
    pub fence: AuthorityFence,
    pub verified_lsn: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableBuildProgress {
    pub authority: BuildAuthority,
    pub last_sequence: u64,
    pub durable_lsn: i64,
    pub completed: bool,
    #[serde(default)]
    pub catch_up_boundary_lsn: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalWritePhase {
    Reserved,
    Registered,
    Committed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableLocalWrite {
    pub operation_id: OperationId,
    pub lsn: i64,
    #[serde(default)]
    pub committed_lsn: i64,
    pub data: Bytes,
    pub phase: LocalWritePhase,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmittedAuthority {
    pub local_identity: ReplicaIdentity,
    pub transition_kind: Option<TransitionKind>,
    pub previous_configuration: Option<ConfigurationDescriptor>,
    pub current_configuration: ConfigurationDescriptor,
    #[serde(default)]
    pub switchover_handoff: Option<SwitchoverHandoff>,
    #[serde(default)]
    pub secondary_removal: Option<SecondaryRemovalEvidence>,
    #[serde(default)]
    pub scale_up: Option<Box<ScaleUpConfigurationEvidence>>,
}

impl AdmittedAuthority {
    pub fn is_current_only_completion_of(&self, existing: &Self) -> bool {
        self.local_identity == existing.local_identity
            && self.current_configuration == existing.current_configuration
            && existing.previous_configuration.is_some()
            && self.previous_configuration.is_none()
            && self.transition_kind.is_none()
            && self.switchover_handoff == existing.switchover_handoff
            && self.scale_up == existing.scale_up
            && match (&self.secondary_removal, &existing.secondary_removal) {
                (Some(next), Some(old)) => {
                    next.preparation == old.preparation
                        && next.previous_read_quorum == old.previous_read_quorum
                        && (old.reduced_write_quorum.is_empty()
                            || next.reduced_write_quorum == old.reduced_write_quorum)
                        && validate_secondary_removal_evidence(next, true).is_ok()
                }
                (None, None) => true,
                _ => false,
            }
    }

    pub fn fence(&self) -> AuthorityFence {
        AuthorityFence {
            epoch: self.current_configuration.epoch,
            previous_configuration_id: self
                .previous_configuration
                .as_ref()
                .map(|configuration| configuration.configuration_id.clone()),
            current_configuration_id: self.current_configuration.configuration_id.clone(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if let Some(evidence) = &self.secondary_removal {
            validate_secondary_removal_evidence(evidence, self.previous_configuration.is_none())
                .map_err(|error| ContractError::AuthorityMismatch(error.to_string()))?;
            let intent = &evidence.preparation.intent;
            if self.current_configuration != intent.current_configuration
                || self.previous_configuration.as_ref()
                    != self
                        .previous_configuration
                        .as_ref()
                        .map(|_| &intent.previous_configuration)
                || self.transition_kind
                    != self
                        .previous_configuration
                        .as_ref()
                        .map(|_| TransitionKind::SecondaryScaleDown)
                || self.switchover_handoff.is_some()
                || self.scale_up.is_some()
                || !intent
                    .current_configuration
                    .members
                    .iter()
                    .any(|m| m.identity == self.local_identity)
            {
                return Err(ContractError::AuthorityMismatch(
                    "reduction differs from exact prepared authority".into(),
                ));
            }
            return Ok(());
        }
        if let Some(evidence) = self.scale_up.as_deref() {
            let intent = evidence.intent();
            validate_scale_up(intent)
                .map_err(|error| ContractError::AuthorityMismatch(error.to_string()))?;
            if self.secondary_removal.is_some() || self.switchover_handoff.is_some() {
                return Err(ContractError::AuthorityMismatch(
                    "scale-up authority cannot carry removal or switchover evidence".into(),
                ));
            }
            match evidence {
                ScaleUpConfigurationEvidence::Admission { .. } => {
                    if self.current_configuration != intent.current_configuration
                        || self.previous_configuration.as_ref()
                            != self
                                .previous_configuration
                                .as_ref()
                                .map(|_| &intent.previous_configuration)
                        || self.transition_kind
                            != self
                                .previous_configuration
                                .as_ref()
                                .map(|_| TransitionKind::ScaleUp)
                    {
                        return Err(ContractError::AuthorityMismatch(
                            "admitted authority differs from exact scale-up intent".into(),
                        ));
                    }
                }
                ScaleUpConfigurationEvidence::Failover { evidence } => {
                    validate_scale_up_failover_evidence(evidence)
                        .map_err(|error| ContractError::AuthorityMismatch(error.to_string()))?;
                    validate_scale_up_failover_transition(
                        evidence,
                        &self.current_configuration,
                        &intent.current_policy,
                    )
                    .map_err(|error| ContractError::AuthorityMismatch(error.to_string()))?;
                    if self.previous_configuration.as_ref()
                        != self
                            .previous_configuration
                            .as_ref()
                            .map(|_| &intent.previous_configuration)
                        || self.transition_kind
                            != self
                                .previous_configuration
                                .as_ref()
                                .map(|_| TransitionKind::Failover)
                    {
                        return Err(ContractError::AuthorityMismatch(
                            "admitted failover differs from carried scale-up intent".into(),
                        ));
                    }
                }
            }
            if !self
                .current_configuration
                .members
                .iter()
                .any(|member| member.identity == self.local_identity)
            {
                return Err(ContractError::AuthorityMismatch(
                    "local identity is outside admitted scale-up authority".into(),
                ));
            }
            return Ok(());
        }
        validate_configuration(&self.current_configuration, None)
            .map_err(|error| ContractError::AuthorityMismatch(error.to_string()))?;
        let policy = EffectivePolicy::fixed(self.current_configuration.members.len() as u32, 0)
            .ok_or_else(|| {
                ContractError::AuthorityMismatch("configuration must contain members".to_string())
            })?;
        match (self.previous_configuration.as_ref(), self.transition_kind) {
            (
                Some(previous),
                Some(
                    kind @ (TransitionKind::Replacement
                    | TransitionKind::Failover
                    | TransitionKind::PlannedSwitchover),
                ),
            ) => {
                validate_transition_relationship(
                    kind,
                    Some(previous),
                    &self.current_configuration,
                    &policy,
                )
                .map_err(|error| ContractError::AuthorityMismatch(error.to_string()))?;
            }
            (None, Some(TransitionKind::Bootstrap)) | (None, None) => {}
            (Some(_), _) => {
                return Err(ContractError::AuthorityMismatch(
                    "Previous Configuration requires replacement, failover, or planned switchover authority"
                        .to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(ContractError::AuthorityMismatch(
                    "non-bootstrap transition requires a Previous Configuration".to_string(),
                ));
            }
        }
        if let Some(handoff) = &self.switchover_handoff {
            let previous_relationship_valid =
                self.previous_configuration.as_ref().is_none_or(|previous| {
                    let source_is_primary = previous.members.iter().any(|member| {
                        member.identity == handoff.source && member.role == ReplicaRole::Primary
                    });
                    let target_is_primary = previous.members.iter().any(|member| {
                        member.identity == handoff.target && member.role == ReplicaRole::Primary
                    });
                    if source_is_primary {
                        previous.configuration_id == handoff.starting_configuration_id
                            && previous.epoch == handoff.starting_epoch
                            && self.current_configuration.primary_id == handoff.target.replica_id
                    } else {
                        target_is_primary
                            && self.current_configuration.primary_id == handoff.source.replica_id
                            && previous.epoch.data_loss_number
                                == handoff.starting_epoch.data_loss_number
                            && previous.epoch.configuration_number
                                > handoff.starting_epoch.configuration_number
                    }
                });
            if (self.transition_kind != Some(TransitionKind::PlannedSwitchover)
                && self.previous_configuration.is_some())
                || handoff.preparation_generation == 0
                || !previous_relationship_valid
                || handoff.starting_epoch.data_loss_number
                    != self.current_configuration.epoch.data_loss_number
                || handoff.starting_epoch.configuration_number
                    >= self.current_configuration.epoch.configuration_number
                || handoff.source == handoff.target
                || !self
                    .current_configuration
                    .members
                    .iter()
                    .any(|member| member.identity == handoff.source)
                || !self
                    .current_configuration
                    .members
                    .iter()
                    .any(|member| member.identity == handoff.target)
                || (self.current_configuration.primary_id != handoff.source.replica_id
                    && self.current_configuration.primary_id != handoff.target.replica_id)
            {
                return Err(ContractError::AuthorityMismatch(
                    "switchover handoff contradicts admitted authority".to_string(),
                ));
            }
        } else if self.transition_kind == Some(TransitionKind::PlannedSwitchover) {
            return Err(ContractError::AuthorityMismatch(
                "planned switchover authority requires its handoff certificate".to_string(),
            ));
        }
        if !self.contains_member(&self.local_identity) {
            return Err(ContractError::AuthorityMismatch(
                "local identity is outside admitted authority".to_string(),
            ));
        }
        Ok(())
    }

    pub fn primary_identity(&self) -> &ReplicaIdentity {
        &self
            .current_configuration
            .members
            .iter()
            .find(|member| {
                member.identity.replica_id == self.current_configuration.primary_id
                    && member.role == ReplicaRole::Primary
            })
            .expect("validated configuration has one primary")
            .identity
    }

    pub fn contains_member(&self, identity: &ReplicaIdentity) -> bool {
        self.current_configuration
            .members
            .iter()
            .any(|member| &member.identity == identity)
            || self
                .previous_configuration
                .as_ref()
                .is_some_and(|previous| {
                    previous
                        .members
                        .iter()
                        .any(|member| &member.identity == identity)
                })
    }

    pub fn local_role(&self) -> ReplicaRole {
        self.current_configuration
            .members
            .iter()
            .find(|member| member.identity == self.local_identity)
            .or_else(|| {
                self.previous_configuration.as_ref().and_then(|previous| {
                    previous
                        .members
                        .iter()
                        .find(|member| member.identity == self.local_identity)
                })
            })
            .expect("validated local identity belongs to authority")
            .role
    }

    pub fn validate_envelope(&self, envelope: &ReplicationItem) -> Result<()> {
        self.validate_fence(
            &envelope.sender,
            &envelope.receiver,
            envelope.epoch,
            envelope.previous_configuration_id.as_ref(),
            &envelope.current_configuration_id,
        )
    }

    pub fn validate_acknowledgement(&self, acknowledgement: &ReplicationAck) -> Result<()> {
        self.validate_fence(
            &acknowledgement.sender,
            &acknowledgement.receiver,
            acknowledgement.epoch,
            acknowledgement.previous_configuration_id.as_ref(),
            &acknowledgement.current_configuration_id,
        )
    }

    fn validate_fence(
        &self,
        sender: &ReplicaIdentity,
        receiver: &ReplicaIdentity,
        epoch: kuberic_protocol::types::Epoch,
        previous_configuration_id: Option<&kuberic_protocol::types::ConfigurationId>,
        current_configuration_id: &kuberic_protocol::types::ConfigurationId,
    ) -> Result<()> {
        if sender != self.primary_identity() {
            return Err(ContractError::AuthorityMismatch(
                "replication sender is not the exact admitted primary".to_string(),
            ));
        }
        if !self.contains_member(receiver) {
            return Err(ContractError::AuthorityMismatch(
                "replication receiver is outside admitted authority".to_string(),
            ));
        }
        if epoch != self.current_configuration.epoch
            || current_configuration_id != &self.current_configuration.configuration_id
            || previous_configuration_id
                != self
                    .previous_configuration
                    .as_ref()
                    .map(|configuration| &configuration.configuration_id)
        {
            return Err(ContractError::AuthorityMismatch(
                "replication epoch or configuration fence is stale".to_string(),
            ));
        }
        Ok(())
    }
}

/// Exact retirement authority, persisted first as intent and then as a terminal
/// tombstone. A started record does not yet attest that application Close returned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetiredAuthority {
    pub committed: SecondaryScaleDownCleanup,
    pub report: ReplicaRetirementReport,
}

impl RetiredAuthority {
    pub fn validate(&self, local: &ReplicaIdentity) -> Result<()> {
        validate_secondary_scale_down_cleanup(&self.committed)
            .and_then(|_| validate_replica_retirement(&self.report))
            .map_err(|error| ContractError::AuthorityMismatch(error.to_string()))?;
        if self.report.intent != self.committed.evidence.preparation.intent
            || &self.report.intent.target != local
            || self
                .committed
                .retirement
                .as_ref()
                .is_some_and(|r| r != &self.report)
        {
            return Err(ContractError::AuthorityMismatch(
                "retirement target or evidence differs".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
pub trait ReplicaAuthorityStore: Send + Sync {
    async fn load(&self) -> Result<Option<AdmittedAuthority>>;

    async fn admit(&self, authority: &AdmittedAuthority) -> Result<()>;

    async fn load_secondary_removal(&self) -> Result<Option<SecondaryRemovalPreparation>> {
        Ok(None)
    }

    /// Replay is exact. A later preparation may replace a completed predecessor
    /// only with a starting epoch at least as new as its reduced configuration.
    async fn record_secondary_removal(
        &self,
        preparation: &SecondaryRemovalPreparation,
    ) -> Result<()> {
        validate_secondary_removal_preparation(preparation)
            .map_err(|error| ContractError::AuthorityMismatch(error.to_string()))?;
        Err(ContractError::Persistence(
            "secondary-removal persistence is unavailable".into(),
        ))
    }

    async fn load_retired_authority(&self) -> Result<Option<RetiredAuthority>> {
        Ok(None)
    }

    /// Must be checked before Open or active authority restoration.
    async fn load_retirement_started(&self) -> Result<Option<RetiredAuthority>> {
        Err(ContractError::Persistence(
            "retirement-started persistence is unavailable".into(),
        ))
    }

    /// Atomically validate the exact local target, resource, committed cleanup and
    /// installed authority, reject conflicts, and permanently fence active admission.
    /// Exact replay (including an already retired authority) must be idempotent.
    async fn record_retirement_started(&self, _authority: &RetiredAuthority) -> Result<()> {
        Err(ContractError::Persistence(
            "retirement-started persistence is unavailable".into(),
        ))
    }

    async fn load_secondary_removal_commit(&self) -> Result<Option<SecondaryScaleDownCleanup>> {
        Ok(None)
    }

    async fn record_secondary_removal_commit(
        &self,
        _committed: &SecondaryScaleDownCleanup,
    ) -> Result<()> {
        Err(ContractError::Persistence(
            "secondary-removal commit persistence is unavailable".into(),
        ))
    }

    /// Must match any started record, atomically write the tombstone, remove active
    /// authority and clear the started record. Exact retired replay is idempotent.
    async fn retire(&self, _authority: &RetiredAuthority) -> Result<()> {
        Err(ContractError::Persistence(
            "retirement persistence is unavailable".into(),
        ))
    }
}

#[async_trait]
pub trait ReplicationProgressStore: Send + Sync {
    async fn load_replication_progress(
        &self,
        fence: &AuthorityFence,
    ) -> Result<Option<ReplicationProgress>>;

    async fn load_configuration_progress(
        &self,
        epoch: Epoch,
        current_configuration_id: &ConfigurationId,
    ) -> Result<Option<ReplicationProgress>>;

    async fn record_replication_progress(&self, progress: &ReplicationProgress) -> Result<()>;
}

#[async_trait]
pub trait LocalWriteJournal: Send + Sync {
    async fn load_local_write(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<DurableLocalWrite>>;

    async fn load_local_writes(&self) -> Result<Vec<DurableLocalWrite>>;

    async fn record_local_write(&self, write: &DurableLocalWrite) -> Result<()>;

    async fn reset_local_writes_after_data_loss(&self, committed_lsn: i64) -> Result<()>;
}

#[async_trait]
pub trait BuildAuthorityStore: Send + Sync {
    async fn load_build(&self, build_id: &OperationId) -> Result<Option<BuildAuthority>>;

    async fn load_builds(&self) -> Result<Vec<BuildAuthority>> {
        Ok(Vec::new())
    }

    async fn admit_build(&self, authority: &BuildAuthority) -> Result<()>;

    async fn select_build(&self, _authority: &BuildAuthority) -> Result<BuildSelection> {
        Err(ContractError::AuthorityMismatch(
            "durable build selection is unavailable".into(),
        ))
    }

    async fn load_build_selection(
        &self,
        _target: &ReplicaIdentity,
    ) -> Result<Option<BuildSelection>> {
        Ok(None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Agent-owned selection of one immutable build for a logical target slot.
/// Superseded selections cannot be restored by delayed receipts or restarts.
pub struct BuildSelection {
    pub authority: BuildAuthority,
    pub generation: u64,
}

#[async_trait]
pub trait BuildProgressStore: Send + Sync {
    async fn load_build_progress(
        &self,
        build_id: &OperationId,
    ) -> Result<Option<DurableBuildProgress>>;

    async fn record_build_progress(&self, progress: &DurableBuildProgress) -> Result<()>;

    async fn record_selected_build_progress(
        &self,
        _selection: &BuildSelection,
        _progress: &DurableBuildProgress,
    ) -> Result<()> {
        Err(ContractError::AuthorityMismatch(
            "durable build selection is unavailable".into(),
        ))
    }
}

pub trait AuthorityStore:
    ReplicaAuthorityStore
    + ReplicationProgressStore
    + LocalWriteJournal
    + BuildAuthorityStore
    + BuildProgressStore
{
}

impl<T> AuthorityStore for T where
    T: ReplicaAuthorityStore
        + ReplicationProgressStore
        + LocalWriteJournal
        + BuildAuthorityStore
        + BuildProgressStore
{
}

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../kuberic-protocol/tests/support/secondary_scale_down.rs"]
mod removal_fixture;

#[cfg(test)]
mod tests {
    use super::*;
    use kuberic_protocol::types::{
        AccessStatus, ProcessSessionId, ScaleUpFailoverEvidence, ScaleUpStage, ScaleUpWitness,
    };

    fn identity(id: i64) -> ReplicaIdentity {
        ReplicaIdentity {
            replica_id: kuberic_protocol::types::ReplicaId::new(id),
            instance_id: kuberic_protocol::types::ReplicaInstanceId::new(format!("pod-{id}")),
            agent_generation: kuberic_protocol::types::AgentGeneration::new(format!("gen-{id}")),
        }
    }

    fn scale_up_authority() -> AdmittedAuthority {
        let previous_policy = EffectivePolicy::fixed(1, 30).unwrap();
        let current_policy = EffectivePolicy::fixed(2, 30).unwrap();
        let primary = identity(1);
        let target = identity(2);
        let previous = ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            primary.replica_id,
            vec![kuberic_protocol::types::ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            }],
            previous_policy.write_quorum,
        );
        let current = ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            primary.replica_id,
            vec![
                kuberic_protocol::types::ConfigurationMember {
                    identity: primary.clone(),
                    role: ReplicaRole::Primary,
                },
                kuberic_protocol::types::ConfigurationMember {
                    identity: target.clone(),
                    role: ReplicaRole::ActiveSecondary,
                },
            ],
            current_policy.write_quorum,
        );
        let mut intent = kuberic_protocol::types::ScaleUpIntent {
            operation_id: OperationId::default(),
            resource_uid: kuberic_protocol::types::ResourceUid::new("set"),
            spec_generation: 2,
            desired_replicas: 2,
            previous_configuration: previous.clone(),
            current_configuration: current.clone(),
            previous_policy,
            current_policy,
            primary: primary.clone(),
            target,
            build_id: OperationId::new("build"),
            snapshot_boundary_lsn: 0,
            catch_up_boundary_lsn: 2,
        };
        intent.operation_id = intent.expected_operation_id();
        AdmittedAuthority {
            local_identity: primary,
            transition_kind: Some(TransitionKind::ScaleUp),
            previous_configuration: Some(previous),
            current_configuration: current,
            switchover_handoff: None,
            secondary_removal: None,
            scale_up: Some(Box::new(ScaleUpConfigurationEvidence::Admission { intent })),
        }
    }

    #[test]
    fn scale_up_authority_preserves_dual_policy_and_current_only_completion() {
        let authority = scale_up_authority();
        authority.validate().unwrap();
        let mut completed = authority.clone();
        completed.previous_configuration = None;
        completed.transition_kind = None;
        completed.validate().unwrap();
        assert!(completed.is_current_only_completion_of(&authority));

        let mut wrong = authority;
        wrong.current_configuration.members.pop();
        assert!(wrong.validate().is_err());
    }

    #[test]
    fn scale_up_failover_preserves_both_recovery_configurations() {
        let authority = scale_up_authority();
        let ScaleUpConfigurationEvidence::Admission { intent } =
            *authority.scale_up.clone().unwrap()
        else {
            unreachable!()
        };
        let witness = |identity: ReplicaIdentity, sequence: u64| ScaleUpWitness {
            resource_uid: intent.resource_uid.clone(),
            role: intent
                .current_configuration
                .members
                .iter()
                .find(|member| member.identity == identity)
                .unwrap()
                .role,
            retained_operation_id: Some(intent.command_operation_id(
                ScaleUpStage::PreviousCurrent,
                &identity,
                &intent.current_configuration,
            )),
            identity,
            process_session_id: ProcessSessionId::new(format!("session-{sequence}")),
            report_sequence: sequence,
            epoch: intent.current_configuration.epoch,
            previous_configuration_id: Some(intent.previous_configuration.configuration_id.clone()),
            current_configuration_id: intent.current_configuration.configuration_id.clone(),
            verified_replication_lsn: intent.catch_up_boundary_lsn,
            write_status: AccessStatus::ReconfigurationPending,
            pending_operation_id: None,
        };
        let mut members = intent.current_configuration.members.clone();
        for member in &mut members {
            member.role = if member.identity == intent.target {
                ReplicaRole::Primary
            } else {
                ReplicaRole::ActiveSecondary
            };
        }
        let failover = ConfigurationDescriptor::new(
            Epoch::new(0, 3),
            intent.target.replica_id,
            members,
            intent.current_policy.write_quorum,
        );
        let evidence = ScaleUpFailoverEvidence {
            provisional_configuration: failover.clone(),
            previous_read_quorum: vec![witness(intent.primary.clone(), 1)],
            current_read_quorum: vec![witness(intent.target.clone(), 2)],
            final_election: None,
            intent: intent.clone(),
        };
        let authority = AdmittedAuthority {
            local_identity: intent.target.clone(),
            transition_kind: Some(TransitionKind::Failover),
            previous_configuration: Some(intent.previous_configuration.clone()),
            current_configuration: failover,
            switchover_handoff: None,
            secondary_removal: None,
            scale_up: Some(Box::new(ScaleUpConfigurationEvidence::Failover {
                evidence,
            })),
        };
        authority.validate().unwrap();
        let mut completed = authority.clone();
        completed.previous_configuration = None;
        completed.transition_kind = None;
        completed.validate().unwrap();
        assert!(completed.is_current_only_completion_of(&authority));
    }

    #[test]
    fn reduction_requires_independent_policies_and_frozen_old_read_evidence() {
        for size in 2..=5 {
            let intent = removal_fixture::intent(&(1..=size).collect::<Vec<_>>(), 1);
            let mut evidence = removal_fixture::evidence(&intent);
            evidence.reduced_write_quorum.clear();
            let mut authority = AdmittedAuthority {
                local_identity: intent.primary.clone(),
                transition_kind: Some(TransitionKind::SecondaryScaleDown),
                previous_configuration: Some(intent.previous_configuration.clone()),
                current_configuration: intent.current_configuration.clone(),
                switchover_handoff: None,
                secondary_removal: Some(evidence),
                scale_up: None,
            };
            authority.validate().unwrap();
            let good = authority.clone();
            authority
                .secondary_removal
                .as_mut()
                .unwrap()
                .previous_read_quorum
                .truncate(intent.previous_policy.read_quorum as usize - 1);
            assert!(authority.validate().is_err());
            authority = good.clone();
            authority
                .secondary_removal
                .as_mut()
                .unwrap()
                .preparation
                .intent
                .current_policy = intent.previous_policy.clone();
            assert!(authority.validate().is_err());
            let mut completed = good.clone();
            completed.previous_configuration = None;
            completed.transition_kind = None;
            assert!(
                completed.validate().is_err(),
                "current-only needs the frozen reduced write quorum"
            );
            completed.secondary_removal = Some(removal_fixture::evidence(&intent));
            completed.validate().unwrap();
            assert!(completed.is_current_only_completion_of(&good));
            completed
                .secondary_removal
                .as_mut()
                .unwrap()
                .previous_read_quorum[0]
                .report_sequence += 1;
            assert!(!completed.is_current_only_completion_of(&good));
        }
    }

    #[test]
    fn retirement_binds_terminal_postconditions_and_exact_committed_target() {
        let intent = removal_fixture::intent(&[1, 2, 3], 1);
        let retired = RetiredAuthority {
            committed: removal_fixture::cleanup(&intent),
            report: removal_fixture::retirement(&intent),
        };
        retired.validate(&intent.target).unwrap();
        assert!(retired.validate(&intent.primary).is_err());
        for mutate in [
            |r: &mut RetiredAuthority| r.report.application_closed = false,
            |r: &mut RetiredAuthority| r.report.peers_fenced = false,
            |r: &mut RetiredAuthority| r.report.role = ReplicaRole::ActiveSecondary,
            |r: &mut RetiredAuthority| {
                r.report.write_status = kuberic_protocol::types::AccessStatus::Granted
            },
            |r: &mut RetiredAuthority| r.committed.current_only_write_quorum.clear(),
        ] {
            let mut invalid = retired.clone();
            mutate(&mut invalid);
            assert!(invalid.validate(&intent.target).is_err());
        }
    }
}
