use crate::authority::{AuthorityFence, BuildAuthorityKind};
use crate::protocol::types::{
    ConfigurationDescriptor, ReplicaIdentity, ReplicaRole, ScaleUpConfigurationEvidence,
    SecondaryRemovalEvidence, SwitchoverHandoff,
};
use crate::transport::{ReplicationAck, ReplicationItem};
use crate::{Result, RuntimeError};

/// Engine-owned projection of the controller's durable authority.
///
/// This deliberately omits transition stage, operation identity, effect
/// sequence, and completion evidence. The host validates the durable candidate,
/// constructs this projection for native preparation, then persists the exact
/// authority before the prepared configuration can commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedReplicaConfiguration {
    pub(crate) local_identity: ReplicaIdentity,
    pub(crate) previous_configuration: Option<ConfigurationDescriptor>,
    pub(crate) current_configuration: ConfigurationDescriptor,
    pub(crate) switchover_handoff: Option<SwitchoverHandoff>,
    pub(crate) secondary_removal: Option<SecondaryRemovalEvidence>,
    pub(crate) scale_up: Option<Box<ScaleUpConfigurationEvidence>>,
    pub(crate) build_kind: BuildAuthorityKind,
}

impl ManagedReplicaConfiguration {
    pub(crate) fn validate(&self) -> Result<()> {
        if !self.contains_member(&self.local_identity) {
            return Err(RuntimeError::AuthorityMismatch(
                "local identity is outside executable replication configuration".into(),
            ));
        }
        if let Some(previous) = &self.previous_configuration
            && previous.epoch >= self.current_configuration.epoch
        {
            return Err(RuntimeError::AuthorityMismatch(
                "executable replication configuration did not advance its epoch".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn is_current_only_completion_of(&self, existing: &Self) -> bool {
        self.local_identity == existing.local_identity
            && self.current_configuration == existing.current_configuration
            && existing.previous_configuration.is_some()
            && self.previous_configuration.is_none()
            && self.switchover_handoff == existing.switchover_handoff
            && self.scale_up == existing.scale_up
            && match (&self.secondary_removal, &existing.secondary_removal) {
                (Some(next), Some(old)) => {
                    next.preparation == old.preparation
                        && next.previous_read_quorum == old.previous_read_quorum
                        && (old.reduced_write_quorum.is_empty()
                            || next.reduced_write_quorum == old.reduced_write_quorum)
                }
                (None, None) => true,
                _ => false,
            }
    }

    pub(crate) fn fence(&self) -> AuthorityFence {
        AuthorityFence {
            epoch: self.current_configuration.epoch,
            previous_configuration_id: self
                .previous_configuration
                .as_ref()
                .map(|configuration| configuration.configuration_id.clone()),
            current_configuration_id: self.current_configuration.configuration_id.clone(),
        }
    }

    pub(crate) fn primary_identity(&self) -> &ReplicaIdentity {
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

    pub(crate) fn contains_member(&self, identity: &ReplicaIdentity) -> bool {
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

    pub(crate) fn local_role(&self) -> ReplicaRole {
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
            .expect("validated local identity belongs to configuration")
            .role
    }

    pub(crate) fn validate_envelope(&self, envelope: &ReplicationItem) -> Result<()> {
        self.validate_fence(
            &envelope.sender,
            &envelope.receiver,
            envelope.epoch,
            envelope.previous_configuration_id.as_ref(),
            &envelope.current_configuration_id,
        )
    }

    pub(crate) fn validate_acknowledgement(&self, acknowledgement: &ReplicationAck) -> Result<()> {
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
        epoch: crate::protocol::types::Epoch,
        previous_configuration_id: Option<&crate::protocol::types::ConfigurationId>,
        current_configuration_id: &crate::protocol::types::ConfigurationId,
    ) -> Result<()> {
        if sender != self.primary_identity() {
            return Err(RuntimeError::AuthorityMismatch(
                "replication sender is not the exact configured primary".into(),
            ));
        }
        if !self.contains_member(receiver) {
            return Err(RuntimeError::AuthorityMismatch(
                "replication receiver is outside executable configuration".into(),
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
            return Err(RuntimeError::AuthorityMismatch(
                "replication epoch or configuration fence is stale".into(),
            ));
        }
        Ok(())
    }
}
