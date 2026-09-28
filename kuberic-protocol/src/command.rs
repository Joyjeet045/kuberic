//! Fenced protocol commands and declarative Kubernetes changes.

use serde::{Deserialize, Serialize};

use crate::types::{
    AgentGeneration, BuildAuthority, ConfigurationDescriptor, EffectivePolicy, Epoch,
    InitializationId, OperationId, PodUid, ProvisioningIntent, PvcUid, ReplicaId, ReplicaIdentity,
    ReplicaInstanceId, ResourceUid, SwitchoverHandoff, SwitchoverPreparationId,
    SwitchoverRequestId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EffectClass {
    ConvergentEnsure,
    ReconfigurationAction,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeAgentStore {
    pub initialization_id: InitializationId,
    pub resource_uid: ResourceUid,
    pub local_replica_id: ReplicaId,
    pub expected_instance_id: ReplicaInstanceId,
    pub expected_pod_uid: PodUid,
    pub expected_pvc_uid: PvcUid,
    pub assigned_agent_generation: AgentGeneration,
    pub effective_policy: EffectivePolicy,
    pub bootstrap_configuration: ConfigurationDescriptor,
    pub provisioning: Option<ProvisioningIntent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnsureConfiguration {
    pub operation_id: OperationId,
    pub previous_configuration: Option<ConfigurationDescriptor>,
    pub current_configuration: ConfigurationDescriptor,
    pub previous_epoch: Option<Epoch>,
    pub current_epoch: Epoch,
    pub effective_policy: EffectivePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_policy: Option<EffectivePolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secondary_removal_evidence: Option<crate::types::SecondaryRemovalEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale_up_evidence: Option<Box<crate::types::ScaleUpConfigurationEvidence>>,
    pub local_replica_id: ReplicaId,
    pub expected_instance_id: ReplicaInstanceId,
    pub expected_agent_generation: AgentGeneration,
    pub transition_kind: crate::types::TransitionKind,
    pub failover_safe_lsn: Option<i64>,
    pub primary_write_status: crate::types::AccessStatus,
    pub current_only: bool,
    #[serde(default)]
    pub retire_build_ids: Vec<OperationId>,
    #[serde(default)]
    pub switchover_handoff: Option<SwitchoverHandoff>,
    #[serde(default)]
    pub retire_switchover_preparation_ids: Vec<SwitchoverPreparationId>,
}

impl EnsureConfiguration {
    /// Exact, write-closed retirement without admitting a newer authority.
    pub fn is_switchover_restoration(&self) -> bool {
        self.transition_kind == crate::types::TransitionKind::PlannedSwitchover
            && !self.current_only
            && self.previous_configuration.is_none()
            && self.previous_epoch.is_none()
            && self.primary_write_status == crate::types::AccessStatus::ReconfigurationPending
            && self.failover_safe_lsn.is_none()
            && self.retire_build_ids.is_empty()
            && self.current_epoch == self.current_configuration.epoch
            && self.current_configuration.primary_id == self.local_replica_id
            && self.retire_switchover_preparation_ids.len() == 1
            && !self.retire_switchover_preparation_ids[0]
                .operation_id
                .is_empty()
            && self.retire_switchover_preparation_ids[0].generation > 0
            && self.switchover_handoff.as_ref().is_none_or(|handoff| {
                self.current_epoch == handoff.starting_epoch
                    && self.current_configuration.epoch == handoff.starting_epoch
                    && self.current_configuration.configuration_id
                        == handoff.starting_configuration_id
                    && self.current_configuration.primary_id == handoff.source.replica_id
                    && self.local_replica_id == handoff.source.replica_id
                    && self.expected_instance_id == handoff.source.instance_id
                    && self.expected_agent_generation == handoff.source.agent_generation
                    && self.retire_switchover_preparation_ids == [handoff.preparation()]
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrepareSwitchover {
    pub preparation_generation: u64,
    pub operation_id: OperationId,
    pub request_id: SwitchoverRequestId,
    pub local_replica_id: ReplicaId,
    pub expected_instance_id: ReplicaInstanceId,
    pub expected_agent_generation: AgentGeneration,
    pub source: ReplicaIdentity,
    pub target: ReplicaIdentity,
    pub current_configuration: ConfigurationDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnsureReplicaBuild {
    pub operation_id: OperationId,
    pub local_replica_id: ReplicaId,
    pub expected_instance_id: ReplicaInstanceId,
    pub expected_agent_generation: AgentGeneration,
    pub target: ReplicaIdentity,
    pub authority: Option<BuildAuthority>,
    pub source_session_id: Option<crate::types::ProcessSessionId>,
    #[serde(default)]
    pub retire: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrepareSecondaryRemoval {
    pub operation_id: OperationId,
    pub local_replica_id: ReplicaId,
    pub expected_instance_id: ReplicaInstanceId,
    pub expected_agent_generation: AgentGeneration,
    pub intent: crate::types::SecondaryScaleDownIntent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetireReplica {
    pub operation_id: OperationId,
    pub local_replica_id: ReplicaId,
    pub expected_instance_id: ReplicaInstanceId,
    pub expected_agent_generation: AgentGeneration,
    pub committed: crate::types::SecondaryScaleDownCleanup,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
/// Publish already-accepted current-only evidence locally before stable access.
pub struct AcceptSecondaryRemovalCommit {
    pub operation_id: OperationId,
    pub target: ReplicaIdentity,
    pub committed: crate::types::SecondaryScaleDownCleanup,
    /// Clear a retained secondary's historical local fence without quorum credit.
    #[serde(default)]
    pub local_recovery: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
/// One fenced, idempotent authority command issued after a full observation.
pub enum ProtocolCommand {
    InitializeAgentStore(Box<InitializeAgentStore>),
    PrepareSwitchover(Box<PrepareSwitchover>),
    EnsureConfiguration(Box<EnsureConfiguration>),
    EnsureReplicaBuild(Box<EnsureReplicaBuild>),
    PrepareSecondaryRemoval(Box<PrepareSecondaryRemoval>),
    RetireReplica(Box<RetireReplica>),
    AcceptSecondaryRemovalCommit(Box<AcceptSecondaryRemovalCommit>),
}

impl ProtocolCommand {
    pub fn effect_class(&self) -> EffectClass {
        match self {
            Self::InitializeAgentStore(_) => EffectClass::ConvergentEnsure,
            Self::PrepareSwitchover(_)
            | Self::PrepareSecondaryRemoval(_)
            | Self::RetireReplica(_)
            | Self::AcceptSecondaryRemovalCommit(_)
            | Self::EnsureConfiguration(_)
            | Self::EnsureReplicaBuild(_) => EffectClass::ReconfigurationAction,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
/// Idempotent Kubernetes or status convergence performed by the controller.
pub enum KubernetesChange {
    EnsureReplicaSupport,
    EnsureReplicaScaffolding {
        replica_ids: Vec<ReplicaId>,
    },
    EnsureReplacementScaffolding {
        replica_id: ReplicaId,
        replacing: ReplicaIdentity,
    },
    DeleteReplicaScaffolding {
        pod_name: Option<String>,
        pod_uid: Option<PodUid>,
        pvc_name: Option<String>,
        pvc_uid: Option<PvcUid>,
    },
    DeleteReplicaEndpoint {
        identity: ReplicaIdentity,
    },
    DeleteExactPod {
        pod_name: String,
        pod_uid: PodUid,
    },
    DeleteScaleDownResource {
        resource: ScaleDownResource,
        name: String,
        uid: String,
        resource_version: String,
    },
    EnsureWriteRoutingService,
    PersistStatus {
        status: Box<crate::types::AcceptedStatus>,
    },
    RemoveWriteRouting,
    PublishWriteRouting {
        primary: crate::types::ReplicaIdentity,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ScaleDownResource {
    Endpoint,
    Pod,
    Pvc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SafetyChange {
    RemoveWriteRouting,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_command_keeps_large_transition_evidence_off_stack() {
        assert!(
            std::mem::size_of::<EnsureConfiguration>() < 2_048,
            "EnsureConfiguration grew to {} bytes",
            std::mem::size_of::<EnsureConfiguration>()
        );
    }
}
