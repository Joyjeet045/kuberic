//! Canonical identities, configurations, policies, and durable status intent.

use std::collections::BTreeSet;
use std::fmt;

use schemars::JsonSchema;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};

macro_rules! string_id {
    ($name:ident) => {
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
            JsonSchema,
            Default,
        )]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

string_id!(ResourceUid);
string_id!(PartitionId);
string_id!(PodUid);
string_id!(PvcUid);
string_id!(ReplicaInstanceId);
string_id!(AgentGeneration);
string_id!(ProcessSessionId);
string_id!(ConfigurationId);
string_id!(TransitionId);
string_id!(InitializationId);
string_id!(OperationId);
string_id!(SwitchoverRequestId);

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
    Default,
)]
#[serde(transparent)]
pub struct ReplicaId(#[schemars(range(min = 1))] i64);

impl ReplicaId {
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> i64 {
        self.0
    }
}

impl fmt::Display for ReplicaId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
    Default,
)]
#[serde(rename_all = "camelCase")]
/// Monotonic replication authority version, ordered by data loss then configuration.
pub struct Epoch {
    #[schemars(range(min = 0))]
    pub data_loss_number: i64,
    #[schemars(range(min = 0))]
    pub configuration_number: i64,
}

impl Epoch {
    pub const fn new(data_loss_number: i64, configuration_number: i64) -> Self {
        Self {
            data_loss_number,
            configuration_number,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// Runtime role of one exact replica incarnation.
pub enum ReplicaRole {
    Primary,
    ActiveSecondary,
    IdleSecondary,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// Current write-access decision exposed by the replica runtime.
pub enum AccessStatus {
    Granted,
    ReconfigurationPending,
    NotPrimary,
    NoWriteQuorum,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PartitionInformation {
    pub partition_id: PartitionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LoadMetric {
    pub name: String,
    pub value: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum FaultType {
    Transient,
    Permanent,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
/// Exact authority identity: logical replica, Pod incarnation, and durable generation.
pub struct ReplicaIdentity {
    pub replica_id: ReplicaId,
    pub instance_id: ReplicaInstanceId,
    pub agent_generation: AgentGeneration,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigurationMember {
    #[serde(flatten)]
    pub identity: ReplicaIdentity,
    pub role: ReplicaRole,
}

impl<'de> Deserialize<'de> for ConfigurationMember {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct FlatMember {
            replica_id: ReplicaId,
            instance_id: ReplicaInstanceId,
            agent_generation: AgentGeneration,
            role: ReplicaRole,
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct LegacyMember {
            identity: ReplicaIdentity,
            role: ReplicaRole,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum WireMember {
            Flat(FlatMember),
            Legacy(LegacyMember),
        }

        Ok(match WireMember::deserialize(deserializer)? {
            WireMember::Flat(FlatMember {
                replica_id,
                instance_id,
                agent_generation,
                role,
            }) => Self {
                identity: ReplicaIdentity {
                    replica_id,
                    instance_id,
                    agent_generation,
                },
                role,
            },
            WireMember::Legacy(LegacyMember { identity, role }) => Self { identity, role },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// Canonical Current or Previous Configuration with a content-derived ID.
pub struct ConfigurationDescriptor {
    pub configuration_id: ConfigurationId,
    pub epoch: Epoch,
    #[serde(skip_serializing)]
    #[schemars(skip)]
    pub primary_id: ReplicaId,
    pub members: Vec<ConfigurationMember>,
    pub write_quorum: u32,
}

impl<'de> Deserialize<'de> for ConfigurationDescriptor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct WireConfiguration {
            configuration_id: ConfigurationId,
            epoch: Epoch,
            #[serde(default)]
            primary_id: Option<ReplicaId>,
            members: Vec<ConfigurationMember>,
            write_quorum: u32,
        }

        let wire = WireConfiguration::deserialize(deserializer)?;
        let mut primaries = wire
            .members
            .iter()
            .filter(|member| member.role == ReplicaRole::Primary);
        let primary_id = primaries
            .next()
            .map(|member| member.identity.replica_id)
            .ok_or_else(|| D::Error::custom("configuration has no Primary member"))?;
        if primaries.next().is_some() {
            return Err(D::Error::custom(
                "configuration has multiple Primary members",
            ));
        }
        if wire
            .primary_id
            .is_some_and(|serialized| serialized != primary_id)
        {
            return Err(D::Error::custom(
                "serialized primaryId differs from the Primary member",
            ));
        }
        Ok(Self {
            configuration_id: wire.configuration_id,
            epoch: wire.epoch,
            primary_id,
            members: wire.members,
            write_quorum: wire.write_quorum,
        })
    }
}

impl ConfigurationDescriptor {
    pub fn new(
        epoch: Epoch,
        primary_id: ReplicaId,
        mut members: Vec<ConfigurationMember>,
        write_quorum: u32,
    ) -> Self {
        members.sort_by_key(|member| member.identity.replica_id);
        let configuration_id = Self::calculate_id(epoch, primary_id, &members, write_quorum);
        Self {
            configuration_id,
            epoch,
            primary_id,
            members,
            write_quorum,
        }
    }

    pub fn expected_id(&self) -> ConfigurationId {
        Self::calculate_id(
            self.epoch,
            self.primary_id,
            &self.members,
            self.write_quorum,
        )
    }

    fn calculate_id(
        epoch: Epoch,
        primary_id: ReplicaId,
        members: &[ConfigurationMember],
        write_quorum: u32,
    ) -> ConfigurationId {
        let mut canonical_members = members.to_vec();
        canonical_members.sort_by_key(|member| member.identity.replica_id);
        let mut hasher = Sha256::new();
        hasher.update(b"kuberic-configuration-v1");
        hasher.update(epoch.data_loss_number.to_be_bytes());
        hasher.update(epoch.configuration_number.to_be_bytes());
        hasher.update(primary_id.value().to_be_bytes());
        hasher.update(write_quorum.to_be_bytes());
        hasher.update((canonical_members.len() as u64).to_be_bytes());
        for member in canonical_members {
            hasher.update(member.identity.replica_id.value().to_be_bytes());
            update_digest_string(&mut hasher, member.identity.instance_id.as_str());
            update_digest_string(&mut hasher, member.identity.agent_generation.as_str());
            hasher.update([role_tag(member.role)]);
        }
        ConfigurationId::new(format!("cfg-{}", format_digest(hasher.finalize())))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// Fixed replica-set size and majority quorum values frozen for an operation.
pub struct EffectivePolicy {
    #[schemars(range(min = 1))]
    pub replica_set_size: u32,
    #[schemars(range(min = 1))]
    pub write_quorum: u32,
    #[schemars(range(min = 1))]
    pub read_quorum: u32,
    pub failover_delay_seconds: u64,
}

impl EffectivePolicy {
    pub fn fixed(replica_set_size: u32, failover_delay_seconds: u64) -> Option<Self> {
        if replica_set_size == 0 {
            return None;
        }
        let write_quorum = replica_set_size / 2 + 1;
        let read_quorum = replica_set_size - write_quorum + 1;
        Some(Self {
            replica_set_size,
            write_quorum,
            read_quorum,
            failover_delay_seconds,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// Last quorum-attested configuration accepted by the operator.
pub struct AcceptedTopology {
    #[serde(flatten)]
    pub configuration: ConfigurationDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScaleUpProvisioning {
    pub resource_uid: ResourceUid,
    #[schemars(range(min = 1))]
    pub spec_generation: u64,
    #[schemars(range(min = 1))]
    pub desired_replicas: u32,
    pub previous_configuration: ConfigurationDescriptor,
    pub previous_policy: EffectivePolicy,
    pub current_policy: EffectivePolicy,
    pub target_replica_id: ReplicaId,
}

impl ScaleUpProvisioning {
    pub fn next_configuration_epoch(&self) -> Option<Epoch> {
        self.previous_configuration
            .epoch
            .configuration_number
            .checked_add(1)
            .map(|configuration_number| {
                Epoch::new(
                    self.previous_configuration.epoch.data_loss_number,
                    configuration_number,
                )
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// Durable scale-up allocation recorded before any candidate resource is created.
pub struct ScaleUpAllocation {
    pub resource_uid: ResourceUid,
    #[schemars(range(min = 1))]
    pub spec_generation: u64,
    #[schemars(range(min = 1))]
    pub desired_replicas: u32,
    pub previous_configuration_id: ConfigurationId,
    pub accepted_configuration_id: ConfigurationId,
    pub target_replica_id: ReplicaId,
    pub operation_id: OperationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_operation_id: Option<OperationId>,
    #[serde(default)]
    pub scaffolding_requested: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_uid: Option<PodUid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc_uid: Option<PvcUid>,
    #[serde(default)]
    pub cancellation_started: bool,
}

impl ScaleUpAllocation {
    pub fn expected_operation_id(&self) -> OperationId {
        let spec_generation = self.spec_generation.to_string();
        let desired_replicas = self.desired_replicas.to_string();
        let target_replica_id = self.target_replica_id.to_string();
        let mut parts = vec![
            self.resource_uid.as_str(),
            spec_generation.as_str(),
            desired_replicas.as_str(),
            self.previous_configuration_id.as_str(),
            target_replica_id.as_str(),
        ];
        if let Some(previous_operation_id) = &self.previous_operation_id {
            parts.push(previous_operation_id.as_str());
        }
        OperationId::new(format!("scale-up-allocation-{}", digest_parts(&parts)))
    }

    pub fn observation_target(&self) -> ReplicaIdentity {
        ReplicaIdentity {
            replica_id: self.target_replica_id,
            instance_id: ReplicaInstanceId::new(self.operation_id.as_str()),
            agent_generation: AgentGeneration::new(self.operation_id.as_str()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProvisioningPurpose {
    pub kind: ProvisioningKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replaces: Option<ReplicaIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale_up: Option<ScaleUpProvisioning>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ProvisioningKind {
    Replacement,
    ScaleUp,
}

impl ProvisioningPurpose {
    pub fn replacement(replaces: ReplicaIdentity) -> Self {
        Self {
            kind: ProvisioningKind::Replacement,
            replaces: Some(replaces),
            scale_up: None,
        }
    }

    pub fn scale_up(scale_up: ScaleUpProvisioning) -> Self {
        Self {
            kind: ProvisioningKind::ScaleUp,
            replaces: None,
            scale_up: Some(scale_up),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// Compact intent for one fresh replica that has not entered PC or CC.
pub struct ProvisioningIntent {
    pub purpose: ProvisioningPurpose,
    pub pod_uid: PodUid,
    pub pvc_uid: PvcUid,
    pub operation_id: OperationId,
}

impl ProvisioningIntent {
    pub fn replica_id(&self) -> ReplicaId {
        match self.purpose.kind {
            ProvisioningKind::Replacement => self
                .purpose
                .replaces
                .as_ref()
                .map_or(ReplicaId::default(), |replaces| replaces.replica_id),
            ProvisioningKind::ScaleUp => self
                .purpose
                .scale_up
                .as_ref()
                .map_or(ReplicaId::default(), |scale_up| scale_up.target_replica_id),
        }
    }

    pub fn replacement(&self) -> Option<&ReplicaIdentity> {
        (self.purpose.kind == ProvisioningKind::Replacement)
            .then_some(self.purpose.replaces.as_ref())
            .flatten()
    }

    pub fn scale_up(&self) -> Option<&ScaleUpProvisioning> {
        (self.purpose.kind == ProvisioningKind::ScaleUp)
            .then_some(self.purpose.scale_up.as_ref())
            .flatten()
    }

    pub fn instance_id(&self) -> ReplicaInstanceId {
        ReplicaInstanceId::new(self.pod_uid.as_str())
    }

    pub fn initialization_id(&self, resource_uid: &ResourceUid) -> InitializationId {
        derive_initialization_id(
            resource_uid,
            self.replica_id(),
            &self.pod_uid,
            &self.pvc_uid,
        )
    }

    pub fn assigned_agent_generation(&self, resource_uid: &ResourceUid) -> AgentGeneration {
        derive_agent_generation(&self.initialization_id(resource_uid))
    }

    pub fn target_identity(&self, resource_uid: &ResourceUid) -> ReplicaIdentity {
        ReplicaIdentity {
            replica_id: self.replica_id(),
            instance_id: self.instance_id(),
            agent_generation: self.assigned_agent_generation(resource_uid),
        }
    }

    pub fn expected_operation_id(&self) -> OperationId {
        match self.purpose.kind {
            ProvisioningKind::Replacement => self.operation_id.clone(),
            ProvisioningKind::ScaleUp => {
                let Some(scale_up) = self.purpose.scale_up.as_ref() else {
                    return OperationId::default();
                };
                OperationId::new(format!(
                    "scale-up-provisioning-{}",
                    digest_parts(&[
                        scale_up.resource_uid.as_str(),
                        &scale_up.spec_generation.to_string(),
                        &scale_up.desired_replicas.to_string(),
                        scale_up.previous_configuration.configuration_id.as_str(),
                        &scale_up.target_replica_id.to_string(),
                        self.pod_uid.as_str(),
                        self.pvc_uid.as_str(),
                    ])
                ))
            }
        }
    }

    pub fn scale_up_build_id(&self, resource_uid: &ResourceUid) -> Option<OperationId> {
        self.scale_up().map(|_| {
            let target = self.target_identity(resource_uid);
            OperationId::new(format!(
                "scale-up-build-{}",
                digest_parts(&[
                    self.operation_id.as_str(),
                    &target.replica_id.to_string(),
                    target.instance_id.as_str(),
                    target.agent_generation.as_str(),
                ])
            ))
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum BuildAuthorityKind {
    Bootstrap,
    Provisioning,
    Failover,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BuildAuthority {
    pub build_id: OperationId,
    pub kind: BuildAuthorityKind,
    pub source: ReplicaIdentity,
    pub target: ReplicaIdentity,
    pub current_configuration: ConfigurationDescriptor,
    pub replication_boundary_lsn: i64,
}

impl BuildAuthority {
    pub fn validate(&self) -> Result<(), crate::protocol::validation::ValidationError> {
        crate::protocol::validation::validate_configuration(&self.current_configuration, None)?;
        let primary = self
            .current_configuration
            .members
            .iter()
            .find(|member| {
                member.identity.replica_id == self.current_configuration.primary_id
                    && member.role == ReplicaRole::Primary
            })
            .expect("validated configuration has one primary");
        if primary.identity != self.source {
            return Err(crate::protocol::validation::ValidationError::BuildSourceNotPrimary);
        }
        let target_member = self
            .current_configuration
            .members
            .iter()
            .find(|member| member.identity == self.target);
        match self.kind {
            BuildAuthorityKind::Bootstrap | BuildAuthorityKind::Failover => {
                if target_member.is_none_or(|member| member.role == ReplicaRole::Primary) {
                    return Err(
                        crate::protocol::validation::ValidationError::InvalidBootstrapBuildTarget,
                    );
                }
            }
            BuildAuthorityKind::Provisioning => {
                if target_member.is_some() {
                    return Err(
                        crate::protocol::validation::ValidationError::ProvisioningBuildTargetInAuthority,
                    );
                }
            }
        }
        if self.replication_boundary_lsn < 0 {
            return Err(crate::protocol::validation::ValidationError::NegativeBuildBoundary);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// Supported authority-changing transition.
pub enum TransitionKind {
    Bootstrap,
    Replacement,
    Failover,
    PlannedSwitchover,
    SecondaryScaleDown,
    ScaleUp,
}

impl TransitionKind {
    pub const fn as_tag(self) -> &'static str {
        match self {
            Self::Bootstrap => "bootstrap",
            Self::Replacement => "replacement",
            Self::Failover => "failover",
            Self::PlannedSwitchover => "planned-switchover",
            Self::SecondaryScaleDown => "secondary-scale-down",
            Self::ScaleUp => "scale-up",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlannedSwitchoverRequest {
    pub request_id: SwitchoverRequestId,
    pub target_replica_id: ReplicaId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum PlannedSwitchoverResolution {
    RequestedTarget,
    RestoringOldPrimary,
    CompensatingOldPrimary,
    Unsafe,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SwitchoverHandoff {
    #[schemars(range(min = 1))]
    pub preparation_generation: u64,
    pub preparation_operation_id: OperationId,
    pub request_id: SwitchoverRequestId,
    pub source: ReplicaIdentity,
    pub target: ReplicaIdentity,
    pub starting_configuration_id: ConfigurationId,
    pub starting_epoch: Epoch,
    pub handoff_lsn: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SwitchoverPreparationId {
    pub generation: u64,
    pub operation_id: OperationId,
}

impl SwitchoverHandoff {
    pub fn preparation(&self) -> SwitchoverPreparationId {
        SwitchoverPreparationId {
            generation: self.preparation_generation,
            operation_id: self.preparation_operation_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlannedSwitchoverIntent {
    #[schemars(range(min = 1))]
    pub preparation_generation: u64,
    pub request_id: SwitchoverRequestId,
    pub source: ReplicaIdentity,
    pub target: ReplicaIdentity,
    pub requested_configuration: ConfigurationDescriptor,
    pub resolution: PlannedSwitchoverResolution,
    #[serde(default)]
    pub handoff: Option<SwitchoverHandoff>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum PlannedSwitchoverOutcome {
    RequestedTargetCompleted,
    OldPrimaryRestored,
    OldPrimaryCompensated,
    Rejected,
    Unsafe,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlannedSwitchoverReceipt {
    pub request_id: SwitchoverRequestId,
    pub requested_target_replica_id: ReplicaId,
    pub accepted_target: Option<ReplicaIdentity>,
    pub resulting_primary: Option<ReplicaIdentity>,
    pub outcome: PlannedSwitchoverOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// Frozen PC/CC target and policy for one active transition.
pub struct TransitionIntent {
    pub transition_id: TransitionId,
    pub kind: TransitionKind,
    pub spec_generation: u64,
    pub effective_policy: EffectivePolicy,
    pub previous_configuration_id: Option<ConfigurationId>,
    pub current_configuration: ConfigurationDescriptor,
    #[serde(default)]
    pub election_lsn: Option<i64>,
    #[serde(default)]
    pub build_id: Option<OperationId>,
    #[serde(default)]
    pub repair: Option<ReplicaRepairIntent>,
    #[serde(default)]
    pub switchover: Option<PlannedSwitchoverIntent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secondary_scale_down: Option<SecondaryScaleDownIntent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secondary_removal_evidence: Option<SecondaryRemovalEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale_up: Option<Box<ScaleUpIntent>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale_up_failover: Option<Box<ScaleUpFailoverEvidence>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// Absence may only be frozen from a successful exact-name lookup.
pub enum CleanupResourceIdentity {
    Present {
        #[schemars(length(min = 1))]
        name: String,
        #[schemars(length(min = 1))]
        uid: String,
    },
    Absent {
        #[schemars(length(min = 1))]
        name: String,
    },
}

impl CleanupResourceIdentity {
    pub fn name(&self) -> &str {
        match self {
            Self::Present { name, .. } | Self::Absent { name } => name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaCleanupIdentity {
    pub pod: CleanupResourceIdentity,
    pub pvc: CleanupResourceIdentity,
    pub endpoint: CleanupResourceIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// Immutable authority for one exact membership increase.
pub struct ScaleUpIntent {
    pub operation_id: OperationId,
    pub resource_uid: ResourceUid,
    #[schemars(range(min = 1))]
    pub spec_generation: u64,
    #[schemars(range(min = 1))]
    pub desired_replicas: u32,
    pub previous_configuration: ConfigurationDescriptor,
    pub current_configuration: ConfigurationDescriptor,
    pub previous_policy: EffectivePolicy,
    pub current_policy: EffectivePolicy,
    pub primary: ReplicaIdentity,
    pub target: ReplicaIdentity,
    pub build_id: OperationId,
    #[schemars(range(min = 0))]
    pub snapshot_boundary_lsn: i64,
    #[schemars(range(min = 0))]
    pub catch_up_boundary_lsn: i64,
}

impl ScaleUpIntent {
    pub fn expected_operation_id(&self) -> OperationId {
        OperationId::new(format!(
            "scale-up-{}",
            digest_parts(&[
                self.resource_uid.as_str(),
                &self.spec_generation.to_string(),
                &self.desired_replicas.to_string(),
                self.previous_configuration.configuration_id.as_str(),
                self.current_configuration.configuration_id.as_str(),
                &self.target.replica_id.to_string(),
                self.target.instance_id.as_str(),
                self.target.agent_generation.as_str(),
                self.build_id.as_str(),
                &self.snapshot_boundary_lsn.to_string(),
                &self.catch_up_boundary_lsn.to_string(),
            ])
        ))
    }

    pub fn command_operation_id(
        &self,
        stage: ScaleUpStage,
        target: &ReplicaIdentity,
        authority: &ConfigurationDescriptor,
    ) -> OperationId {
        OperationId::new(format!(
            "scale-up-command-{}",
            digest_parts(&[
                self.operation_id.as_str(),
                stage.as_tag(),
                &target.replica_id.to_string(),
                target.instance_id.as_str(),
                target.agent_generation.as_str(),
                authority.configuration_id.as_str(),
            ])
        ))
    }

    pub fn transition_id(
        &self,
        kind: TransitionKind,
        authority: &ConfigurationDescriptor,
    ) -> TransitionId {
        TransitionId::new(format!(
            "scale-up-transition-{}",
            digest_parts(&[
                self.operation_id.as_str(),
                kind.as_tag(),
                authority.configuration_id.as_str(),
            ])
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScaleUpStage {
    PreviousCurrent,
    CurrentOnly,
    AcceptCommit,
}

impl ScaleUpStage {
    const fn as_tag(self) -> &'static str {
        match self {
            Self::PreviousCurrent => "pc-cc",
            Self::CurrentOnly => "current-only",
            Self::AcceptCommit => "accept-commit",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScaleUpWitness {
    pub resource_uid: ResourceUid,
    pub identity: ReplicaIdentity,
    pub role: ReplicaRole,
    pub process_session_id: ProcessSessionId,
    #[schemars(range(min = 1))]
    pub report_sequence: u64,
    pub epoch: Epoch,
    pub previous_configuration_id: Option<ConfigurationId>,
    pub current_configuration_id: ConfigurationId,
    #[schemars(range(min = 0))]
    pub verified_replication_lsn: i64,
    pub write_status: AccessStatus,
    pub pending_operation_id: Option<OperationId>,
    pub retained_operation_id: Option<OperationId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScaleUpFinalWitness {
    pub replica_id: ReplicaId,
    pub process_session_id: ProcessSessionId,
    #[schemars(range(min = 1))]
    pub report_sequence: u64,
    #[schemars(range(min = 0))]
    pub current_progress: i64,
    #[schemars(range(min = 0))]
    pub committed_lsn: i64,
    #[schemars(range(min = 0))]
    pub deactivated_lsn: i64,
    pub fence_operation_id: OperationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScaleUpFinalElectionEvidence {
    pub selected_primary_replica_id: ReplicaId,
    pub witnesses: Vec<ScaleUpFinalWitness>,
    pub previous_read_quorum: Vec<ReplicaId>,
    pub current_read_quorum: Vec<ReplicaId>,
}

impl ScaleUpFinalElectionEvidence {
    pub fn selected_primary<'a>(
        &self,
        provisional: &'a ConfigurationDescriptor,
    ) -> Option<&'a ReplicaIdentity> {
        provisional
            .members
            .iter()
            .find(|member| member.identity.replica_id == self.selected_primary_replica_id)
            .map(|member| &member.identity)
    }

    pub fn final_configuration(
        &self,
        provisional: &ConfigurationDescriptor,
    ) -> Option<ConfigurationDescriptor> {
        let selected = self.selected_primary(provisional)?;
        let final_configuration_number = provisional.epoch.configuration_number.checked_add(1)?;
        let members = provisional
            .members
            .iter()
            .map(|member| ConfigurationMember {
                identity: member.identity.clone(),
                role: if member.identity == *selected {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect();
        Some(ConfigurationDescriptor::new(
            Epoch::new(
                provisional.epoch.data_loss_number,
                final_configuration_number,
            ),
            self.selected_primary_replica_id,
            members,
            provisional.write_quorum,
        ))
    }

    pub fn witness(&self, replica_id: ReplicaId) -> Option<&ScaleUpFinalWitness> {
        self.witnesses
            .iter()
            .find(|witness| witness.replica_id == replica_id)
    }

    pub fn safe_lsn(&self) -> Option<i64> {
        self.witness(self.selected_primary_replica_id)
            .map(|witness| witness.current_progress.min(witness.deactivated_lsn))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScaleUpFailoverEvidence {
    pub intent: ScaleUpIntent,
    pub provisional_configuration: ConfigurationDescriptor,
    pub previous_read_quorum: Vec<ScaleUpWitness>,
    pub current_read_quorum: Vec<ScaleUpWitness>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_election: Option<Box<ScaleUpFinalElectionEvidence>>,
}

impl ScaleUpFailoverEvidence {
    pub fn same_provisional_authority(&self, other: &Self) -> bool {
        self.intent == other.intent
            && self.provisional_configuration == other.provisional_configuration
            && self.previous_read_quorum == other.previous_read_quorum
            && self.current_read_quorum == other.current_read_quorum
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum ScaleUpConfigurationEvidence {
    Admission { intent: ScaleUpIntent },
    Failover { evidence: ScaleUpFailoverEvidence },
}

impl ScaleUpConfigurationEvidence {
    pub fn intent(&self) -> &ScaleUpIntent {
        match self {
            Self::Admission { intent } => intent,
            Self::Failover { evidence } => &evidence.intent,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScaleUpCleanup {
    pub provisioning: ProvisioningIntent,
    pub target: ReplicaIdentity,
    pub resources: ReplicaCleanupIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScaleUpFailoverReceiptEvidence {
    pub provisional_primary_replica_id: ReplicaId,
    pub previous_read_quorum: Vec<ScaleUpWitness>,
    pub current_read_quorum: Vec<ScaleUpWitness>,
    pub final_election: Box<ScaleUpFinalElectionEvidence>,
}

impl ScaleUpFailoverReceiptEvidence {
    pub fn from_evidence(evidence: &ScaleUpFailoverEvidence) -> Option<Self> {
        Some(Self {
            provisional_primary_replica_id: evidence.provisional_configuration.primary_id,
            previous_read_quorum: evidence.previous_read_quorum.clone(),
            current_read_quorum: evidence.current_read_quorum.clone(),
            final_election: evidence.final_election.clone()?,
        })
    }

    pub fn expand(&self, intent: &ScaleUpIntent) -> Option<ScaleUpFailoverEvidence> {
        intent
            .current_configuration
            .members
            .iter()
            .any(|member| member.identity.replica_id == self.provisional_primary_replica_id)
            .then_some(())?;
        let provisional_configuration_number = intent
            .current_configuration
            .epoch
            .configuration_number
            .checked_add(1)?;
        let provisional_configuration = ConfigurationDescriptor::new(
            Epoch::new(
                intent.current_configuration.epoch.data_loss_number,
                provisional_configuration_number,
            ),
            self.provisional_primary_replica_id,
            intent
                .current_configuration
                .members
                .iter()
                .map(|member| ConfigurationMember {
                    identity: member.identity.clone(),
                    role: if member.identity.replica_id == self.provisional_primary_replica_id {
                        ReplicaRole::Primary
                    } else {
                        ReplicaRole::ActiveSecondary
                    },
                })
                .collect(),
            intent.current_policy.write_quorum,
        );
        Some(ScaleUpFailoverEvidence {
            intent: intent.clone(),
            provisional_configuration,
            previous_read_quorum: self.previous_read_quorum.clone(),
            current_read_quorum: self.current_read_quorum.clone(),
            final_election: Some(self.final_election.clone()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// One bounded completed-addition proof retained for late local convergence.
pub struct ScaleUpReceipt {
    pub intent: ScaleUpIntent,
    pub accepted_configuration: ConfigurationDescriptor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failover_evidence: Option<ScaleUpFailoverReceiptEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub failover_safe_lsn: Option<i64>,
    pub current_only_write_quorum: Vec<ScaleUpWitness>,
}

impl ScaleUpReceipt {
    pub fn expanded_failover_evidence(&self) -> Option<ScaleUpFailoverEvidence> {
        self.failover_evidence
            .as_ref()
            .and_then(|evidence| evidence.expand(&self.intent))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplacementCleanup {
    pub resource_uid: ResourceUid,
    pub target: ReplicaIdentity,
    pub resources: ReplicaCleanupIdentity,
}

impl ReplacementCleanup {
    pub fn provisioning_operation_id(&self, pod_uid: &PodUid, pvc_uid: &PvcUid) -> OperationId {
        OperationId::new(format!(
            "replacement-provisioning-{}",
            digest_parts(&[&self.provenance(), pod_uid.as_str(), pvc_uid.as_str()])
        ))
    }

    pub fn transition_id(
        &self,
        kind: TransitionKind,
        configuration_id: &ConfigurationId,
    ) -> TransitionId {
        TransitionId::new(format!(
            "transition-{}",
            digest_parts(&[&self.provenance(), kind.as_tag(), configuration_id.as_str()])
        ))
    }

    fn provenance(&self) -> String {
        let replica_id = self.target.replica_id.to_string();
        let mut parts = vec![
            self.resource_uid.as_str(),
            &replica_id,
            self.target.instance_id.as_str(),
            self.target.agent_generation.as_str(),
        ];
        for resource in [
            &self.resources.pod,
            &self.resources.pvc,
            &self.resources.endpoint,
        ] {
            match resource {
                CleanupResourceIdentity::Present { name, uid } => {
                    parts.extend(["present", name, uid])
                }
                CleanupResourceIdentity::Absent { name } => parts.extend(["absent", name, ""]),
            }
        }
        digest_parts(&parts)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// Immutable authority for one highest-ID committed secondary removal.
pub struct SecondaryScaleDownIntent {
    pub operation_id: OperationId,
    pub resource_uid: ResourceUid,
    #[schemars(range(min = 1))]
    pub spec_generation: u64,
    #[schemars(range(min = 1))]
    pub desired_replicas: u32,
    pub previous_configuration: ConfigurationDescriptor,
    pub current_configuration: ConfigurationDescriptor,
    pub previous_policy: EffectivePolicy,
    pub current_policy: EffectivePolicy,
    pub primary: ReplicaIdentity,
    pub target: ReplicaIdentity,
    pub cleanup: ReplicaCleanupIdentity,
}

impl SecondaryScaleDownIntent {
    pub fn expected_operation_id(&self) -> OperationId {
        OperationId::new(format!(
            "secondary-scale-down-{}",
            digest_parts(&[
                self.resource_uid.as_str(),
                &self.spec_generation.to_string(),
                &self.desired_replicas.to_string(),
                self.previous_configuration.configuration_id.as_str(),
                self.current_configuration.configuration_id.as_str(),
                &self.target.replica_id.to_string(),
                self.target.instance_id.as_str(),
                self.target.agent_generation.as_str(),
            ])
        ))
    }

    pub fn command_operation_id(
        &self,
        stage: SecondaryRemovalStage,
        target: &ReplicaIdentity,
    ) -> OperationId {
        OperationId::new(format!(
            "secondary-removal-{}",
            digest_parts(&[
                self.operation_id.as_str(),
                stage.as_tag(),
                &target.replica_id.to_string(),
                target.instance_id.as_str(),
                target.agent_generation.as_str(),
            ])
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecondaryRemovalStage {
    Prepare,
    PreviousCurrent,
    CurrentOnly,
    AcceptCommit,
    Retire,
}

impl SecondaryRemovalStage {
    const fn as_tag(self) -> &'static str {
        match self {
            Self::Prepare => "prepare",
            Self::PreviousCurrent => "pc-cc",
            Self::CurrentOnly => "current-only",
            Self::AcceptCommit => "accept-commit",
            Self::Retire => "retire",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// Durable write-closure receipt, not a raw progress sample or RPC acknowledgement.
pub struct SecondaryRemovalPreparation {
    pub intent: SecondaryScaleDownIntent,
    pub operation_id: OperationId,
    pub process_session_id: ProcessSessionId,
    #[schemars(range(min = 1))]
    pub report_sequence: u64,
    #[schemars(range(min = 0))]
    pub boundary_lsn: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecondaryRemovalWitness {
    pub resource_uid: ResourceUid,
    pub identity: ReplicaIdentity,
    pub role: ReplicaRole,
    pub process_session_id: ProcessSessionId,
    #[schemars(range(min = 1))]
    pub report_sequence: u64,
    pub epoch: Epoch,
    pub previous_configuration_id: Option<ConfigurationId>,
    pub current_configuration_id: ConfigurationId,
    #[schemars(range(min = 0))]
    pub verified_replication_lsn: i64,
    pub write_status: AccessStatus,
    pub pending_operation_id: Option<OperationId>,
    pub retained_operation_id: Option<OperationId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecondaryRemovalEvidence {
    pub preparation: SecondaryRemovalPreparation,
    pub previous_read_quorum: Vec<SecondaryRemovalWitness>,
    #[serde(default)]
    pub reduced_write_quorum: Vec<SecondaryRemovalWitness>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaRetirementReport {
    pub intent: SecondaryScaleDownIntent,
    pub operation_id: OperationId,
    pub process_session_id: ProcessSessionId,
    #[schemars(range(min = 1))]
    pub report_sequence: u64,
    pub epoch: Epoch,
    pub role: ReplicaRole,
    pub read_status: AccessStatus,
    pub write_status: AccessStatus,
    pub application_closed: bool,
    pub peers_fenced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// Published atomically with reduced topology/policy; retained until exact cleanup completes.
pub struct SecondaryScaleDownCleanup {
    pub evidence: SecondaryRemovalEvidence,
    pub current_only_write_quorum: Vec<SecondaryRemovalWitness>,
    #[serde(default)]
    pub retirement: Option<ReplicaRetirementReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// Last completed removal's immutable convergence proof, never deletion authority.
/// One receipt is retained; a later removal supersedes it only after lagging members settle.
pub struct SecondaryRemovalReceipt {
    pub evidence: SecondaryRemovalEvidence,
    pub current_only_write_quorum: Vec<SecondaryRemovalWitness>,
}

impl SecondaryRemovalReceipt {
    pub fn committed(&self) -> SecondaryScaleDownCleanup {
        SecondaryScaleDownCleanup {
            evidence: self.evidence.clone(),
            current_only_write_quorum: self.current_only_write_quorum.clone(),
            retirement: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaRepairIntent {
    pub operation_id: OperationId,
    pub target: ReplicaIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PrimaryFailureObservation {
    pub primary: ReplicaIdentity,
    pub started_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QuorumLossObservation {
    pub configuration_id: ConfigurationId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConditionStatus {
    True,
    False,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StatusCondition {
    pub type_: String,
    pub status: ConditionStatus,
    pub reason: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
/// Durable controller authority: accepted topology plus compact active intent.
pub struct AcceptedStatus {
    pub initialized: bool,
    pub observed_generation: u64,
    #[serde(default)]
    pub effective_policy: Option<EffectivePolicy>,
    pub topology: Option<AcceptedTopology>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale_up_allocation: Option<ScaleUpAllocation>,
    pub provisioning: Option<ProvisioningIntent>,
    pub transition: Option<TransitionIntent>,
    #[serde(default)]
    pub primary_failure: Option<PrimaryFailureObservation>,
    #[serde(default)]
    pub quorum_loss: Option<QuorumLossObservation>,
    #[serde(default)]
    pub last_switchover: Option<PlannedSwitchoverReceipt>,
    /// One admitted replacement's immutable cleanup identity, moved to lastReplacement on acceptance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_replacement_cleanup: Option<ReplacementCleanup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_replacement: Option<ReplacementCleanup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secondary_scale_down_cleanup: Option<SecondaryScaleDownCleanup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_secondary_removal: Option<SecondaryRemovalReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale_up_cleanup: Option<Box<ScaleUpCleanup>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_scale_up: Option<Box<ScaleUpReceipt>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale_up_admission_started: Option<OperationId>,
    pub conditions: Vec<StatusCondition>,
}

impl AcceptedStatus {
    pub fn with_condition(mut self, condition: StatusCondition) -> Self {
        self.conditions
            .retain(|existing| existing.type_ != condition.type_);
        self.conditions.push(condition);
        self
    }

    pub fn without_condition(mut self, type_: &str) -> Self {
        self.conditions.retain(|existing| existing.type_ != type_);
        self
    }
}

pub fn derive_initialization_id(
    resource_uid: &ResourceUid,
    replica_id: ReplicaId,
    pod_uid: &PodUid,
    pvc_uid: &PvcUid,
) -> InitializationId {
    InitializationId::new(format!(
        "init-{}",
        digest_parts(&[
            resource_uid.as_str(),
            &replica_id.to_string(),
            pod_uid.as_str(),
            pvc_uid.as_str(),
        ])
    ))
}

pub fn derive_agent_generation(initialization_id: &InitializationId) -> AgentGeneration {
    AgentGeneration::new(digest_parts(&["agent", initialization_id.as_str()]))
}

pub fn derive_transition_id(
    resource_uid: &ResourceUid,
    kind: TransitionKind,
    configuration_id: &ConfigurationId,
) -> TransitionId {
    TransitionId::new(format!(
        "transition-{}",
        digest_parts(&[
            resource_uid.as_str(),
            kind.as_tag(),
            configuration_id.as_str(),
        ])
    ))
}

pub fn derive_replacement_operation_id(
    resource_uid: &ResourceUid,
    replacing: &ReplicaIdentity,
    pod_uid: &PodUid,
    pvc_uid: &PvcUid,
) -> OperationId {
    OperationId::new(format!(
        "replacement-provisioning-{}",
        digest_parts(&[
            resource_uid.as_str(),
            &replacing.replica_id.to_string(),
            replacing.instance_id.as_str(),
            replacing.agent_generation.as_str(),
            pod_uid.as_str(),
            pvc_uid.as_str(),
        ])
    ))
}

pub fn derive_failover_repair_operation_id(
    resource_uid: &ResourceUid,
    transition_id: &TransitionId,
    target: &ReplicaIdentity,
) -> OperationId {
    OperationId::new(format!(
        "failover-repair-{}",
        digest_parts(&[
            resource_uid.as_str(),
            transition_id.as_str(),
            &target.replica_id.to_string(),
            target.instance_id.as_str(),
            target.agent_generation.as_str(),
        ])
    ))
}

pub fn derive_switchover_preparation_operation_id(
    resource_uid: &ResourceUid,
    request_id: &SwitchoverRequestId,
    generation: u64,
    starting_configuration_id: &ConfigurationId,
    source: &ReplicaIdentity,
    target: &ReplicaIdentity,
) -> OperationId {
    OperationId::new(format!(
        "switchover-prepare-{}",
        digest_parts(&[
            resource_uid.as_str(),
            request_id.as_str(),
            &generation.to_string(),
            starting_configuration_id.as_str(),
            &source.replica_id.to_string(),
            source.instance_id.as_str(),
            source.agent_generation.as_str(),
            &target.replica_id.to_string(),
            target.instance_id.as_str(),
            target.agent_generation.as_str(),
        ])
    ))
}

pub fn derive_replica_endpoint_name(
    resource_uid: &ResourceUid,
    identity: &ReplicaIdentity,
) -> String {
    format!(
        "kr-{}",
        &digest_parts(&[
            resource_uid.as_str(),
            &identity.replica_id.to_string(),
            identity.instance_id.as_str(),
            identity.agent_generation.as_str(),
        ])[..24]
    )
}

pub fn derive_replacement_resource_name(
    resource_uid: &ResourceUid,
    replacing: &ReplicaIdentity,
) -> String {
    format!(
        "krp-{}",
        &digest_parts(&[
            resource_uid.as_str(),
            &replacing.replica_id.to_string(),
            replacing.instance_id.as_str(),
        ])[..20]
    )
}

pub fn duplicate_replica_ids(members: &[ConfigurationMember]) -> BTreeSet<ReplicaId> {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();
    for member in members {
        if !seen.insert(member.identity.replica_id) {
            duplicates.insert(member.identity.replica_id);
        }
    }
    duplicates
}

fn digest_parts(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }
    format_digest(hasher.finalize())
}

fn update_digest_string(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

const fn role_tag(role: ReplicaRole) -> u8 {
    match role {
        ReplicaRole::Primary => 1,
        ReplicaRole::ActiveSecondary => 2,
        ReplicaRole::IdleSecondary => 3,
        ReplicaRole::None => 4,
    }
}

fn format_digest(digest: impl AsRef<[u8]>) -> String {
    use std::fmt::Write;

    let mut output = String::with_capacity(digest.as_ref().len() * 2);
    for byte in digest.as_ref() {
        write!(&mut output, "{byte:02x}").expect("writing to a string cannot fail");
    }
    output
}
