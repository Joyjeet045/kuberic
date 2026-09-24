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
pub struct ReplicaId(i64);

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
    pub data_loss_number: i64,
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
    pub replica_set_size: u32,
    pub write_quorum: u32,
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
/// Compact intent for one fresh replica that has not entered PC or CC.
pub struct ProvisioningIntent {
    pub replaces: ReplicaIdentity,
    pub pod_uid: PodUid,
    pub pvc_uid: PvcUid,
    pub operation_id: OperationId,
}

impl ProvisioningIntent {
    pub fn replica_id(&self) -> ReplicaId {
        self.replaces.replica_id
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
    pub fn validate(&self) -> Result<(), crate::validation::ValidationError> {
        crate::validation::validate_configuration(&self.current_configuration, None)?;
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
            return Err(crate::validation::ValidationError::BuildSourceNotPrimary);
        }
        let target_member = self
            .current_configuration
            .members
            .iter()
            .find(|member| member.identity == self.target);
        match self.kind {
            BuildAuthorityKind::Bootstrap | BuildAuthorityKind::Failover => {
                if target_member.is_none_or(|member| member.role == ReplicaRole::Primary) {
                    return Err(crate::validation::ValidationError::InvalidBootstrapBuildTarget);
                }
            }
            BuildAuthorityKind::Provisioning => {
                if target_member.is_some() {
                    return Err(
                        crate::validation::ValidationError::ProvisioningBuildTargetInAuthority,
                    );
                }
            }
        }
        if self.replication_boundary_lsn < 0 {
            return Err(crate::validation::ValidationError::NegativeBuildBoundary);
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
}

impl TransitionKind {
    pub const fn as_tag(self) -> &'static str {
        match self {
            Self::Bootstrap => "bootstrap",
            Self::Replacement => "replacement",
            Self::Failover => "failover",
        }
    }
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
    pub provisioning: Option<ProvisioningIntent>,
    pub transition: Option<TransitionIntent>,
    #[serde(default)]
    pub primary_failure: Option<PrimaryFailureObservation>,
    #[serde(default)]
    pub quorum_loss: Option<QuorumLossObservation>,
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
