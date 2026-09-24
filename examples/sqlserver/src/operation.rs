use std::collections::BTreeSet;
use std::fmt;

use sha2::{Digest, Sha256};

use crate::config::{SUPPORTED_REPLICA_COUNT, SUPPORTED_REPLICA_COUNT_TEXT};
use crate::error::ContractError;
use crate::types::{
    AvailabilityGroupIdentity, AvailabilityGroupName, DatabaseIdentity, DatabaseLineage,
    DecimalProgress, Endpoint, Guid, OpaqueId, ReplicaDescriptor, ReplicaIdentity, SqlIdentifier,
};

/// Version of the canonical operation encoding.
///
/// [`OperationRequest::new`] always stamps this value, so the version check in
/// [`OperationRequest::validate`] is only reachable through
/// [`OperationRequest::from_decoded_parts`], which is the entry point a decoder
/// will use once stage 2 of the delivery sequence introduces one.
pub const OPERATION_CONTRACT_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationPayload {
    EnsureAvailabilityGroup {
        name: AvailabilityGroupName,
        expected_group_id: Option<Guid>,
        database_name: SqlIdentifier,
        replicas: Vec<ReplicaDescriptor>,
    },
    EnsureReplicaJoined {
        availability_group: AvailabilityGroupIdentity,
        target: ReplicaIdentity,
    },
    EnsureReplicaSeeded {
        availability_group: AvailabilityGroupIdentity,
        database: DatabaseIdentity,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
    },
    ReseedReplica {
        availability_group: AvailabilityGroupIdentity,
        database: DatabaseIdentity,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
    },
    PlannedSwitchover {
        availability_group: AvailabilityGroupIdentity,
        database: DatabaseLineage,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
        commit_boundary: DecimalProgress,
    },
    ForcedFailover {
        availability_group: AvailabilityGroupIdentity,
        database: DatabaseLineage,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
        last_known_commit: Option<DecimalProgress>,
    },
}

impl OperationPayload {
    fn tag(&self) -> u8 {
        match self {
            Self::EnsureAvailabilityGroup { .. } => 1,
            Self::EnsureReplicaJoined { .. } => 2,
            Self::EnsureReplicaSeeded { .. } => 3,
            Self::ReseedReplica { .. } => 4,
            Self::PlannedSwitchover { .. } => 5,
            Self::ForcedFailover { .. } => 6,
        }
    }

    fn destructive(&self) -> bool {
        matches!(
            self,
            Self::ReseedReplica { .. } | Self::ForcedFailover { .. }
        )
    }

    fn changes_primary_authority(&self) -> bool {
        matches!(
            self,
            Self::PlannedSwitchover { .. } | Self::ForcedFailover { .. }
        )
    }

    fn required_fence(&self) -> Option<&ReplicaIdentity> {
        match self {
            Self::ReseedReplica { target, .. } => Some(target),
            Self::PlannedSwitchover { source, .. } | Self::ForcedFailover { source, .. } => {
                Some(source)
            }
            _ => None,
        }
    }

    fn validate(&self) -> Result<(), ContractError> {
        match self {
            Self::EnsureAvailabilityGroup { replicas, .. } => {
                if replicas.len() != usize::from(SUPPORTED_REPLICA_COUNT) {
                    return Err(ContractError::UnsupportedProfile {
                        field: "operation replica count",
                        expected: SUPPORTED_REPLICA_COUNT_TEXT,
                        actual: replicas.len().to_string(),
                    });
                }

                let mut ids = BTreeSet::new();
                let mut names = BTreeSet::new();
                let mut endpoints = BTreeSet::new();
                for replica in replicas {
                    if replica.identity.native_replica_id().is_some() {
                        return Err(ContractError::UnexpectedNativeIdentity {
                            field: "bootstrap replica",
                        });
                    }
                    if !ids.insert(replica.identity.logical_id().to_string()) {
                        return Err(ContractError::DuplicateValue {
                            field: "logical replica ID",
                            value: replica.identity.logical_id().to_string(),
                        });
                    }
                    if !names.insert(replica.server_name.clone()) {
                        return Err(ContractError::DuplicateValue {
                            field: "server name",
                            value: replica.server_name.to_string(),
                        });
                    }
                    if !endpoints.insert(replica.endpoint.clone()) {
                        return Err(ContractError::DuplicateValue {
                            field: "replication endpoint",
                            value: replica.endpoint.to_string(),
                        });
                    }
                }
            }
            Self::EnsureReplicaSeeded { source, target, .. }
            | Self::ReseedReplica { source, target, .. }
            | Self::PlannedSwitchover { source, target, .. }
            | Self::ForcedFailover { source, target, .. } => {
                require_native_replica("source replica", source)?;
                require_native_replica("target replica", target)?;
                if source.logical_id() == target.logical_id()
                    || source.native_replica_id() == target.native_replica_id()
                {
                    return Err(ContractError::DuplicateValue {
                        field: "source and target replica",
                        value: source.logical_id().to_string(),
                    });
                }
            }
            Self::EnsureReplicaJoined { target, .. } => {
                require_native_replica("join target replica", target)?;
            }
        }
        Ok(())
    }

    fn encode(&self, writer: &mut CanonicalWriter) {
        writer.u8(self.tag());
        match self {
            Self::EnsureAvailabilityGroup {
                name,
                expected_group_id,
                database_name,
                replicas,
            } => {
                writer.string(name.as_str());
                writer.optional(expected_group_id.as_ref(), |writer, id| {
                    writer.string(id.as_str());
                });
                writer.string(database_name.as_str());

                let mut replicas = replicas.iter().collect::<Vec<_>>();
                replicas.sort_by(|left, right| {
                    left.identity
                        .logical_id()
                        .cmp(right.identity.logical_id())
                        .then_with(|| {
                            left.identity
                                .incarnation()
                                .cmp(right.identity.incarnation())
                        })
                });
                writer.u32(
                    u32::try_from(replicas.len())
                        .expect("replica count is bounded by the supported profile"),
                );
                for replica in replicas {
                    writer.replica(&replica.identity);
                    writer.string(replica.server_name.as_str());
                    writer.endpoint(&replica.endpoint);
                }
            }
            Self::EnsureReplicaJoined {
                availability_group,
                target,
            } => {
                writer.availability_group(availability_group);
                writer.replica(target);
            }
            Self::EnsureReplicaSeeded {
                availability_group,
                database,
                source,
                target,
            }
            | Self::ReseedReplica {
                availability_group,
                database,
                source,
                target,
            } => {
                writer.availability_group(availability_group);
                writer.database(database);
                writer.replica(source);
                writer.replica(target);
            }
            Self::PlannedSwitchover {
                availability_group,
                database,
                source,
                target,
                commit_boundary,
            } => {
                writer.availability_group(availability_group);
                writer.database_lineage(database);
                writer.replica(source);
                writer.replica(target);
                writer.string(&commit_boundary.to_string());
            }
            Self::ForcedFailover {
                availability_group,
                database,
                source,
                target,
                last_known_commit,
            } => {
                writer.availability_group(availability_group);
                writer.database_lineage(database);
                writer.replica(source);
                writer.replica(target);
                writer.optional(last_known_commit.as_ref(), |writer, progress| {
                    writer.string(&progress.to_string());
                });
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestructiveApproval {
    authorization_id: OpaqueId,
    approved_operation_id: OpaqueId,
    approved_input_signature: InputSignature,
}

impl DestructiveApproval {
    pub fn new(
        authorization_id: impl Into<String>,
        approved_operation_id: impl Into<String>,
        approved_input_signature: InputSignature,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            authorization_id: OpaqueId::new("authorization ID", authorization_id)?,
            approved_operation_id: OpaqueId::new("approved operation ID", approved_operation_id)?,
            approved_input_signature,
        })
    }

    pub fn authorization_id(&self) -> &str {
        self.authorization_id.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceReference {
    provider: OpaqueId,
    receipt_id: OpaqueId,
    operation_id: OpaqueId,
    input_signature: InputSignature,
    fenced_replica: ReplicaIdentity,
}

impl FenceReference {
    pub fn new(
        provider: impl Into<String>,
        receipt_id: impl Into<String>,
        operation_id: impl Into<String>,
        input_signature: InputSignature,
        fenced_replica: ReplicaIdentity,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            provider: OpaqueId::new("fence provider", provider)?,
            receipt_id: OpaqueId::new("fence receipt ID", receipt_id)?,
            operation_id: OpaqueId::new("fenced operation ID", operation_id)?,
            input_signature,
            fenced_replica,
        })
    }

    pub fn provider(&self) -> &str {
        self.provider.as_str()
    }

    pub fn receipt_id(&self) -> &str {
        self.receipt_id.as_str()
    }

    pub fn fenced_replica(&self) -> &ReplicaIdentity {
        &self.fenced_replica
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationRequest {
    contract_version: u16,
    resource_id: OpaqueId,
    operation_id: OpaqueId,
    source_configuration_id: OpaqueId,
    source_epoch: u64,
    target_epoch: u64,
    payload: OperationPayload,
}

impl OperationRequest {
    pub fn new(
        resource_id: impl Into<String>,
        operation_id: impl Into<String>,
        source_configuration_id: impl Into<String>,
        source_epoch: u64,
        target_epoch: u64,
        payload: OperationPayload,
    ) -> Result<Self, ContractError> {
        let request = Self {
            contract_version: OPERATION_CONTRACT_VERSION,
            resource_id: OpaqueId::new("resource ID", resource_id)?,
            operation_id: OpaqueId::new("operation ID", operation_id)?,
            source_configuration_id: OpaqueId::new(
                "source configuration ID",
                source_configuration_id,
            )?,
            source_epoch,
            target_epoch,
            payload,
        };
        request.validate()?;
        Ok(request)
    }

    /// Rebuilds a request from previously encoded parts, validating the contract
    /// version before anything else is trusted.
    ///
    /// This is the seam a decoder plugs into. Until stage 2 of the delivery
    /// sequence adds one, it exists so that the version check is reachable and
    /// testable rather than unreachable by construction.
    pub fn from_decoded_parts(
        contract_version: u16,
        resource_id: impl Into<String>,
        operation_id: impl Into<String>,
        source_configuration_id: impl Into<String>,
        source_epoch: u64,
        target_epoch: u64,
        payload: OperationPayload,
    ) -> Result<Self, ContractError> {
        let request = Self {
            contract_version,
            resource_id: OpaqueId::new("resource ID", resource_id)?,
            operation_id: OpaqueId::new("operation ID", operation_id)?,
            source_configuration_id: OpaqueId::new(
                "source configuration ID",
                source_configuration_id,
            )?,
            source_epoch,
            target_epoch,
            payload,
        };
        request.validate()?;
        Ok(request)
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        if self.contract_version != OPERATION_CONTRACT_VERSION {
            return Err(ContractError::UnsupportedProfile {
                field: "operation contract version",
                expected: "1",
                actual: self.contract_version.to_string(),
            });
        }
        if self.target_epoch < self.source_epoch {
            return Err(ContractError::EpochRegression {
                source: self.source_epoch,
                target: self.target_epoch,
            });
        }
        self.payload.validate()?;
        if self.payload.changes_primary_authority() && self.target_epoch == self.source_epoch {
            return Err(ContractError::EpochNotAdvanced {
                source: self.source_epoch,
                target: self.target_epoch,
            });
        }
        Ok(())
    }

    pub fn contract_version(&self) -> u16 {
        self.contract_version
    }

    pub fn resource_id(&self) -> &str {
        self.resource_id.as_str()
    }

    pub fn operation_id(&self) -> &str {
        self.operation_id.as_str()
    }

    pub fn source_configuration_id(&self) -> &str {
        self.source_configuration_id.as_str()
    }

    pub fn source_epoch(&self) -> u64 {
        self.source_epoch
    }

    pub fn target_epoch(&self) -> u64 {
        self.target_epoch
    }

    pub fn payload(&self) -> &OperationPayload {
        &self.payload
    }

    pub fn canonical_input(&self) -> Vec<u8> {
        let mut writer = CanonicalWriter::default();
        writer.bytes(b"kuberic.sqlserver.operation");
        writer.u16(self.contract_version);
        writer.string(self.resource_id.as_str());
        writer.string(self.operation_id.as_str());
        writer.string(self.source_configuration_id.as_str());
        writer.u64(self.source_epoch);
        writer.u64(self.target_epoch);
        self.payload.encode(&mut writer);
        writer.finish()
    }

    /// Encodes the requested database effect, excluding the operation identity.
    ///
    /// Two requests share a canonical effect when they ask SQL Server for the
    /// same thing, even if a planner restart gave them different operation IDs.
    /// The distinct domain-separation prefix keeps this encoding from colliding
    /// with [`Self::canonical_input`].
    pub fn canonical_effect(&self) -> Vec<u8> {
        let mut writer = CanonicalWriter::default();
        writer.bytes(b"kuberic.sqlserver.operation.effect");
        writer.u16(self.contract_version);
        writer.string(self.resource_id.as_str());
        writer.string(self.source_configuration_id.as_str());
        writer.u64(self.source_epoch);
        writer.u64(self.target_epoch);
        self.payload.encode(&mut writer);
        writer.finish()
    }

    pub fn input_signature(&self) -> InputSignature {
        InputSignature(digest(&self.canonical_input()))
    }

    /// Digest over [`Self::canonical_effect`].
    ///
    /// This is a duplicate-effect detector, not an idempotency key: unlike
    /// [`Self::input_signature`] it deliberately ignores the operation ID.
    pub fn effect_signature(&self) -> EffectSignature {
        EffectSignature(digest(&self.canonical_effect()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationEnvelope {
    request: OperationRequest,
    destructive_approval: Option<DestructiveApproval>,
    fence: Option<FenceReference>,
}

impl OperationEnvelope {
    pub fn new(
        request: OperationRequest,
        destructive_approval: Option<DestructiveApproval>,
        fence: Option<FenceReference>,
    ) -> Result<Self, ContractError> {
        let envelope = Self {
            request,
            destructive_approval,
            fence,
        };
        envelope.validate()?;
        Ok(envelope)
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        self.request.validate()?;
        let input_signature = self.request.input_signature();

        match (
            &self.destructive_approval,
            self.request.payload.destructive(),
        ) {
            (None, true) => return Err(ContractError::MissingDestructiveApproval),
            (Some(_), false) => return Err(ContractError::UnexpectedDestructiveApproval),
            (Some(approval), true)
                if approval.approved_operation_id != self.request.operation_id =>
            {
                return Err(ContractError::ApprovalOperationMismatch);
            }
            (Some(approval), true) if approval.approved_input_signature != input_signature => {
                return Err(ContractError::ApprovalInputMismatch);
            }
            _ => {}
        }

        match (self.request.payload.required_fence(), &self.fence) {
            (Some(_), None) => return Err(ContractError::MissingFence),
            (None, Some(_)) => return Err(ContractError::UnexpectedFence),
            (Some(_), Some(fence)) if fence.operation_id != self.request.operation_id => {
                return Err(ContractError::FenceOperationMismatch);
            }
            (Some(_), Some(fence)) if fence.input_signature != input_signature => {
                return Err(ContractError::FenceInputMismatch);
            }
            (Some(expected), Some(fence)) if expected != &fence.fenced_replica => {
                return Err(ContractError::FenceTargetMismatch);
            }
            _ => {}
        }

        Ok(())
    }

    pub fn request(&self) -> &OperationRequest {
        &self.request
    }

    pub fn destructive_approval(&self) -> Option<&DestructiveApproval> {
        self.destructive_approval.as_ref()
    }

    pub fn fence(&self) -> Option<&FenceReference> {
        self.fence.as_ref()
    }

    pub fn operation_id(&self) -> &str {
        self.request.operation_id()
    }

    pub fn canonical_input(&self) -> Vec<u8> {
        self.request.canonical_input()
    }

    pub fn input_signature(&self) -> InputSignature {
        self.request.input_signature()
    }

    pub fn canonical_effect(&self) -> Vec<u8> {
        self.request.canonical_effect()
    }

    pub fn effect_signature(&self) -> EffectSignature {
        self.request.effect_signature()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InputSignature([u8; 32]);

impl InputSignature {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for InputSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_digest(f, &self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EffectSignature([u8; 32]);

impl EffectSignature {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for EffectSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_digest(f, &self.0)
    }
}

fn digest(value: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(value);
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(&digest);
    bytes
}

fn write_digest(f: &mut fmt::Formatter<'_>, bytes: &[u8; 32]) -> fmt::Result {
    f.write_str("sha256:")?;
    for byte in bytes {
        write!(f, "{byte:02x}")?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationRecord {
    operation_id: OpaqueId,
    input_signature: InputSignature,
    effect_signature: EffectSignature,
}

impl OperationRecord {
    pub fn from_envelope(envelope: &OperationEnvelope) -> Self {
        Self {
            operation_id: envelope.request.operation_id.clone(),
            input_signature: envelope.input_signature(),
            effect_signature: envelope.effect_signature(),
        }
    }

    pub fn operation_id(&self) -> &str {
        self.operation_id.as_str()
    }

    pub fn input_signature(&self) -> InputSignature {
        self.input_signature
    }

    pub fn effect_signature(&self) -> EffectSignature {
        self.effect_signature
    }

    /// Classifies a candidate against a retained terminal result.
    ///
    /// The retained operation ID alone is not a sufficient key. A planner that
    /// crashes after dispatch but before persisting its intent can regenerate
    /// the same native effect under a fresh operation ID, so a candidate that
    /// requests an identical effect under a different ID is reported separately
    /// rather than being treated as unrelated work.
    pub fn classify(
        &self,
        candidate: &OperationEnvelope,
    ) -> Result<ReplayDisposition, ContractError> {
        if self.operation_id != candidate.request.operation_id {
            if self.effect_signature == candidate.effect_signature() {
                return Ok(ReplayDisposition::DuplicateEffectNewOperationId);
            }
            return Ok(ReplayDisposition::DifferentOperation);
        }
        if self.input_signature != candidate.input_signature() {
            return Err(ContractError::OperationIdReuse);
        }
        Ok(ReplayDisposition::ExactDuplicate)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayDisposition {
    /// Same operation ID and same canonical input: the retained result applies.
    ExactDuplicate,
    /// A different operation ID requesting an identical native effect. The
    /// effect may already have been applied, so the caller must reobserve the
    /// native postcondition instead of dispatching again.
    DuplicateEffectNewOperationId,
    /// Unrelated work.
    DifferentOperation,
}

#[derive(Default)]
struct CanonicalWriter {
    bytes: Vec<u8>,
}

impl CanonicalWriter {
    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        let length = u32::try_from(value.len())
            .expect("canonical operation fields are bounded well below 4 GiB");
        self.u32(length);
        self.bytes.extend_from_slice(value);
    }

    fn string(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    fn optional<T>(&mut self, value: Option<&T>, encode: impl FnOnce(&mut Self, &T)) {
        match value {
            Some(value) => {
                self.u8(1);
                encode(self, value);
            }
            None => self.u8(0),
        }
    }

    fn replica(&mut self, value: &ReplicaIdentity) {
        self.string(value.logical_id());
        self.optional(value.native_replica_id(), |writer, id| {
            writer.string(id.as_str());
        });
        self.string(value.incarnation());
    }

    fn endpoint(&mut self, value: &Endpoint) {
        self.string(value.host());
        self.u16(value.port());
    }

    fn availability_group(&mut self, value: &AvailabilityGroupIdentity) {
        self.string(value.name.as_str());
        self.string(value.group_id.as_str());
    }

    fn database(&mut self, value: &DatabaseIdentity) {
        self.string(value.name.as_str());
        self.string(value.group_database_id.as_str());
    }

    fn database_lineage(&mut self, value: &DatabaseLineage) {
        self.database(&value.database);
        self.string(value.recovery_fork_id.as_str());
    }
}

fn require_native_replica(
    field: &'static str,
    replica: &ReplicaIdentity,
) -> Result<(), ContractError> {
    if replica.native_replica_id().is_none() {
        return Err(ContractError::MissingNativeIdentity { field });
    }
    Ok(())
}
