//! Normalized Kubernetes, storage, routing, and replica-agent evidence.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::types::{
    AcceptedStatus, AccessStatus, ConfigurationDescriptor, Epoch, FaultType, LoadMetric,
    OperationId, PodUid, ProcessSessionId, PvcUid, ReplicaId, ReplicaIdentity, ReplicaInstanceId,
    ReplicaRole, ResourceUid,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DesiredState {
    pub generation: u64,
    pub replicas: u32,
    pub image: String,
    pub failover_delay_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubernetesReplicaObservation {
    pub replica_id: ReplicaId,
    pub pod_name: String,
    pub pod_uid: Option<PodUid>,
    pub pvc_name: String,
    pub pvc_uid: Option<PvcUid>,
    #[serde(default)]
    pub image: Option<String>,
    pub pod_ready: bool,
    #[serde(default)]
    pub peer_endpoint_ready: bool,
}

impl KubernetesReplicaObservation {
    pub fn has_exact_scaffolding(&self) -> bool {
        self.pod_uid.is_some() && self.pvc_uid.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UninitializedAgentObservation {
    pub protocol_version: u32,
    pub resource_uid: ResourceUid,
    pub replica_id: ReplicaId,
    pub pod_uid: PodUid,
    pub pvc_uid: PvcUid,
    pub process_session_id: ProcessSessionId,
    pub report_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentReport {
    pub protocol_version: u32,
    pub resource_uid: ResourceUid,
    pub identity: ReplicaIdentity,
    pub process_session_id: ProcessSessionId,
    pub report_sequence: u64,
    pub role: ReplicaRole,
    #[serde(default = "default_access_status")]
    pub read_status: AccessStatus,
    pub write_status: AccessStatus,
    pub healthy: bool,
    pub epoch: Epoch,
    pub previous_configuration: Option<ConfigurationDescriptor>,
    pub current_configuration: Option<ConfigurationDescriptor>,
    pub current_progress: i64,
    #[serde(default)]
    pub verified_replication_lsn: Option<i64>,
    pub committed_lsn: i64,
    pub catch_up_capability: Option<i64>,
    #[serde(default)]
    pub current_configuration_quorum_progress: i64,
    #[serde(default)]
    pub catch_up_boundary: Option<i64>,
    #[serde(default)]
    pub catch_up_complete: bool,
    #[serde(default)]
    pub deactivated_lsn: Option<i64>,
    #[serde(default)]
    pub deactivation_epoch: Option<Epoch>,
    #[serde(default)]
    pub load_metrics: Vec<LoadMetric>,
    #[serde(default)]
    pub reported_fault: Option<FaultType>,
    #[serde(default)]
    pub pending_operation_id: Option<OperationId>,
    #[serde(default)]
    pub retained_operation_id: Option<OperationId>,
    #[serde(default)]
    pub builds: Vec<AgentBuildReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentBuildReport {
    pub build_id: OperationId,
    pub target: ReplicaIdentity,
    pub last_sequence: u64,
    pub durable_lsn: i64,
    pub completed: bool,
}

fn default_access_status() -> AccessStatus {
    AccessStatus::NotPrimary
}

impl Default for AgentReport {
    fn default() -> Self {
        Self {
            protocol_version: 0,
            resource_uid: ResourceUid::default(),
            identity: ReplicaIdentity {
                replica_id: ReplicaId::default(),
                instance_id: ReplicaInstanceId::default(),
                agent_generation: crate::types::AgentGeneration::default(),
            },
            process_session_id: ProcessSessionId::default(),
            report_sequence: 0,
            role: ReplicaRole::None,
            read_status: AccessStatus::NotPrimary,
            write_status: AccessStatus::NotPrimary,
            healthy: false,
            epoch: Epoch::default(),
            previous_configuration: None,
            current_configuration: None,
            current_progress: 0,
            verified_replication_lsn: None,
            committed_lsn: 0,
            catch_up_capability: None,
            current_configuration_quorum_progress: 0,
            catch_up_boundary: None,
            catch_up_complete: false,
            deactivated_lsn: None,
            deactivation_epoch: None,
            load_metrics: Vec::new(),
            reported_fault: None,
            pending_operation_id: None,
            retained_operation_id: None,
            builds: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
/// Lookup key that preserves multiple incarnations of one logical replica.
pub struct ReplicaObservationKey {
    pub replica_id: ReplicaId,
    pub instance_id: ReplicaInstanceId,
}

impl ReplicaObservationKey {
    pub fn new(replica_id: ReplicaId, instance_id: ReplicaInstanceId) -> Self {
        Self {
            replica_id,
            instance_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum AgentObservation {
    Absent,
    Unreachable { message: String },
    Invalid { message: String },
    Uninitialized(UninitializedAgentObservation),
    Report(Box<AgentReport>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaObservation {
    pub kubernetes: Option<KubernetesReplicaObservation>,
    pub agent: AgentObservation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservationFailure {
    pub source: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReportWatermark {
    pub process_session_id: ProcessSessionId,
    pub report_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RoutingObservation {
    #[serde(default)]
    pub service_present: bool,
    #[serde(default)]
    pub unresolved_write_target: bool,
    pub write_target: Option<ReplicaIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
/// Immutable input consumed by one evaluator invocation.
pub struct ObservationSnapshot {
    pub resource_uid: ResourceUid,
    pub resource_version: String,
    pub desired: DesiredState,
    pub status: AcceptedStatus,
    pub replicas: BTreeMap<ReplicaObservationKey, ReplicaObservation>,
    pub previous_report_watermarks: BTreeMap<ReplicaObservationKey, ReportWatermark>,
    pub durable_storage_evidence: bool,
    pub supporting_resources_ready: bool,
    pub routing: RoutingObservation,
    pub observation_failures: Vec<ObservationFailure>,
    pub now_unix_seconds: i64,
}

impl ObservationSnapshot {
    pub fn intended_replica_ids(&self) -> Vec<ReplicaId> {
        (1..=self.desired.replicas)
            .map(|value| ReplicaId::new(i64::from(value)))
            .collect()
    }

    pub fn has_complete_scaffolding(&self) -> bool {
        self.intended_replica_ids().into_iter().all(|replica_id| {
            self.scaffolding_for(replica_id)
                .is_some_and(KubernetesReplicaObservation::has_exact_scaffolding)
        })
    }

    pub fn scaffolding_for(&self, replica_id: ReplicaId) -> Option<&KubernetesReplicaObservation> {
        let mut matching = self
            .replicas
            .iter()
            .filter(|(key, _)| key.replica_id == replica_id)
            .filter_map(|(_, observation)| observation.kubernetes.as_ref());
        let first = matching.next()?;
        matching.next().is_none().then_some(first)
    }

    pub fn scaffolding_observation_for(
        &self,
        replica_id: ReplicaId,
    ) -> Option<&ReplicaObservation> {
        let mut matching = self
            .replicas
            .iter()
            .filter(|(key, _)| key.replica_id == replica_id)
            .filter_map(|(_, observation)| observation.kubernetes.as_ref().map(|_| observation));
        let first = matching.next()?;
        matching.next().is_none().then_some(first)
    }

    pub fn observation_for_identity(
        &self,
        identity: &ReplicaIdentity,
    ) -> Option<&ReplicaObservation> {
        self.replicas.get(&ReplicaObservationKey::new(
            identity.replica_id,
            identity.instance_id.clone(),
        ))
    }
}
