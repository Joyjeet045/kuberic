//! Direct, operation-specific durable switchover execution.
//!
//! Production history is restricted to the version-1 activity identities in
//! [`SWITCHOVER_ACTIVITY_IDENTITIES`]. Only the direct execution entry points
//! are available from a non-test build:
//!
//! ```compile_fail
//! use kuberic_operator::durable::start_switchover;
//! ```

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::ResourceExt;
use kuberic_core::{
    driver::ReplicaHandle,
    types::{ReplicaId, ReplicaInstanceId},
};
#[cfg(test)]
use kuberic_durable_execution::ExactBytes;
use kuberic_durable_execution::{
    CheckpointLimits, ExecutionId, ExecutionSpec, InMemoryCheckpointStore,
    KubernetesCheckpointOwner, KubernetesCheckpointOwnerScope, KubernetesCheckpointStore,
    KubernetesCheckpointStoreOptions,
};
use rand::random;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    cluster_api::ClusterApi,
    crd::{
        DurableOperationStatus, KubericSet, StablePartitionSnapshotStatus,
        SwitchoverAdmissionInputStatus, SwitchoverExecutionStatus,
    },
};

pub mod activities;
mod adapter;
mod model;
mod prepare;
mod quarantine;
mod workflow;

pub use activities::DirectActivityAccounting as SwitchoverActivityAccounting;
pub use adapter::{DirectSwitchoverPreparedActivityResolver, DirectSwitchoverRunnerAdapter};
pub use workflow::{
    DirectSwitchoverTerminalBranch, DirectSwitchoverTerminalRecord, DirectSwitchoverWorkflow,
};

pub use super::checkpoint_store::DurableCheckpointStore;
use super::{
    OperationObservations, ReplicaObservation,
    checkpoint_store::{CheckpointMeasurementDecoder, DurableCheckpointMeasurementsSnapshot},
    workflow_host::{DurableOperatorHost, DurableWorkflowRuntime},
};

pub(crate) use model::admit_direct_switchover as direct_initial_operation;

pub const SWITCHOVER_CONTRACT_VERSION: u32 = activities::DIRECT_SWITCHOVER_CONTRACT_VERSION;
pub const SWITCHOVER_MAX_REPLICAS: usize = crate::crd::KUBERIC_MAX_REPLICAS as usize;
pub const SWITCHOVER_MAX_ACTIVITY_RECORDS: usize = adapter::DIRECT_SWITCHOVER_MAX_ACTIVITY_RECORDS;
pub const SWITCHOVER_MAX_TRANSITION_FUEL: usize = 64;
pub const SWITCHOVER_MAX_RUNNER_FUEL: usize = adapter::DIRECT_SWITCHOVER_MAX_RUNNER_FUEL;
pub const SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES: usize =
    adapter::DIRECT_SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES;
pub const SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES: usize =
    adapter::DIRECT_SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES;
pub const SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES: usize =
    adapter::DIRECT_SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES;
pub const SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES: u64 =
    adapter::DIRECT_SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES;
pub const SWITCHOVER_MAX_ERROR_BYTES: usize = activities::DIRECT_ACTIVITY_ERROR_MAX_BYTES;
pub const SWITCHOVER_ACTIVITY_IDENTITIES: &[(&str, u32)] =
    activities::ALL_DIRECT_ACTIVITY_IDENTITIES;

pub type SwitchoverHost = DurableOperatorHost;
pub type SwitchoverExecution = SwitchoverExecutionStatus;

#[derive(Clone, Debug)]
pub struct SwitchoverExposureFault {
    activity_name: String,
    pending: Arc<AtomicBool>,
    triggered: Arc<AtomicBool>,
}

impl SwitchoverExposureFault {
    pub fn once(activity_name: impl Into<String>) -> Result<Self, String> {
        let activity_name = activity_name.into();
        if !SWITCHOVER_ACTIVITY_IDENTITIES
            .iter()
            .any(|(name, _)| *name == activity_name)
        {
            return Err(format!(
                "unknown direct switchover exposure fault activity {activity_name}"
            ));
        }
        Ok(Self {
            activity_name,
            pending: Arc::new(AtomicBool::new(true)),
            triggered: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn was_triggered(&self) -> bool {
        self.triggered.load(Ordering::SeqCst)
    }

    fn interrupt(&self, activity_name: &str) -> bool {
        if self.activity_name != activity_name
            || self
                .pending
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return false;
        }
        self.triggered.store(true, Ordering::SeqCst);
        true
    }
}

/// Process-local host cache. Checkpoints remain the recovery authority.
pub struct DurableSwitchoverRuntime {
    inner: Arc<DurableWorkflowRuntime>,
}

impl DurableSwitchoverRuntime {
    pub fn kubernetes(client: kube::Client) -> Self {
        Self {
            inner: Arc::new(DurableWorkflowRuntime::kubernetes(client)),
        }
    }

    pub fn in_memory(store: InMemoryCheckpointStore) -> Self {
        Self {
            inner: Arc::new(DurableWorkflowRuntime::in_memory(store)),
        }
    }

    pub fn shared(inner: Arc<DurableWorkflowRuntime>) -> Self {
        Self { inner }
    }

    pub async fn native_host(
        &self,
        namespace: &str,
        set_name: &str,
        set_uid: &str,
        reference: &SwitchoverExecutionStatus,
    ) -> Result<Arc<Mutex<SwitchoverHost>>, String> {
        let execution_id = native_execution_id(reference)?;
        self.inner
            .host(
                namespace,
                set_name,
                set_uid,
                "switchover",
                execution_id,
                &reference.execution_id,
                &reference.checkpoint_name,
                checkpoint_store_options(namespace, set_name, set_uid)?,
                checkpoint_limits(),
                checkpoint_measurement_decoder(),
            )
            .await
    }

    pub async fn forget(&self, namespace: &str, set_name: &str, set_uid: &str, execution_id: &str) {
        self.inner
            .forget(namespace, set_name, set_uid, "switchover", execution_id)
            .await;
    }

    pub async fn host_count(&self) -> usize {
        self.inner.host_count().await
    }

    pub async fn measurements(
        &self,
        namespace: &str,
        set_name: &str,
        set_uid: &str,
        execution_id: &str,
    ) -> Option<DurableCheckpointMeasurementsSnapshot> {
        self.inner
            .measurements(namespace, set_name, set_uid, "switchover", execution_id)
            .await
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwitchoverWorkflowInput {
    pub version: u32,
    pub execution_id: String,
    pub initial_operation: DurableOperationStatus,
}

pub struct SwitchoverRunnerContext {
    pub observations: OperationObservations,
    pub handles: BTreeMap<ReplicaId, Box<dyn ReplicaHandle>>,
    pub addressed_instances: BTreeMap<ReplicaId, ReplicaInstanceId>,
}

pub async fn collect_switchover_runner_context(
    initial: &DurableOperationStatus,
    set: &KubericSet,
    api: &dyn ClusterApi,
    current_pods: &[(ReplicaId, ReplicaInstanceId, &Pod)],
) -> Result<SwitchoverRunnerContext, String> {
    for member in &initial.previous_snapshot.members {
        let Some((_, instance_id, _)) = current_pods.iter().find(|(id, _, _)| *id == member.id)
        else {
            return Err(format!(
                "durable switchover replica {} has no current pod",
                member.id
            ));
        };
        if instance_id.as_str() != member.instance_id {
            return Err(format!(
                "durable switchover replica {} incarnation changed",
                member.id
            ));
        }
    }

    let mut handles: BTreeMap<ReplicaId, Box<dyn ReplicaHandle>> = BTreeMap::new();
    let mut observations = OperationObservations::new();
    for (replica_id, _, pod) in current_pods {
        if !initial
            .target_snapshot
            .members
            .iter()
            .any(|member| member.id == *replica_id)
        {
            continue;
        }
        let Ok(handle) = api.create_replica_handle(*replica_id, pod, &set.spec).await else {
            continue;
        };
        if let Ok(status) = handle.get_status().await {
            observations.insert(
                *replica_id,
                ReplicaObservation {
                    status,
                    control_address: handle.control_address(),
                    replicator_address: handle.replicator_address(),
                    pod_name: pod.name_any(),
                    pod_role_label: pod
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|labels| labels.get("kuberic.io/role"))
                        .cloned(),
                },
            );
        }
        handles.insert(*replica_id, handle);
    }
    let addressed_instances = handles
        .iter()
        .map(|(replica_id, handle)| (*replica_id, handle.instance_id()))
        .collect();
    Ok(SwitchoverRunnerContext {
        observations,
        handles,
        addressed_instances,
    })
}

pub fn new_switchover_execution(
    operation_authority: &str,
    previous_snapshot: StablePartitionSnapshotStatus,
    target_primary_id: ReplicaId,
    now: i64,
) -> Result<SwitchoverExecutionStatus, String> {
    if operation_authority.is_empty() {
        return Err("framework-native switchover requires operation authority".to_string());
    }
    let execution_id = ExecutionId::from_bytes(random());
    let execution_hex = encode_execution_id(execution_id);
    let reference = SwitchoverExecutionStatus {
        contract_version: SWITCHOVER_CONTRACT_VERSION,
        execution_id: execution_hex,
        checkpoint_name: KubernetesCheckpointStore::object_name(execution_id),
        input: SwitchoverAdmissionInputStatus {
            operation_authority: operation_authority.to_string(),
            previous_snapshot,
            target_primary_id,
            accepted_unix_seconds: now,
        },
    };
    native_execution_spec(&reference)?;
    Ok(reference)
}

pub fn native_execution_id(reference: &SwitchoverExecutionStatus) -> Result<ExecutionId, String> {
    if reference.contract_version != SWITCHOVER_CONTRACT_VERSION {
        return Err(format!(
            "unsupported framework-native switchover contract version {}",
            reference.contract_version
        ));
    }
    let bytes = decode_execution_id(&reference.execution_id)?;
    let execution_id = ExecutionId::from_bytes(bytes);
    let expected_name = KubernetesCheckpointStore::object_name(execution_id);
    if reference.checkpoint_name != expected_name {
        return Err(format!(
            "framework-native switchover checkpoint name mismatch: expected {expected_name}, found {}",
            reference.checkpoint_name
        ));
    }
    Ok(execution_id)
}

pub fn native_initial_operation(
    reference: &SwitchoverExecutionStatus,
) -> Result<DurableOperationStatus, String> {
    native_execution_id(reference)?;
    let input = &reference.input;
    if input.operation_authority.is_empty()
        || input.accepted_unix_seconds <= 0
        || input
            .accepted_unix_seconds
            .checked_add(super::ACTION_DEADLINE_SECONDS)
            .is_none()
    {
        return Err("framework-native switchover immutable admission input is invalid".to_string());
    }
    direct_initial_operation(
        &format!(
            "{}:framework-native:{}",
            input.operation_authority, reference.execution_id
        ),
        input.previous_snapshot.clone(),
        input.target_primary_id,
        input.accepted_unix_seconds,
    )
}

pub fn validate_native_operation_authority(
    reference: &SwitchoverExecutionStatus,
    expected_set_uid: &str,
) -> Result<(), String> {
    if expected_set_uid.is_empty() || reference.input.operation_authority != expected_set_uid {
        return Err(
            "framework-native switchover operation authority does not match KubericSet UID"
                .to_string(),
        );
    }
    Ok(())
}

pub fn native_execution_spec(
    reference: &SwitchoverExecutionStatus,
) -> Result<ExecutionSpec, String> {
    let execution_id = native_execution_id(reference)?;
    let initial_operation = native_initial_operation(reference)?;
    adapter::direct_execution_spec(execution_id, initial_operation)
}

pub fn checkpoint_limits() -> CheckpointLimits {
    adapter::direct_checkpoint_limits()
}

pub fn checkpoint_measurement_decoder() -> CheckpointMeasurementDecoder {
    adapter::direct_checkpoint_measurement_decoder()
}

pub(crate) fn validate_runner_fuel(actual: usize) -> Result<(), String> {
    adapter::validate_direct_runner_fuel(actual)
}

pub fn is_switchover_activity_identity(name: &str, version: u32) -> bool {
    SWITCHOVER_ACTIVITY_IDENTITIES
        .iter()
        .any(|(expected_name, expected_version)| {
            *expected_name == name && *expected_version == version
        })
}

pub fn checkpoint_store_options(
    namespace: &str,
    set_name: &str,
    set_uid: &str,
) -> Result<KubernetesCheckpointStoreOptions, String> {
    if namespace.is_empty() || set_name.is_empty() || set_uid.is_empty() {
        return Err(
            "durable switchover checkpoint owner requires namespace, name, and UID".to_string(),
        );
    }
    let owner = KubernetesCheckpointOwner::new(
        OwnerReference {
            api_version: "kuberic.io/v1".to_string(),
            kind: "KubericSet".to_string(),
            name: set_name.to_string(),
            uid: set_uid.to_string(),
            controller: Some(false),
            block_owner_deletion: Some(false),
        },
        KubernetesCheckpointOwnerScope::Namespaced(namespace.to_string()),
    );
    Ok(KubernetesCheckpointStoreOptions::default().with_owner(owner))
}

#[cfg(test)]
pub(crate) fn encode_terminal(
    terminal: &DirectSwitchoverTerminalRecord,
) -> Result<ExactBytes, String> {
    let encoded = serde_json::to_vec(terminal)
        .map_err(|error| format!("serialize direct switchover terminal outcome: {error}"))?;
    adapter::validate_direct_terminal_payload_bytes(encoded.len())?;
    Ok(ExactBytes::new(encoded))
}

pub(crate) fn bounded_utf8(value: &str, maximum_bytes: usize) -> String {
    let mut boundary = value.len().min(maximum_bytes);
    while !value.is_char_boundary(boundary) {
        boundary = boundary.saturating_sub(1);
    }
    value[..boundary].to_string()
}

pub(crate) fn encode_execution_id(execution_id: ExecutionId) -> String {
    let mut encoded = String::with_capacity(32);
    for byte in execution_id.as_bytes() {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn decode_execution_id(value: &str) -> Result<[u8; 16], String> {
    if value.len() != 32 {
        return Err(format!(
            "durable switchover execution ID must contain 32 lowercase hexadecimal characters; found {}",
            value.len()
        ));
    }
    let mut bytes = [0_u8; 16];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_hex(pair[0])?;
        let low = decode_hex(pair[1])?;
        bytes[index] = (high << 4) | low;
    }
    Ok(bytes)
}

fn decode_hex(value: u8) -> Result<u8, String> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err("durable switchover execution ID must be lowercase hexadecimal".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{EpochStatus, StableReplicaRoleStatus, StableReplicaSnapshotStatus};

    fn snapshot() -> StablePartitionSnapshotStatus {
        StablePartitionSnapshotStatus {
            epoch: EpochStatus {
                data_loss_number: 1,
                configuration_number: 7,
            },
            primary_id: 1,
            members: vec![
                StableReplicaSnapshotStatus {
                    id: 1,
                    instance_id: "one".to_string(),
                    role: StableReplicaRoleStatus::Primary,
                    election_metadata: None,
                },
                StableReplicaSnapshotStatus {
                    id: 2,
                    instance_id: "two".to_string(),
                    role: StableReplicaRoleStatus::ActiveSecondary,
                    election_metadata: None,
                },
            ],
            write_quorum: 2,
        }
    }

    #[test]
    fn direct_switchover_reference_uses_only_the_incompatible_current_contract() {
        let reference = new_switchover_execution("set-uid", snapshot(), 2, 100).unwrap();
        assert_eq!(reference.contract_version, SWITCHOVER_CONTRACT_VERSION);
        assert_eq!(SWITCHOVER_CONTRACT_VERSION, 4);
        assert!(native_execution_spec(&reference).is_ok());

        let mut previous = reference;
        previous.contract_version -= 1;
        assert!(native_execution_id(&previous).is_err());
        assert!(native_initial_operation(&previous).is_err());
        assert!(native_execution_spec(&previous).is_err());
    }

    #[test]
    fn direct_switchover_checkpoint_owner_is_exact_noncontrolling_and_namespaced() {
        let options = checkpoint_store_options("default", "set", "set-uid").unwrap();
        let owner = options.owner().unwrap();
        assert_eq!(
            owner.scope(),
            &KubernetesCheckpointOwnerScope::Namespaced("default".to_string())
        );
        assert_eq!(owner.reference().api_version, "kuberic.io/v1");
        assert_eq!(owner.reference().kind, "KubericSet");
        assert_eq!(owner.reference().name, "set");
        assert_eq!(owner.reference().uid, "set-uid");
        assert_eq!(owner.reference().controller, Some(false));
        assert_eq!(owner.reference().block_owner_deletion, Some(false));
        for (namespace, name, uid) in [
            ("", "set", "set-uid"),
            ("default", "", "set-uid"),
            ("default", "set", ""),
        ] {
            assert!(checkpoint_store_options(namespace, name, uid).is_err());
        }
    }

    #[test]
    fn direct_switchover_production_surface_has_only_twenty_version_one_names() {
        assert_eq!(SWITCHOVER_ACTIVITY_IDENTITIES.len(), 20);
        let generic = ["kuberic.switchover.", "native", "-boundary"].concat();
        for (name, version) in SWITCHOVER_ACTIVITY_IDENTITIES {
            assert!(name.starts_with("kuberic.switchover."));
            assert_eq!(*version, 1);
            assert_ne!(*name, generic);
            assert!(is_switchover_activity_identity(name, *version));
        }
    }

    #[test]
    fn direct_switchover_source_has_no_obsolete_production_engine() {
        let execution = include_str!("switchover_execution.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .unwrap();
        let effects = include_str!("effects.rs");
        let durable_mod = include_str!("mod.rs");
        let reconciler = include_str!("../reconciler.rs")
            .split("\n#[cfg(test)]\n#[path = \"reconciler/durable_routing_tests.rs\"]")
            .next()
            .unwrap();
        let obsolete = [
            ["DurableSwitchover", "State"].concat(),
            ["DurableSwitchover", "Activity"].concat(),
            ["SwitchoverActivity", "Kind"].concat(),
            ["evaluate_adapter", "_step"].concat(),
            ["bridge_switchover", "_runner_step"].concat(),
            ["kuberic.switchover.", "native", "-boundary"].concat(),
        ];
        for symbol in obsolete {
            assert!(
                !execution.contains(&symbol),
                "{symbol} remains in production"
            );
            assert!(!effects.contains(&symbol), "{symbol} remains in effects");
            assert!(
                !reconciler.contains(&symbol),
                "{symbol} remains in reconciler"
            );
        }
        assert!(!durable_mod.contains("mod switchover;"));
        assert!(reconciler.contains("DirectSwitchoverRunnerAdapter"));
        assert!(reconciler.contains("&DirectSwitchoverWorkflow"));
    }
}
