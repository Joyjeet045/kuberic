//! Shared operator host cache and checkpoint-provider construction.

use std::{collections::HashMap, sync::Arc};

use kuberic_durable_execution::{
    ActivitySpec, AttemptId, CheckpointLimits, DispatchPermit, DurableHost, ExecutionId, HostEpoch,
    InMemoryCheckpointStore, KubernetesCheckpointStore, KubernetesCheckpointStoreOptions,
    LogicalActivityId,
};
use rand::random;
use tokio::sync::Mutex;

use super::pilot_store::{
    CheckpointMeasurementDecoder, DurableCheckpointMeasurementsSnapshot, DurableCheckpointStore,
    MeasuredDurableCheckpointStore,
};

// COMPLEXITY-BOUNDARY: shared-operator-workflow-host:start
const MAX_COMPLETED_MEASUREMENT_SNAPSHOTS: usize = 64;

pub type DurableOperatorHost = DurableHost<MeasuredDurableCheckpointStore>;

/// Single-use prepared-effect permit guard shared by operator workflows.
pub struct DurablePermitGuard {
    permit: Option<DispatchPermit>,
}

impl DurablePermitGuard {
    pub fn new(permit: DispatchPermit) -> Self {
        Self {
            permit: Some(permit),
        }
    }

    pub fn consume(
        &mut self,
        expected_spec: &ActivitySpec,
        expected_activity: &LogicalActivityId,
        attempt_id: AttemptId,
        workflow: &str,
    ) -> Result<DispatchPermit, String> {
        let permit = self
            .permit
            .as_ref()
            .ok_or_else(|| format!("durable {workflow} dispatch permit was already consumed"))?;
        if permit.attempt_id() != attempt_id
            || permit.activity() != expected_activity
            || permit.activity().spec() != expected_spec
        {
            return Err(format!(
                "durable {workflow} dispatch permit does not match prepared activity or attempt"
            ));
        }
        Ok(self
            .permit
            .take()
            .expect("permit existence checked before consumption"))
    }

    pub fn activity(&self) -> Option<&LogicalActivityId> {
        self.permit.as_ref().map(DispatchPermit::activity)
    }
}

#[derive(Clone)]
enum DurableStoreFactory {
    Kubernetes(kube::Client),
    InMemory(InMemoryCheckpointStore),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DurableHostKey {
    namespace: String,
    set_name: String,
    set_uid: String,
    workflow: &'static str,
    execution_id: String,
}

/// Process-local cache for workflow hosts. Checkpoints remain authoritative.
pub struct DurableWorkflowRuntime {
    factory: DurableStoreFactory,
    host_epoch: HostEpoch,
    hosts: Mutex<HashMap<DurableHostKey, Arc<Mutex<DurableOperatorHost>>>>,
    completed_measurements: Mutex<HashMap<DurableHostKey, DurableCheckpointMeasurementsSnapshot>>,
}

impl DurableWorkflowRuntime {
    pub fn kubernetes(client: kube::Client) -> Self {
        Self {
            factory: DurableStoreFactory::Kubernetes(client),
            host_epoch: HostEpoch::from_bytes(random()),
            hosts: Mutex::new(HashMap::new()),
            completed_measurements: Mutex::new(HashMap::new()),
        }
    }

    pub fn in_memory(store: InMemoryCheckpointStore) -> Self {
        Self {
            factory: DurableStoreFactory::InMemory(store),
            host_epoch: HostEpoch::from_bytes(random()),
            hosts: Mutex::new(HashMap::new()),
            completed_measurements: Mutex::new(HashMap::new()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn host(
        &self,
        namespace: &str,
        set_name: &str,
        set_uid: &str,
        workflow: &'static str,
        execution_id: ExecutionId,
        encoded_execution_id: &str,
        checkpoint_name: &str,
        options: KubernetesCheckpointStoreOptions,
        limits: CheckpointLimits,
        decoder: CheckpointMeasurementDecoder,
    ) -> Result<Arc<Mutex<DurableOperatorHost>>, String> {
        let key = DurableHostKey {
            namespace: namespace.to_string(),
            set_name: set_name.to_string(),
            set_uid: set_uid.to_string(),
            workflow,
            execution_id: encoded_execution_id.to_string(),
        };
        let mut hosts = self.hosts.lock().await;
        if let Some(host) = hosts.get(&key) {
            return Ok(host.clone());
        }
        self.completed_measurements.lock().await.remove(&key);
        let store = match &self.factory {
            DurableStoreFactory::Kubernetes(client) => {
                DurableCheckpointStore::Kubernetes(Box::new(
                    KubernetesCheckpointStore::with_options(client.clone(), namespace, options)
                        .map_err(|error| {
                            format!("construct durable {workflow} checkpoint store: {error}")
                        })?,
                ))
            }
            DurableStoreFactory::InMemory(store) => DurableCheckpointStore::InMemory(store.clone()),
        };
        let host = Arc::new(Mutex::new(DurableHost::new(
            MeasuredDurableCheckpointStore::with_decoder(execution_id, store, decoder),
            self.host_epoch,
            limits,
        )));
        hosts.insert(key.clone(), host.clone());
        let expected_name = KubernetesCheckpointStore::object_name(execution_id);
        if expected_name != checkpoint_name {
            hosts.remove(&key);
            return Err(format!("durable {workflow} checkpoint identity changed"));
        }
        Ok(host)
    }

    pub async fn forget(
        &self,
        namespace: &str,
        set_name: &str,
        set_uid: &str,
        workflow: &'static str,
        execution_id: &str,
    ) {
        let key = DurableHostKey {
            namespace: namespace.to_string(),
            set_name: set_name.to_string(),
            set_uid: set_uid.to_string(),
            workflow,
            execution_id: execution_id.to_string(),
        };
        let host = self.hosts.lock().await.remove(&key);
        if let Some(host) = host {
            let measurements = host.lock().await.store().measurements();
            let mut completed = self.completed_measurements.lock().await;
            if completed.len() == MAX_COMPLETED_MEASUREMENT_SNAPSHOTS {
                if let Some(eviction_candidate) = completed.keys().next().cloned() {
                    completed.remove(&eviction_candidate);
                }
            }
            completed.insert(key, measurements);
        }
    }

    pub async fn host_count(&self) -> usize {
        self.hosts.lock().await.len()
    }

    pub async fn measurements(
        &self,
        namespace: &str,
        set_name: &str,
        set_uid: &str,
        workflow: &'static str,
        execution_id: &str,
    ) -> Option<DurableCheckpointMeasurementsSnapshot> {
        let key = DurableHostKey {
            namespace: namespace.to_string(),
            set_name: set_name.to_string(),
            set_uid: set_uid.to_string(),
            workflow,
            execution_id: execution_id.to_string(),
        };
        if let Some(host) = self.hosts.lock().await.get(&key).cloned() {
            return Some(host.lock().await.store().measurements());
        }
        self.completed_measurements.lock().await.get(&key).copied()
    }
}
// COMPLEXITY-BOUNDARY: shared-operator-workflow-host:end
