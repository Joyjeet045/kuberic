use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod, Service};

use kuberic_core::driver::ReplicaHandle;
use kuberic_core::types::{ReplicaId, ReplicaInstanceId};

use crate::crd::{KubericSetSpec, KubericSetStatus};
use crate::node_maintenance::NodeMaintenanceRequest;

fn uid_fenced_label_patch(
    pod: &Pod,
    expected_uid: &str,
    labels: BTreeMap<String, String>,
) -> Result<serde_json::Value, String> {
    if pod.metadata.uid.as_deref() != Some(expected_uid) {
        return Err("pod UID precondition failed".to_string());
    }
    let resource_version = pod
        .metadata
        .resource_version
        .as_deref()
        .ok_or_else(|| "pod has no resource version".to_string())?;
    let mut merged_labels = pod.metadata.labels.clone().unwrap_or_default();
    merged_labels.extend(labels);
    Ok(serde_json::json!([
        {
            "op": "test",
            "path": "/metadata/uid",
            "value": expected_uid
        },
        {
            "op": "test",
            "path": "/metadata/resourceVersion",
            "value": resource_version
        },
        {
            "op": "add",
            "path": "/metadata/labels",
            "value": merged_labels
        }
    ]))
}

/// Abstraction over Kubernetes API and replica creation.
/// Real impl uses kube::Client; test impl uses in-memory state.
#[async_trait]
pub trait ClusterApi: Send + Sync {
    /// List pods matching the label selector.
    async fn list_pods(&self, namespace: &str, selector: &str) -> Result<Vec<Pod>, String>;

    /// Names of nodes with an active NodeMaintenanceRequest.
    async fn list_maintenance_nodes(&self) -> Result<BTreeSet<String>, String>;

    /// Create a pod.
    async fn create_pod(&self, namespace: &str, pod: &Pod) -> Result<(), String>;

    /// Delete exactly one pod incarnation by name and Kubernetes UID.
    async fn delete_pod(
        &self,
        namespace: &str,
        pod_name: &str,
        expected_uid: &str,
    ) -> Result<(), String>;

    /// Update a pod's labels.
    async fn patch_pod_labels(
        &self,
        namespace: &str,
        pod_name: &str,
        labels: BTreeMap<String, String>,
    ) -> Result<(), String>;

    /// Update labels only if the named pod still has the exact expected UID.
    async fn patch_pod_labels_if_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        expected_uid: &str,
        labels: BTreeMap<String, String>,
    ) -> Result<(), String>;

    /// Replace the complete CRD status using optimistic resource-version fencing.
    async fn patch_set_status(
        &self,
        namespace: &str,
        set_name: &str,
        status: &KubericSetStatus,
        expected_resource_version: Option<&str>,
    ) -> Result<(), String>;

    /// Create a ReplicaHandle for a pod (gRPC or in-process).
    async fn create_replica_handle(
        &self,
        replica_id: ReplicaId,
        pod: &Pod,
        spec: &KubericSetSpec,
    ) -> Result<Box<dyn ReplicaHandle>, String>;

    // -- PVC management --

    /// Get a PVC by name.
    async fn get_pvc(&self, namespace: &str, name: &str) -> Result<PersistentVolumeClaim, String>;

    /// Create a PVC.
    async fn create_pvc(&self, namespace: &str, pvc: &PersistentVolumeClaim) -> Result<(), String>;

    /// List PVCs matching the label selector.
    async fn list_pvcs(
        &self,
        namespace: &str,
        selector: &str,
    ) -> Result<Vec<PersistentVolumeClaim>, String>;

    /// Delete a PVC by name.
    async fn delete_pvc(&self, namespace: &str, name: &str) -> Result<(), String>;

    // -- Service management --

    /// Get a Service by name.
    async fn get_service(&self, namespace: &str, name: &str) -> Result<Service, String>;

    /// Create a Service.
    async fn create_service(&self, namespace: &str, svc: &Service) -> Result<(), String>;

    /// Delete a Service by name.
    async fn delete_service(&self, namespace: &str, name: &str) -> Result<(), String>;
}

// ---------------------------------------------------------------------------
// Real implementation (kube::Client)
// ---------------------------------------------------------------------------

pub struct KubeClusterApi {
    pub client: kube::Client,
}

#[async_trait]
impl ClusterApi for KubeClusterApi {
    async fn list_pods(&self, namespace: &str, selector: &str) -> Result<Vec<Pod>, String> {
        let api: kube::Api<Pod> = kube::Api::namespaced(self.client.clone(), namespace);
        let params = kube::api::ListParams::default().labels(selector);
        api.list(&params)
            .await
            .map(|list| list.items)
            .map_err(|e| e.to_string())
    }

    async fn list_maintenance_nodes(&self) -> Result<BTreeSet<String>, String> {
        let api: kube::Api<NodeMaintenanceRequest> = kube::Api::all(self.client.clone());
        let list = api
            .list(&kube::api::ListParams::default())
            .await
            .map_err(|e| e.to_string())?;
        Ok(list
            .items
            .into_iter()
            .filter(|request| request.metadata.deletion_timestamp.is_none())
            .filter(|request| !request.spec.desired_state.releases_request())
            .filter(|request| {
                request
                    .status
                    .as_ref()
                    .is_some_and(|status| status.phase.excludes_primary_placement())
            })
            .map(|request| request.spec.node_name)
            .collect())
    }

    async fn create_pod(&self, namespace: &str, pod: &Pod) -> Result<(), String> {
        let api: kube::Api<Pod> = kube::Api::namespaced(self.client.clone(), namespace);
        match api.create(&kube::api::PostParams::default(), pod).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(ae)) if ae.code == 409 => Ok(()), // already exists
            Err(e) => Err(e.to_string()),
        }
    }

    async fn delete_pod(
        &self,
        namespace: &str,
        pod_name: &str,
        expected_uid: &str,
    ) -> Result<(), String> {
        let api: kube::Api<Pod> = kube::Api::namespaced(self.client.clone(), namespace);
        let params = kube::api::DeleteParams {
            preconditions: Some(kube::api::Preconditions {
                uid: Some(expected_uid.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        match api.delete(pod_name, &params).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(ae)) if ae.code == 404 => Ok(()), // already gone
            Err(e) => Err(e.to_string()),
        }
    }

    async fn patch_pod_labels(
        &self,
        namespace: &str,
        pod_name: &str,
        labels: BTreeMap<String, String>,
    ) -> Result<(), String> {
        let api: kube::Api<Pod> = kube::Api::namespaced(self.client.clone(), namespace);
        let patch = serde_json::json!({ "metadata": { "labels": labels } });
        api.patch(
            pod_name,
            &kube::api::PatchParams::apply("kuberic-operator"),
            &kube::api::Patch::Merge(&patch),
        )
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
    }

    async fn patch_pod_labels_if_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        expected_uid: &str,
        labels: BTreeMap<String, String>,
    ) -> Result<(), String> {
        let api: kube::Api<Pod> = kube::Api::namespaced(self.client.clone(), namespace);
        let pod = api.get(pod_name).await.map_err(|e| e.to_string())?;
        let operations = uid_fenced_label_patch(&pod, expected_uid, labels)?;
        api.patch(
            pod_name,
            &kube::api::PatchParams::default(),
            &kube::api::Patch::Json::<serde_json::Value>(
                serde_json::from_value(operations).map_err(|error| error.to_string())?,
            ),
        )
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
    }

    async fn patch_set_status(
        &self,
        namespace: &str,
        set_name: &str,
        status: &KubericSetStatus,
        expected_resource_version: Option<&str>,
    ) -> Result<(), String> {
        let api: kube::Api<crate::crd::KubericSet> =
            kube::Api::namespaced(self.client.clone(), namespace);
        let mut current = api.get(set_name).await.map_err(|e| e.to_string())?;
        if let Some(expected) = expected_resource_version
            && current.metadata.resource_version.as_deref() != Some(expected)
        {
            return Err(format!(
                "status resource version changed from {expected} to {}",
                current
                    .metadata
                    .resource_version
                    .as_deref()
                    .unwrap_or("<none>")
            ));
        }
        current.status = Some(status.clone());
        api.replace_status(set_name, &kube::api::PostParams::default(), &current)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn create_replica_handle(
        &self,
        replica_id: ReplicaId,
        pod: &Pod,
        spec: &KubericSetSpec,
    ) -> Result<Box<dyn ReplicaHandle>, String> {
        let pod_ip = pod
            .status
            .as_ref()
            .and_then(|s| s.pod_ip.as_ref())
            .cloned()
            .ok_or("pod has no IP")?;
        let instance_id = pod
            .metadata
            .uid
            .as_ref()
            .filter(|uid| !uid.is_empty())
            .cloned()
            .map(ReplicaInstanceId::new)
            .ok_or("pod has no UID")?;

        let control_addr = format!("http://{}:{}", pod_ip, spec.control_port);
        let data_addr = format!("http://{}:{}", pod_ip, spec.data_port);

        let handle = kuberic_core::grpc::handle::GrpcReplicaHandle::connect(
            replica_id,
            instance_id,
            control_addr,
            data_addr,
        )
        .await
        .map_err(|e| e.to_string())?;

        Ok(Box::new(handle))
    }

    async fn get_pvc(&self, namespace: &str, name: &str) -> Result<PersistentVolumeClaim, String> {
        let api: kube::Api<PersistentVolumeClaim> =
            kube::Api::namespaced(self.client.clone(), namespace);
        api.get(name).await.map_err(|e| e.to_string())
    }

    async fn create_pvc(&self, namespace: &str, pvc: &PersistentVolumeClaim) -> Result<(), String> {
        let api: kube::Api<PersistentVolumeClaim> =
            kube::Api::namespaced(self.client.clone(), namespace);
        match api.create(&kube::api::PostParams::default(), pvc).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(ae)) if ae.code == 409 => Ok(()), // already exists
            Err(e) => Err(e.to_string()),
        }
    }

    async fn list_pvcs(
        &self,
        namespace: &str,
        selector: &str,
    ) -> Result<Vec<PersistentVolumeClaim>, String> {
        let api: kube::Api<PersistentVolumeClaim> =
            kube::Api::namespaced(self.client.clone(), namespace);
        let params = kube::api::ListParams::default().labels(selector);
        api.list(&params)
            .await
            .map(|list| list.items)
            .map_err(|e| e.to_string())
    }

    async fn delete_pvc(&self, namespace: &str, name: &str) -> Result<(), String> {
        let api: kube::Api<PersistentVolumeClaim> =
            kube::Api::namespaced(self.client.clone(), namespace);
        match api.delete(name, &kube::api::DeleteParams::default()).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(ae)) if ae.code == 404 => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    async fn get_service(&self, namespace: &str, name: &str) -> Result<Service, String> {
        let api: kube::Api<Service> = kube::Api::namespaced(self.client.clone(), namespace);
        api.get(name).await.map_err(|e| e.to_string())
    }

    async fn create_service(&self, namespace: &str, svc: &Service) -> Result<(), String> {
        let api: kube::Api<Service> = kube::Api::namespaced(self.client.clone(), namespace);
        match api.create(&kube::api::PostParams::default(), svc).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(ae)) if ae.code == 409 => Ok(()), // already exists
            Err(e) => Err(e.to_string()),
        }
    }

    async fn delete_service(&self, namespace: &str, name: &str) -> Result<(), String> {
        let api: kube::Api<Service> = kube::Api::namespaced(self.client.clone(), namespace);
        match api.delete(name, &kube::api::DeleteParams::default()).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(ae)) if ae.code == 404 => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(labels: Option<BTreeMap<String, String>>) -> Pod {
        Pod {
            metadata: kube::api::ObjectMeta {
                uid: Some("expected-uid".to_string()),
                resource_version: Some("42".to_string()),
                labels,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn uid_fenced_label_patch_preserves_existing_labels_and_creates_missing_map() {
        let existing = BTreeMap::from([
            ("app".to_string(), "kvstore".to_string()),
            ("kuberic.io/role".to_string(), "secondary".to_string()),
        ]);
        let patch = uid_fenced_label_patch(
            &pod(Some(existing)),
            "expected-uid",
            BTreeMap::from([("kuberic.io/role".to_string(), "retired".to_string())]),
        )
        .unwrap();
        assert_eq!(patch[0]["path"], "/metadata/uid");
        assert_eq!(patch[0]["value"], "expected-uid");
        assert_eq!(patch[1]["path"], "/metadata/resourceVersion");
        assert_eq!(patch[1]["value"], "42");
        assert_eq!(patch[2]["path"], "/metadata/labels");
        assert_eq!(patch[2]["value"]["app"], "kvstore");
        assert_eq!(patch[2]["value"]["kuberic.io/role"], "retired");

        let patch = uid_fenced_label_patch(
            &pod(None),
            "expected-uid",
            BTreeMap::from([("kuberic.io/role".to_string(), "retired".to_string())]),
        )
        .unwrap();
        assert_eq!(patch[2]["value"]["kuberic.io/role"], "retired");
        assert!(uid_fenced_label_patch(&pod(None), "replacement-uid", BTreeMap::new()).is_err());
    }
}
