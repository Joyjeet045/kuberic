use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::future::join_all;
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, EnvVarSource, ObjectFieldSelector, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, Pod, PodSecurityContext, PodSpec, Secret, SecretKeySelector,
    Service, ServicePort, ServiceSpec, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use kube::api::{DeleteParams, ListParams, Patch, PatchParams, PostParams, Preconditions};
use kube::{Api, Client, Resource, ResourceExt};
use kuberic_runtime::control::proto;
use kuberic_runtime::protocol::command::{
    EnsureConfiguration, EnsureReplicaBuild, InitializeAgentStore, PrepareSwitchover,
    ProtocolCommand, ScaleDownResource,
};
use kuberic_runtime::protocol::observation::ReplicaObservationKey;
use kuberic_runtime::protocol::types::{
    AcceptedStatus, EffectivePolicy, OperationId, PodUid, ProvisioningIntent, PvcUid, ReplicaId,
    ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid, TransitionKind,
    derive_agent_generation, derive_initialization_id, derive_replacement_resource_name,
    derive_replica_endpoint_name,
};
use tokio::sync::Mutex;
use tonic::Code;

use crate::crd::{
    CONTROL_ADDRESS_ANNOTATION, CONTROLLER_NAME, INSTANCE_LABEL, KubericSet, KubericSetStatus,
    REPLICA_ID_LABEL, SCALE_UP_ALLOCATION_ANNOTATION, SET_UID_LABEL,
};
use crate::observation::{
    ExactLookup, RawAgentObservation, RawObservation, RawObservationFailure, RawScaleDownResources,
};
use crate::{ControllerError, Result};

const CONTROL_PORT: i32 = 50051;
const REPLICATION_PORT: i32 = 50052;
const LIVE_TEST_COPY_GATE_ANNOTATION: &str = "testing.kuberic.io/live-copy-gate";
const LIVE_TEST_COPY_GATE_ADDRESS: &str = "0.0.0.0:18080";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectRecord {
    DeleteScaleDownResource {
        resource: ScaleDownResource,
        name: String,
        uid: String,
        resource_version: String,
    },
    EnsureReplicaSupport,
    EnsureScaffolding(Vec<ReplicaId>),
    EnsureReplacement(ReplicaIdentity),
    DeleteScaffolding {
        pod_name: Option<String>,
        pvc_name: Option<String>,
    },
    DeleteExactPod {
        pod_name: String,
        pod_uid: PodUid,
    },
    EnsureWriteRoutingService,
    ReplaceStatus,
    RemoveWriteRouting,
    PublishWriteRouting(ReplicaIdentity),
    Execute(ProtocolCommand),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentRpcError {
    Unavailable(String),
    Invalid(String),
}

#[async_trait]
pub trait AgentApi: Send + Sync {
    async fn get_status(
        &self,
        endpoint: &str,
        token: &str,
        request: proto::GetAgentStatusRequest,
    ) -> std::result::Result<proto::AgentStatusReport, AgentRpcError>;

    async fn execute(
        &self,
        endpoint: &str,
        token: &str,
        request: proto::ExecuteCommandRequest,
    ) -> std::result::Result<proto::ExecuteCommandResponse, AgentRpcError>;
}

#[async_trait]
pub trait ClusterApi: Send + Sync {
    async fn observe(&self, namespace: &str, name: &str) -> Result<RawObservation>;

    async fn delete_scale_down_resource(
        &self,
        observation: &RawObservation,
        resource: ScaleDownResource,
        name: &str,
        uid: &str,
        resource_version: &str,
    ) -> Result<()>;

    async fn ensure_replica_scaffolding(
        &self,
        observation: &RawObservation,
        replica_ids: &[ReplicaId],
    ) -> Result<()>;

    async fn ensure_replica_support(&self, observation: &RawObservation) -> Result<()>;

    async fn ensure_replacement_scaffolding(
        &self,
        observation: &RawObservation,
        replica_id: ReplicaId,
        replacing: &ReplicaIdentity,
    ) -> Result<()>;

    async fn delete_replica_scaffolding(
        &self,
        observation: &RawObservation,
        pod_name: Option<&str>,
        pod_uid: Option<&PodUid>,
        pvc_name: Option<&str>,
        pvc_uid: Option<&PvcUid>,
    ) -> Result<()>;

    async fn delete_replica_endpoint(
        &self,
        observation: &RawObservation,
        identity: &ReplicaIdentity,
    ) -> Result<()>;

    async fn delete_exact_pod(
        &self,
        observation: &RawObservation,
        pod_name: &str,
        pod_uid: &PodUid,
    ) -> Result<()>;

    async fn ensure_write_routing_service(&self, observation: &RawObservation) -> Result<()>;

    async fn replace_status(
        &self,
        observation: &RawObservation,
        status: &AcceptedStatus,
    ) -> Result<()>;

    async fn remove_write_routing(&self, observation: &RawObservation) -> Result<()>;

    async fn publish_write_routing(
        &self,
        observation: &RawObservation,
        primary: &ReplicaIdentity,
    ) -> Result<()>;

    async fn execute_command(
        &self,
        observation: &RawObservation,
        command: &ProtocolCommand,
    ) -> Result<()>;
}

#[derive(Clone)]
pub struct GrpcAgentApi {
    deadline: Duration,
}

impl GrpcAgentApi {
    pub fn new(deadline: Duration) -> Self {
        Self { deadline }
    }
}

#[async_trait]
impl AgentApi for GrpcAgentApi {
    async fn get_status(
        &self,
        endpoint: &str,
        token: &str,
        request: proto::GetAgentStatusRequest,
    ) -> std::result::Result<proto::AgentStatusReport, AgentRpcError> {
        let mut client = tokio::time::timeout(
            self.deadline,
            proto::agent_control_client::AgentControlClient::connect(endpoint.to_string()),
        )
        .await
        .map_err(|_| AgentRpcError::Unavailable("agent connection timed out".to_string()))?
        .map_err(|error| AgentRpcError::Unavailable(error.to_string()))?;
        let mut request = tonic::Request::new(request);
        add_bearer_token(&mut request, token)?;
        tokio::time::timeout(self.deadline, client.get_status(request))
            .await
            .map_err(|_| AgentRpcError::Unavailable("GetStatus timed out".to_string()))?
            .map(|response| response.into_inner())
            .map_err(classify_status)
    }

    async fn execute(
        &self,
        endpoint: &str,
        token: &str,
        request: proto::ExecuteCommandRequest,
    ) -> std::result::Result<proto::ExecuteCommandResponse, AgentRpcError> {
        let mut client = tokio::time::timeout(
            self.deadline,
            proto::agent_control_client::AgentControlClient::connect(endpoint.to_string()),
        )
        .await
        .map_err(|_| AgentRpcError::Unavailable("agent connection timed out".to_string()))?
        .map_err(|error| AgentRpcError::Unavailable(error.to_string()))?;
        let mut request = tonic::Request::new(request);
        add_bearer_token(&mut request, token)?;
        tokio::time::timeout(self.deadline, client.execute(request))
            .await
            .map_err(|_| AgentRpcError::Unavailable("Execute timed out".to_string()))?
            .map(|response| response.into_inner())
            .map_err(classify_execute_status)
    }
}

fn add_bearer_token<T>(
    request: &mut tonic::Request<T>,
    token: &str,
) -> std::result::Result<(), AgentRpcError> {
    let value = format!("Bearer {token}")
        .parse()
        .map_err(|error| AgentRpcError::Invalid(format!("invalid bearer token: {error}")))?;
    request.metadata_mut().insert("authorization", value);
    Ok(())
}

fn classify_status(status: tonic::Status) -> AgentRpcError {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled | Code::Unknown => {
            AgentRpcError::Unavailable(status.to_string())
        }
        _ => AgentRpcError::Invalid(status.to_string()),
    }
}

fn classify_execute_status(status: tonic::Status) -> AgentRpcError {
    // Session or authority may have advanced after GetStatus. A rejected
    // dispatch is not a contradictory report and must be resolved by observing.
    if matches!(
        status.code(),
        Code::FailedPrecondition
            | Code::Aborted
            | Code::Internal
            | Code::Unknown
            | Code::ResourceExhausted
    ) {
        AgentRpcError::Unavailable(status.to_string())
    } else {
        classify_status(status)
    }
}

pub struct KubeClusterApi<A> {
    client: Client,
    agents: Arc<A>,
    bearer_token: Arc<str>,
}

impl<A> KubeClusterApi<A> {
    pub fn new(client: Client, agents: Arc<A>, bearer_token: impl Into<Arc<str>>) -> Result<Self> {
        let bearer_token = bearer_token.into();
        if bearer_token.is_empty() {
            return Err(ControllerError::Effect(
                "agent bearer token must not be empty".to_string(),
            ));
        }
        Ok(Self {
            client,
            agents,
            bearer_token,
        })
    }
}

#[async_trait]
impl<A> ClusterApi for KubeClusterApi<A>
where
    A: AgentApi + 'static,
{
    async fn delete_scale_down_resource(
        &self,
        observation: &RawObservation,
        resource: ScaleDownResource,
        name: &str,
        uid: &str,
        resource_version: &str,
    ) -> Result<()> {
        let params = scale_down_delete_params(observation, resource, name, uid, resource_version)?;
        let namespace = observation
            .set
            .namespace()
            .ok_or(ControllerError::ObservationStale)?;
        match resource {
            ScaleDownResource::Pod => {
                delete_scale_down_exact(
                    &Api::<Pod>::namespaced(self.client.clone(), &namespace),
                    name,
                    &params,
                )
                .await
            }
            ScaleDownResource::Pvc => {
                delete_scale_down_exact(
                    &Api::<PersistentVolumeClaim>::namespaced(self.client.clone(), &namespace),
                    name,
                    &params,
                )
                .await
            }
            ScaleDownResource::Endpoint => {
                delete_scale_down_exact(
                    &Api::<Service>::namespaced(self.client.clone(), &namespace),
                    name,
                    &params,
                )
                .await
            }
        }
    }
    async fn observe(&self, namespace: &str, name: &str) -> Result<RawObservation> {
        let sets: Api<KubericSet> = Api::namespaced(self.client.clone(), namespace);
        let set = sets.get(name).await.map_err(map_kube_observation_error)?;
        let uid = set
            .uid()
            .ok_or_else(|| ControllerError::Observation("KubericSet has no UID".to_string()))?;
        let selector = format!("{SET_UID_LABEL}={uid}");
        let pods_api: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let pvcs_api: Api<PersistentVolumeClaim> = Api::namespaced(self.client.clone(), namespace);
        let services_api: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        let secrets_api: Api<Secret> = Api::namespaced(self.client.clone(), namespace);
        let params = ListParams::default().labels(&selector);
        let (pods_result, pvcs_result, services_result, secrets_result) = tokio::join!(
            pods_api.list(&params),
            pvcs_api.list(&params),
            services_api.list(&params),
            secrets_api.list(&params)
        );
        let mut failures = Vec::new();
        let pods = list_or_failure(pods_result, "pods", &mut failures);
        let pvcs = list_or_failure(pvcs_result, "pvcs", &mut failures);
        let services = list_or_failure(services_result, "services", &mut failures);
        let secrets = list_or_failure(secrets_result, "secrets", &mut failures);
        let resource_uid = ResourceUid::new(uid);
        let mut raw = RawObservation {
            set,
            pods,
            pvcs,
            services,
            secrets,
            failures,
            agents: BTreeMap::new(),
            exact_resources: Vec::new(),
            now_unix_seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| ControllerError::Observation(error.to_string()))?
                .as_secs() as i64,
        };
        observe_exact_resources(&mut raw, &pods_api, &pvcs_api, &services_api).await;
        let agent_requests = raw
            .pods
            .iter()
            .filter_map(|pod| {
                let replica_id = crate::exact_resources::frozen_pod_target(&raw, pod)
                    .map(|t| t.replica_id)
                    .or_else(|| {
                        pod.labels()
                            .get(REPLICA_ID_LABEL)?
                            .parse::<i64>()
                            .ok()
                            .filter(|value| *value > 0)
                            .map(ReplicaId::new)
                    })?;
                Some((replica_id, pod.clone()))
            })
            .map(|(replica_id, pod)| {
                let resource_uid = resource_uid.clone();
                async move {
                    let instance_id = pod.uid().map(ReplicaInstanceId::new).unwrap_or_else(|| {
                        ReplicaInstanceId::new(format!("missing-pod-uid-{}", pod.name_any()))
                    });
                    (
                        ReplicaObservationKey::new(replica_id, instance_id),
                        self.observe_agent(&pod, &resource_uid, replica_id).await,
                    )
                }
            });
        raw.agents = join_all(agent_requests)
            .await
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        // Permanent faults and lag are known only after observing the agents.
        observe_exact_resources(&mut raw, &pods_api, &pvcs_api, &services_api).await;

        Ok(raw)
    }

    async fn ensure_replica_scaffolding(
        &self,
        observation: &RawObservation,
        replica_ids: &[ReplicaId],
    ) -> Result<()> {
        let namespace = observation
            .set
            .namespace()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no namespace".to_string()))?;
        let uid = observation
            .set
            .uid()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no UID".to_string()))?;
        let owner = owner_reference(&observation.set)?;
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &namespace);
        let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(self.client.clone(), &namespace);
        ensure_agent_credentials(
            self.client.clone(),
            observation,
            &namespace,
            &uid,
            &owner,
            &self.bearer_token,
        )
        .await?;
        ensure_peer_service(self.client.clone(), observation, &namespace, &uid, &owner).await?;
        let image = effective_replica_image(observation)?;
        for replica_id in replica_ids {
            let allocation_operation_id =
                scale_up_allocation_operation_id(&observation.set, *replica_id);
            let configured_identity = observation
                .set
                .status
                .as_ref()
                .and_then(|status| {
                    status
                        .authority
                        .transition
                        .as_ref()
                        .map(|transition| &transition.current_configuration)
                        .or_else(|| {
                            status
                                .authority
                                .topology
                                .as_ref()
                                .map(|topology| &topology.configuration)
                        })
                })
                .and_then(|configuration| {
                    configuration
                        .members
                        .iter()
                        .find(|member| member.identity.replica_id == *replica_id)
                })
                .map(|member| &member.identity);
            if let Some(identity) = configured_identity
                && let Some(pod) = observation
                    .pods
                    .iter()
                    .find(|pod| pod.uid().as_deref() == Some(identity.instance_id.as_str()))
            {
                let pvc_name = pod.spec.as_ref().and_then(|spec| {
                    spec.volumes
                        .as_ref()?
                        .iter()
                        .find(|volume| volume.name == "data")?
                        .persistent_volume_claim
                        .as_ref()
                        .map(|claim| claim.claim_name.as_str())
                });
                let pvc = pvc_name
                    .and_then(|name| observation.pvcs.iter().find(|pvc| pvc.name_any() == name));
                let Some(pvc) = pvc else {
                    return Err(ControllerError::ObservationStale);
                };
                ensure_exact_peer_endpoint(
                    self.client.clone(),
                    observation,
                    &namespace,
                    &uid,
                    &owner,
                    pod,
                    pvc,
                    *replica_id,
                )
                .await?;
                continue;
            }
            let pod_name = replica_name(&observation.set, *replica_id);
            let pvc_name = format!("{pod_name}-data");
            let pvc = observation
                .pvcs
                .iter()
                .find(|pvc| pvc.name_any() == pvc_name);
            let Some(pvc) = pvc else {
                create_exact(
                    &pvcs,
                    &pvc_name,
                    &replica_pvc(
                        &observation.set,
                        *replica_id,
                        &uid,
                        &owner,
                        allocation_operation_id,
                    ),
                )
                .await?;
                continue;
            };
            let pvc_uid = pvc.uid().ok_or(ControllerError::ObservationStale)?;
            if let Some(operation_id) = allocation_operation_id
                && (!pvc_matches_allocation(pvc, operation_id)
                    || observation
                        .set
                        .status
                        .as_ref()
                        .and_then(|status| status.authority.scale_up_allocation.as_ref())
                        .and_then(|allocation| allocation.pvc_uid.as_ref())
                        .is_some_and(|frozen| frozen.as_str() != pvc_uid))
            {
                return Err(ControllerError::ObservationStale);
            }
            if !observation
                .pods
                .iter()
                .any(|pod| pod.name_any() == pod_name)
            {
                let pvc = authoritative_pvc_for_pod_create(
                    &pvcs,
                    pvc,
                    &observation.set,
                    *replica_id,
                    allocation_operation_id,
                    observation
                        .set
                        .status
                        .as_ref()
                        .and_then(|status| status.authority.scale_up_allocation.as_ref())
                        .and_then(|allocation| allocation.pvc_uid.as_ref()),
                )
                .await?;
                let pvc_uid = pvc.uid().ok_or(ControllerError::ObservationStale)?;
                create_exact(
                    &pods,
                    &pod_name,
                    &replica_pod(
                        &observation.set,
                        *replica_id,
                        &uid,
                        &owner,
                        &pvc_name,
                        &pvc_uid,
                        &image,
                        allocation_operation_id,
                    ),
                )
                .await?;
                continue;
            }
            let pod = observation
                .pods
                .iter()
                .find(|pod| pod.name_any() == pod_name)
                .expect("observed existing replica Pod");
            if let Some(operation_id) = allocation_operation_id
                && (!pod_matches_allocation(pod, operation_id)
                    || !crate::exact_resources::pod_matches_scale_up_allocation_metadata(
                        &observation.set,
                        pod,
                        *replica_id,
                        observation
                            .set
                            .status
                            .as_ref()
                            .and_then(|status| status.authority.scale_up_allocation.as_ref())
                            .and_then(|allocation| allocation.pvc_uid.as_ref()),
                    ))
            {
                return Err(ControllerError::ObservationStale);
            }
            ensure_exact_peer_endpoint(
                self.client.clone(),
                observation,
                &namespace,
                &uid,
                &owner,
                pod,
                pvc,
                *replica_id,
            )
            .await?;
        }
        ensure_write_service(self.client.clone(), observation, &namespace, &uid, &owner).await
    }

    async fn ensure_replacement_scaffolding(
        &self,
        observation: &RawObservation,
        replica_id: ReplicaId,
        replacing: &ReplicaIdentity,
    ) -> Result<()> {
        let namespace = observation
            .set
            .namespace()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no namespace".to_string()))?;
        let uid = observation
            .set
            .uid()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no UID".to_string()))?;
        let resource_uid = ResourceUid::new(&uid);
        let owner = owner_reference(&observation.set)?;
        let image = effective_replica_image(observation)?;
        let base = derive_replacement_resource_name(&resource_uid, replacing);
        let pod_name = format!("{}-{base}", observation.set.name_any());
        let pvc_name = format!("{pod_name}-data");
        let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(self.client.clone(), &namespace);
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &namespace);
        let pvc = observation
            .pvcs
            .iter()
            .find(|pvc| pvc.name_any() == pvc_name);
        let Some(pvc) = pvc else {
            create_exact(
                &pvcs,
                &pvc_name,
                &replica_pvc_named(&observation.set, replica_id, &uid, &owner, &pvc_name),
            )
            .await?;
            return Ok(());
        };
        let pvc_uid = pvc.uid().ok_or(ControllerError::ObservationStale)?;
        let pod = observation
            .pods
            .iter()
            .find(|pod| pod.name_any() == pod_name);
        let Some(pod) = pod else {
            create_exact(
                &pods,
                &pod_name,
                &replica_pod_named(
                    &observation.set,
                    replica_id,
                    &uid,
                    &owner,
                    &pod_name,
                    &pvc_name,
                    &pvc_uid,
                    &image,
                    None,
                ),
            )
            .await?;
            return Ok(());
        };
        ensure_exact_peer_endpoint(
            self.client.clone(),
            observation,
            &namespace,
            &uid,
            &owner,
            pod,
            pvc,
            replica_id,
        )
        .await
    }

    async fn delete_replica_scaffolding(
        &self,
        observation: &RawObservation,
        pod_name: Option<&str>,
        pod_uid: Option<&PodUid>,
        pvc_name: Option<&str>,
        pvc_uid: Option<&PvcUid>,
    ) -> Result<()> {
        if crate::exact_resources::protected(observation, pod_name, pod_uid.map(PodUid::as_str))
            || crate::exact_resources::protected(observation, pvc_name, pvc_uid.map(PvcUid::as_str))
        {
            return Err(ControllerError::ObservationStale);
        }
        let namespace = observation
            .set
            .namespace()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no namespace".to_string()))?;
        if let Some(pod_uid) = pod_uid
            && let Some(service) = observation.services.iter().find(|service| {
                service.spec.as_ref().is_some_and(|spec| {
                    spec.selector.as_ref().is_some_and(|selector| {
                        selector.get(INSTANCE_LABEL).map(String::as_str) == Some(pod_uid.as_str())
                    })
                })
            })
        {
            let services: Api<Service> = Api::namespaced(self.client.clone(), &namespace);
            let service_uid = service.uid().ok_or(ControllerError::ObservationStale)?;
            if crate::exact_resources::protected(
                observation,
                Some(&service.name_any()),
                Some(&service_uid),
            ) {
                return Err(ControllerError::ObservationStale);
            }
            delete_exact(&services, &service.name_any(), &service_uid).await?;
        }
        let pod = pod_uid.and_then(|uid| {
            observation
                .pods
                .iter()
                .find(|pod| pod.uid().as_deref() == Some(uid.as_str()))
        });
        let pod_name = pod_name
            .map(str::to_string)
            .or_else(|| pod.map(ResourceExt::name_any));
        if let (Some(name), Some(uid)) = (pod_name.as_deref(), pod_uid) {
            let pods: Api<Pod> = Api::namespaced(self.client.clone(), &namespace);
            delete_exact(&pods, name, uid.as_str()).await?;
        }
        let pvc = pvc_uid.and_then(|uid| {
            observation
                .pvcs
                .iter()
                .find(|pvc| pvc.uid().as_deref() == Some(uid.as_str()))
        });
        let pvc_name = pvc_name
            .map(str::to_string)
            .or_else(|| pvc.map(ResourceExt::name_any));
        if let (Some(name), Some(uid)) = (pvc_name.as_deref(), pvc_uid) {
            let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(self.client.clone(), &namespace);
            delete_exact(&pvcs, name, uid.as_str()).await?;
        }
        Ok(())
    }

    async fn delete_exact_pod(
        &self,
        observation: &RawObservation,
        pod_name: &str,
        pod_uid: &PodUid,
    ) -> Result<()> {
        if crate::exact_resources::protected_from_exact_pod_fence(
            observation,
            Some(pod_name),
            Some(pod_uid.as_str()),
        ) {
            return Err(ControllerError::ObservationStale);
        }
        let params = exact_pod_delete_params(observation, pod_name, pod_uid)?;
        let namespace = observation
            .set
            .namespace()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no namespace".to_string()))?;
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &namespace);
        match pods.delete(pod_name, &params).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(response)) if response.code == 404 => Ok(()),
            Err(error) => Err(map_kube_effect_error(error)),
        }
    }

    async fn delete_replica_endpoint(
        &self,
        observation: &RawObservation,
        identity: &ReplicaIdentity,
    ) -> Result<()> {
        let namespace = observation
            .set
            .namespace()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no namespace".to_string()))?;
        let resource_uid = observation
            .set
            .uid()
            .map(ResourceUid::new)
            .ok_or_else(|| ControllerError::Effect("KubericSet has no UID".to_string()))?;
        let name = derive_replica_endpoint_name(&resource_uid, identity);
        if crate::exact_resources::protected(observation, Some(&name), None) {
            return Err(ControllerError::ObservationStale);
        }
        let Some(service) = observation
            .services
            .iter()
            .find(|service| service.name_any() == name)
        else {
            return Ok(());
        };
        let uid = service.uid().ok_or(ControllerError::ObservationStale)?;
        let services: Api<Service> = Api::namespaced(self.client.clone(), &namespace);
        delete_exact(&services, &name, &uid).await
    }

    async fn ensure_replica_support(&self, observation: &RawObservation) -> Result<()> {
        let namespace = observation
            .set
            .namespace()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no namespace".to_string()))?;
        let uid = observation
            .set
            .uid()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no UID".to_string()))?;
        let owner = owner_reference(&observation.set)?;
        ensure_agent_credentials(
            self.client.clone(),
            observation,
            &namespace,
            &uid,
            &owner,
            &self.bearer_token,
        )
        .await?;
        ensure_peer_service(self.client.clone(), observation, &namespace, &uid, &owner).await
    }

    async fn ensure_write_routing_service(&self, observation: &RawObservation) -> Result<()> {
        if service_observation_failed(observation) {
            return Err(ControllerError::Observation(
                "cannot converge write routing after Service observation failed".to_string(),
            ));
        }
        let namespace = observation
            .set
            .namespace()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no namespace".to_string()))?;
        let uid = observation
            .set
            .uid()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no UID".to_string()))?;
        let owner = owner_reference(&observation.set)?;
        ensure_write_service(self.client.clone(), observation, &namespace, &uid, &owner).await
    }

    async fn replace_status(
        &self,
        observation: &RawObservation,
        status: &AcceptedStatus,
    ) -> Result<()> {
        let namespace = observation
            .set
            .namespace()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no namespace".to_string()))?;
        let sets: Api<KubericSet> = Api::namespaced(self.client.clone(), &namespace);
        let mut set = observation.set.clone();
        set.status = Some(KubericSetStatus {
            authority: status.clone(),
        });
        sets.replace_status(&set.name_any(), &PostParams::default(), &set)
            .await
            .map(|_| ())
            .map_err(map_kube_status_error)
    }

    async fn remove_write_routing(&self, observation: &RawObservation) -> Result<()> {
        if service_observation_failed(observation) {
            return Err(ControllerError::Observation(
                "cannot confirm write routing absence after Service observation failed".to_string(),
            ));
        }
        let service_name = format!("{}-write", observation.set.name_any());
        if !observation
            .services
            .iter()
            .any(|service| service.name_any() == service_name)
        {
            return Ok(());
        }
        patch_write_service(self.client.clone(), observation, "disabled").await
    }

    async fn publish_write_routing(
        &self,
        observation: &RawObservation,
        primary: &ReplicaIdentity,
    ) -> Result<()> {
        let namespace = observation
            .set
            .namespace()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no namespace".to_string()))?;
        let pod = observation
            .pods
            .iter()
            .find(|pod| pod.uid().as_deref() == Some(primary.instance_id.as_str()))
            .ok_or(ControllerError::ObservationStale)?;
        patch_pod_instance(
            self.client.clone(),
            &namespace,
            pod,
            primary.instance_id.as_str(),
        )
        .await?;
        patch_write_service(
            self.client.clone(),
            observation,
            primary.instance_id.as_str(),
        )
        .await
    }

    async fn execute_command(
        &self,
        observation: &RawObservation,
        command: &ProtocolCommand,
    ) -> Result<()> {
        let (target, replica_id) = command_target(command);
        let pod = observation
            .pods
            .iter()
            .find(|pod| {
                (crate::exact_resources::frozen_pod_target(observation, pod)
                    .is_some_and(|t| t == &target)
                    || pod
                        .labels()
                        .get(REPLICA_ID_LABEL)
                        .is_some_and(|value| value == &replica_id.to_string()))
                    && pod.uid().as_deref() == Some(target.instance_id.as_str())
            })
            .ok_or(ControllerError::ObservationStale)?;
        let endpoint = agent_endpoint(pod).ok_or_else(|| {
            ControllerError::AgentUnavailable(format!(
                "replica {replica_id} has no control address"
            ))
        })?;
        let resource_uid = observation
            .set
            .uid()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no UID".to_string()))?;
        let expected_process_session_id =
            observed_process_session(observation, replica_id, &target)?;
        let response = self
            .agents
            .execute(
                &endpoint,
                &self.bearer_token,
                command_request(
                    resource_uid,
                    target,
                    expected_process_session_id,
                    command.clone(),
                )?,
            )
            .await
            .map_err(map_agent_effect_error)?;
        if response.protocol_version != kuberic_runtime::protocol::PROTOCOL_VERSION {
            return Err(ControllerError::InvalidAgentEvidence(
                "Execute response uses an unsupported protocol version".to_string(),
            ));
        }
        let report = response.observation.ok_or_else(|| {
            ControllerError::InvalidAgentEvidence(
                "Execute response omitted its resulting observation".to_string(),
            )
        })?;
        kuberic_runtime::control::validate_agent_status_report(&report)
            .map_err(|error| ControllerError::InvalidAgentEvidence(error.to_string()))
    }
}

fn observed_process_session(
    observation: &RawObservation,
    replica_id: ReplicaId,
    target: &ReplicaIdentity,
) -> Result<String> {
    let observation_key = ReplicaObservationKey::new(replica_id, target.instance_id.clone());
    match observation.agents.get(&observation_key) {
        Some(RawAgentObservation::Report(report)) if !report.process_session_id.is_empty() => {
            Ok(report.process_session_id.clone())
        }
        _ => Err(ControllerError::ObservationStale),
    }
}

impl<A> KubeClusterApi<A>
where
    A: AgentApi,
{
    async fn observe_agent(
        &self,
        pod: &Pod,
        resource_uid: &ResourceUid,
        replica_id: ReplicaId,
    ) -> RawAgentObservation {
        let Some(instance_id) = pod.uid() else {
            return RawAgentObservation::Invalid {
                message: "Pod has no UID".to_string(),
            };
        };
        let Some(endpoint) = agent_endpoint(pod) else {
            return RawAgentObservation::Unavailable {
                message: "Pod has no control address while the agent starts".to_string(),
            };
        };
        let request = proto::GetAgentStatusRequest {
            protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
            resource_uid: resource_uid.to_string(),
            replica_id: replica_id.value(),
            expected_instance_id: instance_id,
        };
        match self
            .agents
            .get_status(&endpoint, &self.bearer_token, request)
            .await
        {
            Ok(report) => RawAgentObservation::Report(Box::new(report)),
            Err(AgentRpcError::Unavailable(message)) => {
                RawAgentObservation::Unavailable { message }
            }
            Err(AgentRpcError::Invalid(message)) => RawAgentObservation::Invalid { message },
        }
    }
}

fn list_or_failure<K>(
    result: std::result::Result<kube::api::ObjectList<K>, kube::Error>,
    source: &str,
    failures: &mut Vec<RawObservationFailure>,
) -> Vec<K>
where
    K: Clone,
{
    match result {
        Ok(list) => list.items,
        Err(error) => {
            failures.push(RawObservationFailure {
                source: source.to_string(),
                message: error.to_string(),
            });
            Vec::new()
        }
    }
}

fn service_observation_failed(observation: &RawObservation) -> bool {
    observation
        .failures
        .iter()
        .any(|failure| failure.source == "services")
}

async fn create_exact<K>(api: &Api<K>, name: &str, object: &K) -> Result<()>
where
    K: Clone
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + kube::Resource<DynamicType = ()>,
{
    api.create(&PostParams::default(), object)
        .await
        .map(|_| ())
        .map_err(|error| match error {
            kube::Error::Api(response) if response.code == 409 => ControllerError::ObservationStale,
            other => ControllerError::Effect(format!("creating {name}: {other}")),
        })
}

fn owner_reference(set: &KubericSet) -> Result<OwnerReference> {
    set.controller_owner_ref(&())
        .ok_or_else(|| ControllerError::Effect("KubericSet has no owner identity".to_string()))
}

fn base_labels(
    set: &KubericSet,
    replica_id: Option<ReplicaId>,
    uid: &str,
) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::from([
        ("app.kubernetes.io/name".to_string(), "kuberic".to_string()),
        (
            "app.kubernetes.io/managed-by".to_string(),
            CONTROLLER_NAME.to_string(),
        ),
        (SET_UID_LABEL.to_string(), uid.to_string()),
        ("operator.kuberic.io/set-name".to_string(), set.name_any()),
    ]);
    if let Some(replica_id) = replica_id {
        labels.insert(REPLICA_ID_LABEL.to_string(), replica_id.to_string());
    }
    labels
}

fn replica_name(set: &KubericSet, replica_id: ReplicaId) -> String {
    format!("{}-{}", set.name_any(), replica_id.value())
}

fn replica_pvc(
    set: &KubericSet,
    replica_id: ReplicaId,
    uid: &str,
    owner: &OwnerReference,
    allocation_operation_id: Option<&OperationId>,
) -> PersistentVolumeClaim {
    let mut pvc = replica_pvc_named(
        set,
        replica_id,
        uid,
        owner,
        &format!("{}-data", replica_name(set, replica_id)),
    );
    if let Some(operation_id) = allocation_operation_id {
        pvc.metadata.annotations = Some(BTreeMap::from([(
            SCALE_UP_ALLOCATION_ANNOTATION.to_string(),
            operation_id.to_string(),
        )]));
    }
    pvc
}

fn replica_pvc_named(
    set: &KubericSet,
    replica_id: ReplicaId,
    uid: &str,
    owner: &OwnerReference,
    name: &str,
) -> PersistentVolumeClaim {
    PersistentVolumeClaim {
        metadata: kube::core::ObjectMeta {
            name: Some(name.to_string()),
            labels: Some(base_labels(set, Some(replica_id), uid)),
            owner_references: Some(vec![owner.clone()]),
            ..Default::default()
        },
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
            resources: Some(k8s_openapi::api::core::v1::VolumeResourceRequirements {
                requests: Some(BTreeMap::from([(
                    "storage".to_string(),
                    Quantity("1Gi".to_string()),
                )])),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn scale_up_allocation_operation_id(
    set: &KubericSet,
    replica_id: ReplicaId,
) -> Option<&OperationId> {
    set.status
        .as_ref()?
        .authority
        .scale_up_allocation
        .as_ref()
        .filter(|allocation| {
            allocation.target_replica_id == replica_id
                && allocation.scaffolding_requested
                && !allocation.cancellation_started
        })
        .map(|allocation| &allocation.operation_id)
}

fn pvc_matches_allocation(pvc: &PersistentVolumeClaim, operation_id: &OperationId) -> bool {
    pvc.annotations()
        .get(SCALE_UP_ALLOCATION_ANNOTATION)
        .map(String::as_str)
        == Some(operation_id.as_str())
}

fn pod_matches_allocation(pod: &Pod, operation_id: &OperationId) -> bool {
    pod.annotations()
        .get(SCALE_UP_ALLOCATION_ANNOTATION)
        .map(String::as_str)
        == Some(operation_id.as_str())
}

async fn authoritative_pvc_for_pod_create(
    pvcs: &Api<PersistentVolumeClaim>,
    observed: &PersistentVolumeClaim,
    set: &KubericSet,
    replica_id: ReplicaId,
    allocation_operation_id: Option<&OperationId>,
    frozen_pvc_uid: Option<&PvcUid>,
) -> Result<PersistentVolumeClaim> {
    let expected_name = format!("{}-{}-data", set.name_any(), replica_id.value());
    let set_uid = set.uid().ok_or(ControllerError::ObservationStale)?;
    let owner = owner_reference(set)?;
    let live = pvcs.get(&expected_name).await.map_err(|error| {
        tracing::warn!(
            %error,
            pvc = %expected_name,
            "authoritative PVC revalidation failed before Pod creation"
        );
        ControllerError::ObservationStale
    })?;
    let live_uid = live.uid().ok_or(ControllerError::ObservationStale)?;
    let expected_uid = observed.uid().ok_or(ControllerError::ObservationStale)?;
    let live_resource_version = live
        .resource_version()
        .ok_or(ControllerError::ObservationStale)?;
    let expected_resource_version = observed
        .resource_version()
        .ok_or(ControllerError::ObservationStale)?;
    let expected_replica_id = replica_id.to_string();
    let owned = live
        .owner_references()
        .iter()
        .any(|candidate| candidate == &owner);
    let provenance_matches = allocation_operation_id
        .is_none_or(|operation_id| pvc_matches_allocation(&live, operation_id));
    if live.name_any() != expected_name
        || live_uid != expected_uid
        || live_resource_version != expected_resource_version
        || frozen_pvc_uid.is_some_and(|frozen| frozen.as_str() != live_uid)
        || live.labels().get(SET_UID_LABEL).map(String::as_str) != Some(set_uid.as_str())
        || live.labels().get(REPLICA_ID_LABEL).map(String::as_str)
            != Some(expected_replica_id.as_str())
        || !owned
        || !provenance_matches
    {
        return Err(ControllerError::ObservationStale);
    }
    Ok(live)
}

#[allow(clippy::too_many_arguments)]
fn replica_pod(
    set: &KubericSet,
    replica_id: ReplicaId,
    uid: &str,
    owner: &OwnerReference,
    pvc_name: &str,
    pvc_uid: &str,
    image: &str,
    allocation_operation_id: Option<&OperationId>,
) -> Pod {
    replica_pod_named(
        set,
        replica_id,
        uid,
        owner,
        &replica_name(set, replica_id),
        pvc_name,
        pvc_uid,
        image,
        allocation_operation_id,
    )
}

#[allow(clippy::too_many_arguments)]
fn replica_pod_named(
    set: &KubericSet,
    replica_id: ReplicaId,
    uid: &str,
    owner: &OwnerReference,
    pod_name: &str,
    pvc_name: &str,
    pvc_uid: &str,
    image: &str,
    allocation_operation_id: Option<&OperationId>,
) -> Pod {
    let labels = base_labels(set, Some(replica_id), uid);
    let annotations = allocation_operation_id.map(|operation_id| {
        BTreeMap::from([(
            SCALE_UP_ALLOCATION_ANNOTATION.to_string(),
            operation_id.to_string(),
        )])
    });
    let credential_name = agent_credential_name(set);
    let mut env = vec![
        EnvVar {
            name: "KUBERIC_RESOURCE_UID".to_string(),
            value: Some(uid.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "KUBERIC_REPLICA_ID".to_string(),
            value: Some(replica_id.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "KUBERIC_PVC_UID".to_string(),
            value: Some(pvc_uid.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "KUBERIC_SET_NAME".to_string(),
            value: Some(set.name_any()),
            ..Default::default()
        },
        EnvVar {
            name: "KUBERIC_NAMESPACE".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_string()),
                    field_path: "metadata.namespace".to_string(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "KUBERIC_POD_UID".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_string()),
                    field_path: "metadata.uid".to_string(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "KUBERIC_POD_IP".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_string()),
                    field_path: "status.podIP".to_string(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "KUBERIC_AGENT_BEARER_TOKEN".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    key: "bearer-token".to_string(),
                    name: credential_name,
                    optional: Some(false),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "KUBERIC_DATA_ROOT".to_string(),
            value: Some("/var/lib/kuberic".to_string()),
            ..Default::default()
        },
    ];
    if set
        .annotations()
        .get(LIVE_TEST_COPY_GATE_ANNOTATION)
        .is_some_and(|value| value == "enabled")
    {
        env.push(EnvVar {
            name: "KUBERIC_LIVE_TEST_COPY_GATE_ADDRESS".to_string(),
            value: Some(LIVE_TEST_COPY_GATE_ADDRESS.to_string()),
            ..Default::default()
        });
    }
    Pod {
        metadata: kube::core::ObjectMeta {
            name: Some(pod_name.to_string()),
            labels: Some(labels),
            annotations,
            owner_references: Some(vec![owner.clone()]),
            ..Default::default()
        },
        spec: Some(PodSpec {
            hostname: Some(pod_name.to_string()),
            subdomain: Some(format!("{}-peer", set.name_any())),
            security_context: Some(PodSecurityContext {
                fs_group: Some(10001),
                run_as_non_root: Some(true),
                run_as_user: Some(10001),
                ..Default::default()
            }),
            containers: vec![Container {
                name: "application".to_string(),
                image: Some(image.to_string()),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ports: Some(vec![
                    ContainerPort {
                        container_port: CONTROL_PORT,
                        name: Some("control".to_string()),
                        ..Default::default()
                    },
                    ContainerPort {
                        container_port: REPLICATION_PORT,
                        name: Some("replication".to_string()),
                        ..Default::default()
                    },
                    ContainerPort {
                        container_port: 8080,
                        name: Some("application".to_string()),
                        ..Default::default()
                    },
                ]),
                env: Some(env),
                volume_mounts: Some(vec![VolumeMount {
                    mount_path: "/var/lib/kuberic".to_string(),
                    name: "data".to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            volumes: Some(vec![Volume {
                name: "data".to_string(),
                persistent_volume_claim: Some(
                    k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
                        claim_name: pvc_name.to_string(),
                        ..Default::default()
                    },
                ),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn effective_replica_image(observation: &RawObservation) -> Result<String> {
    let authority = observation
        .set
        .status
        .as_ref()
        .map(|status| &status.authority);
    let identities = authority
        .and_then(|authority| {
            authority
                .transition
                .as_ref()
                .map(|transition| &transition.current_configuration.members)
                .or_else(|| {
                    authority
                        .topology
                        .as_ref()
                        .map(|topology| &topology.configuration.members)
                })
        })
        .into_iter()
        .flatten()
        .map(|member| &member.identity)
        .collect::<Vec<_>>();
    if identities.is_empty() {
        return Ok(observation.set.spec.image.clone());
    }

    let images = observation
        .pods
        .iter()
        .filter(|pod| {
            pod.uid().is_some_and(|uid| {
                identities
                    .iter()
                    .any(|identity| identity.instance_id.as_str() == uid)
            })
        })
        .filter_map(|pod| {
            pod.spec
                .as_ref()?
                .containers
                .iter()
                .find(|container| container.name == "application")?
                .image
                .clone()
        })
        .collect::<BTreeSet<_>>();
    match images.len() {
        1 => Ok(images.into_iter().next().expect("one image")),
        0 => Err(ControllerError::ObservationStale),
        _ => Err(ControllerError::Effect(
            "accepted replica incarnations run inconsistent images".to_string(),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
async fn ensure_exact_peer_endpoint(
    client: Client,
    observation: &RawObservation,
    namespace: &str,
    uid: &str,
    owner: &OwnerReference,
    pod: &Pod,
    pvc: &PersistentVolumeClaim,
    replica_id: ReplicaId,
) -> Result<()> {
    let pod_uid = pod.uid().ok_or(ControllerError::ObservationStale)?;
    let pvc_uid = pvc.uid().ok_or(ControllerError::ObservationStale)?;
    let resource_uid = ResourceUid::new(uid);
    let initialization_id = derive_initialization_id(
        &resource_uid,
        replica_id,
        &PodUid::new(&pod_uid),
        &PvcUid::new(&pvc_uid),
    );
    let identity = ReplicaIdentity {
        replica_id,
        instance_id: ReplicaInstanceId::new(&pod_uid),
        agent_generation: derive_agent_generation(&initialization_id),
    };
    if pod.labels().get(INSTANCE_LABEL).map(String::as_str) != Some(pod_uid.as_str()) {
        patch_pod_instance(client.clone(), namespace, pod, &pod_uid).await?;
        return Ok(());
    }
    let name = derive_replica_endpoint_name(&resource_uid, &identity);
    if observation
        .services
        .iter()
        .any(|service| service.name_any() == name)
    {
        return Ok(());
    }
    let services: Api<Service> = Api::namespaced(client, namespace);
    create_exact(
        &services,
        &name,
        &Service {
            metadata: kube::core::ObjectMeta {
                name: Some(name.clone()),
                labels: Some(base_labels(&observation.set, Some(replica_id), uid)),
                owner_references: Some(vec![owner.clone()]),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                selector: Some(BTreeMap::from([(INSTANCE_LABEL.to_string(), pod_uid)])),
                ports: Some(vec![
                    ServicePort {
                        name: Some("control".to_string()),
                        port: CONTROL_PORT,
                        target_port: Some(
                            k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(
                                CONTROL_PORT,
                            ),
                        ),
                        ..Default::default()
                    },
                    ServicePort {
                        name: Some("replication".to_string()),
                        port: REPLICATION_PORT,
                        target_port: Some(
                            k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(
                                REPLICATION_PORT,
                            ),
                        ),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
}

async fn ensure_write_service(
    client: Client,
    observation: &RawObservation,
    namespace: &str,
    uid: &str,
    owner: &OwnerReference,
) -> Result<()> {
    let service_name = format!("{}-write", observation.set.name_any());
    if observation
        .services
        .iter()
        .any(|service| service.name_any() == service_name)
    {
        return Ok(());
    }
    let services: Api<Service> = Api::namespaced(client, namespace);
    let service = Service {
        metadata: kube::core::ObjectMeta {
            name: Some(service_name.clone()),
            labels: Some(base_labels(&observation.set, None, uid)),
            owner_references: Some(vec![owner.clone()]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(BTreeMap::from([(
                INSTANCE_LABEL.to_string(),
                "disabled".to_string(),
            )])),
            ports: Some(vec![ServicePort {
                name: Some("application".to_string()),
                port: 80,
                target_port: Some(
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(8080),
                ),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };
    create_exact(&services, &service_name, &service).await
}

fn agent_credential_name(set: &KubericSet) -> String {
    format!("{}-agent-credentials", set.name_any())
}

async fn ensure_agent_credentials(
    client: Client,
    observation: &RawObservation,
    namespace: &str,
    uid: &str,
    owner: &OwnerReference,
    bearer_token: &str,
) -> Result<()> {
    let name = agent_credential_name(&observation.set);
    let secrets: Api<Secret> = Api::namespaced(client, namespace);
    let desired = Secret {
        metadata: kube::core::ObjectMeta {
            name: Some(name.clone()),
            labels: Some(base_labels(&observation.set, None, uid)),
            owner_references: Some(vec![owner.clone()]),
            ..Default::default()
        },
        string_data: Some(BTreeMap::from([(
            "bearer-token".to_string(),
            bearer_token.to_string(),
        )])),
        type_: Some("Opaque".to_string()),
        ..Default::default()
    };
    match secrets
        .get_opt(&name)
        .await
        .map_err(map_kube_effect_error)?
    {
        None => create_exact(&secrets, &name, &desired).await,
        Some(existing) => {
            if existing.labels().get(SET_UID_LABEL).map(String::as_str) != Some(uid) {
                return Err(ControllerError::Effect(format!(
                    "Secret {name} is not owned by this KubericSet"
                )));
            }
            let mut replacement = desired;
            replacement.metadata.resource_version = existing.resource_version();
            secrets
                .replace(&name, &PostParams::default(), &replacement)
                .await
                .map(|_| ())
                .map_err(map_kube_effect_error)
        }
    }
}

async fn ensure_peer_service(
    client: Client,
    observation: &RawObservation,
    namespace: &str,
    uid: &str,
    owner: &OwnerReference,
) -> Result<()> {
    let name = format!("{}-peer", observation.set.name_any());
    if let Some(existing) = observation
        .services
        .iter()
        .find(|service| service.name_any() == name)
    {
        if existing.labels().get(SET_UID_LABEL).map(String::as_str) != Some(uid) {
            return Err(ControllerError::Effect(format!(
                "Service {name} is not owned by this KubericSet"
            )));
        }
        let ready = existing.spec.as_ref().is_some_and(|spec| {
            spec.cluster_ip.as_deref() == Some("None")
                && spec.publish_not_ready_addresses == Some(true)
                && spec.selector.as_ref().is_some_and(|selector| {
                    selector.get(SET_UID_LABEL).map(String::as_str) == Some(uid)
                })
                && spec.ports.as_ref().is_some_and(|ports| {
                    [("control", CONTROL_PORT), ("replication", REPLICATION_PORT)]
                        .into_iter()
                        .all(|(name, port)| {
                            ports.iter().any(|candidate| {
                                candidate.name.as_deref() == Some(name) && candidate.port == port
                            })
                        })
                })
        });
        if ready {
            return Ok(());
        }
        let services: Api<Service> = Api::namespaced(client, namespace);
        services
            .delete(
                &name,
                &DeleteParams {
                    preconditions: Some(Preconditions {
                        uid: existing.uid(),
                        resource_version: existing.resource_version(),
                    }),
                    ..Default::default()
                },
            )
            .await
            .map(|_| ())
            .map_err(map_kube_effect_error)?;
        return Ok(());
    }
    let services: Api<Service> = Api::namespaced(client, namespace);
    let service = Service {
        metadata: kube::core::ObjectMeta {
            name: Some(name.clone()),
            labels: Some(base_labels(&observation.set, None, uid)),
            owner_references: Some(vec![owner.clone()]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            publish_not_ready_addresses: Some(true),
            selector: Some(BTreeMap::from([(
                SET_UID_LABEL.to_string(),
                uid.to_string(),
            )])),
            ports: Some(vec![
                ServicePort {
                    name: Some("control".to_string()),
                    port: CONTROL_PORT,
                    target_port: Some(
                        k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(
                            CONTROL_PORT,
                        ),
                    ),
                    ..Default::default()
                },
                ServicePort {
                    name: Some("replication".to_string()),
                    port: REPLICATION_PORT,
                    target_port: Some(
                        k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(
                            REPLICATION_PORT,
                        ),
                    ),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    };
    create_exact(&services, &name, &service).await
}

async fn patch_pod_instance(
    client: Client,
    namespace: &str,
    pod: &Pod,
    instance: &str,
) -> Result<()> {
    let uid = pod.uid().ok_or(ControllerError::ObservationStale)?;
    let resource_version = pod
        .resource_version()
        .ok_or(ControllerError::ObservationStale)?;
    let mut labels = pod.labels().clone();
    labels.insert(INSTANCE_LABEL.to_string(), instance.to_string());
    let patch = serde_json::json!([
        {"op": "test", "path": "/metadata/uid", "value": uid},
        {"op": "test", "path": "/metadata/resourceVersion", "value": resource_version},
        {"op": "add", "path": "/metadata/labels", "value": labels}
    ]);
    let pods: Api<Pod> = Api::namespaced(client, namespace);
    pods.patch(
        &pod.name_any(),
        &PatchParams::default(),
        &Patch::<serde_json::Value>::Json(
            serde_json::from_value(patch)
                .map_err(|error| ControllerError::Effect(error.to_string()))?,
        ),
    )
    .await
    .map(|_| ())
    .map_err(map_kube_effect_error)
}

async fn patch_write_service(
    client: Client,
    observation: &RawObservation,
    instance: &str,
) -> Result<()> {
    let namespace = observation
        .set
        .namespace()
        .ok_or_else(|| ControllerError::Effect("KubericSet has no namespace".to_string()))?;
    let service_name = format!("{}-write", observation.set.name_any());
    let service = observation
        .services
        .iter()
        .find(|service| service.name_any() == service_name)
        .ok_or(ControllerError::ObservationStale)?;
    let uid = service.uid().ok_or(ControllerError::ObservationStale)?;
    let resource_version = service
        .resource_version()
        .ok_or(ControllerError::ObservationStale)?;
    let patch = serde_json::json!([
        {"op": "test", "path": "/metadata/uid", "value": uid},
        {"op": "test", "path": "/metadata/resourceVersion", "value": resource_version},
        {"op": "add", "path": "/spec/selector", "value": {INSTANCE_LABEL: instance}}
    ]);
    let services: Api<Service> = Api::namespaced(client, &namespace);
    services
        .patch(
            &service_name,
            &PatchParams::default(),
            &Patch::<serde_json::Value>::Json(
                serde_json::from_value(patch)
                    .map_err(|error| ControllerError::Effect(error.to_string()))?,
            ),
        )
        .await
        .map(|_| ())
        .map_err(map_kube_effect_error)
}

fn agent_endpoint(pod: &Pod) -> Option<String> {
    pod.annotations()
        .get(CONTROL_ADDRESS_ANNOTATION)
        .cloned()
        .or_else(|| {
            pod.status
                .as_ref()
                .and_then(|status| status.pod_ip.as_ref())
                .map(|ip| format!("http://{ip}:{CONTROL_PORT}"))
        })
}

fn map_kube_effect_error(error: kube::Error) -> ControllerError {
    match error {
        kube::Error::Api(response) if matches!(response.code, 409 | 412 | 422) => {
            ControllerError::ObservationStale
        }
        other => ControllerError::Effect(other.to_string()),
    }
}

fn map_kube_observation_error(error: kube::Error) -> ControllerError {
    let transient = match &error {
        kube::Error::Api(response) => response.code == 429 || response.code >= 500,
        kube::Error::HyperError(_) | kube::Error::Service(_) | kube::Error::ReadEvents(_) => true,
        _ => false,
    };
    if transient {
        ControllerError::TransientObservation(error.to_string())
    } else {
        ControllerError::Observation(error.to_string())
    }
}

#[cfg(test)]
#[test]
fn exact_pod_precondition_failures_require_reobservation() {
    for code in [409, 412, 422] {
        let error = kube::Error::Api(Box::new(kube::core::Status {
            message: "precondition changed".into(),
            reason: "Conflict".into(),
            code,
            ..Default::default()
        }));
        assert!(matches!(
            map_kube_effect_error(error),
            ControllerError::ObservationStale
        ));
    }
}

#[cfg(test)]
#[test]
fn observation_http_errors_preserve_permanent_and_transient_classification() {
    for code in [400, 401, 403, 404, 422] {
        let error = kube::Error::Api(Box::new(kube::core::Status {
            message: "permanent observation failure".into(),
            reason: "Permanent".into(),
            code,
            ..Default::default()
        }));
        assert!(
            matches!(
                map_kube_observation_error(error),
                ControllerError::Observation(_)
            ),
            "HTTP {code} must fail immediately"
        );
    }
    for code in [429, 500, 502, 503, 504] {
        let error = kube::Error::Api(Box::new(kube::core::Status {
            message: "transient observation failure".into(),
            reason: "Transient".into(),
            code,
            ..Default::default()
        }));
        assert!(
            matches!(
                map_kube_observation_error(error),
                ControllerError::TransientObservation(_)
            ),
            "HTTP {code} must remain retryable"
        );
    }
}

fn map_kube_status_error(error: kube::Error) -> ControllerError {
    match error {
        kube::Error::Api(response) if response.code == 409 => ControllerError::ObservationStale,
        other => ControllerError::Effect(other.to_string()),
    }
}

fn map_agent_effect_error(error: AgentRpcError) -> ControllerError {
    match error {
        AgentRpcError::Unavailable(message) => ControllerError::AgentUnavailable(message),
        AgentRpcError::Invalid(message) => ControllerError::InvalidAgentEvidence(message),
    }
}

fn command_target(command: &ProtocolCommand) -> (ReplicaIdentity, ReplicaId) {
    match command {
        ProtocolCommand::AcceptSecondaryRemovalCommit(command) => {
            (command.target.clone(), command.target.replica_id)
        }
        ProtocolCommand::PrepareSecondaryRemoval(command) => {
            (command.intent.primary.clone(), command.local_replica_id)
        }
        ProtocolCommand::RetireReplica(command) => (
            command.committed.evidence.preparation.intent.target.clone(),
            command.local_replica_id,
        ),
        ProtocolCommand::InitializeAgentStore(command) => {
            let identity = ReplicaIdentity {
                replica_id: command.local_replica_id,
                instance_id: command.expected_instance_id.clone(),
                agent_generation: command.assigned_agent_generation.clone(),
            };
            (identity, command.local_replica_id)
        }
        ProtocolCommand::EnsureConfiguration(command) => {
            let identity = ReplicaIdentity {
                replica_id: command.local_replica_id,
                instance_id: command.expected_instance_id.clone(),
                agent_generation: command.expected_agent_generation.clone(),
            };
            (identity, command.local_replica_id)
        }
        ProtocolCommand::PrepareSwitchover(command) => {
            (command.source.clone(), command.local_replica_id)
        }
        ProtocolCommand::EnsureReplicaBuild(command) => {
            let identity = ReplicaIdentity {
                replica_id: command.local_replica_id,
                instance_id: command.expected_instance_id.clone(),
                agent_generation: command.expected_agent_generation.clone(),
            };
            (identity, command.local_replica_id)
        }
    }
}

fn command_request(
    resource_uid: String,
    target: ReplicaIdentity,
    expected_process_session_id: String,
    command: ProtocolCommand,
) -> Result<proto::ExecuteCommandRequest> {
    let command = match command {
        ProtocolCommand::AcceptSecondaryRemovalCommit(command) => {
            proto::execute_command_request::Command::AcceptSecondaryRemovalCommit(Box::new(
                (*command).into(),
            ))
        }
        ProtocolCommand::PrepareSecondaryRemoval(command) => {
            proto::execute_command_request::Command::PrepareSecondaryRemoval(Box::new(
                (*command).into(),
            ))
        }
        ProtocolCommand::RetireReplica(command) => {
            proto::execute_command_request::Command::RetireReplica(Box::new((*command).into()))
        }
        ProtocolCommand::InitializeAgentStore(command) => {
            proto::execute_command_request::Command::InitializeAgentStore(initialize_command(
                *command,
            ))
        }
        ProtocolCommand::EnsureConfiguration(command) => {
            proto::execute_command_request::Command::EnsureConfiguration(Box::new(ensure_command(
                *command,
            )))
        }
        ProtocolCommand::PrepareSwitchover(command) => {
            proto::execute_command_request::Command::PrepareSwitchover(prepare_switchover_command(
                *command,
            ))
        }
        ProtocolCommand::EnsureReplicaBuild(command) => {
            proto::execute_command_request::Command::EnsureReplicaBuild(ensure_build_command(
                *command,
            ))
        }
    };
    Ok(proto::ExecuteCommandRequest {
        protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
        resource_uid,
        target: Some(target.into()),
        expected_process_session_id,
        command: Some(command),
    })
}

fn initialize_command(command: InitializeAgentStore) -> proto::InitializeAgentStoreCommand {
    proto::InitializeAgentStoreCommand {
        initialization_id: command.initialization_id.to_string(),
        resource_uid: command.resource_uid.to_string(),
        local_replica_id: command.local_replica_id.value(),
        expected_instance_id: command.expected_instance_id.to_string(),
        expected_pod_uid: command.expected_pod_uid.to_string(),
        expected_pvc_uid: command.expected_pvc_uid.to_string(),
        assigned_agent_generation: command.assigned_agent_generation.to_string(),
        effective_policy: Some(policy(command.effective_policy)),
        bootstrap_configuration: Some(command.bootstrap_configuration.into()),
        provisioning: command.provisioning.map(provisioning),
    }
}

fn ensure_command(command: EnsureConfiguration) -> proto::EnsureConfigurationCommand {
    proto::EnsureConfigurationCommand {
        previous_policy: command.previous_policy.map(Into::into),
        secondary_removal_evidence: command.secondary_removal_evidence.map(Into::into),
        scale_up_evidence: command.scale_up_evidence.map(|evidence| (*evidence).into()),
        operation_id: command.operation_id.to_string(),
        previous_configuration: command.previous_configuration.map(Into::into),
        current_configuration: Some(command.current_configuration.into()),
        previous_epoch: command.previous_epoch.map(Into::into),
        current_epoch: Some(command.current_epoch.into()),
        effective_policy: Some(policy(command.effective_policy)),
        local_replica_id: command.local_replica_id.value(),
        expected_instance_id: command.expected_instance_id.to_string(),
        expected_agent_generation: command.expected_agent_generation.to_string(),
        transition_kind: transition_kind(command.transition_kind) as i32,
        grant_write: command.primary_write_status
            == kuberic_runtime::protocol::types::AccessStatus::Granted,
        current_only: command.current_only,
        retire_build_id: command
            .retire_build_ids
            .first()
            .map_or_else(String::new, ToString::to_string),
        primary_write_status: match command.primary_write_status {
            kuberic_runtime::protocol::types::AccessStatus::Granted => {
                proto::AccessStatus::Granted as i32
            }
            kuberic_runtime::protocol::types::AccessStatus::ReconfigurationPending => {
                proto::AccessStatus::ReconfigurationPending as i32
            }
            kuberic_runtime::protocol::types::AccessStatus::NotPrimary => {
                proto::AccessStatus::NotPrimary as i32
            }
            kuberic_runtime::protocol::types::AccessStatus::NoWriteQuorum => {
                proto::AccessStatus::NoWriteQuorum as i32
            }
        },
        retire_build_ids: command
            .retire_build_ids
            .iter()
            .map(ToString::to_string)
            .collect(),
        failover_safe_lsn: command.failover_safe_lsn,
        switchover_handoff: command.switchover_handoff.map(Into::into),
        retire_switchover_preparation_ids: command
            .retire_switchover_preparation_ids
            .into_iter()
            .map(Into::into)
            .collect(),
    }
}

fn prepare_switchover_command(command: PrepareSwitchover) -> proto::PrepareSwitchoverCommand {
    proto::PrepareSwitchoverCommand {
        preparation_generation: command.preparation_generation,
        operation_id: command.operation_id.to_string(),
        request_id: command.request_id.to_string(),
        local_replica_id: command.local_replica_id.value(),
        expected_instance_id: command.expected_instance_id.to_string(),
        expected_agent_generation: command.expected_agent_generation.to_string(),
        source: Some(command.source.into()),
        target: Some(command.target.into()),
        current_configuration: Some(command.current_configuration.into()),
    }
}

fn ensure_build_command(command: EnsureReplicaBuild) -> proto::EnsureReplicaBuildCommand {
    proto::EnsureReplicaBuildCommand {
        operation_id: command.operation_id.to_string(),
        local_replica_id: command.local_replica_id.value(),
        expected_instance_id: command.expected_instance_id.to_string(),
        expected_agent_generation: command.expected_agent_generation.to_string(),
        target: Some(command.target.into()),
        authority: command.authority.map(Into::into),
        source_session_id: command
            .source_session_id
            .map_or_else(String::new, |session| session.to_string()),
        retire: command.retire,
    }
}

fn provisioning(provisioning: ProvisioningIntent) -> proto::ProvisioningIntent {
    use proto::provisioning_intent::Purpose;
    proto::ProvisioningIntent {
        purpose: Some(match provisioning.purpose.kind {
            kuberic_runtime::protocol::types::ProvisioningKind::Replacement => Purpose::Replaces(
                provisioning
                    .purpose
                    .replaces
                    .expect("validated replacement provisioning")
                    .into(),
            ),
            kuberic_runtime::protocol::types::ProvisioningKind::ScaleUp => Purpose::ScaleUp(
                provisioning
                    .purpose
                    .scale_up
                    .expect("validated scale-up provisioning")
                    .into(),
            ),
        }),
        pod_uid: provisioning.pod_uid.to_string(),
        pvc_uid: provisioning.pvc_uid.to_string(),
        operation_id: provisioning.operation_id.to_string(),
    }
}

fn policy(policy: EffectivePolicy) -> proto::EffectivePolicy {
    proto::EffectivePolicy {
        replica_set_size: policy.replica_set_size,
        write_quorum: policy.write_quorum,
        read_quorum: policy.read_quorum,
        failover_delay_seconds: policy.failover_delay_seconds,
    }
}

fn transition_kind(kind: TransitionKind) -> proto::TransitionKind {
    match kind {
        TransitionKind::Bootstrap => proto::TransitionKind::Bootstrap,
        TransitionKind::Replacement => proto::TransitionKind::Replacement,
        TransitionKind::Failover => proto::TransitionKind::Failover,
        TransitionKind::PlannedSwitchover => proto::TransitionKind::PlannedSwitchover,
        TransitionKind::SecondaryScaleDown => proto::TransitionKind::SecondaryScaleDown,
        TransitionKind::ScaleUp => proto::TransitionKind::ScaleUp,
    }
}

#[derive(Clone)]
pub struct InMemoryClusterApi {
    state: Arc<Mutex<InMemoryState>>,
}

struct InMemoryState {
    observation: RawObservation,
    effects: Vec<EffectRecord>,
    conflict_next_status: bool,
    observation_count: usize,
    active_observations: usize,
    max_active_observations: usize,
    observation_delay: Duration,
    unavailable_next_execute: bool,
    lost_next_create_reply: bool,
    lost_next_delete_reply: bool,
    exact_lookup_failures: BTreeMap<String, String>,
    next_resource_uid: u64,
}

impl InMemoryClusterApi {
    pub fn new(observation: RawObservation) -> Self {
        Self {
            state: Arc::new(Mutex::new(InMemoryState {
                observation,
                effects: Vec::new(),
                conflict_next_status: false,
                observation_count: 0,
                active_observations: 0,
                max_active_observations: 0,
                observation_delay: Duration::ZERO,
                unavailable_next_execute: false,
                lost_next_create_reply: false,
                lost_next_delete_reply: false,
                exact_lookup_failures: BTreeMap::new(),
                next_resource_uid: 1,
            })),
        }
    }

    pub async fn set_observation(&self, observation: RawObservation) {
        self.state.lock().await.observation = observation;
    }

    pub async fn conflict_next_status(&self) {
        self.state.lock().await.conflict_next_status = true;
    }

    pub async fn set_observation_delay(&self, delay: Duration) {
        self.state.lock().await.observation_delay = delay;
    }

    pub async fn unavailable_next_execute(&self) {
        self.state.lock().await.unavailable_next_execute = true;
    }

    pub async fn lose_next_create_reply(&self) {
        self.state.lock().await.lost_next_create_reply = true;
    }

    pub async fn lose_next_delete_reply(&self) {
        self.state.lock().await.lost_next_delete_reply = true;
    }

    pub async fn fail_exact_lookup(&self, kind_and_name: String, message: Option<String>) {
        let mut state = self.state.lock().await;
        if let Some(message) = message {
            state.exact_lookup_failures.insert(kind_and_name, message);
        } else {
            state.exact_lookup_failures.remove(&kind_and_name);
        }
    }

    pub async fn effects(&self) -> Vec<EffectRecord> {
        self.state.lock().await.effects.clone()
    }

    pub async fn observation(&self) -> RawObservation {
        self.state.lock().await.observation.clone()
    }

    pub async fn observation_count(&self) -> usize {
        self.state.lock().await.observation_count
    }

    pub async fn max_active_observations(&self) -> usize {
        self.state.lock().await.max_active_observations
    }
}

#[async_trait]
impl ClusterApi for InMemoryClusterApi {
    async fn delete_scale_down_resource(
        &self,
        observation: &RawObservation,
        resource: ScaleDownResource,
        name: &str,
        uid: &str,
        resource_version: &str,
    ) -> Result<()> {
        let params = scale_down_delete_params(observation, resource, name, uid, resource_version)?;
        let mut state = self.state.lock().await;
        if state.observation.set.resource_version() != observation.set.resource_version() {
            return Err(ControllerError::ObservationStale);
        }
        match resource {
            ScaleDownResource::Pod => memory_delete(&mut state.observation.pods, name, &params)?,
            ScaleDownResource::Pvc => memory_delete(&mut state.observation.pvcs, name, &params)?,
            ScaleDownResource::Endpoint => {
                memory_delete(&mut state.observation.services, name, &params)?
            }
        }
        state.effects.push(EffectRecord::DeleteScaleDownResource {
            resource,
            name: name.into(),
            uid: uid.into(),
            resource_version: resource_version.into(),
        });
        if state.lost_next_delete_reply {
            state.lost_next_delete_reply = false;
            return Err(ControllerError::ObservationStale);
        }
        Ok(())
    }

    async fn observe(&self, _namespace: &str, _name: &str) -> Result<RawObservation> {
        let delay = {
            let mut state = self.state.lock().await;
            state.observation_count += 1;
            state.active_observations += 1;
            state.max_active_observations =
                state.max_active_observations.max(state.active_observations);
            state.observation_delay
        };
        tokio::time::sleep(delay).await;
        let mut state = self.state.lock().await;
        state.active_observations -= 1;
        let physical = &state.observation;
        let mut raw = physical.clone();
        raw.exact_resources.clear();
        let uid = raw.set.uid().unwrap_or_default();
        raw.pods
            .retain(|p| p.labels().get(SET_UID_LABEL) == Some(&uid));
        raw.pvcs
            .retain(|p| p.labels().get(SET_UID_LABEL) == Some(&uid));
        raw.services
            .retain(|p| p.labels().get(SET_UID_LABEL) == Some(&uid));
        for (target, identity, frozen) in crate::exact_resources::requests(&raw) {
            let pod = memory_lookup(
                &physical.pods,
                "Pod",
                crate::exact_resources::name(&identity.pod),
                &state.exact_lookup_failures,
            );
            let pvc = memory_lookup(
                &physical.pvcs,
                "PVC",
                crate::exact_resources::name(&identity.pvc),
                &state.exact_lookup_failures,
            );
            let endpoint = memory_lookup(
                &physical.services,
                "Service",
                crate::exact_resources::name(&identity.endpoint),
                &state.exact_lookup_failures,
            );
            crate::exact_resources::finish(
                &mut raw,
                RawScaleDownResources {
                    target,
                    identity,
                    pod,
                    pvc,
                    endpoint,
                },
                frozen,
            );
        }
        Ok(raw)
    }

    async fn ensure_replica_scaffolding(
        &self,
        observation: &RawObservation,
        replica_ids: &[ReplicaId],
    ) -> Result<()> {
        let uid = observation
            .set
            .uid()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no UID".to_string()))?;
        let owner = owner_reference(&observation.set)?;
        let image = effective_replica_image(observation)?;
        let mut state = self.state.lock().await;
        if state.observation.set.resource_version() != observation.set.resource_version() {
            return Err(ControllerError::ObservationStale);
        }
        for replica_id in replica_ids {
            let allocation_operation_id =
                scale_up_allocation_operation_id(&observation.set, *replica_id);
            let pod_name = replica_name(&observation.set, *replica_id);
            let pvc_name = format!("{pod_name}-data");
            let Some(pvc) = state
                .observation
                .pvcs
                .iter()
                .find(|pvc| pvc.name_any() == pvc_name)
                .cloned()
            else {
                let resource_number = state.next_resource_uid;
                state.next_resource_uid += 1;
                let mut pvc = replica_pvc(
                    &observation.set,
                    *replica_id,
                    &uid,
                    &owner,
                    allocation_operation_id,
                );
                pvc.metadata.namespace = observation.set.namespace();
                pvc.metadata.uid = Some(format!(
                    "in-memory-pvc-{}-{resource_number}",
                    replica_id.value()
                ));
                pvc.metadata.resource_version = Some(resource_number.to_string());
                state.observation.pvcs.push(pvc);
                state
                    .effects
                    .push(EffectRecord::EnsureScaffolding(replica_ids.to_vec()));
                if state.lost_next_create_reply {
                    state.lost_next_create_reply = false;
                    return Err(ControllerError::ObservationStale);
                }
                return Ok(());
            };
            let pvc_uid = pvc.uid().ok_or(ControllerError::ObservationStale)?;
            if let Some(operation_id) = allocation_operation_id
                && (!pvc_matches_allocation(&pvc, operation_id)
                    || observation
                        .set
                        .status
                        .as_ref()
                        .and_then(|status| status.authority.scale_up_allocation.as_ref())
                        .and_then(|allocation| allocation.pvc_uid.as_ref())
                        .is_some_and(|frozen| frozen.as_str() != pvc_uid))
            {
                return Err(ControllerError::ObservationStale);
            }
            let Some(pod) = state
                .observation
                .pods
                .iter()
                .find(|pod| pod.name_any() == pod_name)
                .cloned()
            else {
                let resource_number = state.next_resource_uid;
                state.next_resource_uid += 1;
                let mut pod = replica_pod(
                    &observation.set,
                    *replica_id,
                    &uid,
                    &owner,
                    &pvc_name,
                    &pvc_uid,
                    &image,
                    allocation_operation_id,
                );
                pod.metadata.namespace = observation.set.namespace();
                pod.metadata.uid = Some(format!(
                    "in-memory-pod-{}-{resource_number}",
                    replica_id.value()
                ));
                pod.metadata.resource_version = Some(resource_number.to_string());
                pod.metadata.annotations.get_or_insert_default().insert(
                    CONTROL_ADDRESS_ANNOTATION.to_string(),
                    "http://127.0.0.1:50051".to_string(),
                );
                pod.status = Some(k8s_openapi::api::core::v1::PodStatus {
                    pod_ip: Some("127.0.0.1".to_string()),
                    conditions: Some(vec![k8s_openapi::api::core::v1::PodCondition {
                        type_: "Ready".to_string(),
                        status: "True".to_string(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                });
                state.observation.pods.push(pod);
                state
                    .effects
                    .push(EffectRecord::EnsureScaffolding(replica_ids.to_vec()));
                if state.lost_next_create_reply {
                    state.lost_next_create_reply = false;
                    return Err(ControllerError::ObservationStale);
                }
                return Ok(());
            };
            let pod_uid = pod.uid().ok_or(ControllerError::ObservationStale)?;
            if let Some(operation_id) = allocation_operation_id
                && (!pod_matches_allocation(&pod, operation_id)
                    || !crate::exact_resources::pod_matches_scale_up_allocation_metadata(
                        &observation.set,
                        &pod,
                        *replica_id,
                        observation
                            .set
                            .status
                            .as_ref()
                            .and_then(|status| status.authority.scale_up_allocation.as_ref())
                            .and_then(|allocation| allocation.pvc_uid.as_ref()),
                    ))
            {
                return Err(ControllerError::ObservationStale);
            }
            if pod.labels().get(INSTANCE_LABEL).map(String::as_str) != Some(pod_uid.as_str()) {
                state
                    .observation
                    .pods
                    .iter_mut()
                    .find(|candidate| candidate.name_any() == pod_name)
                    .expect("observed Pod remains present")
                    .metadata
                    .labels
                    .get_or_insert_default()
                    .insert(INSTANCE_LABEL.to_string(), pod_uid.clone());
                state
                    .effects
                    .push(EffectRecord::EnsureScaffolding(replica_ids.to_vec()));
                return Ok(());
            }
            let initialization_id = derive_initialization_id(
                &ResourceUid::new(&uid),
                *replica_id,
                &PodUid::new(&pod_uid),
                &PvcUid::new(&pvc_uid),
            );
            let target = ReplicaIdentity {
                replica_id: *replica_id,
                instance_id: ReplicaInstanceId::new(&pod_uid),
                agent_generation: derive_agent_generation(&initialization_id),
            };
            let endpoint_name = derive_replica_endpoint_name(&ResourceUid::new(&uid), &target);
            if !state
                .observation
                .services
                .iter()
                .any(|service| service.name_any() == endpoint_name)
            {
                let resource_number = state.next_resource_uid;
                state.next_resource_uid += 1;
                state.observation.services.push(Service {
                    metadata: kube::core::ObjectMeta {
                        name: Some(endpoint_name),
                        namespace: observation.set.namespace(),
                        uid: Some(format!(
                            "in-memory-endpoint-{}-{resource_number}",
                            replica_id.value()
                        )),
                        resource_version: Some(resource_number.to_string()),
                        labels: Some(base_labels(&observation.set, Some(*replica_id), &uid)),
                        owner_references: Some(vec![owner.clone()]),
                        ..Default::default()
                    },
                    spec: Some(ServiceSpec {
                        selector: Some(BTreeMap::from([(INSTANCE_LABEL.to_string(), pod_uid)])),
                        ports: Some(vec![
                            ServicePort {
                                name: Some("control".to_string()),
                                port: CONTROL_PORT,
                                ..Default::default()
                            },
                            ServicePort {
                                name: Some("replication".to_string()),
                                port: REPLICATION_PORT,
                                ..Default::default()
                            },
                        ]),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
            }
        }
        state
            .effects
            .push(EffectRecord::EnsureScaffolding(replica_ids.to_vec()));
        Ok(())
    }

    async fn ensure_replica_support(&self, observation: &RawObservation) -> Result<()> {
        let mut state = self.state.lock().await;
        let uid = observation
            .set
            .uid()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no UID".to_string()))?;
        let peer_name = format!("{}-peer", observation.set.name_any());
        if !state
            .observation
            .services
            .iter()
            .any(|service| service.name_any() == peer_name)
        {
            state.observation.services.push(Service {
                metadata: kube::core::ObjectMeta {
                    name: Some(peer_name),
                    labels: Some(BTreeMap::from([(SET_UID_LABEL.to_string(), uid.clone())])),
                    ..Default::default()
                },
                spec: Some(ServiceSpec {
                    cluster_ip: Some("None".to_string()),
                    publish_not_ready_addresses: Some(true),
                    selector: Some(BTreeMap::from([(SET_UID_LABEL.to_string(), uid.clone())])),
                    ports: Some(vec![
                        ServicePort {
                            name: Some("control".to_string()),
                            port: CONTROL_PORT,
                            ..Default::default()
                        },
                        ServicePort {
                            name: Some("replication".to_string()),
                            port: REPLICATION_PORT,
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
        let credential_name = format!("{}-agent-credentials", observation.set.name_any());
        if !state
            .observation
            .secrets
            .iter()
            .any(|secret| secret.name_any() == credential_name)
        {
            state.observation.secrets.push(Secret {
                metadata: kube::core::ObjectMeta {
                    name: Some(credential_name),
                    labels: Some(BTreeMap::from([(SET_UID_LABEL.to_string(), uid)])),
                    ..Default::default()
                },
                data: Some(BTreeMap::from([(
                    "bearer-token".to_string(),
                    k8s_openapi::ByteString(b"test-token".to_vec()),
                )])),
                ..Default::default()
            });
        }
        state.effects.push(EffectRecord::EnsureReplicaSupport);
        Ok(())
    }

    async fn ensure_replacement_scaffolding(
        &self,
        observation: &RawObservation,
        replica_id: ReplicaId,
        replacing: &ReplicaIdentity,
    ) -> Result<()> {
        let uid = observation
            .set
            .uid()
            .ok_or_else(|| ControllerError::Effect("KubericSet has no UID".to_string()))?;
        let owner = owner_reference(&observation.set)?;
        let image = effective_replica_image(observation)?;
        let base = derive_replacement_resource_name(&ResourceUid::new(&uid), replacing);
        let pod_name = format!("{}-{base}", observation.set.name_any());
        let pvc_name = format!("{pod_name}-data");
        let mut state = self.state.lock().await;
        if state.observation.set.resource_version() != observation.set.resource_version() {
            return Err(ControllerError::ObservationStale);
        }
        if !state
            .observation
            .pvcs
            .iter()
            .any(|pvc| pvc.name_any() == pvc_name)
        {
            let resource_number = state.next_resource_uid;
            state.next_resource_uid += 1;
            let mut pvc = replica_pvc_named(&observation.set, replica_id, &uid, &owner, &pvc_name);
            pvc.metadata.namespace = observation.set.namespace();
            pvc.metadata.uid = Some(format!(
                "in-memory-replacement-pvc-{}-{resource_number}",
                replica_id.value()
            ));
            pvc.metadata.resource_version = Some(resource_number.to_string());
            state.observation.pvcs.push(pvc);
            state
                .effects
                .push(EffectRecord::EnsureReplacement(replacing.clone()));
            return Ok(());
        }
        let pvc = state
            .observation
            .pvcs
            .iter()
            .find(|pvc| pvc.name_any() == pvc_name)
            .cloned()
            .expect("replacement PVC remains present");
        let pvc_uid = pvc.uid().ok_or(ControllerError::ObservationStale)?;
        if !state
            .observation
            .pods
            .iter()
            .any(|pod| pod.name_any() == pod_name)
        {
            let resource_number = state.next_resource_uid;
            state.next_resource_uid += 1;
            let mut pod = replica_pod_named(
                &observation.set,
                replica_id,
                &uid,
                &owner,
                &pod_name,
                &pvc_name,
                &pvc_uid,
                &image,
                None,
            );
            let pod_uid = format!(
                "in-memory-replacement-pod-{}-{resource_number}",
                replica_id.value()
            );
            pod.metadata.namespace = observation.set.namespace();
            pod.metadata.uid = Some(pod_uid.clone());
            pod.metadata.resource_version = Some(resource_number.to_string());
            pod.metadata
                .labels
                .get_or_insert_default()
                .insert(INSTANCE_LABEL.to_string(), pod_uid);
            pod.metadata.annotations.get_or_insert_default().insert(
                CONTROL_ADDRESS_ANNOTATION.to_string(),
                "http://127.0.0.1:50051".to_string(),
            );
            pod.status = Some(k8s_openapi::api::core::v1::PodStatus {
                pod_ip: Some("127.0.0.1".to_string()),
                conditions: Some(vec![k8s_openapi::api::core::v1::PodCondition {
                    type_: "Ready".to_string(),
                    status: "True".to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            });
            state.observation.pods.push(pod);
            state
                .effects
                .push(EffectRecord::EnsureReplacement(replacing.clone()));
            return Ok(());
        }
        let pod = state
            .observation
            .pods
            .iter()
            .find(|pod| pod.name_any() == pod_name)
            .cloned()
            .expect("replacement Pod remains present");
        let pod_uid = pod.uid().ok_or(ControllerError::ObservationStale)?;
        let initialization_id = derive_initialization_id(
            &ResourceUid::new(&uid),
            replica_id,
            &PodUid::new(&pod_uid),
            &PvcUid::new(&pvc_uid),
        );
        let target = ReplicaIdentity {
            replica_id,
            instance_id: ReplicaInstanceId::new(&pod_uid),
            agent_generation: derive_agent_generation(&initialization_id),
        };
        let endpoint_name = derive_replica_endpoint_name(&ResourceUid::new(&uid), &target);
        if !state
            .observation
            .services
            .iter()
            .any(|service| service.name_any() == endpoint_name)
        {
            let resource_number = state.next_resource_uid;
            state.next_resource_uid += 1;
            state.observation.services.push(Service {
                metadata: kube::core::ObjectMeta {
                    name: Some(endpoint_name),
                    namespace: observation.set.namespace(),
                    uid: Some(format!(
                        "in-memory-replacement-endpoint-{}-{resource_number}",
                        replica_id.value()
                    )),
                    resource_version: Some(resource_number.to_string()),
                    labels: Some(base_labels(&observation.set, Some(replica_id), &uid)),
                    owner_references: Some(vec![owner]),
                    ..Default::default()
                },
                spec: Some(ServiceSpec {
                    selector: Some(BTreeMap::from([(INSTANCE_LABEL.to_string(), pod_uid)])),
                    ports: Some(vec![
                        ServicePort {
                            name: Some("control".to_string()),
                            port: CONTROL_PORT,
                            ..Default::default()
                        },
                        ServicePort {
                            name: Some("replication".to_string()),
                            port: REPLICATION_PORT,
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
        state
            .effects
            .push(EffectRecord::EnsureReplacement(replacing.clone()));
        Ok(())
    }

    async fn delete_replica_scaffolding(
        &self,
        observation: &RawObservation,
        pod_name: Option<&str>,
        pod_uid: Option<&PodUid>,
        pvc_name: Option<&str>,
        pvc_uid: Option<&PvcUid>,
    ) -> Result<()> {
        if crate::exact_resources::protected(observation, pod_name, pod_uid.map(PodUid::as_str))
            || crate::exact_resources::protected(observation, pvc_name, pvc_uid.map(PvcUid::as_str))
        {
            return Err(ControllerError::ObservationStale);
        }
        let service = pod_uid.and_then(|pod_uid| {
            observation.services.iter().find(|service| {
                service.spec.as_ref().is_some_and(|spec| {
                    spec.selector.as_ref().is_some_and(|selector| {
                        selector.get(INSTANCE_LABEL).map(String::as_str) == Some(pod_uid.as_str())
                    })
                })
            })
        });
        if let Some(service) = service {
            let service_uid = service.uid().ok_or(ControllerError::ObservationStale)?;
            if crate::exact_resources::protected(
                observation,
                Some(&service.name_any()),
                Some(&service_uid),
            ) {
                return Err(ControllerError::ObservationStale);
            }
        }
        let mut state = self.state.lock().await;
        if let Some(service) = service {
            let name = service.name_any();
            let uid = service.uid().ok_or(ControllerError::ObservationStale)?;
            state.observation.services.retain(|candidate| {
                candidate.name_any() != name || candidate.uid().as_deref() != Some(uid.as_str())
            });
        }
        if let Some(name) = pod_name {
            state.observation.pods.retain(|pod| pod.name_any() != name);
        } else if let Some(name) = pvc_name {
            state.observation.pvcs.retain(|pvc| pvc.name_any() != name);
        }
        state.effects.push(EffectRecord::DeleteScaffolding {
            pod_name: pod_name.map(ToString::to_string),
            pvc_name: pvc_name.map(ToString::to_string),
        });
        Ok(())
    }

    async fn delete_exact_pod(
        &self,
        observation: &RawObservation,
        pod_name: &str,
        pod_uid: &PodUid,
    ) -> Result<()> {
        if crate::exact_resources::protected_from_exact_pod_fence(
            observation,
            Some(pod_name),
            Some(pod_uid.as_str()),
        ) {
            return Err(ControllerError::ObservationStale);
        }
        let params = exact_pod_delete_params(observation, pod_name, pod_uid)?;
        let mut state = self.state.lock().await;
        if let Some(pod) = state
            .observation
            .pods
            .iter()
            .find(|pod| pod.name_any() == pod_name)
        {
            let preconditions = params.preconditions.unwrap();
            if pod.uid() != preconditions.uid
                || pod.resource_version() != preconditions.resource_version
            {
                return Err(ControllerError::ObservationStale);
            }
            state
                .observation
                .pods
                .retain(|pod| pod.uid().as_deref() != Some(pod_uid.as_str()));
        }
        state.effects.push(EffectRecord::DeleteExactPod {
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.clone(),
        });
        Ok(())
    }

    async fn delete_replica_endpoint(
        &self,
        observation: &RawObservation,
        identity: &ReplicaIdentity,
    ) -> Result<()> {
        let resource_uid = observation
            .set
            .uid()
            .map(ResourceUid::new)
            .ok_or_else(|| ControllerError::Effect("KubericSet has no UID".to_string()))?;
        let name = derive_replica_endpoint_name(&resource_uid, identity);
        let mut state = self.state.lock().await;
        if crate::exact_resources::protected(observation, Some(&name), None) {
            return Err(ControllerError::ObservationStale);
        }
        state
            .observation
            .services
            .retain(|service| service.name_any() != name);
        Ok(())
    }

    async fn ensure_write_routing_service(&self, observation: &RawObservation) -> Result<()> {
        if service_observation_failed(observation) {
            return Err(ControllerError::Observation(
                "cannot converge write routing after Service observation failed".to_string(),
            ));
        }
        let mut state = self.state.lock().await;
        let service_name = format!("{}-write", observation.set.name_any());
        if !state
            .observation
            .services
            .iter()
            .any(|service| service.name_any() == service_name)
        {
            state.observation.services.push(Service {
                metadata: kube::core::ObjectMeta {
                    name: Some(service_name),
                    uid: Some("in-memory-write-service".to_string()),
                    resource_version: Some("1".to_string()),
                    labels: observation
                        .set
                        .uid()
                        .map(|uid| BTreeMap::from([(SET_UID_LABEL.to_string(), uid)])),
                    ..Default::default()
                },
                spec: Some(ServiceSpec {
                    selector: Some(BTreeMap::from([(
                        INSTANCE_LABEL.to_string(),
                        "disabled".to_string(),
                    )])),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
        state.effects.push(EffectRecord::EnsureWriteRoutingService);
        Ok(())
    }

    async fn replace_status(
        &self,
        observation: &RawObservation,
        status: &AcceptedStatus,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
        if state.conflict_next_status
            || state.observation.set.resource_version() != observation.set.resource_version()
        {
            state.conflict_next_status = false;
            return Err(ControllerError::ObservationStale);
        }
        state.observation.set.status = Some(KubericSetStatus {
            authority: status.clone(),
        });
        let next = state
            .observation
            .set
            .resource_version()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_default()
            + 1;
        state.observation.set.metadata.resource_version = Some(next.to_string());
        state.effects.push(EffectRecord::ReplaceStatus);
        Ok(())
    }

    async fn remove_write_routing(&self, observation: &RawObservation) -> Result<()> {
        if service_observation_failed(observation) {
            return Err(ControllerError::Observation(
                "cannot confirm write routing absence after Service observation failed".to_string(),
            ));
        }
        let mut state = self.state.lock().await;
        if let Some(service) = state
            .observation
            .services
            .iter_mut()
            .find(|service| service.name_any().ends_with("-write"))
            && let Some(spec) = service.spec.as_mut()
        {
            spec.selector = Some(BTreeMap::from([(
                INSTANCE_LABEL.to_string(),
                "disabled".to_string(),
            )]));
        }
        state.effects.push(EffectRecord::RemoveWriteRouting);
        Ok(())
    }

    async fn publish_write_routing(
        &self,
        _observation: &RawObservation,
        primary: &ReplicaIdentity,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
        let pod = state
            .observation
            .pods
            .iter_mut()
            .find(|pod| pod.uid().as_deref() == Some(primary.instance_id.as_str()))
            .ok_or(ControllerError::ObservationStale)?;
        pod.metadata
            .labels
            .get_or_insert_default()
            .insert(INSTANCE_LABEL.to_string(), primary.instance_id.to_string());
        let service = state
            .observation
            .services
            .iter_mut()
            .find(|service| service.name_any().ends_with("-write"))
            .ok_or(ControllerError::ObservationStale)?;
        service.spec.get_or_insert_default().selector = Some(BTreeMap::from([(
            INSTANCE_LABEL.to_string(),
            primary.instance_id.to_string(),
        )]));
        state
            .effects
            .push(EffectRecord::PublishWriteRouting(primary.clone()));
        Ok(())
    }

    async fn execute_command(
        &self,
        observation: &RawObservation,
        command: &ProtocolCommand,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
        let (target, replica_id) = command_target(command);
        let session = observed_process_session(observation, replica_id, &target)?;
        if observed_process_session(&state.observation, replica_id, &target)? != session {
            return Err(ControllerError::ObservationStale);
        }
        kuberic_runtime::control::validate_execute_request(&command_request(
            observation
                .set
                .uid()
                .ok_or(ControllerError::ObservationStale)?,
            target,
            session,
            command.clone(),
        )?)
        .map_err(|e| ControllerError::InvalidAgentEvidence(e.to_string()))?;
        state.effects.push(EffectRecord::Execute(command.clone()));
        if state.unavailable_next_execute {
            state.unavailable_next_execute = false;
            return Err(ControllerError::AgentUnavailable(
                "ambiguous command result after dispatch".to_string(),
            ));
        }
        Ok(())
    }
}

fn memory_lookup<K: ResourceExt + Clone>(
    objects: &[K],
    kind: &str,
    name: &str,
    failures: &BTreeMap<String, String>,
) -> ExactLookup<K> {
    if let Some(message) = failures.get(&format!("{kind}/{name}")) {
        ExactLookup::Failed(message.clone())
    } else {
        objects
            .iter()
            .find(|o| o.name_any() == name)
            .cloned()
            .map(ExactLookup::Present)
            .unwrap_or(ExactLookup::NotFound)
    }
}

fn memory_delete<K: ResourceExt>(
    objects: &mut Vec<K>,
    name: &str,
    params: &DeleteParams,
) -> Result<()> {
    let object = objects
        .iter()
        .find(|o| o.name_any() == name)
        .ok_or(ControllerError::ObservationStale)?;
    let conditions = params
        .preconditions
        .as_ref()
        .ok_or(ControllerError::ObservationStale)?;
    if object.uid() != conditions.uid
        || (conditions.resource_version.is_some()
            && object.resource_version() != conditions.resource_version)
    {
        return Err(ControllerError::ObservationStale);
    }
    if object.meta().finalizers.as_ref().is_none_or(Vec::is_empty) {
        objects.retain(|o| o.name_any() != name);
    }
    Ok(())
}

async fn observe_exact_resources(
    raw: &mut RawObservation,
    pods: &Api<Pod>,
    pvcs: &Api<PersistentVolumeClaim>,
    services: &Api<Service>,
) {
    for (target, identity, frozen) in crate::exact_resources::requests(raw) {
        if raw.exact_resources.iter().any(|r| r.target == target) {
            continue;
        }
        let (pod, pvc, endpoint) = tokio::join!(
            exact_lookup(pods, crate::exact_resources::name(&identity.pod)),
            exact_lookup(pvcs, crate::exact_resources::name(&identity.pvc)),
            exact_lookup(services, crate::exact_resources::name(&identity.endpoint)),
        );
        crate::exact_resources::finish(
            raw,
            RawScaleDownResources {
                target,
                identity,
                pod,
                pvc,
                endpoint,
            },
            frozen,
        );
    }
}

async fn exact_lookup<K>(api: &Api<K>, name: &str) -> ExactLookup<K>
where
    K: Clone + std::fmt::Debug + serde::de::DeserializeOwned + kube::Resource<DynamicType = ()>,
{
    match api.get(name).await {
        Ok(object) => ExactLookup::Present(object),
        Err(kube::Error::Api(response)) if response.code == 404 => ExactLookup::NotFound,
        Err(error) => ExactLookup::Failed(error.to_string()),
    }
}

fn scale_down_delete_params(
    observation: &RawObservation,
    resource: ScaleDownResource,
    name: &str,
    uid: &str,
    resource_version: &str,
) -> Result<DeleteParams> {
    // Re-evaluate the immutable observation at the effect boundary. Only the
    // active accepted cleanup receipt can authorize this exact single deletion.
    let snapshot = crate::normalize::normalize(observation.clone(), BTreeMap::new())?;
    let plan = crate::evaluator::evaluate(
        &snapshot,
        &crate::evaluator::EvaluationConfig {
            enable_secondary_scale_down: true,
            allow_scale_up: true,
            ..Default::default()
        },
    );
    let expected = kuberic_runtime::protocol::command::KubernetesChange::DeleteScaleDownResource {
        resource,
        name: name.into(),
        uid: uid.into(),
        resource_version: resource_version.into(),
    };
    if !matches!(plan, crate::plan::Plan::Apply { changes } if changes == vec![expected]) {
        return Err(ControllerError::ObservationStale);
    }
    Ok(DeleteParams {
        preconditions: Some(Preconditions {
            uid: Some(uid.into()),
            resource_version: Some(resource_version.into()),
        }),
        ..Default::default()
    })
}

async fn delete_scale_down_exact<K>(api: &Api<K>, name: &str, params: &DeleteParams) -> Result<()>
where
    K: Clone + std::fmt::Debug + serde::de::DeserializeOwned + kube::Resource<DynamicType = ()>,
{
    match api.delete(name, params).await {
        Ok(_) => Ok(()),
        // A racing 404 or an ambiguous response requires another exact GET.
        Err(kube::Error::Api(response))
            if matches!(response.code, 404 | 408 | 409 | 412 | 422 | 429 | 500..=599) =>
        {
            Err(ControllerError::ObservationStale)
        }
        Err(error @ kube::Error::Api(_)) => Err(map_kube_effect_error(error)),
        Err(error) => {
            tracing::warn!(%error, %name, "exact deletion reply is unconfirmed; re-observe");
            Err(ControllerError::ObservationStale)
        }
    }
}

fn exact_pod_delete_params(
    observation: &RawObservation,
    pod_name: &str,
    pod_uid: &PodUid,
) -> Result<DeleteParams> {
    // Pod-only safety fencing is distinct from generic candidate cleanup. The
    // executor already selected this effect from the immutable plan; fence the
    // exact observed incarnation while retaining its PVC and receipt evidence.
    let pod = observation
        .pods
        .iter()
        .find(|pod| pod.name_any() == pod_name && pod.uid().as_deref() == Some(pod_uid.as_str()))
        .ok_or(ControllerError::ObservationStale)?;
    let resource_version = pod
        .resource_version()
        .ok_or(ControllerError::ObservationStale)?;
    Ok(DeleteParams {
        preconditions: Some(Preconditions {
            uid: Some(pod_uid.to_string()),
            resource_version: Some(resource_version),
        }),
        ..Default::default()
    })
}

#[allow(dead_code)]
async fn delete_exact<K>(api: &Api<K>, name: &str, uid: &str) -> Result<()>
where
    K: Clone + std::fmt::Debug + serde::de::DeserializeOwned + kube::Resource<DynamicType = ()>,
{
    match api
        .delete(
            name,
            &DeleteParams {
                preconditions: Some(Preconditions {
                    uid: Some(uid.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(response)) if response.code == 404 => Ok(()),
        Err(error) => Err(map_kube_effect_error(error)),
    }
}

#[allow(dead_code)]
fn owned_selector(uid: &str) -> LabelSelector {
    LabelSelector {
        match_labels: Some(BTreeMap::from([(
            SET_UID_LABEL.to_string(),
            uid.to_string(),
        )])),
        ..Default::default()
    }
}

#[allow(dead_code)]
fn role_label(role: ReplicaRole) -> &'static str {
    match role {
        ReplicaRole::Primary => "primary",
        ReplicaRole::ActiveSecondary => "active-secondary",
        ReplicaRole::IdleSecondary => "idle-secondary",
        ReplicaRole::None => "none",
    }
}

#[cfg(test)]
#[allow(dead_code)]
#[path = "../tests/protocol_support/secondary_scale_down.rs"]
mod scale_down_fixture;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::KubericSetSpec;
    use kuberic_runtime::protocol::command::{
        EnsureConfiguration, EnsureReplicaBuild, InitializeAgentStore, PrepareSwitchover,
    };
    use kuberic_runtime::protocol::types::{
        AccessStatus, AgentGeneration, ConfigurationDescriptor, ConfigurationMember,
        EffectivePolicy, Epoch, InitializationId, OperationId, PodUid, PvcUid, SwitchoverRequestId,
    };

    fn identity(replica_id: i64) -> ReplicaIdentity {
        ReplicaIdentity {
            replica_id: ReplicaId::new(replica_id),
            instance_id: ReplicaInstanceId::new(format!("pod-{replica_id}")),
            agent_generation: AgentGeneration::new(format!("generation-{replica_id}")),
        }
    }

    fn replica_environment(set: &KubericSet) -> Vec<EnvVar> {
        let owner = OwnerReference {
            api_version: "operator.kuberic.io/v1alpha1".to_string(),
            kind: "KubericSet".to_string(),
            name: set.name_any(),
            uid: "set-uid".to_string(),
            block_owner_deletion: None,
            controller: Some(true),
        };
        replica_pod(
            set,
            ReplicaId::new(1),
            "set-uid",
            &owner,
            "kvstore2-1-data",
            "pvc-uid",
            "kvstore2:test",
            None,
        )
        .spec
        .unwrap()
        .containers
        .into_iter()
        .find(|container| container.name == "application")
        .unwrap()
        .env
        .unwrap()
    }

    async fn http_response(
        status: u16,
        body: serde_json::Value,
    ) -> (Client, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut chunk = [0; 4096];
                let count = stream.read(&mut chunk).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&chunk[..count]);
                if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                    let length = headers
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length: "))
                        .map(|value| value.parse::<usize>().unwrap())
                        .unwrap_or(0);
                    if bytes.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            if status == 0 {
                return String::from_utf8(bytes).unwrap();
            }
            let body = body.to_string();
            stream.write_all(format!("HTTP/1.1 {status} response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            String::from_utf8(bytes).unwrap()
        });
        let config = kube::Config::new(format!("http://{address}").parse().unwrap());
        (Client::try_from(config).unwrap(), server)
    }

    #[tokio::test]
    async fn exact_get_and_delete_http_semantics_are_fail_closed() {
        for code in [404, 403, 500] {
            let (client, request) = http_response(
                code,
                serde_json::json!({
                    "apiVersion": "v1", "kind": "Status", "status": "Failure",
                    "message": "exact lookup error", "reason": "error", "code": code,
                }),
            )
            .await;
            let api = Api::<Pod>::namespaced(client, "tests");
            let result = exact_lookup(&api, "frozen-pod").await;
            assert_eq!(matches!(result, ExactLookup::NotFound), code == 404);
            assert_eq!(matches!(result, ExactLookup::Failed(_)), code != 404);
            assert!(
                request
                    .await
                    .unwrap()
                    .starts_with("GET /api/v1/namespaces/tests/pods/frozen-pod ")
            );
        }
        let params = DeleteParams {
            preconditions: Some(Preconditions {
                uid: Some("frozen-uid".into()),
                resource_version: Some("fresh-rv".into()),
            }),
            ..Default::default()
        };
        for code in [200, 0, 404, 409, 412, 422, 500] {
            let (client, request) = http_response(
                code,
                serde_json::json!({
                    "apiVersion": "v1", "kind": "Status",
                    "status": if code == 200 { "Success" } else { "Failure" },
                    "message": "delete response", "reason": "response", "code": code,
                }),
            )
            .await;
            let result = delete_scale_down_exact(
                &Api::<Pod>::namespaced(client, "tests"),
                "frozen-pod",
                &params,
            )
            .await;
            assert_eq!(result.is_ok(), code == 200);
            if code != 200 {
                assert!(matches!(result, Err(ControllerError::ObservationStale)));
            }
            let request = request.await.unwrap();
            assert!(request.starts_with("DELETE /api/v1/namespaces/tests/pods/frozen-pod"));
            let json: serde_json::Value =
                serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
            assert_eq!(
                json["preconditions"],
                serde_json::json!({"uid": "frozen-uid", "resourceVersion": "fresh-rv"})
            );
        }
    }

    #[test]
    fn secondary_removal_encoding_preserves_frozen_authority_and_session_target() {
        let intent = scale_down_fixture::intent(&[2, 8, 19, 40], 40);
        for command in [
            ProtocolCommand::PrepareSecondaryRemoval(Box::new(
                scale_down_fixture::prepare_command(&intent),
            )),
            ProtocolCommand::EnsureConfiguration(Box::new(
                scale_down_fixture::configuration_command(&intent, false),
            )),
            ProtocolCommand::EnsureConfiguration(Box::new(
                scale_down_fixture::configuration_command(&intent, true),
            )),
            ProtocolCommand::RetireReplica(Box::new(scale_down_fixture::retire_command(&intent))),
        ] {
            let (target, id) = command_target(&command);
            assert_eq!(id, target.replica_id);
            let expected = if matches!(command, ProtocolCommand::RetireReplica(_)) {
                &intent.target
            } else {
                &intent.primary
            };
            assert_eq!(&target, expected);
            let request = command_request(
                intent.resource_uid.to_string(),
                target.clone(),
                "fresh-session".into(),
                command.clone(),
            )
            .unwrap();
            let normalized = kuberic_runtime::control::normalize_execute_request(request).unwrap();
            assert_eq!(normalized.command, command);
            assert_eq!(normalized.target, target);
            assert_eq!(
                normalized.expected_process_session_id.as_str(),
                "fresh-session"
            );
        }
    }

    #[test]
    fn secondary_removal_commit_publication_round_trips() {
        let intent = scale_down_fixture::intent(&[1, 2], 1);
        let command = ProtocolCommand::AcceptSecondaryRemovalCommit(Box::new(
            kuberic_runtime::protocol::command::AcceptSecondaryRemovalCommit {
                operation_id: intent.command_operation_id(
                    kuberic_runtime::protocol::types::SecondaryRemovalStage::AcceptCommit,
                    &intent.primary,
                ),
                target: intent.primary.clone(),
                committed: scale_down_fixture::cleanup(&intent),
                local_recovery: false,
            },
        ));
        let request = command_request(
            intent.resource_uid.to_string(),
            intent.primary,
            "session".into(),
            command.clone(),
        )
        .unwrap();
        assert_eq!(
            kuberic_runtime::control::normalize_execute_request(request)
                .unwrap()
                .command,
            command
        );
    }

    #[test]
    fn connection_reset_is_unavailability_not_contradictory_agent_evidence() {
        assert!(matches!(
            classify_status(tonic::Status::unknown("transport error: connection reset")),
            AgentRpcError::Unavailable(_)
        ));
        assert!(matches!(
            classify_status(tonic::Status::invalid_argument("malformed authority")),
            AgentRpcError::Invalid(_)
        ));
    }

    #[test]
    fn command_dispatch_uses_the_exact_observed_process_session() {
        assert!(matches!(
            classify_execute_status(tonic::Status::failed_precondition(
                "command targets a stale agent process session"
            )),
            AgentRpcError::Unavailable(_)
        ));
        assert!(matches!(
            classify_status(tonic::Status::failed_precondition("invalid durable status")),
            AgentRpcError::Invalid(_)
        ));
        let source = identity(1);
        let target = identity(2);
        let mut agents = BTreeMap::new();
        agents.insert(
            ReplicaObservationKey::new(source.replica_id, source.instance_id.clone()),
            RawAgentObservation::Report(Box::new(proto::AgentStatusReport {
                process_session_id: "session-1".to_string(),
                ..Default::default()
            })),
        );
        let mut observation = RawObservation {
            exact_resources: Vec::new(),
            set: KubericSet::new(
                "db",
                KubericSetSpec {
                    replicas: 2,
                    image: "example/db:latest".to_string(),
                    failover_delay_seconds: 30,
                    switchover: None,
                },
            ),
            pods: Vec::new(),
            pvcs: Vec::new(),
            services: Vec::new(),
            secrets: Vec::new(),
            agents,
            failures: Vec::new(),
            now_unix_seconds: 0,
        };
        observation.pods.push(Pod {
            metadata: kube::core::ObjectMeta {
                name: Some("exact-pod".to_string()),
                uid: Some("pod-1".to_string()),
                resource_version: Some("42".to_string()),
                ..Default::default()
            },
            ..Default::default()
        });
        let deletion =
            exact_pod_delete_params(&observation, "exact-pod", &PodUid::new("pod-1")).unwrap();
        let serialized = serde_json::to_value(deletion).unwrap();
        assert_eq!(serialized["preconditions"]["uid"], "pod-1");
        assert_eq!(serialized["preconditions"]["resourceVersion"], "42");
        assert!(
            exact_pod_delete_params(&observation, "exact-pod", &PodUid::new("replaced")).is_err()
        );
        let session = observed_process_session(&observation, source.replica_id, &source).unwrap();
        assert_eq!(session, "session-1");

        let configuration = ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            source.replica_id,
            vec![
                ConfigurationMember {
                    identity: source.clone(),
                    role: ReplicaRole::Primary,
                },
                ConfigurationMember {
                    identity: target.clone(),
                    role: ReplicaRole::ActiveSecondary,
                },
            ],
            2,
        );
        let preparation = ProtocolCommand::PrepareSwitchover(Box::new(PrepareSwitchover {
            preparation_generation: 1,
            operation_id: OperationId::new("prepare-1"),
            request_id: SwitchoverRequestId::new("request-1"),
            local_replica_id: source.replica_id,
            expected_instance_id: source.instance_id.clone(),
            expected_agent_generation: source.agent_generation.clone(),
            source: source.clone(),
            target: target.clone(),
            current_configuration: configuration.clone(),
        }));
        let request = command_request(
            "resource".to_string(),
            source.clone(),
            session,
            preparation.clone(),
        )
        .unwrap();
        assert_eq!(request.expected_process_session_id, "session-1");
        assert!(matches!(
            &request.command,
            Some(proto::execute_command_request::Command::PrepareSwitchover(
                _
            ))
        ));
        let RawAgentObservation::Report(report) = observation
            .agents
            .get_mut(&ReplicaObservationKey::new(
                source.replica_id,
                source.instance_id.clone(),
            ))
            .unwrap()
        else {
            unreachable!()
        };
        report.process_session_id = "session-restarted".to_string();
        let replay = command_request(
            "resource".to_string(),
            source.clone(),
            observed_process_session(&observation, source.replica_id, &source).unwrap(),
            preparation,
        )
        .unwrap();
        assert_eq!(replay.expected_process_session_id, "session-restarted");
        assert_eq!(replay.command, request.command);

        let policy = EffectivePolicy::fixed(2, 30).unwrap();
        let existing_commands = vec![
            (
                ProtocolCommand::InitializeAgentStore(Box::new(InitializeAgentStore {
                    initialization_id: InitializationId::new("init-1"),
                    resource_uid: ResourceUid::new("resource"),
                    local_replica_id: source.replica_id,
                    expected_instance_id: source.instance_id.clone(),
                    expected_pod_uid: PodUid::new(source.instance_id.as_str()),
                    expected_pvc_uid: PvcUid::new("pvc-1"),
                    assigned_agent_generation: source.agent_generation.clone(),
                    effective_policy: policy.clone(),
                    bootstrap_configuration: configuration.clone(),
                    provisioning: None,
                })),
                source.clone(),
            ),
            (
                ProtocolCommand::EnsureConfiguration(Box::new(EnsureConfiguration {
                    previous_policy: None,
                    secondary_removal_evidence: None,
                    scale_up_evidence: None,
                    operation_id: OperationId::new("configuration-1"),
                    previous_configuration: None,
                    current_configuration: configuration.clone(),
                    previous_epoch: None,
                    current_epoch: configuration.epoch,
                    effective_policy: policy,
                    local_replica_id: source.replica_id,
                    expected_instance_id: source.instance_id.clone(),
                    expected_agent_generation: source.agent_generation.clone(),
                    transition_kind: TransitionKind::Bootstrap,
                    failover_safe_lsn: None,
                    primary_write_status: AccessStatus::ReconfigurationPending,
                    current_only: false,
                    retire_build_ids: Vec::new(),
                    switchover_handoff: None,
                    retire_switchover_preparation_ids: Vec::new(),
                })),
                source.clone(),
            ),
            (
                ProtocolCommand::EnsureReplicaBuild(Box::new(EnsureReplicaBuild {
                    operation_id: OperationId::new("build-1"),
                    local_replica_id: source.replica_id,
                    expected_instance_id: source.instance_id.clone(),
                    expected_agent_generation: source.agent_generation.clone(),
                    target,
                    authority: None,
                    source_session_id: None,
                    retire: false,
                })),
                source,
            ),
        ];
        for (command, target) in existing_commands {
            let request = command_request(
                "resource".to_string(),
                target,
                "session-current".to_string(),
                command,
            )
            .unwrap();
            assert_eq!(request.expected_process_session_id, "session-current");
        }
    }

    #[test]
    fn default_replica_pod_does_not_enable_live_test_copy_gate() {
        let set = KubericSet::new(
            "kvstore2",
            KubericSetSpec {
                replicas: 3,
                image: "kvstore2:test".to_string(),
                failover_delay_seconds: 10,
                switchover: None,
            },
        );
        assert!(
            replica_environment(&set)
                .iter()
                .all(|variable| variable.name != "KUBERIC_LIVE_TEST_COPY_GATE_ADDRESS")
        );
    }

    #[test]
    fn owned_live_test_annotation_enables_fixed_diagnostic_address() {
        let mut set = KubericSet::new(
            "kvstore2",
            KubericSetSpec {
                replicas: 3,
                image: "kvstore2:test".to_string(),
                failover_delay_seconds: 10,
                switchover: None,
            },
        );
        set.metadata.annotations = Some(BTreeMap::from([(
            LIVE_TEST_COPY_GATE_ANNOTATION.to_string(),
            "enabled".to_string(),
        )]));
        let environment = replica_environment(&set);
        assert_eq!(
            environment
                .iter()
                .find(|variable| variable.name == "KUBERIC_LIVE_TEST_COPY_GATE_ADDRESS")
                .and_then(|variable| variable.value.as_deref()),
            Some(LIVE_TEST_COPY_GATE_ADDRESS)
        );
    }
}
