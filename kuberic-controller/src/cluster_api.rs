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
use kuberic_protocol::command::{
    EnsureConfiguration, EnsureReplicaBuild, InitializeAgentStore, ProtocolCommand,
};
use kuberic_protocol::observation::ReplicaObservationKey;
use kuberic_protocol::types::{
    AcceptedStatus, EffectivePolicy, PodUid, ProvisioningIntent, PvcUid, ReplicaId,
    ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid, TransitionKind,
    derive_agent_generation, derive_initialization_id, derive_replacement_resource_name,
    derive_replica_endpoint_name,
};
use kuberic_wire::proto;
use tokio::sync::Mutex;
use tonic::Code;

use crate::crd::{
    CONTROL_ADDRESS_ANNOTATION, CONTROLLER_NAME, INSTANCE_LABEL, KubericSet, KubericSetStatus,
    REPLICA_ID_LABEL, SET_UID_LABEL,
};
use crate::observation::{RawAgentObservation, RawObservation, RawObservationFailure};
use crate::{ControllerError, Result};

const CONTROL_PORT: i32 = 50051;
const REPLICATION_PORT: i32 = 50052;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectRecord {
    EnsureReplicaSupport,
    EnsureScaffolding(Vec<ReplicaId>),
    EnsureReplacement(ReplicaIdentity),
    DeleteScaffolding {
        pod_name: Option<String>,
        pvc_name: Option<String>,
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
            .map_err(classify_status)
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
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled => {
            AgentRpcError::Unavailable(status.to_string())
        }
        _ => AgentRpcError::Invalid(status.to_string()),
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
    async fn observe(&self, namespace: &str, name: &str) -> Result<RawObservation> {
        let sets: Api<KubericSet> = Api::namespaced(self.client.clone(), namespace);
        let set = sets
            .get(name)
            .await
            .map_err(|error| ControllerError::Observation(error.to_string()))?;
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
        let agent_requests = pods
            .iter()
            .filter_map(|pod| {
                let replica_id = pod
                    .labels()
                    .get(REPLICA_ID_LABEL)?
                    .parse::<i64>()
                    .ok()
                    .filter(|value| *value > 0)
                    .map(ReplicaId::new)?;
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
        let agents = join_all(agent_requests)
            .await
            .into_iter()
            .collect::<BTreeMap<_, _>>();

        Ok(RawObservation {
            set,
            pods,
            pvcs,
            services,
            secrets,
            agents,
            failures,
            now_unix_seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| ControllerError::Observation(error.to_string()))?
                .as_secs() as i64,
        })
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
                    &replica_pvc(&observation.set, *replica_id, &uid, &owner),
                )
                .await?;
                continue;
            };
            let pvc_uid = pvc.uid().ok_or(ControllerError::ObservationStale)?;
            if !observation
                .pods
                .iter()
                .any(|pod| pod.name_any() == pod_name)
            {
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
                pod.labels()
                    .get(REPLICA_ID_LABEL)
                    .is_some_and(|value| value == &replica_id.to_string())
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
        let response = self
            .agents
            .execute(
                &endpoint,
                &self.bearer_token,
                command_request(resource_uid, target, command.clone()),
            )
            .await
            .map_err(map_agent_effect_error)?;
        if response.protocol_version != kuberic_protocol::PROTOCOL_VERSION {
            return Err(ControllerError::InvalidAgentEvidence(
                "Execute response uses an unsupported protocol version".to_string(),
            ));
        }
        let report = response.observation.ok_or_else(|| {
            ControllerError::InvalidAgentEvidence(
                "Execute response omitted its resulting observation".to_string(),
            )
        })?;
        kuberic_wire::validate_agent_status_report(&report)
            .map_err(|error| ControllerError::InvalidAgentEvidence(error.to_string()))
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
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
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
) -> PersistentVolumeClaim {
    replica_pvc_named(
        set,
        replica_id,
        uid,
        owner,
        &format!("{}-data", replica_name(set, replica_id)),
    )
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

fn replica_pod(
    set: &KubericSet,
    replica_id: ReplicaId,
    uid: &str,
    owner: &OwnerReference,
    pvc_name: &str,
    pvc_uid: &str,
    image: &str,
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
) -> Pod {
    let labels = base_labels(set, Some(replica_id), uid);
    let credential_name = agent_credential_name(set);
    Pod {
        metadata: kube::core::ObjectMeta {
            name: Some(pod_name.to_string()),
            labels: Some(labels),
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
                env: Some(vec![
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
                ]),
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
        kube::Error::Api(response) if response.code == 409 || response.code == 422 => {
            ControllerError::ObservationStale
        }
        other => ControllerError::Effect(other.to_string()),
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
    command: ProtocolCommand,
) -> proto::ExecuteCommandRequest {
    let command = match command {
        ProtocolCommand::InitializeAgentStore(command) => {
            proto::execute_command_request::Command::InitializeAgentStore(initialize_command(
                *command,
            ))
        }
        ProtocolCommand::EnsureConfiguration(command) => {
            proto::execute_command_request::Command::EnsureConfiguration(ensure_command(*command))
        }
        ProtocolCommand::EnsureReplicaBuild(command) => {
            proto::execute_command_request::Command::EnsureReplicaBuild(ensure_build_command(
                *command,
            ))
        }
    };
    proto::ExecuteCommandRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid,
        target: Some(target.into()),
        command: Some(command),
    }
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
        grant_write: command.primary_write_status == kuberic_protocol::types::AccessStatus::Granted,
        current_only: command.current_only,
        retire_build_id: command
            .retire_build_ids
            .first()
            .map_or_else(String::new, ToString::to_string),
        primary_write_status: match command.primary_write_status {
            kuberic_protocol::types::AccessStatus::Granted => proto::AccessStatus::Granted as i32,
            kuberic_protocol::types::AccessStatus::ReconfigurationPending => {
                proto::AccessStatus::ReconfigurationPending as i32
            }
            kuberic_protocol::types::AccessStatus::NotPrimary => {
                proto::AccessStatus::NotPrimary as i32
            }
            kuberic_protocol::types::AccessStatus::NoWriteQuorum => {
                proto::AccessStatus::NoWriteQuorum as i32
            }
        },
        retire_build_ids: command
            .retire_build_ids
            .iter()
            .map(ToString::to_string)
            .collect(),
        failover_safe_lsn: command.failover_safe_lsn,
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
    }
}

fn provisioning(provisioning: ProvisioningIntent) -> proto::ProvisioningIntent {
    proto::ProvisioningIntent {
        replaces: Some(provisioning.replaces.into()),
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
        Ok(state.observation.clone())
    }

    async fn ensure_replica_scaffolding(
        &self,
        _observation: &RawObservation,
        replica_ids: &[ReplicaId],
    ) -> Result<()> {
        self.state
            .lock()
            .await
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
        _observation: &RawObservation,
        _replica_id: ReplicaId,
        replacing: &ReplicaIdentity,
    ) -> Result<()> {
        self.state
            .lock()
            .await
            .effects
            .push(EffectRecord::EnsureReplacement(replacing.clone()));
        Ok(())
    }

    async fn delete_replica_scaffolding(
        &self,
        _observation: &RawObservation,
        pod_name: Option<&str>,
        _pod_uid: Option<&PodUid>,
        pvc_name: Option<&str>,
        _pvc_uid: Option<&PvcUid>,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
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
        _observation: &RawObservation,
        command: &ProtocolCommand,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
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
