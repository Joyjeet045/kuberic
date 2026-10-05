use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod, Secret, Service};
use kuberic_runtime::control::proto;
use kuberic_runtime::protocol::observation::ReplicaObservationKey;
use kuberic_runtime::protocol::types::{ReplicaCleanupIdentity, ReplicaIdentity};

use crate::crd::KubericSet;

#[derive(Debug, Clone)]
pub enum RawAgentObservation {
    Absent,
    Unavailable { message: String },
    Invalid { message: String },
    Report(Box<proto::AgentStatusReport>),
}

#[derive(Debug, Clone)]
pub struct RawObservation {
    pub set: KubericSet,
    pub pods: Vec<Pod>,
    pub pvcs: Vec<PersistentVolumeClaim>,
    pub services: Vec<Service>,
    pub secrets: Vec<Secret>,
    pub agents: BTreeMap<ReplicaObservationKey, RawAgentObservation>,
    pub exact_resources: Vec<RawScaleDownResources>,
    pub failures: Vec<RawObservationFailure>,
    pub now_unix_seconds: i64,
}

#[derive(Debug, Clone)]
pub enum ExactLookup<T> {
    Present(T),
    NotFound,
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct RawScaleDownResources {
    pub target: ReplicaIdentity,
    pub identity: ReplicaCleanupIdentity,
    pub pod: ExactLookup<Pod>,
    pub pvc: ExactLookup<PersistentVolumeClaim>,
    pub endpoint: ExactLookup<Service>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawObservationFailure {
    pub source: String,
    pub message: String,
}
