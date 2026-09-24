use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod, Secret, Service};
use kuberic_protocol::observation::ReplicaObservationKey;
use kuberic_wire::proto;

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
    pub failures: Vec<RawObservationFailure>,
    pub now_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawObservationFailure {
    pub source: String,
    pub message: String,
}
