use std::collections::BTreeMap;

use kuberic_runtime::protocol::observation::{
    DesiredState, ObservationFailure, ObservationSnapshot, ReplicaObservation,
    ReplicaObservationKey, ReportWatermark, RoutingObservation,
    SecondaryScaleDownResourceObservation,
};
use kuberic_runtime::protocol::types::{AcceptedStatus, ResourceUid};
use serde::Deserialize;

use super::EvaluationConfig;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotBridge {
    resource_uid: ResourceUid,
    resource_version: String,
    desired: DesiredState,
    status: AcceptedStatus,
    replicas: Vec<(ReplicaObservationKey, ReplicaObservation)>,
    #[serde(default)]
    secondary_scale_down_resources: Vec<SecondaryScaleDownResourceObservation>,
    previous_report_watermarks: Vec<(ReplicaObservationKey, ReportWatermark)>,
    durable_storage_evidence: bool,
    supporting_resources_ready: bool,
    routing: RoutingObservation,
    observation_failures: Vec<ObservationFailure>,
    now_unix_seconds: i64,
}

impl From<SnapshotBridge> for ObservationSnapshot {
    fn from(snapshot: SnapshotBridge) -> Self {
        Self {
            resource_uid: snapshot.resource_uid,
            resource_version: snapshot.resource_version,
            desired: snapshot.desired,
            status: snapshot.status,
            replicas: snapshot.replicas.into_iter().collect::<BTreeMap<_, _>>(),
            secondary_scale_down_resources: snapshot.secondary_scale_down_resources,
            previous_report_watermarks: snapshot
                .previous_report_watermarks
                .into_iter()
                .collect::<BTreeMap<_, _>>(),
            durable_storage_evidence: snapshot.durable_storage_evidence,
            supporting_resources_ready: snapshot.supporting_resources_ready,
            routing: snapshot.routing,
            observation_failures: snapshot.observation_failures,
            now_unix_seconds: snapshot.now_unix_seconds,
        }
    }
}

pub fn evaluate_json(snapshot: &[u8], config: &EvaluationConfig) -> serde_json::Result<Vec<u8>> {
    let snapshot: SnapshotBridge = serde_json::from_slice(snapshot)?;
    serde_json::to_vec(&super::evaluate(&snapshot.into(), config))
}
