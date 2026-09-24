use std::time::Duration;

use kuberic_protocol::command::{KubernetesChange, SafetyChange};
use kuberic_protocol::observation::ObservationSnapshot;
use kuberic_protocol::plan::Plan;

use crate::Result;
use crate::cluster_api::ClusterApi;
use crate::observation::RawObservation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionKind {
    Stable,
    Applied,
    Executed,
    Waiting,
    Unsafe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionOutcome {
    pub kind: ExecutionKind,
    pub requeue_after: Duration,
}

pub async fn execute_plan(
    api: &dyn ClusterApi,
    observation: &RawObservation,
    snapshot: &ObservationSnapshot,
    plan: Plan,
) -> Result<ExecutionOutcome> {
    match plan {
        Plan::Stable {
            status,
            requeue_after_seconds,
        } => {
            persist_if_changed(api, observation, snapshot, &status).await?;
            Ok(ExecutionOutcome {
                kind: ExecutionKind::Stable,
                requeue_after: Duration::from_secs(requeue_after_seconds),
            })
        }
        Plan::Apply { changes } => {
            for change in changes {
                execute_change(api, observation, change).await?;
            }
            Ok(ExecutionOutcome {
                kind: ExecutionKind::Applied,
                requeue_after: Duration::ZERO,
            })
        }
        Plan::Execute { command } => {
            api.execute_command(observation, &command).await?;
            Ok(ExecutionOutcome {
                kind: ExecutionKind::Executed,
                requeue_after: Duration::ZERO,
            })
        }
        Plan::Wait {
            status,
            requeue_after_seconds,
            ..
        } => {
            persist_if_changed(api, observation, snapshot, &status).await?;
            Ok(ExecutionOutcome {
                kind: ExecutionKind::Waiting,
                requeue_after: Duration::from_secs(requeue_after_seconds),
            })
        }
        Plan::Unsafe {
            status,
            safety_changes,
            requeue_after_seconds,
            ..
        } => {
            for change in safety_changes {
                match change {
                    SafetyChange::RemoveWriteRouting => {
                        api.remove_write_routing(observation).await?;
                    }
                }
            }
            persist_if_changed(api, observation, snapshot, &status).await?;
            Ok(ExecutionOutcome {
                kind: ExecutionKind::Unsafe,
                requeue_after: Duration::from_secs(requeue_after_seconds),
            })
        }
    }
}

async fn execute_change(
    api: &dyn ClusterApi,
    observation: &RawObservation,
    change: KubernetesChange,
) -> Result<()> {
    match change {
        KubernetesChange::EnsureReplicaSupport => api.ensure_replica_support(observation).await,
        KubernetesChange::EnsureReplicaScaffolding { replica_ids } => {
            api.ensure_replica_scaffolding(observation, &replica_ids)
                .await
        }
        KubernetesChange::EnsureReplacementScaffolding {
            replica_id,
            replacing,
        } => {
            api.ensure_replacement_scaffolding(observation, replica_id, &replacing)
                .await
        }
        KubernetesChange::DeleteReplicaScaffolding {
            pod_name,
            pod_uid,
            pvc_name,
            pvc_uid,
        } => {
            api.delete_replica_scaffolding(
                observation,
                pod_name.as_deref(),
                pod_uid.as_ref(),
                pvc_name.as_deref(),
                pvc_uid.as_ref(),
            )
            .await
        }
        KubernetesChange::DeleteReplicaEndpoint { identity } => {
            api.delete_replica_endpoint(observation, &identity).await
        }
        KubernetesChange::EnsureWriteRoutingService => {
            api.ensure_write_routing_service(observation).await
        }
        KubernetesChange::PersistStatus { status } => {
            api.replace_status(observation, &status).await
        }
        KubernetesChange::RemoveWriteRouting => api.remove_write_routing(observation).await,
        KubernetesChange::PublishWriteRouting { primary } => {
            api.publish_write_routing(observation, &primary).await
        }
    }
}

async fn persist_if_changed(
    api: &dyn ClusterApi,
    observation: &RawObservation,
    snapshot: &ObservationSnapshot,
    status: &kuberic_protocol::types::AcceptedStatus,
) -> Result<()> {
    if &snapshot.status != status {
        api.replace_status(observation, status).await?;
    }
    Ok(())
}
