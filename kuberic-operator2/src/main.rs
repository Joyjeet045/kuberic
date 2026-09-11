use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::StreamExt;
use kube::{
    Api, Client, ResourceExt,
    api::{Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        watcher,
    },
};
use kuberic_dex::{
    ActivityInvocationError, ActivityRunner, CheckpointLimits, DurableHost, HostEpoch, HostOutcome,
    KubernetesCheckpointStore, decode_workflow_result,
};
use kuberic_operator2::{
    DurableWorkflowPhase, DurableWorkflowStatus, KubericSet, RECONCILE_WORKFLOW_NAME,
    ReconcileResult, activity_registry, execution_id, execution_spec, orchestration_registry,
    reconcile_input,
};
use serde_json::json;
use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct OperatorError(String);

struct Context {
    api: Api<KubericSet>,
    orchestrations: kuberic_dex::OrchestrationRegistry,
    runner: Mutex<ActivityRunner<KubernetesCheckpointStore>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let namespace = std::env::var("KUBERIC_NAMESPACE").unwrap_or_else(|_| "default".to_string());
    let client = Client::try_default().await?;
    let api = Api::<KubericSet>::namespaced(client.clone(), &namespace);
    let store = KubernetesCheckpointStore::new(client, namespace.clone())?;
    let activities = activity_registry(api.clone())?;
    let host_epoch = HostEpoch::from_bytes(*Uuid::new_v4().as_bytes());
    let host = DurableHost::new(
        store,
        host_epoch,
        CheckpointLimits::new(4, 64 * 1024, 64 * 1024)?,
    );
    let context = Arc::new(Context {
        api: api.clone(),
        orchestrations: orchestration_registry()?,
        runner: Mutex::new(ActivityRunner::new(host, activities)),
    });

    info!(
        namespace,
        api_group = "dex.kuberic.io",
        "starting kuberic-operator2"
    );

    Controller::new(api, watcher::Config::default())
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok(object) => info!(?object, "reconciled KubericSet"),
                Err(error) => warn!(?error, "KubericSet reconciliation failed"),
            }
        })
        .await;

    Err("KubericSet controller stream ended unexpectedly".into())
}

async fn reconcile(set: Arc<KubericSet>, context: Arc<Context>) -> Result<Action, OperatorError> {
    let input = reconcile_input(&set).map_err(operator_error)?;
    let execution = execution_spec(&input).map_err(operator_error)?;
    let execution_id = execution_id(&input);
    let workflow = context
        .orchestrations
        .get(RECONCILE_WORKFLOW_NAME)
        .map_err(operator_error)?;
    let now = unix_millis()?;
    let outcome = context
        .runner
        .lock()
        .await
        .run_once(workflow, execution, now)
        .await;

    match outcome {
        HostOutcome::ScheduleAccepted { .. }
        | HostOutcome::ObservationAccepted { .. }
        | HostOutcome::ReloadRequired { .. } => {
            patch_workflow_status(
                &context.api,
                &set,
                execution_id,
                DurableWorkflowPhase::Running,
            )
            .await?;
            Ok(Action::requeue(Duration::ZERO))
        }
        HostOutcome::RetryScheduled {
            retry_not_before_unix_millis,
            ..
        }
        | HostOutcome::Waiting {
            wake_at_unix_millis: retry_not_before_unix_millis,
            ..
        } => {
            patch_workflow_status(
                &context.api,
                &set,
                execution_id,
                DurableWorkflowPhase::Running,
            )
            .await?;
            Ok(Action::requeue(delay_until(
                now,
                retry_not_before_unix_millis,
            )))
        }
        HostOutcome::WorkflowCompleted { outcome, .. } => {
            let result =
                decode_workflow_result::<ReconcileResult, ActivityInvocationError>(&outcome)
                    .map_err(operator_error)?
                    .map_err(operator_error)?;
            if result.generation != input.generation {
                return Err(OperatorError(
                    "completed workflow returned the wrong generation".to_string(),
                ));
            }
            patch_workflow_status(
                &context.api,
                &set,
                execution_id,
                DurableWorkflowPhase::Completed,
            )
            .await?;
            Ok(Action::await_change())
        }
        HostOutcome::Quarantined { .. } => {
            patch_workflow_status(
                &context.api,
                &set,
                execution_id,
                DurableWorkflowPhase::Quarantined,
            )
            .await?;
            Ok(Action::await_change())
        }
        failure => Err(OperatorError(format!("DEX host failed: {failure:?}"))),
    }
}

fn error_policy(_set: Arc<KubericSet>, error: &OperatorError, _context: Arc<Context>) -> Action {
    warn!(?error, "kuberic-operator2 controller error");
    Action::requeue(Duration::from_secs(10))
}

async fn patch_workflow_status(
    api: &Api<KubericSet>,
    set: &KubericSet,
    execution_id: kuberic_dex::ExecutionId,
    phase: DurableWorkflowPhase,
) -> Result<(), OperatorError> {
    let name = set.name_any();
    let generation = set
        .metadata
        .generation
        .ok_or_else(|| OperatorError("KubericSet has no generation".to_string()))?;
    let workflow = DurableWorkflowStatus {
        workflow_name: RECONCILE_WORKFLOW_NAME.to_string(),
        execution_id: execution_id.to_string(),
        checkpoint_name: KubernetesCheckpointStore::object_name(execution_id),
        observed_generation: generation,
        phase,
    };
    if set
        .status
        .as_ref()
        .and_then(|status| status.workflow.as_ref())
        == Some(&workflow)
    {
        return Ok(());
    }
    api.patch_status(
        &name,
        &PatchParams::default(),
        &Patch::Merge(json!({ "status": { "workflow": workflow } })),
    )
    .await
    .map(|_| ())
    .map_err(operator_error)
}

fn delay_until(now_unix_millis: i64, wake_at_unix_millis: i64) -> Duration {
    Duration::from_millis(wake_at_unix_millis.saturating_sub(now_unix_millis) as u64)
}

fn unix_millis() -> Result<i64, OperatorError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(operator_error)?;
    i64::try_from(duration.as_millis()).map_err(operator_error)
}

fn operator_error(error: impl std::fmt::Display) -> OperatorError {
    OperatorError(error.to_string())
}
