use std::sync::Arc;

use futures::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::events::{Recorder, Reporter};
use kube::runtime::watcher;
use kube::{Api, Client, Resource};
use tracing::info;

use kuberic_operator::cluster_api::KubeClusterApi;
use kuberic_operator::crd::KubericSet;
use kuberic_operator::node_maintenance::observability::MaintenanceMetrics;
use kuberic_operator::node_maintenance::{
    KubeMaintenanceApi, NodeMaintenanceRequest, RequestContext, reconcile_request,
};
use kuberic_operator::reconciler::{ReconcileAction, ReconcilerState};

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct OperatorError(String);

struct Context {
    api: KubeClusterApi,
    state: ReconcilerState,
}

struct MaintenanceContext {
    api: KubeMaintenanceApi,
    events: Recorder,
    metrics: MaintenanceMetrics,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    info!("Starting kuberic-operator");

    let client = Client::try_default().await?;
    let metrics = MaintenanceMetrics::new()?;
    let metrics_address =
        std::env::var("KUBERIC_METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:8081".to_string());
    let metrics_listener = tokio::net::TcpListener::bind(&metrics_address).await?;
    let metrics_router = metrics.router();
    let metrics_server = async move {
        if let Err(error) = axum::serve(metrics_listener, metrics_router).await {
            tracing::error!(%error, "metrics server failed");
        }
    };
    info!(address = %metrics_address, "serving operator metrics");

    let sets: Api<KubericSet> = Api::all(client.clone());
    let pods: Api<Pod> = Api::all(client.clone());

    let ctx = Arc::new(Context {
        api: KubeClusterApi {
            client: client.clone(),
        },
        state: ReconcilerState::default(),
    });

    info!("Watching KubericSets");

    let maintenance_client = client.clone();
    let maintenance = async move {
        let requests: Api<NodeMaintenanceRequest> = Api::all(maintenance_client.clone());
        let maintenance_context = Arc::new(MaintenanceContext {
            events: Recorder::new(
                maintenance_client.clone(),
                Reporter {
                    controller: "kuberic.io/node-maintenance".to_string(),
                    instance: std::env::var("POD_NAME").ok(),
                },
            ),
            metrics,
            api: KubeMaintenanceApi {
                client: maintenance_client,
            },
        });

        Controller::new(requests, watcher::Config::default())
            .run(
                |request: Arc<NodeMaintenanceRequest>, context: Arc<MaintenanceContext>| async move {
                    let name = request
                        .metadata
                        .name
                        .clone()
                        .ok_or_else(|| OperatorError("request has no name".to_string()))?;
                    let previous = request.status.clone().unwrap_or_default();

                    let outcome = reconcile_request(
                        &context.api,
                        RequestContext {
                            name: &name,
                            uid: request.metadata.uid.as_deref().ok_or_else(|| OperatorError("request has no UID".to_string()))?,
                            resource_version: request.metadata.resource_version.as_deref().ok_or_else(|| OperatorError("request has no resource version".to_string()))?,
                            deleting: request.metadata.deletion_timestamp.is_some(),
                            spec: &request.spec,
                            generation: request.metadata.generation,
                            previous: &previous,
                            now: k8s_openapi::jiff::Timestamp::now(),
                        },
                    )
                    .await
                    .map_err(OperatorError)?;
                    if let Some(event) = context.metrics.observe(&request.spec, &previous, &outcome) {
                        if let Err(error) = context.events.publish(&event, &request.object_ref(&())).await {
                            context.metrics.event_error();
                            tracing::warn!(request = %name, %error, "maintenance Event publication failed");
                        }
                    }
                    if outcome.persisted {
                        info!(request = %name, phase = ?outcome.status.phase, "node maintenance status updated");
                    }
                    Ok(if outcome.status.phase.is_terminal() {
                        Action::await_change()
                    } else {
                        Action::requeue(std::time::Duration::from_secs(30))
                    })
                },
                |_request: Arc<NodeMaintenanceRequest>, error: &OperatorError, context: Arc<MaintenanceContext>| {
                    context.metrics.reconcile_error();
                    tracing::warn!(?error, "node maintenance controller error");
                    Action::requeue(std::time::Duration::from_secs(10))
                },
                maintenance_context,
            )
            .for_each(|res| async move {
                match res {
                    Ok(o) => info!("reconciled maintenance request {:?}", o),
                    Err(e) => tracing::warn!("maintenance reconcile failed: {}", e),
                }
            })
            .await;
    };

    info!("Watching NodeMaintenanceRequests");

    let sets_controller = async move {
        Controller::new(sets, watcher::Config::default())
            .owns(pods, watcher::Config::default())
            .run(
                |set: Arc<KubericSet>, ctx: Arc<Context>| async move {
                    match kuberic_operator::reconciler::reconcile_set(&set, &ctx.api, &ctx.state)
                        .await
                    {
                        Ok(ReconcileAction::Requeue(d)) => Ok(Action::requeue(d)),
                        Err(e) => Err(OperatorError(e)),
                    }
                },
                |_set: Arc<KubericSet>, error, _ctx: Arc<Context>| {
                    tracing::warn!(?error, "controller error");
                    Action::requeue(std::time::Duration::from_secs(10))
                },
                ctx,
            )
            .for_each(|res| async move {
                match res {
                    Ok(o) => info!("reconciled {:?}", o),
                    Err(e) => tracing::warn!("reconcile failed: {}", e),
                }
            })
            .await;
    };

    let exited = tokio::select! {
        _ = maintenance => "node maintenance",
        _ = sets_controller => "kubericset",
        _ = metrics_server => "metrics",
    };

    tracing::error!(controller = exited, "controller stream ended unexpectedly");
    Err(format!("{exited} controller exited").into())
}
