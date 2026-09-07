use std::sync::Arc;

use futures::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kube::{Api, Client};
use tracing::info;

use kuberic_operator::cluster_api::KubeClusterApi;
use kuberic_operator::crd::KubericSet;
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    info!("Starting kuberic-operator");

    let client = Client::try_default().await?;

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
        let maintenance_api = Arc::new(KubeMaintenanceApi {
            client: maintenance_client,
        });

        Controller::new(requests, watcher::Config::default())
            .run(
                |request: Arc<NodeMaintenanceRequest>, api: Arc<KubeMaintenanceApi>| async move {
                    if request.metadata.deletion_timestamp.is_some() {
                        return Ok(Action::await_change());
                    }
                    let name = request
                        .metadata
                        .name
                        .clone()
                        .ok_or_else(|| OperatorError("request has no name".to_string()))?;
                    let previous = request.status.clone().unwrap_or_default();

                    reconcile_request(
                        api.as_ref(),
                        RequestContext {
                            name: &name,
                            spec: &request.spec,
                            generation: request.metadata.generation,
                            previous: &previous,
                            now: k8s_openapi::jiff::Timestamp::now(),
                        },
                    )
                    .await
                    .map(|outcome| {
                        if outcome.persisted {
                            info!(
                                request = %name,
                                phase = ?outcome.status.phase,
                                "node maintenance status updated"
                            );
                        }
                        Action::requeue(std::time::Duration::from_secs(30))
                    })
                    .map_err(OperatorError)
                },
                |_request: Arc<NodeMaintenanceRequest>, error, _api: Arc<KubeMaintenanceApi>| {
                    tracing::warn!(?error, "node maintenance controller error");
                    Action::requeue(std::time::Duration::from_secs(10))
                },
                maintenance_api,
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
    };

    tracing::error!(controller = exited, "controller stream ended unexpectedly");
    Err(format!("{exited} controller exited").into())
}
