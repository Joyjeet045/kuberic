use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use futures::StreamExt;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod, Secret, Service};
use kube::Api;
use kube::ResourceExt;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kuberic_controller::cluster_api::{GrpcAgentApi, KubeClusterApi};
use kuberic_controller::crd::KubericSet;
use kuberic_controller::reconciler::Reconciler;
use kuberic_protocol::evaluator::EvaluationConfig;

#[derive(Debug, Parser)]
struct Config {
    #[arg(long, env = "KUBERIC_AGENT_BEARER_TOKEN")]
    agent_bearer_token: String,
    #[arg(long, env = "KUBERIC_STABLE_RESYNC_SECONDS", default_value_t = 30)]
    stable_resync_seconds: u64,
    #[arg(long, env = "KUBERIC_WAIT_REQUEUE_SECONDS", default_value_t = 5)]
    wait_requeue_seconds: u64,
    #[arg(long, env = "KUBERIC_UNSAFE_REQUEUE_SECONDS", default_value_t = 30)]
    unsafe_requeue_seconds: u64,
    #[arg(long, env = "KUBERIC_RPC_DEADLINE_SECONDS", default_value_t = 5)]
    rpc_deadline_seconds: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let config = Config::parse();
    let client = kube::Client::try_default().await?;
    let agents = Arc::new(GrpcAgentApi::new(Duration::from_secs(
        config.rpc_deadline_seconds,
    )));
    let api = Arc::new(KubeClusterApi::new(
        client.clone(),
        agents,
        config.agent_bearer_token,
    )?);
    let reconciler = Arc::new(Reconciler::new(
        api,
        EvaluationConfig {
            supported_protocol_version: kuberic_protocol::PROTOCOL_VERSION,
            stable_resync_seconds: config.stable_resync_seconds,
            wait_requeue_seconds: config.wait_requeue_seconds,
            unsafe_requeue_seconds: config.unsafe_requeue_seconds,
        },
    ));
    let sets = Api::<KubericSet>::all(client.clone());
    let pods = Api::<Pod>::all(client.clone());
    let pvcs = Api::<PersistentVolumeClaim>::all(client.clone());
    let services = Api::<Service>::all(client.clone());
    let secrets = Api::<Secret>::all(client);
    Controller::new(sets, watcher::Config::default())
        .owns(pods, watcher::Config::default())
        .owns(pvcs, watcher::Config::default())
        .owns(services, watcher::Config::default())
        .owns(secrets, watcher::Config::default())
        .run(
            move |set: Arc<KubericSet>, _| {
                let reconciler = reconciler.clone();
                async move {
                    let namespace = set.namespace().ok_or_else(|| {
                        kuberic_controller::ControllerError::Observation(
                            "KubericSet has no namespace".to_string(),
                        )
                    })?;
                    let action = reconciler.reconcile(&namespace, &set.name_any()).await?;
                    Ok::<Action, kuberic_controller::ControllerError>(Action::requeue(
                        action.requeue_after,
                    ))
                }
            },
            |_set, error, _| {
                tracing::warn!(%error, "controller reconcile failed");
                Action::requeue(Duration::from_secs(10))
            },
            Arc::new(()),
        )
        .for_each(|result| async move {
            if let Err(error) = result {
                tracing::warn!(%error, "controller stream error");
            }
        })
        .await;
    Ok(())
}
