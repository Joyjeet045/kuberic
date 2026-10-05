//! Kubernetes control plane and pure level-triggered reconciliation policy.
//!
//! Canonical command, observation, validation, and transport contracts are
//! owned by `kuberic_runtime::protocol` and `kuberic_runtime::control`.
//! They are re-exported here so controller integrations need only this crate.

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use futures::StreamExt;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod, Secret, Service};
use kube::Api;
use kube::ResourceExt;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;

pub mod cluster_api;
pub mod crd;
mod error;
pub mod evaluator;
mod exact_resources;
pub mod executor;
pub mod normalize;
pub mod observation;
pub mod plan;
pub mod reconciler;

pub use error::{ControllerError, Result};
pub use evaluator::{EvaluationConfig, evaluate};
pub use kuberic_runtime::{control, protocol};
pub use plan::{Plan, UnsafeReason, WaitReason};

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

/// Runs the production controller with configuration parsed from command-line
/// arguments and environment variables.
///
/// ```no_run
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     kuberic_controller::default_main().await
/// }
/// ```
///
/// The caller owns the Tokio runtime, so this can also run inside an existing
/// asynchronous application:
///
/// ```no_run
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let runtime = tokio::runtime::Runtime::new()?;
///     runtime.block_on(kuberic_controller::default_main())
/// }
/// ```
pub async fn default_main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let config = Config::parse();
    let client = kube::Client::try_default().await?;
    let agents = Arc::new(cluster_api::GrpcAgentApi::new(Duration::from_secs(
        config.rpc_deadline_seconds,
    )));
    let api = Arc::new(cluster_api::KubeClusterApi::new(
        client.clone(),
        agents,
        config.agent_bearer_token,
    )?);
    let reconciler = Arc::new(reconciler::Reconciler::new(
        api,
        production_evaluation_config(
            config.stable_resync_seconds,
            config.wait_requeue_seconds,
            config.unsafe_requeue_seconds,
        ),
    ));
    let sets = Api::<crd::KubericSet>::all(client.clone());
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
            move |set: Arc<crd::KubericSet>, _| {
                let reconciler = reconciler.clone();
                async move {
                    let namespace = set.namespace().ok_or_else(|| {
                        ControllerError::Observation("KubericSet has no namespace".to_string())
                    })?;
                    let action = reconciler.reconcile(&namespace, &set.name_any()).await?;
                    Ok::<Action, ControllerError>(Action::requeue(action.requeue_after))
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

pub fn production_evaluation_config(
    stable_resync_seconds: u64,
    wait_requeue_seconds: u64,
    unsafe_requeue_seconds: u64,
) -> EvaluationConfig {
    EvaluationConfig {
        enable_secondary_scale_down: true,
        allow_scale_up: true,
        supported_protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
        stable_resync_seconds,
        wait_requeue_seconds,
        unsafe_requeue_seconds,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn production_enables_only_completed_membership_gates() {
        let config = super::production_evaluation_config(31, 7, 13);
        assert!(config.enable_secondary_scale_down);
        assert!(config.allow_scale_up);
        assert_eq!(
            config.supported_protocol_version,
            kuberic_runtime::protocol::PROTOCOL_VERSION
        );
        assert_eq!(config.stable_resync_seconds, 31);
        assert_eq!(config.wait_requeue_seconds, 7);
        assert_eq!(config.unsafe_requeue_seconds, 13);
    }
}
