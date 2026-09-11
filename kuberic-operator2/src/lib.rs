use kube::{Api, CustomResource, ResourceExt};
use kuberic_dex::{
    ActivityContext, ActivityHandlerError, ActivityInvocationError, ActivityRegistry, ExactBytes,
    ExecutionId, ExecutionSpec, OrchestrationContext, OrchestrationRegistry,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const RECONCILE_WORKFLOW_NAME: &str = "ReconcileKubericSetV2";
pub const OBSERVE_ACTIVITY_NAME: &str = "ObserveKubericSet";
pub const MAX_TERMINAL_PAYLOAD_BYTES: u64 = 1024;

const EXECUTION_NAMESPACE: Uuid = Uuid::from_bytes([
    0x81, 0x41, 0x9a, 0x80, 0x2f, 0x3f, 0x4d, 0xcb, 0x9f, 0x22, 0xe4, 0x2f, 0x0a, 0x4c, 0x9b, 0xd8,
]);

#[derive(CustomResource, Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "dex.kuberic.io",
    version = "v1alpha1",
    kind = "KubericSet",
    plural = "kubericsets",
    shortname = "kls2",
    namespaced,
    status = "KubericSetStatus",
    printcolumn = r#"{"name":"Replicas","type":"integer","jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Workflow","type":"string","jsonPath":".status.workflow.phase"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct KubericSetSpec {
    #[serde(default = "default_replicas")]
    pub replicas: i32,
    pub image: String,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KubericSetStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow: Option<DurableWorkflowStatus>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DurableWorkflowStatus {
    pub workflow_name: String,
    pub execution_id: String,
    pub checkpoint_name: String,
    pub observed_generation: i64,
    pub phase: DurableWorkflowPhase,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DurableWorkflowPhase {
    Running,
    Completed,
    Failed,
    Quarantined,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReconcileInput {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub generation: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReconcileResult {
    pub generation: i64,
    pub replicas: i32,
}

pub fn reconcile_input(set: &KubericSet) -> Result<ReconcileInput, &'static str> {
    Ok(ReconcileInput {
        namespace: set.namespace().ok_or("KubericSet has no namespace")?,
        name: set.name_any(),
        uid: set.uid().ok_or("KubericSet has no UID")?,
        generation: set
            .metadata
            .generation
            .ok_or("KubericSet has no generation")?,
    })
}

pub fn execution_id(input: &ReconcileInput) -> ExecutionId {
    let identity = format!(
        "{}:{}:{}",
        input.uid, input.generation, RECONCILE_WORKFLOW_NAME
    );
    ExecutionId::from_bytes(*Uuid::new_v5(&EXECUTION_NAMESPACE, identity.as_bytes()).as_bytes())
}

pub fn execution_spec(
    input: &ReconcileInput,
) -> Result<ExecutionSpec, kuberic_dex::WorkflowCodecError> {
    ExecutionSpec::typed(execution_id(input), input, MAX_TERMINAL_PAYLOAD_BYTES)
}

pub fn orchestration_registry()
-> Result<OrchestrationRegistry, kuberic_dex::OrchestrationRegistryError> {
    OrchestrationRegistry::builder()
        .register_typed::<ReconcileInput, ReconcileResult, ActivityInvocationError, _, _>(
            RECONCILE_WORKFLOW_NAME,
            |context: OrchestrationContext, input| async move {
                context
                    .schedule_activity_typed::<ReconcileInput, ReconcileResult>(
                        OBSERVE_ACTIVITY_NAME,
                        &input,
                    )
                    .await
            },
        )
        .build()
}

pub fn activity_registry(
    api: Api<KubericSet>,
) -> Result<ActivityRegistry, kuberic_dex::ActivityRegistryError> {
    ActivityRegistry::builder()
        .register_typed::<ReconcileInput, ReconcileResult, _, _>(
            OBSERVE_ACTIVITY_NAME,
            move |_context: ActivityContext, input| {
                let api = api.clone();
                async move {
                    let set = api.get(&input.name).await.map_err(|_| {
                        ActivityHandlerError::Retryable(ExactBytes::new(b"kubernetes_get_failed"))
                    })?;
                    if set.uid().as_deref() != Some(input.uid.as_str())
                        || set.metadata.generation != Some(input.generation)
                    {
                        return Err(ActivityHandlerError::Terminal(ExactBytes::new(
                            b"kubericset_identity_changed",
                        )));
                    }
                    Ok(ReconcileResult {
                        generation: input.generation,
                        replicas: set.spec.replicas,
                    })
                }
            },
        )
        .build()
}

const fn default_replicas() -> i32 {
    3
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use kube::{CustomResourceExt, core::ObjectMeta};
    use kuberic_dex::{
        ActivityRegistry, ActivityRunner, CheckpointLimits, DurableHost, HostEpoch, HostOutcome,
        InMemoryCheckpointStore, decode_workflow_result,
    };

    use super::*;

    fn set(generation: i64) -> KubericSet {
        KubericSet {
            metadata: ObjectMeta {
                name: Some("demo".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("2e319e5b-9d29-4580-9ca0-66d0c1438560".to_string()),
                generation: Some(generation),
                ..Default::default()
            },
            spec: KubericSetSpec {
                replicas: 3,
                image: "example:latest".to_string(),
            },
            status: None,
        }
    }

    #[test]
    fn crd_is_independent_from_the_classic_api() {
        let crd = serde_json::to_value(KubericSet::crd()).unwrap();
        assert_eq!(
            crd.pointer("/metadata/name"),
            Some(&serde_json::json!("kubericsets.dex.kuberic.io"))
        );
        assert_eq!(
            crd.pointer("/spec/group"),
            Some(&serde_json::json!("dex.kuberic.io"))
        );
        assert_eq!(
            crd.pointer("/spec/versions/0/name"),
            Some(&serde_json::json!("v1alpha1"))
        );

        let deployment = include_str!("../deploy/deployment.yaml");
        assert!(deployment.contains("name: kubericsets.dex.kuberic.io"));
        assert!(deployment.contains("apiGroups: [\"dex.kuberic.io\"]"));
        assert!(!deployment.contains("apiGroups: [\"kuberic.io\"]"));
    }

    #[test]
    fn execution_identity_is_stable_and_generation_specific() {
        let first = reconcile_input(&set(1)).unwrap();
        let same = reconcile_input(&set(1)).unwrap();
        let changed = reconcile_input(&set(2)).unwrap();

        assert_eq!(execution_id(&first), execution_id(&same));
        assert_ne!(execution_id(&first), execution_id(&changed));
    }

    #[test]
    fn minimal_workflow_replays_one_typed_activity_to_completion() {
        let input = reconcile_input(&set(1)).unwrap();
        let execution = execution_spec(&input).unwrap();
        let orchestrations = orchestration_registry().unwrap();
        let workflow = orchestrations.get(RECONCILE_WORKFLOW_NAME).unwrap();
        let expected = ReconcileResult {
            generation: input.generation,
            replicas: 3,
        };
        let activities = ActivityRegistry::builder()
            .register_typed::<ReconcileInput, ReconcileResult, _, _>(
                OBSERVE_ACTIVITY_NAME,
                move |_context, _input| {
                    let result = expected.clone();
                    async move { Ok(result) }
                },
            )
            .build()
            .unwrap();
        let host = DurableHost::new(
            InMemoryCheckpointStore::new(),
            HostEpoch::from_bytes([7; 16]),
            CheckpointLimits::new(4, 64 * 1024, 64 * 1024).unwrap(),
        );
        let mut runner = ActivityRunner::new(host, activities);

        let mut completed = None;
        for now in 0..4 {
            let outcome = block_on(runner.run_once(workflow, execution.clone(), now));
            if let HostOutcome::WorkflowCompleted { outcome, .. } = outcome {
                completed = Some(outcome);
                break;
            }
        }

        let outcome = completed.expect("workflow must complete");
        assert_eq!(
            decode_workflow_result::<ReconcileResult, ActivityInvocationError>(&outcome).unwrap(),
            Ok(ReconcileResult {
                generation: 1,
                replicas: 3,
            })
        );
    }
}
