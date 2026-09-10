#![cfg(feature = "kubernetes")]

use std::{
    error::Error,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::TryStreamExt;
use k8s_openapi::{
    api::{
        authorization::v1::{
            ResourceAttributes, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec,
        },
        core::v1::{ConfigMap, Namespace},
    },
    apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference},
};
use kube::{
    Api, Client, ResourceExt,
    api::{DeleteParams, PostParams, WatchEvent, WatchParams},
    config::{KubeConfigOptions, Kubeconfig},
};

use kuberic_dex::{
    ActivityName, ActivityRecord, ActivitySequence, ActivitySpec, CasOutcome, CheckpointEnvelope,
    CheckpointLimits, CheckpointPayload, CheckpointStore, DurableHost, ExactBytes,
    ExecutionContract, ExecutionId, ExecutionSpec, HostEpoch, HostOutcome,
    KubernetesCheckpointOwner, KubernetesCheckpointOwnerScope, KubernetesCheckpointStore,
    KubernetesCheckpointStoreOptions, ReloadReason, StorageRevision, StoreError, StoreErrorKind,
    StoredCheckpoint, TerminalOutcome, Workflow, WorkflowContext,
};

type TestResult<T> = Result<T, Box<dyn Error>>;

const CHECKPOINT_LIMIT: usize = 1_000_000;

#[derive(Clone, Copy)]
enum MaskMode {
    ApplyThenUnknown,
    NoApplyUnknown,
}

#[derive(Clone)]
struct MaskOnceStore {
    inner: KubernetesCheckpointStore,
    mode: MaskMode,
    armed: Arc<AtomicBool>,
}

impl MaskOnceStore {
    fn new(inner: KubernetesCheckpointStore, mode: MaskMode) -> Self {
        Self {
            inner,
            mode,
            armed: Arc::new(AtomicBool::new(true)),
        }
    }
}

#[async_trait]
impl CheckpointStore for MaskOnceStore {
    async fn load(
        &self,
        execution_id: ExecutionId,
    ) -> Result<Option<StoredCheckpoint>, StoreError> {
        self.inner.load(execution_id).await
    }

    async fn compare_and_swap(
        &self,
        execution_id: ExecutionId,
        expected: Option<StorageRevision>,
        checkpoint: CheckpointEnvelope,
    ) -> Result<CasOutcome, StoreError> {
        if !self.armed.swap(false, Ordering::SeqCst) {
            return self
                .inner
                .compare_and_swap(execution_id, expected, checkpoint)
                .await;
        }
        match self.mode {
            MaskMode::ApplyThenUnknown => {
                let outcome = self
                    .inner
                    .compare_and_swap(execution_id, expected, checkpoint)
                    .await?;
                if !matches!(outcome, CasOutcome::Accepted(_)) {
                    return Err(StoreError::new(
                        StoreErrorKind::Other,
                        "apply-then-unknown mask expected the real API write to be accepted",
                    ));
                }
                Ok(CasOutcome::OutcomeUnknown)
            }
            MaskMode::NoApplyUnknown => Ok(CasOutcome::OutcomeUnknown),
        }
    }
}

#[derive(Clone)]
struct OneActivityWorkflow {
    activity: ActivitySpec,
}

#[derive(Clone)]
struct AttemptCountingStore {
    inner: KubernetesCheckpointStore,
    write_attempts: Arc<AtomicU64>,
}

#[async_trait]
impl CheckpointStore for AttemptCountingStore {
    async fn load(
        &self,
        execution_id: ExecutionId,
    ) -> Result<Option<StoredCheckpoint>, StoreError> {
        self.inner.load(execution_id).await
    }

    async fn compare_and_swap(
        &self,
        execution_id: ExecutionId,
        expected: Option<StorageRevision>,
        checkpoint: CheckpointEnvelope,
    ) -> Result<CasOutcome, StoreError> {
        self.write_attempts.fetch_add(1, Ordering::Relaxed);
        self.inner
            .compare_and_swap(execution_id, expected, checkpoint)
            .await
    }
}

#[async_trait]
impl Workflow for OneActivityWorkflow {
    async fn run(&self, context: &mut WorkflowContext<'_>, _input: ExactBytes) -> TerminalOutcome {
        TerminalOutcome::succeeded(context.activity(self.activity.clone()).await)
    }
}

#[tokio::test]
async fn validates_real_api_cas_watch_compaction_and_ambiguous_recovery() -> TestResult<()> {
    let kubeconfig_path =
        std::env::var("KUBECONFIG").expect("real-API tests require an isolated KUBECONFIG");
    let context =
        std::env::var("KUBE_CONTEXT").expect("real-API tests require an explicit KUBE_CONTEXT");
    let cluster_name = std::env::var("KIND_CLUSTER_NAME")
        .expect("real-API tests require an explicit KIND_CLUSTER_NAME");
    assert_ne!(cluster_name, "kind", "default KinD cluster is forbidden");
    assert_eq!(context, format!("kind-{cluster_name}"));
    if let Ok(home) = std::env::var("HOME") {
        assert_ne!(
            std::path::Path::new(&kubeconfig_path),
            std::path::Path::new(&home).join(".kube/config"),
            "default user kubeconfig is forbidden"
        );
    }
    let kubeconfig = Kubeconfig::read_from(&kubeconfig_path)?;
    let config = kube::Config::from_custom_kubeconfig(
        kubeconfig,
        &KubeConfigOptions {
            context: Some(context.clone()),
            ..Default::default()
        },
    )
    .await?;
    let client = match Client::try_from(config) {
        Ok(client) => client,
        Err(error) => {
            eprintln!("KUBERNETES_PREFLIGHT endpoint=failed error={error}");
            return Err(error.into());
        }
    };
    let version = match client.apiserver_version().await {
        Ok(version) => version,
        Err(error) => {
            eprintln!("KUBERNETES_PREFLIGHT endpoint=failed error={error}");
            return Err(error.into());
        }
    };
    println!(
        "KUBERNETES_PREFLIGHT endpoint=passed context={} server_version={}",
        context, version.git_version
    );

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos()
        .to_string();
    let namespace = format!("kuberic-checkpoint-{}", &suffix[suffix.len() - 12..]);
    check_authorization(&client, &namespace).await?;

    let namespaces: Api<Namespace> = Api::all(client.clone());
    namespaces
        .create(
            &PostParams::default(),
            &Namespace {
                metadata: ObjectMeta {
                    name: Some(namespace.clone()),
                    ..ObjectMeta::default()
                },
                ..Namespace::default()
            },
        )
        .await?;

    let result = run_real_api_scenarios(client.clone(), &namespace).await;
    let cleanup = async {
        namespaces
            .delete(&namespace, &DeleteParams::default())
            .await?;
        wait_for_namespace_absence(&namespaces, &namespace).await
    }
    .await;
    match cleanup {
        Ok(()) => println!("KUBERNETES_PREFLIGHT namespace_cleanup=passed"),
        Err(ref error) => eprintln!("KUBERNETES_PREFLIGHT namespace_cleanup=failed error={error}"),
    }
    result?;
    cleanup?;
    Ok(())
}

async fn check_authorization(client: &Client, namespace: &str) -> TestResult<()> {
    for (resource, verb, target_namespace) in [
        ("namespaces", "create", None),
        ("namespaces", "get", None),
        ("namespaces", "delete", None),
        ("configmaps", "get", Some(namespace)),
        ("configmaps", "create", Some(namespace)),
        ("configmaps", "update", Some(namespace)),
        ("configmaps", "watch", Some(namespace)),
        ("configmaps", "delete", Some(namespace)),
    ] {
        let reviews: Api<SelfSubjectAccessReview> = Api::all(client.clone());
        let response = reviews
            .create(
                &PostParams::default(),
                &SelfSubjectAccessReview {
                    spec: SelfSubjectAccessReviewSpec {
                        resource_attributes: Some(ResourceAttributes {
                            group: Some(String::new()),
                            version: Some("v1".to_string()),
                            resource: Some(resource.to_string()),
                            verb: Some(verb.to_string()),
                            namespace: target_namespace.map(str::to_string),
                            ..ResourceAttributes::default()
                        }),
                        ..SelfSubjectAccessReviewSpec::default()
                    },
                    ..SelfSubjectAccessReview::default()
                },
            )
            .await?;
        let status = response.status.ok_or_else(|| {
            io::Error::other(format!(
                "authorization review omitted status for {verb} {resource}"
            ))
        })?;
        println!(
            "KUBERNETES_PREFLIGHT resource={resource} verb={verb} allowed={} reason={}",
            status.allowed,
            status.reason.as_deref().unwrap_or("none")
        );
        if !status.allowed {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("authorization denied for {verb} {resource}"),
            )
            .into());
        }
    }
    Ok(())
}

async fn run_real_api_scenarios(client: Client, namespace: &str) -> TestResult<()> {
    validate_cas_and_deletion(client.clone(), namespace).await?;
    validate_owner_garbage_collection(client.clone(), namespace).await?;
    let measurement = validate_measurement_and_watch(client.clone(), namespace).await?;
    validate_ambiguous_recovery(client, namespace).await?;
    println!(
        "KUBERNETES_CHECKPOINT_MEASUREMENT \
         fixture=active-completed-to-terminal-v1 \
         active_checkpoint_bytes={} active_object_bytes={} \
         terminal_checkpoint_bytes={} terminal_object_bytes={} \
         write_attempts={} accepted_writes={} measurement_failures={} \
         watch_events={} watch_typed_event_json_bytes={} \
         watch_byte_boundary=canonical_typed_WatchEvent_ConfigMap_JSON_excludes_HTTP_framing \
         apply_then_unknown_recovery=successor \
         no_apply_unknown_recovery=predecessor_then_terminal \
         ambiguity_evidence=store-boundary-mask",
        measurement.active_checkpoint_bytes,
        measurement.active_object_bytes,
        measurement.terminal_checkpoint_bytes,
        measurement.terminal_object_bytes,
        measurement.write_attempts,
        measurement.accepted_writes,
        measurement.measurement_failures,
        measurement.watch_events,
        measurement.watch_bytes,
    );
    Ok(())
}

async fn validate_owner_garbage_collection(client: Client, namespace: &str) -> TestResult<()> {
    let config_maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let owner_name = "durable-checkpoint-owner";
    let owner = config_maps
        .create(
            &PostParams::default(),
            &ConfigMap {
                metadata: ObjectMeta {
                    name: Some(owner_name.to_string()),
                    ..ObjectMeta::default()
                },
                ..ConfigMap::default()
            },
        )
        .await?;
    let owner_uid = owner
        .metadata
        .uid
        .ok_or_else(|| io::Error::other("owner ConfigMap has no UID"))?;
    let owner = KubernetesCheckpointOwner::new(
        OwnerReference {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            name: owner_name.to_string(),
            uid: owner_uid,
            controller: Some(false),
            block_owner_deletion: Some(false),
        },
        KubernetesCheckpointOwnerScope::Namespaced(namespace.to_string()),
    );
    let store = KubernetesCheckpointStore::with_options(
        client,
        namespace,
        KubernetesCheckpointStoreOptions::default().with_owner(owner),
    )?;
    let execution_id = ExecutionId::from_bytes([0x15; 16]);
    accepted_revision(
        store
            .compare_and_swap(
                execution_id,
                None,
                opaque_checkpoint(b"owner-bound-terminal"),
            )
            .await?,
    )?;
    let checkpoint_name = KubernetesCheckpointStore::object_name(execution_id);
    config_maps
        .delete(owner_name, &DeleteParams::default())
        .await?;
    wait_for_absence(&config_maps, &checkpoint_name).await?;
    println!("KUBERNETES_CHECKPOINT_LIFECYCLE owner_gc=passed");
    Ok(())
}

async fn validate_cas_and_deletion(client: Client, namespace: &str) -> TestResult<()> {
    let execution_id = ExecutionId::from_bytes([0x10; 16]);
    let first = opaque_checkpoint(b"real-api-first");
    let second = opaque_checkpoint(b"real-api-second");
    let store = KubernetesCheckpointStore::new(client.clone(), namespace)?;
    assert_eq!(store.load(execution_id).await?, None);

    let first_revision = accepted_revision(
        store
            .compare_and_swap(execution_id, None, first.clone())
            .await?,
    )?;
    assert_eq!(
        store
            .compare_and_swap(execution_id, None, second.clone())
            .await?,
        CasOutcome::Conflict
    );
    assert_eq!(
        store
            .load(execution_id)
            .await?
            .ok_or_else(|| io::Error::other("create-race winner disappeared"))?
            .checkpoint(),
        &first
    );

    let second_revision = accepted_revision(
        store
            .compare_and_swap(execution_id, Some(first_revision.clone()), second.clone())
            .await?,
    )?;
    assert_eq!(
        store
            .compare_and_swap(execution_id, Some(first_revision), first)
            .await?,
        CasOutcome::Conflict
    );
    assert_eq!(
        store
            .load(execution_id)
            .await?
            .ok_or_else(|| io::Error::other("stale-write winner disappeared"))?
            .checkpoint(),
        &second
    );

    let config_maps: Api<ConfigMap> = Api::namespaced(client, namespace);
    let name = KubernetesCheckpointStore::object_name(execution_id);
    config_maps.delete(&name, &DeleteParams::default()).await?;
    wait_for_absence(&config_maps, &name).await?;
    assert_eq!(store.load(execution_id).await?, None);
    assert_eq!(
        store
            .compare_and_swap(
                execution_id,
                Some(second_revision),
                opaque_checkpoint(b"must-not-recreate"),
            )
            .await?,
        CasOutcome::Conflict
    );
    assert_eq!(store.load(execution_id).await?, None);
    Ok(())
}

#[derive(Debug)]
struct Measurement {
    active_checkpoint_bytes: u64,
    active_object_bytes: u64,
    terminal_checkpoint_bytes: u64,
    terminal_object_bytes: u64,
    write_attempts: u64,
    accepted_writes: u64,
    measurement_failures: u64,
    watch_events: u64,
    watch_bytes: u64,
}

async fn validate_measurement_and_watch(
    client: Client,
    namespace: &str,
) -> TestResult<Measurement> {
    let execution_id = ExecutionId::from_bytes([0x42; 16]);
    let (execution, activity, active, terminal, limits) = fixed_fixture(execution_id)?;
    let store = KubernetesCheckpointStore::new(client.clone(), namespace)?;
    let before = store.metrics().snapshot();
    let write_attempts = Arc::new(AtomicU64::new(0));
    let measured_store = AttemptCountingStore {
        inner: store.clone(),
        write_attempts: write_attempts.clone(),
    };
    let active_revision = accepted_revision(
        measured_store
            .compare_and_swap(execution_id, None, active.clone())
            .await?,
    )?;
    let after_active = store.metrics().snapshot();

    let name = KubernetesCheckpointStore::object_name(execution_id);
    let config_maps: Api<ConfigMap> = Api::namespaced(client, namespace);
    let watch_params = WatchParams::default()
        .fields(&format!("metadata.name={name}"))
        .timeout(10);
    let stream = config_maps
        .watch(&watch_params, active_revision.as_str())
        .await?;
    futures::pin_mut!(stream);
    let watch_future = async {
        let mut event_count = 0_u64;
        let mut byte_count = 0_u64;
        loop {
            let event = tokio::time::timeout(Duration::from_secs(12), stream.try_next())
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "watch event timed out"))??
                .ok_or_else(|| io::Error::other("watch ended before terminal modification"))?;
            event_count += 1;
            byte_count += serde_json::to_vec(&event)?.len() as u64;
            match event {
                WatchEvent::Modified(object) => {
                    if object.name_any() != name {
                        return Err(io::Error::other("watch delivered an unexpected object").into());
                    }
                    return Ok::<_, Box<dyn Error>>((
                        event_count,
                        byte_count,
                        object.resource_version(),
                    ));
                }
                WatchEvent::Bookmark(_) => {}
                WatchEvent::Error(status) => {
                    return Err(io::Error::other(format!(
                        "watch returned API error code={} reason={}",
                        status.code, status.reason
                    ))
                    .into());
                }
                WatchEvent::Added(_) | WatchEvent::Deleted(_) => {
                    return Err(io::Error::other("watch delivered an unexpected event type").into());
                }
            }
        }
    };
    let write_future =
        measured_store.compare_and_swap(execution_id, Some(active_revision), terminal.clone());
    let (write_result, watch_result) = tokio::join!(write_future, watch_future);
    let terminal_revision = accepted_revision(write_result?)?;
    let (watch_events, watch_bytes, watched_revision) = watch_result?;
    assert_eq!(
        watched_revision.as_deref(),
        Some(terminal_revision.as_str())
    );

    let after_terminal = store.metrics().snapshot();
    assert_eq!(
        after_terminal.accepted_writes() - before.accepted_writes(),
        2
    );
    let write_attempts = write_attempts.load(Ordering::Relaxed);
    assert_eq!(write_attempts, 2);
    assert!(after_terminal.accepted_writes() - before.accepted_writes() <= write_attempts);
    assert_eq!(after_terminal.measurement_failures(), 0);
    let loaded = store
        .load(execution_id)
        .await?
        .ok_or_else(|| io::Error::other("terminal checkpoint disappeared"))?;
    assert_eq!(loaded.checkpoint(), &terminal);
    let terminal_payload = loaded
        .checkpoint()
        .decode_and_validate(&execution, limits)?;
    assert!(terminal_payload.active_activities().is_none());
    assert!(terminal_payload.terminal_outcome().is_some());
    assert!(terminal.encoded_len()? < active.encoded_len()?);

    let _ = activity;
    Ok(Measurement {
        active_checkpoint_bytes: after_active.checkpoint_bytes() - before.checkpoint_bytes(),
        active_object_bytes: after_active.object_bytes() - before.object_bytes(),
        terminal_checkpoint_bytes: after_terminal.checkpoint_bytes()
            - after_active.checkpoint_bytes(),
        terminal_object_bytes: after_terminal.object_bytes() - after_active.object_bytes(),
        write_attempts,
        accepted_writes: after_terminal.accepted_writes() - before.accepted_writes(),
        measurement_failures: after_terminal.measurement_failures() - before.measurement_failures(),
        watch_events,
        watch_bytes,
    })
}

async fn validate_ambiguous_recovery(client: Client, namespace: &str) -> TestResult<()> {
    for (byte, mode, successor_expected) in [
        (0x51, MaskMode::ApplyThenUnknown, true),
        (0x52, MaskMode::NoApplyUnknown, false),
    ] {
        let execution_id = ExecutionId::from_bytes([byte; 16]);
        let (execution, activity, active, _terminal, limits) = fixed_fixture(execution_id)?;
        let inner = KubernetesCheckpointStore::new(client.clone(), namespace)?;
        accepted_revision(inner.compare_and_swap(execution_id, None, active).await?)?;
        let masked = MaskOnceStore::new(inner.clone(), mode);
        let workflow = OneActivityWorkflow { activity };
        let mut host = DurableHost::new(masked, HostEpoch::from_bytes([byte; 16]), limits);
        assert!(matches!(
            host.turn(&workflow, execution.clone()).await,
            HostOutcome::ReloadRequired {
                reason: ReloadReason::OutcomeUnknown,
                ..
            }
        ));

        let after_unknown = inner
            .load(execution_id)
            .await?
            .ok_or_else(|| io::Error::other("recovery checkpoint disappeared"))?;
        let payload = after_unknown
            .checkpoint()
            .decode_and_validate(&execution, limits)?;
        assert_eq!(payload.terminal_outcome().is_some(), successor_expected);
        assert_eq!(payload.active_activities().is_some(), !successor_expected);

        assert!(matches!(
            host.turn(&workflow, execution.clone()).await,
            HostOutcome::WorkflowCompleted { .. }
        ));
        let recovered = inner
            .load(execution_id)
            .await?
            .ok_or_else(|| io::Error::other("recovered checkpoint disappeared"))?;
        assert!(
            recovered
                .checkpoint()
                .decode_and_validate(&execution, limits)?
                .terminal_outcome()
                .is_some()
        );
    }
    Ok(())
}

fn fixed_fixture(
    execution_id: ExecutionId,
) -> TestResult<(
    ExecutionSpec,
    ActivitySpec,
    CheckpointEnvelope,
    CheckpointEnvelope,
    CheckpointLimits,
)> {
    let execution = ExecutionSpec::new(
        execution_id,
        ExactBytes::new(b"kubernetes-provider-spike-v1".to_vec()),
        64,
    );
    let activity = ActivitySpec::new(
        ActivityName::new("measurement-activity", 1)?,
        ExactBytes::new(b"measurement-input".to_vec()),
        64,
    );
    let limits = CheckpointLimits::new(8, CHECKPOINT_LIMIT, CHECKPOINT_LIMIT)?;
    let contract = ExecutionContract::new(execution.clone(), CHECKPOINT_LIMIT as u64);
    let active = CheckpointEnvelope::encode_with_limits(
        &CheckpointPayload::active(
            contract.clone(),
            vec![ActivityRecord::completed(
                ActivitySequence::new(0),
                activity.clone(),
                ExactBytes::new(b"measurement-complete".to_vec()),
            )],
        ),
        limits,
    )?;
    let terminal = CheckpointEnvelope::encode_with_limits(
        &CheckpointPayload::terminal(
            contract,
            TerminalOutcome::succeeded(ExactBytes::new(b"measurement-complete".to_vec())),
            1,
        ),
        limits,
    )?;
    Ok((execution, activity, active, terminal, limits))
}

fn opaque_checkpoint(label: &[u8]) -> CheckpointEnvelope {
    CheckpointEnvelope::new(3, ExactBytes::new(label))
}

fn accepted_revision(outcome: CasOutcome) -> TestResult<StorageRevision> {
    match outcome {
        CasOutcome::Accepted(revision) => Ok(revision),
        other => Err(io::Error::other(format!("expected accepted write, got {other:?}")).into()),
    }
}

async fn wait_for_absence(api: &Api<ConfigMap>, name: &str) -> TestResult<()> {
    for _ in 0..50 {
        if api.get_opt(name).await?.is_none() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!("ConfigMap {name} was not deleted"),
    )
    .into())
}

async fn wait_for_namespace_absence(api: &Api<Namespace>, name: &str) -> TestResult<()> {
    for _ in 0..100 {
        if api.get_opt(name).await?.is_none() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!("Namespace {name} was not deleted"),
    )
    .into())
}
