#![cfg(feature = "kubernetes")]

use std::{
    collections::VecDeque,
    io,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use http::{Method, Request, Response, StatusCode};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::{Client, client::Body};
use serde_json::{Value, json};
use tower::service_fn;

use kuberic_dex::{
    CasOutcome, CheckpointEnvelope, CheckpointLimits, CheckpointStore,
    DEFAULT_CONFIG_MAP_DATA_BUDGET_BYTES, DurableHost, ExactBytes, ExecutionId, ExecutionSpec,
    HostEpoch, HostOutcome, KubernetesCheckpointOwner, KubernetesCheckpointOwnerScope,
    KubernetesCheckpointStore, KubernetesCheckpointStoreOptions, MAX_CONFIG_MAP_DATA_BUDGET_BYTES,
    ReloadReason, StorageRevision, StoreErrorKind, StoreOperation, TerminalOutcome, Workflow,
    WorkflowContext,
};

#[derive(Clone, Debug)]
enum Step {
    Response(StatusCode, Value),
    TransportFailure(&'static str),
}

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: Method,
    uri: String,
    body: Value,
}

#[derive(Clone, Debug)]
struct Harness {
    steps: Arc<Mutex<VecDeque<Step>>>,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
}

impl Harness {
    fn new(steps: impl IntoIterator<Item = Step>) -> Self {
        Self {
            steps: Arc::new(Mutex::new(steps.into_iter().collect())),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn client(&self) -> Client {
        let steps = Arc::clone(&self.steps);
        let requests = Arc::clone(&self.requests);
        let service = service_fn(move |request: Request<Body>| {
            let steps = Arc::clone(&steps);
            let requests = Arc::clone(&requests);
            async move {
                let (parts, body) = request.into_parts();
                let body = body
                    .collect_bytes()
                    .await
                    .map_err(|error| io::Error::other(error.to_string()))?;
                let body = if body.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&body)
                        .map_err(|error| io::Error::other(error.to_string()))?
                };
                requests
                    .lock()
                    .expect("requests lock")
                    .push(RecordedRequest {
                        method: parts.method,
                        uri: parts.uri.to_string(),
                        body,
                    });
                match steps.lock().expect("steps lock").pop_front() {
                    Some(Step::Response(status, body)) => Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&body).expect("response JSON"),
                        ))
                        .map_err(io::Error::other),
                    Some(Step::TransportFailure(message)) => Err(io::Error::other(message)),
                    None => Err(io::Error::other("unexpected Kubernetes request")),
                }
            }
        });
        Client::new(service, "default")
    }

    fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().expect("requests lock").clone()
    }

    fn assert_consumed(&self) {
        assert!(
            self.steps.lock().expect("steps lock").is_empty(),
            "unconsumed scripted responses"
        );
    }
}

fn execution(byte: u8) -> ExecutionId {
    ExecutionId::from_bytes([byte; 16])
}

fn assert_send<T: Send>(_: T) {}

#[tokio::test]
async fn kubernetes_store_futures_are_send() {
    let harness = Harness::new([]);
    let store = KubernetesCheckpointStore::new(harness.client(), "checkpoint-tests").unwrap();
    assert_send(store.load(execution(0x01)));
    assert_send(store.compare_and_swap(execution(0x01), None, checkpoint(b"send-contract")));
}

fn checkpoint(label: &[u8]) -> CheckpointEnvelope {
    CheckpointEnvelope::new(3, ExactBytes::new(label))
}

fn owner_reference() -> OwnerReference {
    OwnerReference {
        api_version: "kuberic.io/v1alpha1".to_string(),
        kind: "WorkflowRun".to_string(),
        name: "example-run".to_string(),
        uid: "opaque-owner-uid".to_string(),
        controller: None,
        block_owner_deletion: None,
    }
}

fn config_map_response(
    execution_id: ExecutionId,
    revision: Option<&str>,
    checkpoint: Option<&CheckpointEnvelope>,
) -> Value {
    config_map_response_with_owner(execution_id, revision, checkpoint, None)
}

fn config_map_response_with_owner(
    execution_id: ExecutionId,
    revision: Option<&str>,
    checkpoint: Option<&CheckpointEnvelope>,
    owner: Option<&OwnerReference>,
) -> Value {
    let mut metadata = json!({
        "name": KubernetesCheckpointStore::object_name(execution_id),
        "namespace": "checkpoint-tests"
    });
    if let Some(revision) = revision {
        metadata["resourceVersion"] = json!(revision);
    }
    if let Some(owner) = owner {
        metadata["ownerReferences"] = json!([owner]);
    }
    let mut object = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": metadata
    });
    if let Some(checkpoint) = checkpoint {
        object["data"] = json!({
            "checkpoint.json": serde_json::to_string(checkpoint).expect("checkpoint JSON")
        });
    }
    object
}

fn api_error(code: u16, reason: &str) -> Step {
    Step::Response(
        StatusCode::from_u16(code).expect("valid status"),
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "metadata": {},
            "status": "Failure",
            "details": {"name": "sensitive-api-diagnostic-marker"},
            "message": format!("{reason} without credential or checkpoint content"),
            "reason": reason,
            "code": code
        }),
    )
}

#[tokio::test]
async fn deterministic_names_and_namespace_validation() {
    let expected = format!("kuberic-checkpoint-{}", "ab".repeat(16));
    assert_eq!(
        KubernetesCheckpointStore::object_name(execution(0xab)),
        expected
    );
    assert!(expected.len() < 253);
    assert!(
        expected
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    );

    let harness = Harness::new([]);
    let error = KubernetesCheckpointStore::new(harness.client(), "Invalid_Namespace")
        .expect_err("invalid namespace must fail before requests");
    assert_eq!(error.kind(), StoreErrorKind::Other);
    assert!(harness.requests().is_empty());

    let defaults = KubernetesCheckpointStoreOptions::default();
    assert_eq!(
        defaults.data_budget_bytes(),
        DEFAULT_CONFIG_MAP_DATA_BUDGET_BYTES
    );
    assert!(defaults.owner().is_none());
}

#[tokio::test]
async fn create_and_replace_round_trip_exact_opaque_revisions_and_metrics() {
    let execution_id = execution(0x11);
    let first = checkpoint(b"first");
    let second = checkpoint(b"second");
    let harness = Harness::new([
        Step::Response(
            StatusCode::CREATED,
            config_map_response(execution_id, Some("opaque-alpha"), Some(&first)),
        ),
        Step::Response(
            StatusCode::OK,
            config_map_response(execution_id, Some("opaque-alpha"), Some(&first)),
        ),
        Step::Response(
            StatusCode::OK,
            config_map_response(execution_id, Some("rv/not-a-number"), Some(&second)),
        ),
    ]);
    let store =
        KubernetesCheckpointStore::new(harness.client(), "checkpoint-tests").expect("store");

    let first_outcome = store
        .compare_and_swap(execution_id, None, first.clone())
        .await
        .expect("create result");
    assert_eq!(
        first_outcome,
        CasOutcome::Accepted(StorageRevision::new("opaque-alpha").expect("revision"))
    );
    let second_outcome = store
        .compare_and_swap(
            execution_id,
            Some(StorageRevision::new("opaque-alpha").expect("revision")),
            second.clone(),
        )
        .await
        .expect("replace result");
    assert_eq!(
        second_outcome,
        CasOutcome::Accepted(StorageRevision::new("rv/not-a-number").expect("revision"))
    );

    let requests = harness.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, Method::POST);
    assert_eq!(requests[1].method, Method::GET);
    assert_eq!(requests[2].method, Method::PUT);
    assert!(
        requests[0]
            .uri
            .starts_with("/api/v1/namespaces/checkpoint-tests/configmaps?")
    );
    assert!(requests[0].body["metadata"]["resourceVersion"].is_null());
    assert!(requests[0].body["metadata"]["ownerReferences"].is_null());
    assert_eq!(
        requests[2].body["metadata"]["resourceVersion"],
        "opaque-alpha"
    );
    assert_eq!(
        requests[0].body["data"]["checkpoint.json"],
        serde_json::to_string(&first).expect("checkpoint JSON")
    );
    assert_eq!(
        requests[2].body["data"]["checkpoint.json"],
        serde_json::to_string(&second).expect("checkpoint JSON")
    );

    let metrics = store.metrics().snapshot();
    let authoritative_write_attempts = requests
        .iter()
        .filter(|request| matches!(request.method, Method::POST | Method::PUT))
        .count() as u64;
    assert_eq!(authoritative_write_attempts, 2);
    assert_eq!(metrics.accepted_writes(), 2);
    assert_eq!(metrics.accepted_writes(), authoritative_write_attempts);
    assert_eq!(
        metrics.checkpoint_bytes(),
        (serde_json::to_vec(&first).expect("JSON").len()
            + serde_json::to_vec(&second).expect("JSON").len()) as u64
    );
    assert!(metrics.object_bytes() > metrics.checkpoint_bytes());
    assert_eq!(metrics.measurement_failures(), 0);
    harness.assert_consumed();
}

#[tokio::test]
async fn owner_reference_is_validated_locally_and_preserved_across_writes() {
    let execution_id = execution(0x12);
    let first = checkpoint(b"owned-first");
    let second = checkpoint(b"owned-second");
    let owner_reference = owner_reference();
    let harness = Harness::new([
        Step::Response(
            StatusCode::CREATED,
            config_map_response_with_owner(
                execution_id,
                Some("owned-alpha"),
                Some(&first),
                Some(&owner_reference),
            ),
        ),
        Step::Response(
            StatusCode::OK,
            config_map_response_with_owner(
                execution_id,
                Some("owned-alpha"),
                Some(&first),
                Some(&owner_reference),
            ),
        ),
        Step::Response(
            StatusCode::OK,
            config_map_response_with_owner(
                execution_id,
                Some("owned-beta"),
                Some(&second),
                Some(&owner_reference),
            ),
        ),
    ]);
    let owner = KubernetesCheckpointOwner::new(
        owner_reference,
        KubernetesCheckpointOwnerScope::Namespaced("checkpoint-tests".to_string()),
    );
    let store = KubernetesCheckpointStore::with_options(
        harness.client(),
        "checkpoint-tests",
        KubernetesCheckpointStoreOptions::default().with_owner(owner),
    )
    .expect("owned store");

    let first_outcome = store
        .compare_and_swap(execution_id, None, first)
        .await
        .expect("owned create");
    assert_eq!(
        first_outcome,
        CasOutcome::Accepted(StorageRevision::new("owned-alpha").expect("revision"))
    );
    let second_outcome = store
        .compare_and_swap(
            execution_id,
            Some(StorageRevision::new("owned-alpha").expect("revision")),
            second,
        )
        .await
        .expect("owned replace");
    assert_eq!(
        second_outcome,
        CasOutcome::Accepted(StorageRevision::new("owned-beta").expect("revision"))
    );

    let requests = harness.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[1].method, Method::GET);
    for request in requests
        .iter()
        .filter(|request| matches!(request.method, Method::POST | Method::PUT))
    {
        let owner = &request.body["metadata"]["ownerReferences"][0];
        assert_eq!(owner["apiVersion"], "kuberic.io/v1alpha1");
        assert_eq!(owner["kind"], "WorkflowRun");
        assert_eq!(owner["name"], "example-run");
        assert_eq!(owner["uid"], "opaque-owner-uid");
        assert!(owner["controller"].is_null());
        assert!(owner["blockOwnerDeletion"].is_null());
    }
    harness.assert_consumed();
}

#[tokio::test]
async fn replacement_rejects_lifecycle_owner_changes_without_mutation() {
    let execution_id = execution(0x13);
    let persisted_checkpoint = checkpoint(b"persisted");
    let configured_owner = owner_reference();
    let mut different_owner = owner_reference();
    different_owner.uid = "different-owner-uid".to_string();

    let cases = [
        (
            None,
            Some(owner_reference()),
            "owned checkpoint must not become independent",
        ),
        (
            Some(configured_owner.clone()),
            None,
            "independent checkpoint must not become owned",
        ),
        (
            Some(different_owner),
            Some(owner_reference()),
            "checkpoint owner must not change",
        ),
    ];

    for (configured, persisted, description) in cases {
        let harness = Harness::new([Step::Response(
            StatusCode::OK,
            config_map_response_with_owner(
                execution_id,
                Some("stable-owner-revision"),
                Some(&persisted_checkpoint),
                persisted.as_ref(),
            ),
        )]);
        let mut options = KubernetesCheckpointStoreOptions::default();
        if let Some(owner) = configured {
            options = options.with_owner(KubernetesCheckpointOwner::new(
                owner,
                KubernetesCheckpointOwnerScope::Namespaced("checkpoint-tests".to_string()),
            ));
        }

        let store =
            KubernetesCheckpointStore::with_options(harness.client(), "checkpoint-tests", options)
                .expect("store");

        let error = store
            .compare_and_swap(
                execution_id,
                Some(StorageRevision::new("stable-owner-revision").expect("revision")),
                checkpoint(description.as_bytes()),
            )
            .await
            .expect_err(description);
        assert_eq!(error.kind(), StoreErrorKind::Other);
        assert!(error.description().contains("owner relationship"));
        let requests = harness.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, Method::GET);
        harness.assert_consumed();
    }
}

#[tokio::test]
async fn load_rejects_missing_mismatched_or_additional_owners() {
    let execution_id = execution(0x23);
    let configured_owner = owner_reference();
    let mut different_owner = owner_reference();
    different_owner.uid = "different-owner-uid".to_string();

    let missing = config_map_response_with_owner(
        execution_id,
        Some("owner-revision"),
        Some(&checkpoint(b"terminal")),
        None,
    );
    let mismatched = config_map_response_with_owner(
        execution_id,
        Some("owner-revision"),
        Some(&checkpoint(b"terminal")),
        Some(&different_owner),
    );
    let mut additional = config_map_response_with_owner(
        execution_id,
        Some("owner-revision"),
        Some(&checkpoint(b"terminal")),
        Some(&configured_owner),
    );
    additional["metadata"]["ownerReferences"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::to_value(different_owner).unwrap());

    for response in [missing, mismatched, additional] {
        let harness = Harness::new([Step::Response(StatusCode::OK, response)]);
        let store = KubernetesCheckpointStore::with_options(
            harness.client(),
            "checkpoint-tests",
            KubernetesCheckpointStoreOptions::default().with_owner(KubernetesCheckpointOwner::new(
                configured_owner.clone(),
                KubernetesCheckpointOwnerScope::Namespaced("checkpoint-tests".to_string()),
            )),
        )
        .unwrap();
        let error = store.load(execution_id).await.unwrap_err();
        assert_eq!(error.kind(), StoreErrorKind::MalformedResponse);
        assert!(error.description().contains("owner relationship"));
        harness.assert_consumed();
    }
}

#[tokio::test]
async fn invalid_owner_configuration_is_rejected_before_dispatch() {
    let invalid_owners = [
        (
            KubernetesCheckpointOwner::new(
                OwnerReference {
                    uid: String::new(),
                    ..owner_reference()
                },
                KubernetesCheckpointOwnerScope::Namespaced("checkpoint-tests".to_string()),
            ),
            "uid",
        ),
        (
            KubernetesCheckpointOwner::new(
                owner_reference(),
                KubernetesCheckpointOwnerScope::Namespaced("other-namespace".to_string()),
            ),
            "must match",
        ),
        (
            KubernetesCheckpointOwner::new(
                OwnerReference {
                    controller: Some(true),
                    ..owner_reference()
                },
                KubernetesCheckpointOwnerScope::Namespaced("checkpoint-tests".to_string()),
            ),
            "controller",
        ),
        (
            KubernetesCheckpointOwner::new(
                OwnerReference {
                    block_owner_deletion: Some(true),
                    ..owner_reference()
                },
                KubernetesCheckpointOwnerScope::Namespaced("checkpoint-tests".to_string()),
            ),
            "blockOwnerDeletion",
        ),
    ];

    for (owner, expected_diagnostic) in invalid_owners {
        let harness = Harness::new([]);
        let error = KubernetesCheckpointStore::with_options(
            harness.client(),
            "checkpoint-tests",
            KubernetesCheckpointStoreOptions::default().with_owner(owner),
        )
        .expect_err("invalid owner must fail");
        assert_eq!(error.kind(), StoreErrorKind::Other);
        assert!(error.description().contains(expected_diagnostic));
        assert!(harness.requests().is_empty());
    }

    let harness = Harness::new([]);
    KubernetesCheckpointStore::with_options(
        harness.client(),
        "checkpoint-tests",
        KubernetesCheckpointStoreOptions::default().with_owner(KubernetesCheckpointOwner::new(
            OwnerReference {
                controller: Some(false),
                block_owner_deletion: Some(false),
                ..owner_reference()
            },
            KubernetesCheckpointOwnerScope::ClusterScoped,
        )),
    )
    .expect("explicit cluster-scoped non-controlling owner");
    assert!(harness.requests().is_empty());
}

#[tokio::test]
async fn load_distinguishes_absence_success_and_malformed_objects() {
    let execution_id = execution(0x22);
    let stored = checkpoint(b"stored");
    let mut invalid = config_map_response(execution_id, Some("rv-invalid"), None);
    invalid["data"] = json!({"checkpoint.json": "not-json"});
    let harness = Harness::new([
        api_error(404, "NotFound"),
        Step::Response(
            StatusCode::OK,
            config_map_response(execution_id, Some("opaque-load"), Some(&stored)),
        ),
        Step::Response(
            StatusCode::OK,
            config_map_response(execution_id, None, Some(&stored)),
        ),
        Step::Response(
            StatusCode::OK,
            config_map_response(execution_id, Some("rv-missing-data"), None),
        ),
        Step::Response(
            StatusCode::OK,
            config_map_response(execution_id, Some(""), Some(&stored)),
        ),
        Step::Response(StatusCode::OK, invalid),
    ]);
    let store =
        KubernetesCheckpointStore::new(harness.client(), "checkpoint-tests").expect("store");

    assert_eq!(store.load(execution_id).await.expect("absence"), None);
    let loaded = store
        .load(execution_id)
        .await
        .expect("load")
        .expect("stored object");
    assert_eq!(loaded.revision().as_str(), "opaque-load");
    assert_eq!(loaded.checkpoint(), &stored);

    let missing_revision = store
        .load(execution_id)
        .await
        .expect_err("missing revision");
    assert_eq!(missing_revision.kind(), StoreErrorKind::MalformedResponse);
    assert!(missing_revision.description().contains("load"));
    let missing_data = store.load(execution_id).await.expect_err("missing data");
    assert_eq!(missing_data.kind(), StoreErrorKind::MalformedResponse);
    assert!(missing_data.description().contains("checkpoint.json"));
    let empty_revision = store.load(execution_id).await.expect_err("empty revision");
    assert_eq!(empty_revision.kind(), StoreErrorKind::MalformedResponse);
    assert!(empty_revision.description().contains("resourceVersion"));
    let malformed_checkpoint = store.load(execution_id).await.expect_err("invalid JSON");
    assert_eq!(
        malformed_checkpoint.kind(),
        StoreErrorKind::MalformedResponse
    );
    assert!(!malformed_checkpoint.description().contains("not-json"));
    harness.assert_consumed();
}

#[tokio::test]
async fn conflicts_cover_create_stale_replace_and_deleted_object_without_recreation() {
    let execution_id = execution(0x33);
    let harness = Harness::new([
        api_error(409, "AlreadyExists"),
        Step::Response(
            StatusCode::OK,
            config_map_response(
                execution_id,
                Some("stale"),
                Some(&checkpoint(b"current-stale")),
            ),
        ),
        api_error(409, "Conflict"),
        Step::Response(
            StatusCode::OK,
            config_map_response(
                execution_id,
                Some("deleted"),
                Some(&checkpoint(b"current-deleted")),
            ),
        ),
        api_error(404, "NotFound"),
    ]);
    let store =
        KubernetesCheckpointStore::new(harness.client(), "checkpoint-tests").expect("store");

    assert_eq!(
        store
            .compare_and_swap(execution_id, None, checkpoint(b"create-race"))
            .await
            .expect("create conflict"),
        CasOutcome::Conflict
    );
    for revision in ["stale", "deleted"] {
        assert_eq!(
            store
                .compare_and_swap(
                    execution_id,
                    Some(StorageRevision::new(revision).expect("revision")),
                    checkpoint(revision.as_bytes()),
                )
                .await
                .expect("replace conflict"),
            CasOutcome::Conflict
        );
    }

    let requests = harness.requests();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.method.clone())
            .collect::<Vec<_>>(),
        [
            Method::POST,
            Method::GET,
            Method::PUT,
            Method::GET,
            Method::PUT
        ]
    );
    assert_eq!(store.metrics().snapshot(), Default::default());
    harness.assert_consumed();
}

#[tokio::test]
async fn mutation_classification_is_conservative_and_diagnostics_are_portable() {
    let execution_id = execution(0x44);
    let secret_checkpoint = checkpoint(b"secret-checkpoint-content");
    let harness = Harness::new([
        Step::Response(
            StatusCode::FORBIDDEN,
            json!({
                "apiVersion": "v1",
                "kind": "Status",
                "metadata": {},
                "status": "Failure",
                "message": "Bearer credential-secret is forbidden",
                "reason": "Forbidden",
                "code": 403
            }),
        ),
        api_error(400, "BadRequest"),
        api_error(401, "Unauthorized"),
        api_error(413, "RequestEntityTooLarge"),
        api_error(415, "UnsupportedMediaType"),
        api_error(422, "Invalid"),
        api_error(429, "TooManyRequests"),
        api_error(500, "InternalError"),
        Step::Response(
            StatusCode::OK,
            config_map_response(execution_id, Some("timeout"), Some(&secret_checkpoint)),
        ),
        api_error(504, "Timeout"),
        Step::TransportFailure("sensitive-mutation-transport-marker"),
        Step::Response(
            StatusCode::CREATED,
            config_map_response(execution_id, None, Some(&secret_checkpoint)),
        ),
        Step::Response(
            StatusCode::CREATED,
            config_map_response(execution_id, Some(""), Some(&secret_checkpoint)),
        ),
    ]);
    let store =
        KubernetesCheckpointStore::new(harness.client(), "checkpoint-tests").expect("store");

    let forbidden = store
        .compare_and_swap(execution_id, None, secret_checkpoint.clone())
        .await
        .expect_err("403 is a definite rejection");
    assert_eq!(forbidden.kind(), StoreErrorKind::Authorization);
    assert!(forbidden.description().contains("compare-and-swap"));
    assert!(forbidden.description().contains("code=403"));
    assert!(forbidden.description().contains("reason=Forbidden"));
    assert!(
        !forbidden
            .description()
            .contains("secret-checkpoint-content")
    );
    for (expected_kind, reason) in [
        (StoreErrorKind::Other, "BadRequest"),
        (StoreErrorKind::Authorization, "Unauthorized"),
        (StoreErrorKind::Other, "RequestEntityTooLarge"),
        (StoreErrorKind::Other, "UnsupportedMediaType"),
        (StoreErrorKind::Other, "Invalid"),
        (StoreErrorKind::Unavailable, "TooManyRequests"),
    ] {
        let rejected = store
            .compare_and_swap(execution_id, None, secret_checkpoint.clone())
            .await
            .expect_err("API response proves rejection");
        assert_eq!(rejected.kind(), expected_kind);
        assert!(rejected.description().contains(reason));
        assert!(!rejected.description().contains("secret-checkpoint-content"));
        assert!(
            !rejected
                .description()
                .contains("sensitive-api-diagnostic-marker")
        );
    }

    for expected in [
        None,
        Some(StorageRevision::new("timeout").expect("revision")),
        None,
        None,
        None,
    ] {
        assert_eq!(
            store
                .compare_and_swap(execution_id, expected, secret_checkpoint.clone())
                .await
                .expect("ambiguous mutation"),
            CasOutcome::OutcomeUnknown
        );
    }
    assert_eq!(store.metrics().snapshot(), Default::default());
    harness.assert_consumed();
}

#[tokio::test]
async fn load_errors_map_to_every_relevant_portable_category() {
    let execution_id = execution(0x55);
    let harness = Harness::new([
        api_error(403, "Forbidden"),
        api_error(504, "Timeout"),
        api_error(500, "InternalError"),
        api_error(422, "Invalid"),
        Step::TransportFailure("sensitive-load-transport-marker"),
    ]);
    let store =
        KubernetesCheckpointStore::new(harness.client(), "checkpoint-tests").expect("store");

    for (expected, excluded_marker) in [
        (StoreErrorKind::Authorization, None),
        (StoreErrorKind::Timeout, None),
        (StoreErrorKind::Unavailable, None),
        (StoreErrorKind::Other, None),
        (
            StoreErrorKind::Unavailable,
            Some("sensitive-load-transport-marker"),
        ),
    ] {
        let error = store.load(execution_id).await.expect_err("load failure");
        assert_eq!(error.kind(), expected);
        assert!(error.description().contains("load"));
        assert!(!error.description().contains("checkpoint-content"));
        if let Some(marker) = excluded_marker {
            assert!(!error.description().contains(marker));
        }
    }
    harness.assert_consumed();
}

#[tokio::test]
async fn data_budget_validates_configuration_and_exact_write_boundary() {
    for valid_budget in [1, MAX_CONFIG_MAP_DATA_BUDGET_BYTES] {
        let harness = Harness::new([]);
        let options =
            KubernetesCheckpointStoreOptions::default().with_data_budget_bytes(valid_budget);
        KubernetesCheckpointStore::with_options(harness.client(), "checkpoint-tests", options)
            .expect("valid budget");
        assert!(harness.requests().is_empty());
    }

    for invalid_budget in [0, MAX_CONFIG_MAP_DATA_BUDGET_BYTES + 1] {
        let harness = Harness::new([]);
        let options =
            KubernetesCheckpointStoreOptions::default().with_data_budget_bytes(invalid_budget);
        let error =
            KubernetesCheckpointStore::with_options(harness.client(), "checkpoint-tests", options)
                .expect_err("invalid budget");
        assert_eq!(error.kind(), StoreErrorKind::Other);
        assert!(error.description().contains("data budget"));
        assert!(error.description().contains(&invalid_budget.to_string()));
        assert!(harness.requests().is_empty());
    }

    let execution_id = execution(0x66);
    let boundary = checkpoint(b"exact configured boundary");
    let data_bytes = "checkpoint.json".len()
        + serde_json::to_string(&boundary)
            .expect("checkpoint JSON")
            .len();
    let exact_harness = Harness::new([Step::Response(
        StatusCode::CREATED,
        config_map_response(execution_id, Some("boundary"), Some(&boundary)),
    )]);
    let exact_store = KubernetesCheckpointStore::with_options(
        exact_harness.client(),
        "checkpoint-tests",
        KubernetesCheckpointStoreOptions::default().with_data_budget_bytes(data_bytes as u64),
    )
    .expect("exact-bound store");
    assert_eq!(
        exact_store
            .compare_and_swap(execution_id, None, boundary.clone())
            .await
            .expect("exact boundary"),
        CasOutcome::Accepted(StorageRevision::new("boundary").expect("revision"))
    );
    exact_harness.assert_consumed();

    let over_harness = Harness::new([]);
    let over_store = KubernetesCheckpointStore::with_options(
        over_harness.client(),
        "checkpoint-tests",
        KubernetesCheckpointStoreOptions::default().with_data_budget_bytes((data_bytes - 1) as u64),
    )
    .expect("one-byte-tight store");
    let error = over_store
        .compare_and_swap(execution_id, None, boundary)
        .await
        .expect_err("one byte over budget");
    assert_eq!(error.kind(), StoreErrorKind::Other);
    assert!(error.description().contains("ConfigMap data budget"));
    assert!(error.description().contains(&data_bytes.to_string()));
    assert!(over_harness.requests().is_empty());
}

struct ImmediateWorkflow;

#[async_trait]
impl Workflow for ImmediateWorkflow {
    async fn run(&self, _context: &mut WorkflowContext<'_>, _input: ExactBytes) -> TerminalOutcome {
        TerminalOutcome::succeeded(ExactBytes::new(b"done".to_vec()))
    }
}

#[tokio::test]
async fn host_rejects_unsupported_format_and_reloads_after_unit_conflict() {
    let execution_id = execution(0x77);
    let unsupported = CheckpointEnvelope::new(99, ExactBytes::default());
    let unsupported_harness = Harness::new([Step::Response(
        StatusCode::OK,
        config_map_response(execution_id, Some("rv-unsupported"), Some(&unsupported)),
    )]);
    let unsupported_store =
        KubernetesCheckpointStore::new(unsupported_harness.client(), "checkpoint-tests")
            .expect("store");
    let mut unsupported_host = DurableHost::new(
        unsupported_store,
        HostEpoch::from_bytes([1; 16]),
        CheckpointLimits::new(8, 4096, 4096).expect("limits"),
    );
    let execution_spec = ExecutionSpec::new(execution_id, ExactBytes::default(), 64);
    assert!(matches!(
        unsupported_host
            .turn(&ImmediateWorkflow, execution_spec.clone())
            .await,
        HostOutcome::CheckpointRejected(_)
    ));

    let reload_harness = Harness::new([
        api_error(404, "NotFound"),
        api_error(409, "AlreadyExists"),
        api_error(403, "Forbidden"),
    ]);
    let reload_store =
        KubernetesCheckpointStore::new(reload_harness.client(), "checkpoint-tests").expect("store");
    let mut reload_host = DurableHost::new(
        reload_store,
        HostEpoch::from_bytes([2; 16]),
        CheckpointLimits::new(8, 4096, 4096).expect("limits"),
    );
    assert!(matches!(
        reload_host
            .turn(&ImmediateWorkflow, execution_spec.clone())
            .await,
        HostOutcome::ReloadRequired {
            reason: ReloadReason::Conflict,
            ..
        }
    ));
    assert!(matches!(
        reload_host.turn(&ImmediateWorkflow, execution_spec).await,
        HostOutcome::StoreFailed {
            operation: StoreOperation::Load,
            ..
        }
    ));
    reload_harness.assert_consumed();
}
