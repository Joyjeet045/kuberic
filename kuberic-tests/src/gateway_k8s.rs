use std::time::Duration;

use anyhow::{Context, Result, ensure};
use k8s_openapi::api::{apps::v1::Deployment, core::v1::Service, discovery::v1::EndpointSlice};
use kube::api::{ApiResource, DynamicObject, ListParams, Patch, PatchParams};
use kube::{Api, Client};
use kvstore::proto::kv_store_client::KvStoreClient;
use kvstore::proto::{GetRequest, PutRequest};
use serde_json::Value;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

const NAMESPACE: &str = "xedio";
const APPLICATIONS: [&str; 2] = ["kvstore-a", "kvstore-b"];
const ENDPOINT: &str = "http://127.0.0.1:30090";

async fn connect_gateway(address: &str, authority: &str) -> Result<KvStoreClient<Channel>> {
    let channel = Endpoint::from_shared(address.to_string())?
        .origin(format!("http://{authority}").parse()?)
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(5))
        .connect()
        .await?;
    Ok(KvStoreClient::new(channel))
}

fn resources(client: Client, group: &str, kind: &str, plural: &str) -> Api<DynamicObject> {
    Api::namespaced_with(
        client,
        NAMESPACE,
        &ApiResource {
            group: group.into(),
            version: "v1".into(),
            kind: kind.into(),
            api_version: format!("{group}/v1"),
            plural: plural.into(),
        },
    )
}

fn conditions_ready(conditions: Option<&Value>, generation: i64, required: &[&str]) -> bool {
    conditions
        .and_then(Value::as_array)
        .is_some_and(|conditions| {
            required.iter().all(|required| {
                conditions.iter().any(|condition| {
                    condition["type"].as_str() == Some(required)
                        && condition["status"].as_str() == Some("True")
                        && condition["observedGeneration"].as_i64() == Some(generation)
                })
            })
        })
}

async fn wait_resource(
    api: &Api<DynamicObject>,
    name: &str,
    description: &str,
    ready: impl Fn(&DynamicObject) -> bool,
) -> Result<DynamicObject> {
    let deadline = Instant::now() + Duration::from_secs(240);
    loop {
        let object = api.get_opt(name).await?;
        if let Some(object) = &object
            && ready(object)
        {
            return Ok(object.clone());
        }
        ensure!(
            Instant::now() < deadline,
            "timed out waiting for {description}: {object:?}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_set(client: Client, name: &str, primary: Option<&str>) -> Result<DynamicObject> {
    wait_resource(
        &resources(client, "kuberic.io", "KubericSet", "kubericsets"),
        name,
        "healthy KubericSet",
        |object| {
            let status = &object.data["status"];
            status["phase"] == "Healthy"
                && status["readyReplicas"] == 3
                && status["replicas"] == 3
                && status["currentPrimary"]
                    .as_str()
                    .is_some_and(|actual| primary.is_none_or(|expected| actual == expected))
        },
    )
    .await
}

async fn wait_gateway(client: Client) -> Result<()> {
    let classes: Api<DynamicObject> = Api::all_with(
        client.clone(),
        &ApiResource {
            group: "gateway.networking.k8s.io".into(),
            version: "v1".into(),
            api_version: "gateway.networking.k8s.io/v1".into(),
            kind: "GatewayClass".into(),
            plural: "gatewayclasses".into(),
        },
    );
    wait_resource(
        &classes,
        "kuberic-envoy",
        "accepted GatewayClass",
        |object| {
            conditions_ready(
                object.data.pointer("/status/conditions"),
                object.metadata.generation.unwrap_or_default(),
                &["Accepted"],
            )
        },
    )
    .await?;
    let gateway = wait_resource(
        &resources(
            client.clone(),
            "gateway.networking.k8s.io",
            "Gateway",
            "gateways",
        ),
        "kuberic",
        "programmed Gateway",
        |object| {
            conditions_ready(
                object.data.pointer("/status/conditions"),
                object.metadata.generation.unwrap_or_default(),
                &["Accepted", "Programmed"],
            )
        },
    )
    .await?;
    ensure!(
        gateway.data["spec"]["gatewayClassName"] == "kuberic-envoy",
        "unexpected GatewayClass"
    );
    for application in APPLICATIONS {
        let route = wait_resource(
            &resources(
                client.clone(),
                "gateway.networking.k8s.io",
                "GRPCRoute",
                "grpcroutes",
            ),
            application,
            "accepted and resolved GRPCRoute",
            |object| {
                object
                    .data
                    .pointer("/status/parents")
                    .and_then(Value::as_array)
                    .is_some_and(|parents| {
                        parents.iter().any(|parent| {
                            parent["parentRef"]["name"] == "kuberic"
                                && parent["controllerName"]
                                    == "gateway.envoyproxy.io/gatewayclass-controller"
                                && conditions_ready(
                                    parent.get("conditions"),
                                    object.metadata.generation.unwrap_or_default(),
                                    &["Accepted", "ResolvedRefs"],
                                )
                        })
                    })
            },
        )
        .await?;
        ensure!(
            route.data["spec"]["hostnames"]
                == serde_json::json!([format!("{application}.kuberic.test")]),
            "unexpected route authorities"
        );
        let rules = route.data["spec"]["rules"]
            .as_array()
            .context("route rules missing")?;
        ensure!(rules.len() == 1, "route must have one rule");
        let backends = rules[0]["backendRefs"]
            .as_array()
            .context("route backends missing")?;
        ensure!(
            backends.len() == 1
                && backends[0]["name"] == format!("{application}-rw")
                && backends[0]["port"] == 8080
                && backends[0]["namespace"]
                    .as_str()
                    .is_none_or(|namespace| namespace == NAMESPACE)
                && backends[0]["group"].as_str().is_none_or(str::is_empty)
                && backends[0]["kind"]
                    .as_str()
                    .is_none_or(|kind| kind == "Service"),
            "route must target only its own Service"
        );
        let services: Api<Service> = Api::namespaced(client.clone(), NAMESPACE);
        let service = services.get(&format!("{application}-rw")).await?;
        let spec = service.spec.context("Service has no spec")?;
        ensure!(
            spec.type_.as_deref() == Some("ClusterIP"),
            "backend must remain ClusterIP"
        );
        ensure!(
            spec.selector
                == Some(std::collections::BTreeMap::from([
                    ("kuberic.io/set".into(), application.into()),
                    ("kuberic.io/role".into(), "primary".into()),
                ])),
            "backend must select only its own primary"
        );
    }
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), "envoy-gateway-system");
    let services: Api<Service> = Api::all(client.clone());
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let proxies = deployments.list(&ListParams::default().labels("gateway.envoyproxy.io/owning-gateway-name=kuberic,gateway.envoyproxy.io/owning-gateway-namespace=xedio")).await?;
        let controller = deployments.get("envoy-gateway").await?;
        let deployment_ready = |deployment: &Deployment| {
            deployment.status.as_ref().is_some_and(|status| {
                status.observed_generation == deployment.metadata.generation
                    && status.available_replicas.unwrap_or_default() >= 1
            })
        };
        let services = services.list(&ListParams::default()).await?;
        let owners: Vec<_> = services
            .items
            .iter()
            .filter(|service| {
                service
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.ports.as_ref())
                    .is_some_and(|ports| ports.iter().any(|port| port.node_port == Some(30090)))
            })
            .collect();
        if let [service] = owners.as_slice() {
            let labels = service
                .metadata
                .labels
                .as_ref()
                .context("NodePort owner has no labels")?;
            ensure!(
                labels
                    .get("gateway.envoyproxy.io/owning-gateway-name")
                    .map(String::as_str)
                    == Some("kuberic"),
                "NodePort 30090 belongs to another Service"
            );
            ensure!(
                labels
                    .get("gateway.envoyproxy.io/owning-gateway-namespace")
                    .map(String::as_str)
                    == Some(NAMESPACE),
                "NodePort belongs to another namespace's Gateway"
            );
            ensure!(
                service
                    .spec
                    .as_ref()
                    .unwrap()
                    .ports
                    .as_ref()
                    .unwrap()
                    .iter()
                    .any(|port| port.port == 8080 && port.node_port == Some(30090)),
                "Envoy listener/NodePort mismatch"
            );
            if deployment_ready(&controller)
                && !proxies.items.is_empty()
                && proxies.items.iter().all(deployment_ready)
            {
                break;
            }
        }
        ensure!(
            Instant::now() < deadline,
            "Envoy controller, proxy Deployment or NodePort not ready"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

async fn wait_primary_endpoint(client: Client, application: &str, primary: &str) -> Result<()> {
    let slices: Api<EndpointSlice> = Api::namespaced(client, NAMESPACE);
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let slices = slices
            .list(
                &ListParams::default()
                    .labels(&format!("kubernetes.io/service-name={application}-rw")),
            )
            .await?;
        let ready: Vec<_> = slices
            .items
            .iter()
            .flat_map(|slice| &slice.endpoints)
            .filter(|endpoint| {
                endpoint.conditions.as_ref().is_some_and(|conditions| {
                    conditions.ready == Some(true) && conditions.terminating != Some(true)
                })
            })
            .collect();
        if !ready.is_empty()
            && ready.iter().all(|endpoint| {
                endpoint
                    .target_ref
                    .as_ref()
                    .and_then(|target| target.name.as_deref())
                    == Some(primary)
            })
        {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "{application}-rw endpoints do not select promoted primary {primary}: {slices:?}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn round_trip(client: &mut KvStoreClient<Channel>, key: &str, value: &str) -> Result<()> {
    client
        .put(PutRequest {
            key: key.into(),
            value: value.into(),
        })
        .await?;
    let result = client
        .get(GetRequest { key: key.into() })
        .await?
        .into_inner();
    ensure!(
        result.found && result.value == value,
        "unexpected routed value for {key}: {result:?}"
    );
    Ok(())
}

async fn reconnect(application: &str, key: &str, value: &str) -> Result<KvStoreClient<Channel>> {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let result = async {
            let mut client =
                connect_gateway(ENDPOINT, &format!("{application}.kuberic.test")).await?;
            round_trip(&mut client, key, value).await?;
            Ok::<_, anyhow::Error>(client)
        }
        .await;
        match result {
            Ok(client) => return Ok(client),
            Err(error) if Instant::now() >= deadline => {
                return Err(error.context(format!("reconnect timed out for {application}")));
            }
            Err(error) => {
                tracing::info!(application, %error, "retrying a fresh Gateway connection")
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn verify_isolation(clients: &mut [KvStoreClient<Channel>; 2]) -> Result<()> {
    for (index, client) in clients.iter_mut().enumerate() {
        let result = client
            .get(GetRequest {
                key: "shared-key".into(),
            })
            .await?
            .into_inner();
        ensure!(
            result.found && result.value == APPLICATIONS[index],
            "route crossed application boundary: {result:?}"
        );
        let other_key = format!("only-{}", APPLICATIONS[1 - index]);
        ensure!(
            !client
                .get(GetRequest { key: other_key })
                .await?
                .into_inner()
                .found,
            "data leaked between applications"
        );
    }
    Ok(())
}

async fn switch_primary(
    client: Client,
    application: &str,
    existing: &mut KvStoreClient<Channel>,
) -> Result<()> {
    let before = wait_set(client.clone(), application, None).await?;
    let primary = before.data["status"]["currentPrimary"]
        .as_str()
        .context("missing primary")?;
    let target = before.data["status"]["members"]
        .as_array()
        .context("missing members")?
        .iter()
        .filter_map(|member| member["name"].as_str())
        .find(|name| *name != primary)
        .context("missing switchover target")?
        .to_string();
    resources(client.clone(), "kuberic.io", "KubericSet", "kubericsets")
        .patch_status(
            application,
            &PatchParams::default(),
            &Patch::Merge(serde_json::json!({"status": {"targetPrimary": target}})),
        )
        .await?;
    match existing
        .get(GetRequest {
            key: "shared-key".into(),
        })
        .await
    {
        Ok(response) => ensure!(
            response.into_inner().value == application,
            "old connection returned another application's data"
        ),
        Err(error) => {
            tracing::info!(application, %error, "existing connection interrupted by primary transition")
        }
    }
    wait_set(client.clone(), application, Some(&target)).await?;
    wait_primary_endpoint(client, application, &target).await?;
    let replacement = reconnect(application, "after-switchover", application).await?;
    *existing = replacement;
    tracing::info!(
        application,
        old_primary = primary,
        new_primary = target,
        "reconnected through unchanged authority and host port"
    );
    Ok(())
}

async fn scenario() -> Result<()> {
    ensure!(
        crate::test_utils::isolated_kvstore_endpoint() == ENDPOINT,
        "Gateway test requires its dedicated fixed-port KinD config"
    );
    let client = crate::test_utils::isolated_kube_client().await;
    for application in APPLICATIONS {
        let object = wait_set(client.clone(), application, None).await?;
        wait_primary_endpoint(
            client.clone(),
            application,
            object.data["status"]["currentPrimary"].as_str().unwrap(),
        )
        .await?;
    }
    wait_gateway(client.clone()).await?;
    let mut clients = [
        reconnect(APPLICATIONS[0], "shared-key", APPLICATIONS[0]).await?,
        reconnect(APPLICATIONS[1], "shared-key", APPLICATIONS[1]).await?,
    ];
    for (index, routed) in clients.iter_mut().enumerate() {
        round_trip(
            routed,
            &format!("only-{}", APPLICATIONS[index]),
            APPLICATIONS[index],
        )
        .await?;
    }
    verify_isolation(&mut clients).await?;
    let mut unknown = connect_gateway(ENDPOINT, "unknown.kuberic.test").await?;
    ensure!(
        unknown
            .get(GetRequest {
                key: "shared-key".into()
            })
            .await
            .is_err(),
        "unknown authority reached a backend"
    );

    for index in 0..APPLICATIONS.len() {
        let other = 1 - index;
        let stopping = CancellationToken::new();
        let mut unaffected = clients[other].clone();
        let monitor = async {
            let mut probes = 0;
            loop {
                round_trip(&mut unaffected, "unaffected-probe", APPLICATIONS[other])
                    .await
                    .context("unaffected application's route failed during peer switchover")?;
                probes += 1;
                tokio::select! {
                    _ = stopping.cancelled() => return Ok::<_, anyhow::Error>(probes),
                    _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                }
            }
        };
        let transition = async {
            let result =
                switch_primary(client.clone(), APPLICATIONS[index], &mut clients[index]).await;
            stopping.cancel();
            result
        };
        let (transition, monitor) = tokio::join!(transition, monitor);
        transition?;
        ensure!(monitor? > 0, "unaffected route was not exercised");
        verify_isolation(&mut clients).await?;
    }
    wait_gateway(client).await?;
    Ok(())
}

#[tokio::test]
#[test_log::test]
#[ignore = "requires the separate Envoy Gateway KinD deployment; run just gateway-test"]
async fn test_gateway_k8s_multi_application() {
    let result = tokio::time::timeout(Duration::from_secs(900), scenario()).await;
    if !matches!(result, Ok(Ok(()))) {
        let diagnostic = tokio::process::Command::new("bash")
            .arg(crate::test_utils::get_repo_root().join("scripts/gateway_kind.sh"))
            .arg("diagnostics")
            .kill_on_drop(true)
            .status();
        let _ = tokio::time::timeout(Duration::from_secs(180), diagnostic).await;
        panic!("Gateway integration failed: {result:?}");
    }
}

#[test]
fn gateway_readiness_rejects_stale_and_unresolved_conditions() {
    let mut conditions = serde_json::json!([
        {"type": "Accepted", "status": "True", "observedGeneration": 2},
        {"type": "ResolvedRefs", "status": "True", "observedGeneration": 2},
    ]);
    assert!(conditions_ready(
        Some(&conditions),
        2,
        &["Accepted", "ResolvedRefs"]
    ));
    assert!(!conditions_ready(
        Some(&conditions),
        3,
        &["Accepted", "ResolvedRefs"]
    ));
    conditions[1]["status"] = "False".into();
    assert!(!conditions_ready(
        Some(&conditions),
        2,
        &["Accepted", "ResolvedRefs"]
    ));
    assert!(!conditions_ready(None, 2, &["Accepted"]));
}

#[tokio::test]
async fn gateway_client_dials_loopback_with_distinct_authorities() {
    use std::convert::Infallible;
    use std::future::{Ready, ready};
    use std::task::{Context, Poll};
    use tonic::codegen::Service;
    use tonic::codegen::http::{Request, Response};

    #[derive(Clone)]
    struct AuthorityRecorder(tokio::sync::mpsc::UnboundedSender<String>);

    impl tonic::server::NamedService for AuthorityRecorder {
        const NAME: &'static str = "kvstore.v1.KvStore";
    }

    impl Service<Request<tonic::body::Body>> for AuthorityRecorder {
        type Response = Response<tonic::body::Body>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: Request<tonic::body::Body>) -> Self::Future {
            self.0
                .send(request.uri().authority().unwrap().to_string())
                .unwrap();
            ready(Ok(Response::builder()
                .header("content-type", "application/grpc")
                .header("grpc-status", "12")
                .body(tonic::body::Body::empty())
                .unwrap()))
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let (sender, mut received) = tokio::sync::mpsc::unbounded_channel();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let stopping = shutdown.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(AuthorityRecorder(sender))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                stopping.cancelled(),
            )
            .await
            .unwrap();
    });
    for authority in ["kvstore-a.kuberic.test", "kvstore-b.kuberic.test"] {
        let mut client = connect_gateway(&address, authority).await.unwrap();
        assert_eq!(
            client
                .get(kvstore::proto::GetRequest {
                    key: "probe".into()
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unimplemented
        );
        assert_eq!(received.recv().await.unwrap(), authority);
    }
    shutdown.cancel();
    server.await.unwrap();
}
