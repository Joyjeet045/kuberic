use std::collections::BTreeSet;

use async_trait::async_trait;
use futures::future::{join_all, try_join_all};
use kuberic_runtime::native::{
    MAX_LEASE_MILLIS, NativeAuthority, NativeError, NativeObservation, NativeOperation,
    NativeOperationCommand, NativeOperationStatus,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeNodePlan {
    pub node: String,
    pub operation: Option<NativeOperation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePlan {
    pub revision: u64,
    pub enabled: bool,
    pub lease_millis: u64,
    pub nodes: Vec<NativeNodePlan>,
}

#[async_trait]
pub trait NativeNodeApi: Sync {
    async fn observe(&self, node: &str) -> Result<NativeObservation, NativeError>;
    async fn authorize(&self, node: &str, authority: &NativeAuthority) -> Result<(), NativeError>;
    async fn status(
        &self,
        node: &str,
        operation: &NativeOperation,
    ) -> Result<Option<NativeOperationStatus>, NativeError>;
    async fn apply(
        &self,
        node: &str,
        command: &NativeOperationCommand,
    ) -> Result<NativeOperationStatus, NativeError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativePlanStatus {
    Pending,
    Applied,
}

pub async fn reconcile(
    api: &impl NativeNodeApi,
    plan: &NativePlan,
) -> Result<NativePlanStatus, NativeError> {
    let invalid = |message: &str| NativeError::Application(message.into());
    let closed_revision = plan
        .revision
        .checked_mul(2)
        .filter(|revision| *revision > 0 && *revision < u64::MAX)
        .ok_or(NativeError::Revision)?;
    if plan.lease_millis == 0 || plan.lease_millis > MAX_LEASE_MILLIS {
        return Err(NativeError::Lease);
    }
    let mut nodes = BTreeSet::new();
    if plan.nodes.is_empty()
        || plan
            .nodes
            .iter()
            .any(|node| node.node.is_empty() || !nodes.insert(&node.node))
    {
        return Err(invalid(
            "native plan must contain unique, nonempty node identities",
        ));
    }
    let observations = join_all(plan.nodes.iter().map(|node| api.observe(&node.node))).await;
    let mut incarnations = BTreeSet::new();
    for observation in observations
        .iter()
        .filter_map(|observation| observation.as_ref().ok())
    {
        if observation.revision > closed_revision + 1 {
            return Err(NativeError::Revision);
        }
        if observation.incarnation.is_empty() || !incarnations.insert(&observation.incarnation) {
            return Err(invalid(
                "native plan resolved multiple nodes to the same incarnation",
            ));
        }
    }
    let authority = |observation: &NativeObservation, revision, enabled| NativeAuthority {
        incarnation: observation.incarnation.clone(),
        revision,
        enabled,
        lease_millis: plan.lease_millis,
    };
    let established = plan.nodes.iter().all(|node| node.operation.is_none())
        || observations.iter().any(|observation| {
            observation
                .as_ref()
                .is_ok_and(|observation| observation.revision == closed_revision + 1)
        });
    if established {
        let results = join_all(plan.nodes.iter().zip(observations).map(|(node, observed)| {
            let authority = &authority;
            async move {
                let observation = observed?;
                if let Some(operation) = &node.operation
                    && !matches!(
                        api.status(&node.node, operation).await?,
                        Some(NativeOperationStatus::Complete { .. })
                    )
                {
                    return Err(invalid(
                        "an opened plan is missing native operation evidence; advance its revision",
                    ));
                }
                api.authorize(
                    &node.node,
                    &authority(&observation, closed_revision + 1, plan.enabled),
                )
                .await
            }
        }))
        .await;
        let errors: Vec<String> = results
            .into_iter()
            .filter_map(Result::err)
            .map(|error| error.to_string())
            .collect();
        if !errors.is_empty() {
            return Err(NativeError::Application(errors.join("; ")));
        }
        return Ok(NativePlanStatus::Applied);
    }
    let observations: Vec<NativeObservation> =
        observations.into_iter().collect::<Result<_, _>>()?;
    let complete = try_join_all(plan.nodes.iter().map(|node| async {
        match &node.operation {
            None => Ok(true),
            Some(operation) => Ok(matches!(
                api.status(&node.node, operation).await?,
                Some(NativeOperationStatus::Complete { .. })
            )),
        }
    }))
    .await?;
    if complete.iter().any(|complete| !complete) {
        if observations
            .iter()
            .any(|observation| observation.revision > closed_revision)
        {
            return Err(invalid(
                "an opened plan is missing native operation evidence; advance its revision",
            ));
        }
        try_join_all(
            plan.nodes
                .iter()
                .zip(&observations)
                .map(|(node, observation)| {
                    let authority = authority(observation, closed_revision, false);
                    async move { api.authorize(&node.node, &authority).await }
                }),
        )
        .await?;
        let results = try_join_all(plan.nodes.iter().zip(&observations).zip(&complete).map(
            |((node, observation), complete)| {
                let authority = authority(observation, closed_revision, false);
                async move {
                    if *complete {
                        return Ok(true);
                    }
                    let operation = node
                        .operation
                        .as_ref()
                        .ok_or_else(|| invalid("missing native operation"))?;
                    Ok(matches!(
                        api.apply(
                            &node.node,
                            &NativeOperationCommand {
                                authority,
                                operation: operation.clone(),
                            }
                        )
                        .await?,
                        NativeOperationStatus::Complete { .. }
                    ))
                }
            },
        ))
        .await?;
        if results.iter().any(|complete| !complete) {
            return Ok(NativePlanStatus::Pending);
        }
    }
    try_join_all(
        plan.nodes
            .iter()
            .zip(&observations)
            .map(|(node, observation)| {
                let authority = authority(observation, closed_revision + 1, plan.enabled);
                async move { api.authorize(&node.node, &authority).await }
            }),
    )
    .await?;
    Ok(NativePlanStatus::Applied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kuberic_runtime::native::{NativeGate, NativeHealth};
    use std::sync::Mutex;

    struct Fake {
        gates: Mutex<Vec<NativeGate>>,
        complete: Mutex<Vec<bool>>,
        offline: Mutex<Vec<bool>>,
    }

    #[async_trait]
    impl NativeNodeApi for Fake {
        async fn observe(&self, node: &str) -> Result<NativeObservation, NativeError> {
            let index: usize = node.parse().unwrap();
            if self.offline.lock().unwrap()[index] {
                return Err(NativeError::Application("node unavailable".into()));
            }
            Ok(
                self.gates.lock().unwrap()[index].observation(Ok(NativeHealth {
                    live: true,
                    ready: true,
                    readable: true,
                    writable: false,
                })),
            )
        }

        async fn authorize(
            &self,
            node: &str,
            authority: &NativeAuthority,
        ) -> Result<(), NativeError> {
            self.gates.lock().unwrap()[node.parse::<usize>().unwrap()].authorize(authority)
        }

        async fn status(
            &self,
            node: &str,
            _: &NativeOperation,
        ) -> Result<Option<NativeOperationStatus>, NativeError> {
            Ok(
                self.complete.lock().unwrap()[node.parse::<usize>().unwrap()].then(|| {
                    NativeOperationStatus::Complete {
                        evidence: serde_json::json!({"native": true}),
                    }
                }),
            )
        }

        async fn apply(
            &self,
            node: &str,
            command: &NativeOperationCommand,
        ) -> Result<NativeOperationStatus, NativeError> {
            assert!(!command.authority.enabled);
            assert!(
                self.gates
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|gate| gate.admit().is_err())
            );
            self.complete.lock().unwrap()[node.parse::<usize>().unwrap()] = true;
            Ok(NativeOperationStatus::Pending)
        }
    }

    #[tokio::test]
    async fn all_nodes_are_fenced_before_native_work_and_only_evidence_allows_reopening() {
        let api = Fake {
            gates: Mutex::new(vec![NativeGate::new(), NativeGate::new()]),
            complete: Mutex::new(vec![false, false]),
            offline: Mutex::new(vec![false, false]),
        };
        let mut plan = NativePlan {
            revision: 1,
            enabled: true,
            lease_millis: 10_000,
            nodes: (0..2)
                .map(|index| NativeNodePlan {
                    node: index.to_string(),
                    operation: Some(NativeOperation {
                        id: "expand-1".into(),
                        request: serde_json::json!({"pool": 1}),
                    }),
                })
                .collect(),
        };
        assert_eq!(
            reconcile(&api, &plan).await.unwrap(),
            NativePlanStatus::Pending
        );
        assert!(
            api.gates
                .lock()
                .unwrap()
                .iter()
                .all(|gate| gate.admit().is_err())
        );
        assert_eq!(
            reconcile(&api, &plan).await.unwrap(),
            NativePlanStatus::Applied
        );
        assert!(
            api.gates
                .lock()
                .unwrap()
                .iter()
                .all(|gate| gate.admit().is_ok())
        );
        assert_eq!(
            reconcile(&api, &plan).await.unwrap(),
            NativePlanStatus::Applied
        );
        plan.revision = 2;
        plan.enabled = false;
        assert_eq!(
            reconcile(&api, &plan).await.unwrap(),
            NativePlanStatus::Applied
        );
        assert!(
            api.gates
                .lock()
                .unwrap()
                .iter()
                .all(|gate| gate.admit().is_err())
        );
        plan.revision = 1;
        assert_eq!(reconcile(&api, &plan).await, Err(NativeError::Revision));
    }

    #[tokio::test]
    async fn unavailable_peers_do_not_replace_native_quorum_with_an_all_node_requirement() {
        let api = Fake {
            gates: Mutex::new(vec![NativeGate::new(), NativeGate::new()]),
            complete: Mutex::new(vec![false, false]),
            offline: Mutex::new(vec![false, true]),
        };
        let plan = NativePlan {
            revision: 1,
            enabled: true,
            lease_millis: 10_000,
            nodes: (0..2)
                .map(|index| NativeNodePlan {
                    node: index.to_string(),
                    operation: None,
                })
                .collect(),
        };
        assert!(reconcile(&api, &plan).await.is_err());
        assert!(api.gates.lock().unwrap()[0].admit().is_ok());
        assert!(api.gates.lock().unwrap()[1].admit().is_err());
        api.offline.lock().unwrap()[1] = false;
        assert_eq!(
            reconcile(&api, &plan).await.unwrap(),
            NativePlanStatus::Applied
        );
        assert!(api.gates.lock().unwrap()[1].admit().is_ok());
    }
}
