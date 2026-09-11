use std::time::Duration;

use kuberic_core::driver::ReplicaHandle;
use kuberic_core::grpc::handle::GrpcReplicaHandle;
use kuberic_core::pod::PodRuntime;
use kuberic_core::types::{
    CorrelatedControlActionRequest, DurableActionState, DurableReplicaAction, Epoch, OpenMode,
    ReplicaInfo, ReplicaInstanceId, ReplicaSetConfig, ReplicaStatus, Role,
};
use kuberic_rocksdb::{Mutation, RocksReplica};

struct Pod {
    replica: RocksReplica,
    handle: GrpcReplicaHandle,
    runtime: tokio::task::JoinHandle<()>,
    service: tokio::task::JoinHandle<()>,
}

impl Pod {
    async fn start(id: i64, path: std::path::PathBuf) -> Self {
        let replica = RocksReplica::open(path).await.unwrap();
        let instance_id =
            ReplicaInstanceId::new(format!("rocks-{id}-{:032x}", rand::random::<u128>()));
        let data_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let data_bind = data_listener.local_addr().unwrap().to_string();
        drop(data_listener);
        let bundle = PodRuntime::builder(id)
            .instance_id(instance_id.clone())
            .control_bind("127.0.0.1:0".into())
            .data_bind(data_bind.clone())
            .build()
            .await
            .unwrap();
        let address = bundle.control_address.clone();
        let runtime = tokio::spawn(bundle.runtime.serve());
        let service = tokio::spawn(replica.clone().run(bundle.lifecycle_rx));
        let handle =
            GrpcReplicaHandle::connect(id, instance_id, address, format!("http://{data_bind}"))
                .await
                .unwrap();
        Self {
            replica,
            handle,
            runtime,
            service,
        }
    }

    async fn execute(&self, action: DurableReplicaAction) {
        let status = self.handle.get_status().await.unwrap();
        let action_id = format!("test-{:032x}", rand::random::<u128>());
        let ack = self
            .handle
            .execute_correlated_control_action(CorrelatedControlActionRequest {
                protocol_version: kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
                action_id: action_id.clone(),
                input_signature: action.signature(),
                target_replica_id: self.handle.id(),
                target_instance_id: status.instance_id,
                expected_agent_generation: status.agent.generation,
                expected_control_version: status.agent.control_version,
                observed_runtime_epoch: status.epoch,
                action,
            })
            .await
            .unwrap();
        assert_ne!(
            ack.observation.action.state,
            DurableActionState::Failed,
            "{:?}",
            ack.observation.action.error
        );
        if ack.observation.action.state == DurableActionState::Completed {
            return;
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let status = self.handle.get_status().await.unwrap();
                if let Some(terminal) = status
                    .agent
                    .retained_terminal_actions
                    .iter()
                    .find(|entry| entry.action.action_id == action_id)
                {
                    assert_eq!(
                        terminal.action.state,
                        DurableActionState::Completed,
                        "{:?}",
                        terminal.action.error
                    );
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    async fn info(&self) -> ReplicaInfo {
        let status = self.handle.get_status().await.unwrap();
        ReplicaInfo {
            id: self.handle.id(),
            instance_id: status.instance_id,
            role: status.role,
            status: ReplicaStatus::Up,
            replicator_address: self.handle.replicator_address(),
            current_progress: status.current_progress,
            catch_up_capability: status.catch_up_capability.unwrap_or_default(),
            must_catch_up: false,
        }
    }
}

impl Drop for Pod {
    fn drop(&mut self) {
        self.runtime.abort();
        self.service.abort();
    }
}

fn put(key: &str, value: &str) -> Mutation {
    Mutation::Put {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    }
}

#[tokio::test]
async fn three_replicas_copy_quorum_failover_restart_and_fencing() {
    let directory = tempfile::tempdir().unwrap();
    let primary = Pod::start(1, directory.path().join("one")).await;
    let second = Pod::start(2, directory.path().join("two")).await;
    let mut third = Pod::start(3, directory.path().join("three")).await;
    let epoch = Epoch::new(0, 1);
    for pod in [&primary, &second, &third] {
        pod.execute(DurableReplicaAction::Open {
            mode: OpenMode::New,
        })
        .await;
    }
    primary
        .execute(DurableReplicaAction::ChangeRole {
            epoch,
            role: Role::Primary,
        })
        .await;
    primary
        .execute(DurableReplicaAction::UpdateCurrentConfiguration {
            current: ReplicaSetConfig {
                members: vec![],
                write_quorum: 1,
            },
        })
        .await;
    primary
        .replica
        .write(vec![put("seed", "copied")])
        .await
        .unwrap();
    for secondary in [&second, &third] {
        secondary
            .execute(DurableReplicaAction::ChangeRole {
                epoch,
                role: Role::IdleSecondary,
            })
            .await;
        primary
            .execute(DurableReplicaAction::BuildReplica {
                replica: secondary.info().await,
            })
            .await;
        assert_eq!(secondary.replica.applied_lsn().await.unwrap(), 1);
        secondary
            .execute(DurableReplicaAction::ChangeRole {
                epoch,
                role: Role::ActiveSecondary,
            })
            .await;
    }
    let configuration = ReplicaSetConfig {
        members: vec![second.info().await, third.info().await],
        write_quorum: 3,
    };
    primary
        .execute(DurableReplicaAction::UpdateCatchUpConfiguration {
            current: configuration.clone(),
            previous: ReplicaSetConfig {
                members: vec![],
                write_quorum: 0,
            },
        })
        .await;
    primary
        .execute(DurableReplicaAction::UpdateCurrentConfiguration {
            current: configuration,
        })
        .await;
    let mut writers = Vec::new();
    for index in 0..12 {
        let replica = primary.replica.clone();
        writers.push(tokio::spawn(async move {
            replica
                .write(vec![put(&format!("key-{index}"), "value")])
                .await
                .unwrap()
        }));
    }
    let mut lsns = Vec::new();
    for writer in writers {
        lsns.push(writer.await.unwrap());
    }
    lsns.sort();
    assert_eq!(lsns, (2..=13).collect::<Vec<_>>());
    let committed = primary
        .replica
        .write(vec![
            put("left", "value"),
            put("right", "value"),
            Mutation::Merge {
                key: b"left".to_vec(),
                value: b"+merge".to_vec(),
            },
            Mutation::Delete {
                key: b"seed".to_vec(),
            },
        ])
        .await
        .unwrap();
    assert_eq!(second.replica.applied_lsn().await.unwrap(), committed);
    assert_eq!(third.replica.applied_lsn().await.unwrap(), committed);
    assert!(
        second
            .replica
            .write(vec![put("forbidden", "value")])
            .await
            .is_err()
    );
    assert!(
        primary
            .replica
            .write_column_family("other", vec![put("forbidden", "value")])
            .await
            .is_err()
    );
    primary
        .execute(DurableReplicaAction::RevokeWriteStatus)
        .await;
    assert!(
        primary
            .replica
            .write(vec![put("fenced", "value")])
            .await
            .is_err()
    );
    primary.runtime.abort();
    primary.service.abort();
    let epoch = Epoch::new(0, 2);
    second
        .execute(DurableReplicaAction::UpdateEpoch { epoch })
        .await;
    second
        .execute(DurableReplicaAction::ChangeRole {
            epoch,
            role: Role::Primary,
        })
        .await;
    third
        .execute(DurableReplicaAction::UpdateEpoch { epoch })
        .await;
    second
        .execute(DurableReplicaAction::UpdateCatchUpConfiguration {
            current: ReplicaSetConfig {
                members: vec![third.info().await],
                write_quorum: 2,
            },
            previous: ReplicaSetConfig {
                members: vec![],
                write_quorum: 0,
            },
        })
        .await;
    second
        .execute(DurableReplicaAction::UpdateCurrentConfiguration {
            current: ReplicaSetConfig {
                members: vec![third.info().await],
                write_quorum: 2,
            },
        })
        .await;
    assert_eq!(
        second.replica.get(b"left".to_vec()).await.unwrap(),
        Some(b"value+merge".to_vec())
    );
    assert_eq!(
        second.replica.get(b"right".to_vec()).await.unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(second.replica.get(b"seed".to_vec()).await.unwrap(), None);
    second
        .replica
        .write(vec![put("after-failover", "durable")])
        .await
        .unwrap();
    let epoch = Epoch::new(0, 3);
    second
        .execute(DurableReplicaAction::RevokeWriteStatus)
        .await;
    second
        .execute(DurableReplicaAction::ChangeRole {
            epoch,
            role: Role::ActiveSecondary,
        })
        .await;
    third
        .execute(DurableReplicaAction::ChangeRole {
            epoch,
            role: Role::Primary,
        })
        .await;
    third
        .execute(DurableReplicaAction::UpdateCurrentConfiguration {
            current: ReplicaSetConfig {
                members: vec![],
                write_quorum: 1,
            },
        })
        .await;
    assert_eq!(
        third.replica.get(b"after-failover".to_vec()).await.unwrap(),
        Some(b"durable".to_vec())
    );
    assert!(
        second
            .replica
            .write(vec![put("stale", "value")])
            .await
            .is_err()
    );
    third.execute(DurableReplicaAction::Close).await;
    let lsn = third.replica.applied_lsn().await.unwrap();
    (&mut third.service).await.unwrap();
    drop(third);
    let restarted = Pod::start(3, directory.path().join("three")).await;
    restarted
        .execute(DurableReplicaAction::Open {
            mode: OpenMode::Existing,
        })
        .await;
    restarted
        .execute(DurableReplicaAction::ChangeRole {
            epoch: Epoch::new(0, 4),
            role: Role::Primary,
        })
        .await;
    restarted
        .execute(DurableReplicaAction::UpdateCurrentConfiguration {
            current: ReplicaSetConfig {
                members: vec![],
                write_quorum: 1,
            },
        })
        .await;
    assert_eq!(restarted.replica.applied_lsn().await.unwrap(), lsn);
    assert_eq!(
        restarted
            .replica
            .get(b"after-failover".to_vec())
            .await
            .unwrap(),
        Some(b"durable".to_vec())
    );
}
