use std::path::PathBuf;
use std::time::Duration;

use kuberic_core::driver::ReplicaHandle;
use kuberic_core::grpc::handle::GrpcReplicaHandle;
use kuberic_core::pod::PodRuntime;
use kuberic_core::types::{
    CorrelatedControlActionRequest, DurableActionState, DurableReplicaAction, Epoch, OpenMode,
    ReplicaInfo, ReplicaInstanceId, ReplicaSetConfig, ReplicaStatus, Role,
};
use kuberic_state_manager::{Error, StateManager, TransactionOptions};

struct Pod {
    manager: StateManager,
    handle: GrpcReplicaHandle,
    runtime: tokio::task::JoinHandle<()>,
    service: tokio::task::JoinHandle<()>,
}

impl Pod {
    async fn start(id: i64, path: PathBuf) -> Self {
        let manager = StateManager::open(path).await.unwrap();
        let instance_id =
            ReplicaInstanceId::new(format!("rc-{id}-{:032x}", rand::random::<u128>()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let data = listener.local_addr().unwrap().to_string();
        drop(listener);
        let bundle = PodRuntime::builder(id)
            .instance_id(instance_id.clone())
            .control_bind("127.0.0.1:0".into())
            .data_bind(data.clone())
            .build()
            .await
            .unwrap();
        let address = bundle.control_address.clone();
        let runtime = tokio::spawn(bundle.runtime.serve());
        let service = tokio::spawn(manager.clone().run(bundle.lifecycle_rx));
        let handle = GrpcReplicaHandle::connect(id, instance_id, address, format!("http://{data}"))
            .await
            .unwrap();
        Self {
            manager,
            handle,
            runtime,
            service,
        }
    }

    async fn execute(&self, action: DurableReplicaAction) {
        let status = self.handle.get_status().await.unwrap();
        let action_id = format!("action-{:032x}", rand::random::<u128>());
        let result = self
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
            result.observation.action.state,
            DurableActionState::Failed,
            "{:?}",
            result.observation.action.error
        );
        if result.observation.action.state == DurableActionState::Completed {
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

    async fn primary(&self, mode: OpenMode, epoch: Epoch) {
        self.execute(DurableReplicaAction::Open { mode }).await;
        self.execute(DurableReplicaAction::ChangeRole {
            epoch,
            role: Role::Primary,
        })
        .await;
        self.execute(DurableReplicaAction::UpdateCurrentConfiguration {
            current: ReplicaSetConfig {
                members: vec![],
                write_quorum: 1,
            },
        })
        .await;
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

    async fn add(&self, secondary: &Self) {
        secondary
            .execute(DurableReplicaAction::Open {
                mode: OpenMode::New,
            })
            .await;
        secondary
            .execute(DurableReplicaAction::ChangeRole {
                epoch: Epoch::new(0, 1),
                role: Role::IdleSecondary,
            })
            .await;
        self.execute(DurableReplicaAction::BuildReplica {
            replica: secondary.info().await,
        })
        .await;
        secondary
            .execute(DurableReplicaAction::ChangeRole {
                epoch: Epoch::new(0, 1),
                role: Role::ActiveSecondary,
            })
            .await;
    }

    async fn configure(&self, secondaries: &[&Self]) {
        let mut members = Vec::new();
        for secondary in secondaries {
            members.push(secondary.info().await);
        }
        let config = ReplicaSetConfig {
            members,
            write_quorum: secondaries.len() as u32 + 1,
        };
        self.execute(DurableReplicaAction::UpdateCatchUpConfiguration {
            current: config.clone(),
            previous: ReplicaSetConfig {
                members: vec![],
                write_quorum: 0,
            },
        })
        .await;
        self.execute(DurableReplicaAction::UpdateCurrentConfiguration { current: config })
            .await;
    }
}

impl Drop for Pod {
    fn drop(&mut self) {
        self.runtime.abort();
        self.service.abort();
    }
}

#[tokio::test]
async fn dictionary_operations_conflicts_and_provider_lifetimes() {
    let directory = tempfile::tempdir().unwrap();
    let pod = Pod::start(1, directory.path().join("one")).await;
    pod.primary(OpenMode::New, Epoch::new(0, 1)).await;
    let mut create = pod.manager.create_transaction().await.unwrap();
    let accounts = create
        .get_or_add_dictionary::<String, i64>("accounts")
        .unwrap();
    let audit = create
        .get_or_add_dictionary::<String, String>("audit")
        .unwrap();
    assert!(accounts.insert(&mut create, &"alice".into(), &100).unwrap());
    assert!(!accounts.insert(&mut create, &"alice".into(), &999).unwrap());
    assert_eq!(
        accounts.get(&mut create, &"alice".into()).unwrap(),
        Some(100)
    );
    audit
        .set(&mut create, &"event".into(), &"created".into())
        .unwrap();
    assert_eq!(create.provider_names().unwrap(), ["accounts", "audit"]);
    assert_eq!(create.commit().await.unwrap().0, 1);

    let mut first = pod.manager.create_transaction().await.unwrap();
    let mut second = pod.manager.create_transaction().await.unwrap();
    accounts.set(&mut first, &"alice".into(), &90).unwrap();
    accounts.set(&mut second, &"alice".into(), &80).unwrap();
    audit
        .set(&mut first, &"event".into(), &"debited".into())
        .unwrap();
    let (first, second) = tokio::join!(first.commit(), second.commit());
    assert_ne!(first.is_ok(), second.is_ok());
    assert!(matches!(
        first.err().or(second.err()).unwrap(),
        Error::Conflict(_)
    ));

    let mut first = pod.manager.create_transaction().await.unwrap();
    let mut second = pod.manager.create_transaction().await.unwrap();
    accounts.set(&mut first, &"left".into(), &1).unwrap();
    accounts.set(&mut second, &"right".into(), &2).unwrap();
    let (first, second) = tokio::join!(first.commit(), second.commit());
    first.unwrap();
    second.unwrap();
    let mut scan = pod.manager.create_transaction().await.unwrap();
    assert!(accounts.entries(&mut scan).unwrap().len() >= 3);
    let mut insert = pod.manager.create_transaction().await.unwrap();
    accounts.set(&mut insert, &"phantom".into(), &3).unwrap();
    insert.commit().await.unwrap();
    assert!(matches!(scan.commit().await, Err(Error::Conflict(_))));

    let mut operations = pod.manager.create_transaction().await.unwrap();
    assert_eq!(
        accounts
            .get_or_add(&mut operations, &"left".into(), 99)
            .unwrap(),
        1
    );
    assert_eq!(
        accounts
            .add_or_update(&mut operations, &"left".into(), |value| value.unwrap() + 1)
            .unwrap(),
        2
    );
    assert!(
        !accounts
            .update(&mut operations, &"left".into(), &99, &4)
            .unwrap()
    );
    assert!(
        accounts
            .update(&mut operations, &"left".into(), &2, &4)
            .unwrap()
    );
    assert_eq!(
        accounts.remove(&mut operations, &"left".into()).unwrap(),
        Some(4)
    );
    accounts.clear(&mut operations).unwrap();
    assert!(accounts.entries(&mut operations).unwrap().is_empty());
    operations.commit().await.unwrap();
    let mut removal = pod.manager.create_transaction().await.unwrap();
    assert!(
        removal
            .get_dictionary::<String, String>("accounts")
            .is_err()
    );
    assert!(removal.remove_provider("accounts").unwrap());
    let replacement = removal
        .get_or_add_dictionary::<String, i64>("accounts")
        .unwrap();
    assert!(accounts.set(&mut removal, &"stale".into(), &1).is_err());
    replacement.set(&mut removal, &"new".into(), &5).unwrap();
    removal.commit().await.unwrap();
}

#[tokio::test]
async fn read_skew_abort_timeout_and_request_retries() {
    let directory = tempfile::tempdir().unwrap();
    let pod = Pod::start(1, directory.path().join("one")).await;
    pod.primary(OpenMode::New, Epoch::new(0, 1)).await;
    let values = pod
        .manager
        .get_or_add_dictionary::<String, i64>("values")
        .await
        .unwrap();
    let mut first = pod.manager.create_transaction().await.unwrap();
    let mut second = pod.manager.create_transaction().await.unwrap();
    for transaction in [&mut first, &mut second] {
        assert!(!values.contains_key(transaction, &"left".into()).unwrap());
        assert!(!values.contains_key(transaction, &"right".into()).unwrap());
    }
    values.set(&mut first, &"left".into(), &1).unwrap();
    values.set(&mut second, &"right".into(), &1).unwrap();
    first.commit().await.unwrap();
    assert!(matches!(second.commit().await, Err(Error::Conflict(_))));
    let mut aborted = pod.manager.create_transaction().await.unwrap();
    values.set(&mut aborted, &"aborted".into(), &1).unwrap();
    aborted.abort();
    let expired = pod
        .manager
        .transaction_with_options(TransactionOptions {
            timeout: Duration::from_millis(1),
            ..Default::default()
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(matches!(expired.commit().await, Err(Error::Expired)));
    let mut original = pod.manager.create_transaction().await.unwrap();
    let identity = original.id().clone();
    values.set(&mut original, &"retry".into(), &7).unwrap();
    let version = original.commit().await.unwrap();
    let mut duplicate = pod
        .manager
        .create_transaction()
        .await
        .unwrap()
        .with_identity(identity.clone())
        .unwrap();
    values.set(&mut duplicate, &"retry".into(), &7).unwrap();
    assert_eq!(duplicate.commit().await.unwrap(), version);
    assert_eq!(
        pod.manager
            .committed_result(identity.clone())
            .await
            .unwrap(),
        Some(version)
    );
    let mut incompatible = pod
        .manager
        .create_transaction()
        .await
        .unwrap()
        .with_identity(identity)
        .unwrap();
    values.set(&mut incompatible, &"retry".into(), &8).unwrap();
    assert!(matches!(
        incompatible.commit().await,
        Err(Error::DuplicateRequest)
    ));
    let mut read = pod.manager.create_transaction().await.unwrap();
    assert_eq!(values.get(&mut read, &"aborted".into()).unwrap(), None);
    assert_eq!(values.get(&mut read, &"retry".into()).unwrap(), Some(7));
}

#[tokio::test]
async fn absence_and_registry_enumeration_conflicts_are_not_lost() {
    let directory = tempfile::tempdir().unwrap();
    let pod = Pod::start(1, directory.path().join("one")).await;
    pod.primary(OpenMode::New, Epoch::new(0, 1)).await;
    let values = pod
        .manager
        .get_or_add_dictionary::<String, i64>("values")
        .await
        .unwrap();
    let mut absent = pod.manager.create_transaction().await.unwrap();
    assert_eq!(values.get(&mut absent, &"key".into()).unwrap(), None);
    let mut insert = pod.manager.create_transaction().await.unwrap();
    values.set(&mut insert, &"key".into(), &1).unwrap();
    insert.commit().await.unwrap();
    let mut remove = pod.manager.create_transaction().await.unwrap();
    values.remove(&mut remove, &"key".into()).unwrap();
    remove.commit().await.unwrap();
    assert!(matches!(absent.commit().await, Err(Error::Conflict(_))));
    let mut enumeration = pod.manager.create_transaction().await.unwrap();
    assert_eq!(enumeration.provider_names().unwrap(), ["values"]);
    pod.manager
        .get_or_add_dictionary::<String, i64>("new")
        .await
        .unwrap();
    assert!(matches!(
        enumeration.commit().await,
        Err(Error::Conflict(_))
    ));
}

#[tokio::test]
async fn readers_never_observe_half_a_cross_provider_commit() {
    let directory = tempfile::tempdir().unwrap();
    let pod = Pod::start(1, directory.path().join("one")).await;
    pod.primary(OpenMode::New, Epoch::new(0, 1)).await;
    let mut setup = pod.manager.create_transaction().await.unwrap();
    let left = setup.get_or_add_dictionary::<String, i64>("left").unwrap();
    let right = setup.get_or_add_dictionary::<String, i64>("right").unwrap();
    setup.commit().await.unwrap();
    let write = async {
        for version in 1..=30 {
            let mut transaction = pod.manager.create_transaction().await.unwrap();
            left.set(&mut transaction, &"value".into(), &version)
                .unwrap();
            right
                .set(&mut transaction, &"value".into(), &version)
                .unwrap();
            transaction.commit().await.unwrap();
        }
    };
    let read = async {
        for _ in 0..100 {
            let mut transaction = pod.manager.create_transaction().await.unwrap();
            assert_eq!(
                left.get(&mut transaction, &"value".into()).unwrap(),
                right.get(&mut transaction, &"value".into()).unwrap()
            );
            transaction.abort();
        }
    };
    tokio::join!(write, read);
}

#[tokio::test]
async fn incomplete_copy_never_advances_progress_or_allows_promotion() {
    let directory = tempfile::tempdir().unwrap();
    let primary = Pod::start(1, directory.path().join("one")).await;
    primary.primary(OpenMode::New, Epoch::new(0, 1)).await;
    primary
        .manager
        .get_or_add_dictionary::<String, i64>("values")
        .await
        .unwrap();
    let backup = directory.path().join("backup");
    primary.manager.backup(backup.clone()).await.unwrap();
    for (index, corrupt) in [true, false].into_iter().enumerate() {
        let path = directory.path().join(format!("secondary-{index}"));
        let secondary = Pod::start(index as i64 + 2, path.clone()).await;
        secondary
            .execute(DurableReplicaAction::Open {
                mode: OpenMode::New,
            })
            .await;
        secondary
            .execute(DurableReplicaAction::ChangeRole {
                epoch: Epoch::new(0, 1),
                role: Role::IdleSecondary,
            })
            .await;
        let payload = if corrupt {
            b"corrupted-checkpoint".to_vec()
        } else {
            std::fs::create_dir(path.join("active")).unwrap();
            std::fs::read(&backup).unwrap()
        };
        let mut client =
            kuberic_core::proto::replicator_data_client::ReplicatorDataClient::connect(
                secondary.handle.replicator_address(),
            )
            .await
            .unwrap();
        let copy = client
            .copy_stream(tokio_stream::iter([
                kuberic_core::proto::CopyItem {
                    lsn: 1,
                    data: payload,
                    is_boundary: false,
                },
                kuberic_core::proto::CopyItem {
                    lsn: 1,
                    data: vec![],
                    is_boundary: true,
                },
            ]))
            .await;
        assert!(copy.is_err());
        let status = secondary.handle.get_status().await.unwrap();
        assert_eq!(status.current_progress, 0);
        assert_eq!(status.committed_lsn, 0);
        let action = DurableReplicaAction::ChangeRole {
            epoch: Epoch::new(0, 2),
            role: Role::Primary,
        };
        let result = secondary
            .handle
            .execute_correlated_control_action(CorrelatedControlActionRequest {
                protocol_version: kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
                action_id: "promote-failed-copy".into(),
                input_signature: action.signature(),
                target_replica_id: secondary.handle.id(),
                target_instance_id: status.instance_id,
                expected_agent_generation: status.agent.generation,
                expected_control_version: status.agent.control_version,
                observed_runtime_epoch: status.epoch,
                action,
            })
            .await;
        if let Ok(result) = result {
            assert_eq!(result.observation.action.state, DurableActionState::Failed);
        }
        assert!(secondary.manager.create_transaction().await.is_err());
        assert_eq!(secondary.manager.applied_lsn().await.unwrap(), 0);
    }
}

#[tokio::test]
async fn cross_provider_copy_failover_checkpoint_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let primary = Pod::start(1, directory.path().join("one")).await;
    let second = Pod::start(2, directory.path().join("two")).await;
    let mut third = Pod::start(3, directory.path().join("three")).await;
    primary.primary(OpenMode::New, Epoch::new(0, 1)).await;
    let mut create = primary.manager.create_transaction().await.unwrap();
    let left = create.get_or_add_dictionary::<String, i64>("left").unwrap();
    let right = create
        .get_or_add_dictionary::<String, i64>("right")
        .unwrap();
    left.set(&mut create, &"balance".into(), &100).unwrap();
    right.set(&mut create, &"balance".into(), &100).unwrap();
    create.commit().await.unwrap();
    primary.manager.checkpoint().await.unwrap();
    primary.add(&second).await;
    primary.add(&third).await;
    primary.configure(&[&second, &third]).await;
    let mut transfer = primary.manager.create_transaction().await.unwrap();
    left.set(&mut transfer, &"balance".into(), &90).unwrap();
    right.set(&mut transfer, &"balance".into(), &110).unwrap();
    let identity = transfer.id().clone();
    let version = transfer.commit().await.unwrap();
    assert_eq!(second.manager.applied_lsn().await.unwrap(), version.0);
    assert_eq!(third.manager.applied_lsn().await.unwrap(), version.0);
    primary
        .execute(DurableReplicaAction::RevokeWriteStatus)
        .await;
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
    second.configure(&[&third]).await;
    assert_eq!(
        second
            .manager
            .committed_result(identity.clone())
            .await
            .unwrap(),
        Some(version)
    );
    let mut read = second.manager.create_transaction().await.unwrap();
    let left = read.get_dictionary::<String, i64>("left").unwrap().unwrap();
    let right = read
        .get_dictionary::<String, i64>("right")
        .unwrap()
        .unwrap();
    assert_eq!(left.get(&mut read, &"balance".into()).unwrap(), Some(90));
    assert_eq!(right.get(&mut read, &"balance".into()).unwrap(), Some(110));
    read.commit().await.unwrap();
    second.manager.checkpoint().await.unwrap();
    let backup = directory.path().join("backup");
    second.manager.backup(backup.clone()).await.unwrap();
    let restored = StateManager::open(directory.path().join("restored"))
        .await
        .unwrap();
    restored.restore_backup(backup).await.unwrap();
    assert_eq!(
        restored.applied_lsn().await.unwrap(),
        second.manager.applied_lsn().await.unwrap()
    );
    third.execute(DurableReplicaAction::Close).await;
    (&mut third.service).await.unwrap();
    drop(third);
    let restarted = Pod::start(3, directory.path().join("three")).await;
    restarted
        .primary(OpenMode::Existing, Epoch::new(0, 3))
        .await;
    assert_eq!(
        restarted.manager.committed_result(identity).await.unwrap(),
        Some(version)
    );
    let mut read = restarted.manager.create_transaction().await.unwrap();
    let right = read
        .get_dictionary::<String, i64>("right")
        .unwrap()
        .unwrap();
    assert_eq!(right.get(&mut read, &"balance".into()).unwrap(), Some(110));
}
