/// K8s Lease-based leader election integration tests.
/// Tests both kube-lease-manager (watch API) and kube-leader-election (LeaseLock API).
use std::{
    sync::{Arc, atomic::AtomicU32},
    time::Duration,
};

use kube_lease_manager::LeaseManagerBuilder;

const TEST_NAMESPACE: &str = "xedio-test-ns";

async fn ensure_test_namespace() {
    static INIT: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    INIT.get_or_init(|| async {
        let client = crate::test_utils::isolated_kube_client().await;
        let namespaces: kube::Api<k8s_openapi::api::core::v1::Namespace> = kube::Api::all(client);
        let ns = k8s_openapi::api::core::v1::Namespace {
            metadata: kube::api::ObjectMeta {
                name: Some(TEST_NAMESPACE.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        match namespaces
            .create(&kube::api::PostParams::default(), &ns)
            .await
        {
            Ok(_) => (),
            Err(kube::Error::Api(ae)) if ae.code == 409 => (),
            Err(e) => panic!("Failed to create test namespace: {}", e),
        }
    })
    .await;
}

// -- kube-lease-manager tests (watch API) --

#[tokio::test]
#[test_log::test]
async fn test_lease_leader_election() {
    ensure_test_namespace().await;

    let client = crate::test_utils::isolated_kube_client().await;
    let lease_name = "test-watch-lease";

    // Simulate 5 competing instances
    let count = 5;
    let mut lease_managers = Vec::new();
    for i in 0..count {
        let manager = LeaseManagerBuilder::new(client.clone(), lease_name)
            .with_namespace(TEST_NAMESPACE)
            .with_duration(10)
            .with_grace(5)
            .with_create_mode(kube_lease_manager::LeaseCreateMode::AutoCreate)
            .with_identity(format!("instance-{}", i))
            .build()
            .await
            .unwrap();
        lease_managers.push(manager);
    }

    let mut join_set = tokio::task::JoinSet::new();
    let leader_count: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
    let standby_count: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
    let barrier = Arc::new(tokio::sync::Barrier::new(count));

    for manager in lease_managers {
        let leader_count = leader_count.clone();
        let standby_count = standby_count.clone();
        let barrier = barrier.clone();
        join_set.spawn(async move {
            barrier.wait().await;
            let (mut channel, task) = manager.watch().await;
            tokio::select! {
                _ = channel.changed() => {
                    let lock_state = *channel.borrow_and_update();
                    if lock_state {
                        leader_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        tracing::info!("Became leader");
                        tokio::time::sleep(Duration::from_secs(3)).await;
                    } else {
                        standby_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        tracing::info!("Became standby");
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    tracing::info!("Unable to get lock during 1s");
                }
            }

            drop(channel);
            let manager = tokio::join!(task).0.unwrap().unwrap();
            manager.release().await.unwrap();
        });
    }

    while let Some(res) = join_set.join_next().await {
        res.unwrap();
    }

    assert_eq!(
        leader_count.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "exactly one instance should become leader"
    );
    assert_eq!(
        standby_count.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no standby notifications expected in first round"
    );
}

// -- kube-leader-election tests (LeaseLock API) --

type Epoch = i32;

#[derive(Debug)]
enum LeaderElectionEvent {
    Leader(Epoch),
    StandBy(Epoch),
    Shutdown(Result<(), kube_leader_election::Error>),
}

#[derive(Clone)]
struct LeaderElection {
    namespace: String,
    lease_name: String,
    lease_ttl: Duration,
}

impl LeaderElection {
    fn new(namespace: String, lease_name: String, lease_ttl: Duration) -> Self {
        Self {
            namespace,
            lease_name,
            lease_ttl,
        }
    }

    async fn election_loop(
        self,
        sender: tokio::sync::mpsc::Sender<LeaderElectionEvent>,
        token: tokio_util::sync::CancellationToken,
    ) {
        use kube_leader_election::{LeaseLock, LeaseLockParams, LeaseLockResult};
        use rand::{RngExt, distr::Alphanumeric};

        let client = crate::test_utils::isolated_kube_client().await;
        let random: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();
        let holder_id = format!("shared-lease-{}", random.to_lowercase());
        tracing::info!("Starting leader election with holder id {}", holder_id);

        let leadership = LeaseLock::new(
            client,
            &self.namespace,
            LeaseLockParams {
                holder_id: holder_id.clone(),
                lease_name: self.lease_name.clone(),
                lease_ttl: self.lease_ttl,
            },
        );

        let mut cur_epoch: Epoch = 0;
        let mut on_new_leader = async |ll: LeaseLockResult| match ll {
            LeaseLockResult::Acquired(lease) => {
                let epoch = lease.spec.unwrap().lease_transitions.unwrap();
                tracing::info!("Acquired leadership {} for epoch {}", holder_id, epoch);
                assert!(epoch >= cur_epoch, "Epoch should not go backwards");
                cur_epoch = epoch;
                sender
                    .send(LeaderElectionEvent::Leader(epoch))
                    .await
                    .unwrap();
            }
            LeaseLockResult::NotAcquired(_) => {
                tracing::info!("No lease acquired, standing by");
                sender
                    .send(LeaderElectionEvent::StandBy(cur_epoch))
                    .await
                    .unwrap();
            }
        };

        loop {
            let start_time = tokio::time::Instant::now();
            tokio::select! {
                result = leadership.try_acquire_or_renew() => {
                    match result {
                        Ok(ll) => on_new_leader(ll).await,
                        Err(err) => tracing::error!("Leader election error {:?}", err),
                    }
                }
                _ = token.cancelled() => {
                    tracing::info!("Leader election loop received shutdown signal");
                    break;
                }
            }
            let elapsed = start_time.elapsed();
            let sleep_threshold = self.lease_ttl / 2;
            if elapsed >= sleep_threshold {
                continue;
            }
            tokio::select! {
                _ = tokio::time::sleep(sleep_threshold) => {},
                _ = token.cancelled() => {
                    tracing::info!("Leader election loop received shutdown signal");
                    break;
                }
            }
        }
        sender
            .send(LeaderElectionEvent::Shutdown(leadership.step_down().await))
            .await
            .unwrap();
        tracing::info!("Leader election loop exited");
    }
}

struct LETestReceiver {
    rx: tokio::sync::mpsc::Receiver<LeaderElectionEvent>,
    token: tokio_util::sync::CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

impl LETestReceiver {
    fn new(le: LeaderElection) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let token = tokio_util::sync::CancellationToken::new();
        let token_cp = token.clone();
        let join_handle = tokio::spawn(async move {
            le.election_loop(tx, token_cp).await;
        });
        Self {
            rx,
            token,
            join_handle,
        }
    }

    async fn must_receive_event(&mut self) -> LeaderElectionEvent {
        self.rx.recv().await.unwrap()
    }

    async fn shutdown(mut self) {
        self.token.cancel();
        self.join_handle.await.unwrap();
        let e = self.rx.recv().await.unwrap();
        matches!(e, LeaderElectionEvent::Shutdown(Ok(())));
    }
}

#[tokio::test]
#[test_log::test]
async fn test_leader_election_new() {
    ensure_test_namespace().await;

    let le = LeaderElection::new(
        TEST_NAMESPACE.to_string(),
        "test-lease1".to_string(),
        Duration::from_secs(2),
    );
    let token = tokio_util::sync::CancellationToken::new();
    let token_cp = token.clone();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        le.election_loop(sender, token_cp).await;
    });
    let ev = receiver
        .recv()
        .await
        .expect("Should receive at least one election event");
    matches!(ev, LeaderElectionEvent::Leader(0));

    token.cancel();
    let ev = receiver
        .recv()
        .await
        .expect("Should receive shutdown event");
    matches!(ev, LeaderElectionEvent::Shutdown(Ok(())));
}

#[tokio::test]
#[test_log::test]
#[ignore = "Has race conditions, needs to be fixed"]
async fn test_change_leader() {
    ensure_test_namespace().await;

    let lease_name = "test-lease2".to_string();
    let le1 = LeaderElection::new(
        TEST_NAMESPACE.to_string(),
        lease_name.clone(),
        Duration::from_secs(2),
    );
    let le2 = LeaderElection::new(
        TEST_NAMESPACE.to_string(),
        lease_name.clone(),
        Duration::from_secs(2),
    );

    let mut receiver1 = LETestReceiver::new(le1);
    let mut receiver2 = LETestReceiver::new(le2);

    // First receiver should become leader
    let ev1 = receiver1.must_receive_event().await;
    let leader_epoch = if let LeaderElectionEvent::Leader(epoch) = ev1 {
        epoch
    } else {
        panic!("First receiver should become leader: got {:?}", ev1);
    };

    // Second receiver should be standby
    let ev2 = receiver2.must_receive_event().await;
    let standby_epoch = if let LeaderElectionEvent::StandBy(epoch) = ev2 {
        epoch
    } else {
        panic!("Second receiver should be standby: got {:?}", ev2);
    };
    assert_eq!(leader_epoch, standby_epoch);

    // Shutdown first receiver — second should take over
    receiver1.shutdown().await;
    let ev2 = receiver2.must_receive_event().await;
    let leader_epoch2 = if let LeaderElectionEvent::Leader(epoch) = ev2 {
        epoch
    } else {
        panic!("Second receiver should become leader: got {:?}", ev2);
    };
    assert_eq!(leader_epoch2, leader_epoch + 1);
}
