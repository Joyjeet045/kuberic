use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex as TokioMutex, Notify, mpsc};
use tracing::{info, warn};

use crate::error::KubericError;
use crate::events::{ReplicateRequest, ReplicatorControlEvent, StateProviderEvent};
use crate::handles::PartitionState;
use crate::replicator::primary::PrimarySender;
use crate::replicator::queue::ReplicationQueue;
use crate::replicator::quorum::{DEFAULT_QUORUM_TIMEOUT, QuorumTracker};
use crate::types::{CancellationToken, DataLossAction, Epoch, Lsn, OpenMode, ReplicaId, Role};

/// The WalReplicator actor. Processes control and data events in a single
/// loop with biased select (control has priority). The data path is
/// non-blocking because PrimarySender::send_to_all uses unbounded channels
/// with per-secondary drain tasks (matching SF's async dispatch model).
///
/// Owns a ReplicationQueue that retains ops for replay to new replicas,
/// matching SF's ReplicationQueueManager pattern.
pub struct WalReplicatorActor {
    replica_id: ReplicaId,
    quorum_timeout: Duration,
}

impl WalReplicatorActor {
    pub fn new(replica_id: ReplicaId) -> Self {
        Self::with_quorum_timeout(replica_id, DEFAULT_QUORUM_TIMEOUT)
    }

    pub fn with_quorum_timeout(replica_id: ReplicaId, quorum_timeout: Duration) -> Self {
        Self {
            replica_id,
            quorum_timeout,
        }
    }

    #[allow(unused_assignments)]
    pub async fn run(
        self,
        mut control_rx: mpsc::Receiver<ReplicatorControlEvent>,
        mut data_rx: mpsc::Receiver<ReplicateRequest>,
        state: Arc<PartitionState>,
        state_provider_tx: mpsc::UnboundedSender<StateProviderEvent>,
    ) {
        let mut role = Role::Unknown;
        let mut epoch = Epoch::default();
        let mut next_lsn: Lsn = 1;
        let quorum_tracker = Arc::new(TokioMutex::new(QuorumTracker::with_timeout(
            self.quorum_timeout,
        )));
        let expiration_wakeup = Arc::new(Notify::new());
        let expiration_shutdown = CancellationToken::new();
        let expiration_task = tokio::spawn(run_expiration_scheduler(
            quorum_tracker.clone(),
            expiration_wakeup.clone(),
            expiration_shutdown.clone(),
        ));
        let mut primary_sender: Option<PrimarySender> = None;
        let mut replication_queue = ReplicationQueue::new();

        loop {
            tokio::select! {
                biased;

                event = control_rx.recv() => {
                    let Some(event) = event else { break };
                    match event {
                        ReplicatorControlEvent::Open { mode, reply } => {
                            if mode == OpenMode::Existing {
                                let (sp_tx, sp_rx) = tokio::sync::oneshot::channel();
                                if state_provider_tx
                                    .send(StateProviderEvent::GetLastCommittedLsn { reply: sp_tx })
                                    .is_err()
                                {
                                    let _ = reply.send(Err(KubericError::Closed));
                                    continue;
                                }
                                match tokio::time::timeout(Duration::from_secs(30), sp_rx).await {
                                    Ok(Ok(Ok(lsn))) => {
                                        state.set_current_progress(lsn);
                                        state.set_catch_up_capability(lsn);
                                        state.set_committed_lsn(lsn);
                                        next_lsn = next_lsn.max(lsn.saturating_add(1));
                                    }
                                    Ok(Ok(Err(error))) => {
                                        let _ = reply.send(Err(error));
                                        continue;
                                    }
                                    Ok(Err(_)) => {
                                        let _ = reply.send(Err(KubericError::Closed));
                                        continue;
                                    }
                                    Err(_) => {
                                        let _ = reply.send(Err(KubericError::Internal(
                                            "state provider GetLastCommittedLsn timeout".into(),
                                        )));
                                        continue;
                                    }
                                }
                            } else {
                                state.set_catch_up_capability(0);
                            }
                            info!(replica_id = self.replica_id, "replicator opened");
                            let _ = reply.send(Ok(()));
                        }
                        ReplicatorControlEvent::Close { reply } => {
                            info!(replica_id = self.replica_id, "replicator closing");
                            quorum_tracker.lock().await.fail_all(KubericError::Closed);
                            if let Some(mut sender) = primary_sender.take() {
                                sender.close_all();
                            }
                            replication_queue.clear();
                            let _ = reply.send(Ok(()));
                            break;
                        }
                        ReplicatorControlEvent::Abort => {
                            quorum_tracker.lock().await.fail_all(KubericError::Closed);
                            if let Some(mut sender) = primary_sender.take() {
                                sender.close_all();
                            }
                            replication_queue.clear();
                            break;
                        }
                        ReplicatorControlEvent::ChangeRole {
                            epoch: new_epoch,
                            role: new_role,
                            reply,
                        } => {
                            let was_primary = role == Role::Primary;
                            info!(
                                replica_id = self.replica_id,
                                ?new_role,
                                ?new_epoch,
                                "replicator changing role"
                            );

                            if role == Role::Primary && new_role != Role::Primary {
                                quorum_tracker.lock().await.fail_all(KubericError::NotPrimary);
                                if let Some(mut sender) = primary_sender.take() {
                                    sender.close_all();
                                }
                                replication_queue.clear();
                            }

                            epoch = new_epoch;
                            role = new_role;

                            if role == Role::Primary {
                                if was_primary {
                                    if let Some(sender) = &mut primary_sender {
                                        sender.set_epoch(epoch);
                                    }
                                } else {
                                    let current_progress = state.current_progress();
                                    let committed_lsn = state.committed_lsn();
                                    quorum_tracker
                                        .lock()
                                        .await
                                        .seed_progress(current_progress, committed_lsn);
                                    next_lsn = next_lsn.max(current_progress + 1);
                                    primary_sender =
                                        Some(PrimarySender::new(self.replica_id, epoch));
                                }
                            }
                            state.set_active_replica_connections(
                                primary_sender
                                    .as_ref()
                                    .map(PrimarySender::active_connections)
                                    .unwrap_or_default(),
                            );

                            let _ = reply.send(Ok(()));
                        }
                        ReplicatorControlEvent::UpdateEpoch {
                            epoch: new_epoch,
                            reply,
                        } => {
                            info!(
                                replica_id = self.replica_id,
                                ?new_epoch,
                                "updating epoch"
                            );
                            // Update local epoch first
                            let prev_lsn = if new_epoch.data_loss_number == epoch.data_loss_number {
                                state.current_progress().max(state.committed_lsn())
                            } else {
                                state.committed_lsn()
                            };
                            epoch = new_epoch;

                            // Forward to state provider (inline — must complete before next event)
                            let (sp_tx, sp_rx) = tokio::sync::oneshot::channel();
                            if state_provider_tx.send(StateProviderEvent::UpdateEpoch {
                                epoch: new_epoch,
                                previous_epoch_last_lsn: prev_lsn,
                                reply: sp_tx,
                            }).is_err() {
                                let _ = reply.send(Err(KubericError::Closed));
                                continue;
                            }
                            match tokio::time::timeout(
                                std::time::Duration::from_secs(30), sp_rx
                            ).await {
                                Ok(Ok(result)) => { let _ = reply.send(result); }
                                Ok(Err(_)) => { let _ = reply.send(Err(KubericError::Closed)); }
                                Err(_) => { let _ = reply.send(Err(KubericError::Internal(
                                    "state provider UpdateEpoch timeout".into()))); }
                            }
                        }
                        ReplicatorControlEvent::UpdateCatchUpConfiguration {
                            current,
                            previous,
                            required_build_key,
                            reply,
                        } => {
                            let mut cc_members: HashSet<ReplicaId> =
                                current.members.iter().map(|r| r.id).collect();
                            cc_members.insert(self.replica_id);
                            let mut pc_members: HashSet<ReplicaId> =
                                previous.members.iter().map(|r| r.id).collect();
                            if !pc_members.is_empty() {
                                pc_members.insert(self.replica_id);
                            }

                            let must_catch_up: HashSet<ReplicaId> = current
                                .members
                                .iter()
                                .filter(|r| r.must_catch_up)
                                .map(|r| r.id)
                                .collect();

                            let member_progress: HashMap<ReplicaId, Lsn> = current
                                .members
                                .iter()
                                .map(|r| (r.id, r.current_progress))
                                .collect();

                            quorum_tracker.lock().await.set_catch_up_configuration(
                                cc_members,
                                current.write_quorum,
                                pc_members,
                                previous.write_quorum,
                                must_catch_up,
                                member_progress,
                            );
                            let tracker_committed =
                                quorum_tracker.lock().await.committed_lsn();
                            state.advance_committed_lsn(tracker_committed);

                            // Connect new secondaries and replay pending ops
                            let mut required_connection_error = None;
                            if let Some(sender) = &mut primary_sender {
                                for member in &current.members {
                                    if member.id != self.replica_id
                                        && !sender.has_connection(
                                            &member.id,
                                            &member.instance_id,
                                        )
                                    {
                                        let copy_lsn = state.copy_lsn(
                                            member.id,
                                            &member.instance_id,
                                            if member.must_catch_up {
                                                required_build_key.as_deref()
                                            } else {
                                                None
                                            },
                                        );
                                        if member.must_catch_up && copy_lsn.is_none() {
                                            required_connection_error = Some(
                                                crate::KubericError::Internal(
                                                    format!(
                                                        "missing exact copy boundary for replica {}@{}",
                                                        member.id, member.instance_id
                                                    )
                                                    .into(),
                                                ),
                                            );
                                            break;
                                        }
                                        if let Err(e) = sender
                                            .add_secondary(
                                                member.id,
                                                member.instance_id.clone(),
                                                member.replicator_address.clone(),
                                                quorum_tracker.clone(),
                                                state.clone(),
                                            )
                                            .await
                                        {
                                            warn!(
                                                replica_id = member.id,
                                                instance_id = %member.instance_id,
                                                error = %e,
                                                "failed to connect to secondary"
                                            );
                                            if member.must_catch_up {
                                                required_connection_error = Some(e);
                                                break;
                                            }
                                            continue;
                                        }

                                        // Replay ops beyond the copy boundary.
                                        // copy_lsn is the snapshot LSN recorded by
                                        // run_build_replica_copy — the secondary
                                        // already has state through this LSN.
                                        let copy_lsn = copy_lsn.unwrap_or(0);
                                        let replay_from = copy_lsn + 1;
                                        let pending = replication_queue.ops_from(replay_from);
                                        if !pending.is_empty() {
                                            info!(
                                                replica_id = member.id,
                                                copy_lsn,
                                                replay_from,
                                                count = pending.len(),
                                                "replaying ops from replication queue"
                                            );
                                            for (lsn, data) in &pending {
                                                sender.send_to_one(member.id, *lsn, data, state.committed_lsn());
                                            }
                                        }
                                    }
                                }
                            }

                            if let Some(error) = required_connection_error {
                                let _ = reply.send(Err(error));
                            } else {
                                state.set_active_replica_connections(
                                    primary_sender
                                        .as_ref()
                                        .map(PrimarySender::active_connections)
                                        .unwrap_or_default(),
                                );
                                let _ = reply.send(Ok(()));
                            }
                        }
                        ReplicatorControlEvent::UpdateCurrentConfiguration {
                            current,
                            reply,
                        } => {
                            let mut cc_members: HashSet<ReplicaId> =
                                current.members.iter().map(|r| r.id).collect();
                            cc_members.insert(self.replica_id);

                            quorum_tracker.lock().await.set_current_configuration(
                                cc_members.clone(),
                                current.write_quorum,
                            );
                            let tracker_committed =
                                quorum_tracker.lock().await.committed_lsn();
                            state.advance_committed_lsn(tracker_committed);

                            let _ = reply.send(Ok(()));

                            // GC replication queue — config is finalized,
                            // all replicas are caught up. Safe to remove
                            // ops up to committed_lsn.
                            let committed = state.committed_lsn();
                            replication_queue.gc(committed);
                        }
                        ReplicatorControlEvent::WaitForCatchUpQuorum {
                            wait_id,
                            mode,
                            reply,
                        } => {
                            if let Some(wait_id) = wait_id {
                                quorum_tracker
                                    .lock()
                                    .await
                                    .wait_for_catch_up_tracked(wait_id, mode, reply);
                            } else {
                                quorum_tracker.lock().await.wait_for_catch_up(mode, reply);
                            }
                            expiration_wakeup.notify_one();
                        }
                        ReplicatorControlEvent::CancelCatchUpQuorumWait {
                            wait_id,
                            reply,
                        } => {
                            quorum_tracker.lock().await.cancel_catch_up_wait(&wait_id);
                            let _ = reply.send(Ok(()));
                        }
                        ReplicatorControlEvent::BuildReplica {
                            replica,
                            build_key,
                            cancellation,
                            reply,
                        } => {
                            // Replication queue ops are replayed at add_secondary time.
                            // Spawn the copy protocol as a background task.
                            info!(
                                replica_id = replica.id,
                                queue_len = replication_queue.len(),
                                "BuildReplica: spawning copy task"
                            );
                            let sp_tx = state_provider_tx.clone();
                            let st = state.clone();
                            tokio::spawn(async move {
                                let result = crate::replicator::copy::run_build_replica_copy(
                                    replica,
                                    build_key,
                                    sp_tx,
                                    st,
                                    std::time::Duration::from_secs(600),
                                    cancellation,
                                ).await;
                                let _ = reply.send(result);
                            });
                        }
                        ReplicatorControlEvent::RemoveReplica {
                            replica_id,
                            instance_id,
                            reply,
                        } => {
                            if let Some(sender) = &mut primary_sender {
                                sender.remove_secondary(replica_id, &instance_id);
                            }
                            state.set_active_replica_connections(
                                primary_sender
                                    .as_ref()
                                    .map(PrimarySender::active_connections)
                                    .unwrap_or_default(),
                            );
                            let _ = reply.send(Ok(()));
                        }
                        ReplicatorControlEvent::OnDataLoss {
                            expected_epoch,
                            reply,
                        } => {
                            if expected_epoch.is_some_and(|expected| expected != epoch) {
                                let _ = reply.send(Err(KubericError::Internal(
                                    format!(
                                        "data-loss expected epoch {:?} does not match actor epoch {:?}",
                                        expected_epoch.unwrap(),
                                        epoch
                                    )
                                    .into(),
                                )));
                                continue;
                            }
                            // Forward to state provider, convert bool → DataLossAction
                            let (sp_tx, sp_rx) = tokio::sync::oneshot::channel();
                            if state_provider_tx.send(StateProviderEvent::OnDataLoss {
                                reply: sp_tx,
                            }).is_err() {
                                let _ = reply.send(Err(KubericError::Closed));
                                continue;
                            }
                            match tokio::time::timeout(
                                std::time::Duration::from_secs(30), sp_rx
                            ).await {
                                Ok(Ok(Ok(state_changed))) => {
                                    let action = if state_changed {
                                        DataLossAction::StateChanged
                                    } else {
                                        DataLossAction::None
                                    };
                                    if state_changed {
                                        let (lsn_tx, lsn_rx) = tokio::sync::oneshot::channel();
                                        if state_provider_tx
                                            .send(StateProviderEvent::GetLastCommittedLsn {
                                                reply: lsn_tx,
                                            })
                                            .is_err()
                                        {
                                            let _ = reply.send(Err(KubericError::Closed));
                                            continue;
                                        }
                                        match tokio::time::timeout(Duration::from_secs(30), lsn_rx)
                                            .await
                                        {
                                            Ok(Ok(Ok(lsn))) => {
                                                state.set_current_progress(lsn);
                                                state.set_catch_up_capability(lsn);
                                                state.set_committed_lsn(lsn);
                                                quorum_tracker
                                                    .lock()
                                                    .await
                                                    .reset_progress_after_data_loss(lsn, lsn);
                                                replication_queue.clear();
                                                next_lsn = lsn.saturating_add(1);
                                            }
                                            Ok(Ok(Err(error))) => {
                                                let _ = reply.send(Err(error));
                                                continue;
                                            }
                                            Ok(Err(_)) => {
                                                let _ = reply.send(Err(KubericError::Closed));
                                                continue;
                                            }
                                            Err(_) => {
                                                let _ = reply.send(Err(KubericError::Internal(
                                                    "state provider progress refresh timeout"
                                                        .into(),
                                                )));
                                                continue;
                                            }
                                        }
                                    }
                                    let _ = reply.send(Ok(action));
                                }
                                Ok(Ok(Err(e))) => { let _ = reply.send(Err(e)); }
                                Ok(Err(_)) => { let _ = reply.send(Err(KubericError::Closed)); }
                                Err(_) => { let _ = reply.send(Err(KubericError::Internal(
                                    "state provider OnDataLoss timeout".into()))); }
                            }
                        }
                    }
                }

                req = data_rx.recv() => {
                    let Some(req) = req else { break };
                    if role != Role::Primary {
                        let _ = req.reply.send(Err(KubericError::NotPrimary));
                        continue;
                    }
                    let lsn = next_lsn;
                    next_lsn += 1;

                    // Store in replication queue for replay to new replicas
                    replication_queue.push(lsn, req.data.clone());

                    // Register with quorum tracker (primary's own ACK counted)
                    quorum_tracker.lock().await.register(lsn, self.replica_id, req.reply);
                    expiration_wakeup.notify_one();

                    // Read committed_lsn AFTER register — the registration may
                    // have triggered immediate commit (single replica case), and
                    // previous ops' ACKs may have been processed by the background
                    // ACK reader, advancing committed_lsn further.
                    let committed = quorum_tracker.lock().await.committed_lsn();
                    state.set_current_progress(lsn);
                    state.advance_committed_lsn(committed);

                    // Non-blocking: send_to_all uses unbounded channels.
                    // Include committed_lsn so secondaries can track commit progress.
                    if let Some(sender) = &mut primary_sender {
                        sender.send_to_all(lsn, &req.data, committed);
                        state.set_active_replica_connections(sender.active_connections());
                    }
                }

                else => break,
            }
        }

        expiration_shutdown.cancel();
        let _ = expiration_task.await;
    }
}

async fn run_expiration_scheduler(
    quorum_tracker: Arc<TokioMutex<QuorumTracker>>,
    wakeup: Arc<Notify>,
    shutdown: CancellationToken,
) {
    loop {
        let next_deadline = {
            let mut tracker = quorum_tracker.lock().await;
            tracker.expire_due(tokio::time::Instant::now());
            tracker.next_deadline()
        };
        match next_deadline {
            Some(deadline) => {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    _ = wakeup.notified() => {}
                    _ = tokio::time::sleep_until(deadline) => {
                        quorum_tracker.lock().await.expire_due(tokio::time::Instant::now());
                    }
                }
            }
            None => {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    _ = wakeup.notified() => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::sync::{mpsc, oneshot};

    use crate::events::{ReplicateRequest, ReplicatorControlEvent, StateProviderEvent};
    use crate::types::{
        DataLossAction, Epoch, OpenMode, ReplicaInfo, ReplicaSetConfig, ReplicaStatus, Role,
    };

    struct ActorHarness {
        control_tx: mpsc::Sender<ReplicatorControlEvent>,
        data_tx: mpsc::Sender<ReplicateRequest>,
        state: Arc<PartitionState>,
        state_provider_rx: mpsc::UnboundedReceiver<StateProviderEvent>,
        task: tokio::task::JoinHandle<()>,
    }

    impl ActorHarness {
        async fn start(timeout: Duration) -> Self {
            let (control_tx, control_rx) = mpsc::channel(16);
            let (data_tx, data_rx) = mpsc::channel(16);
            let (state_provider_tx, state_provider_rx) = mpsc::unbounded_channel();
            let state = Arc::new(PartitionState::new());

            let actor = WalReplicatorActor::with_quorum_timeout(1, timeout);
            let actor_state = state.clone();
            let task = tokio::spawn(async move {
                actor
                    .run(control_rx, data_rx, actor_state, state_provider_tx)
                    .await;
            });

            let (role_tx, role_rx) = oneshot::channel();
            control_tx
                .send(ReplicatorControlEvent::ChangeRole {
                    epoch: Epoch::new(1, 1),
                    role: Role::Primary,
                    reply: role_tx,
                })
                .await
                .unwrap();
            role_rx.await.unwrap().unwrap();

            let (config_tx, config_rx) = oneshot::channel();
            control_tx
                .send(ReplicatorControlEvent::UpdateCurrentConfiguration {
                    current: three_replica_config(),
                    reply: config_tx,
                })
                .await
                .unwrap();
            config_rx.await.unwrap().unwrap();

            Self {
                control_tx,
                data_tx,
                state,
                state_provider_rx,
                task,
            }
        }

        async fn register_write(&self) -> oneshot::Receiver<crate::Result<Lsn>> {
            let expected_lsn = self.state.current_progress() + 1;
            let (reply, receiver) = oneshot::channel();
            self.data_tx
                .send(ReplicateRequest {
                    data: Bytes::from_static(b"test"),
                    reply,
                })
                .await
                .unwrap();

            while self.state.current_progress() < expected_lsn {
                tokio::task::yield_now().await;
            }
            receiver
        }
    }

    #[tokio::test]
    async fn existing_open_and_state_changed_data_loss_refresh_progress() {
        let mut harness = ActorHarness::start(Duration::from_secs(5)).await;

        let (open_tx, open_rx) = oneshot::channel();
        harness
            .control_tx
            .send(ReplicatorControlEvent::Open {
                mode: OpenMode::Existing,
                reply: open_tx,
            })
            .await
            .unwrap();
        match harness.state_provider_rx.recv().await.unwrap() {
            StateProviderEvent::GetLastCommittedLsn { reply } => {
                reply.send(Ok(21)).unwrap();
            }
            _ => panic!("existing open did not query provider progress"),
        }
        open_rx.await.unwrap().unwrap();
        assert_eq!(harness.state.current_progress(), 21);
        assert_eq!(harness.state.catch_up_capability(), 21);
        assert_eq!(harness.state.committed_lsn(), 21);

        let (loss_tx, loss_rx) = oneshot::channel();
        harness
            .control_tx
            .send(ReplicatorControlEvent::OnDataLoss {
                expected_epoch: Some(Epoch::new(1, 1)),
                reply: loss_tx,
            })
            .await
            .unwrap();
        match harness.state_provider_rx.recv().await.unwrap() {
            StateProviderEvent::OnDataLoss { reply } => reply.send(Ok(true)).unwrap(),
            _ => panic!("data loss callback was not forwarded"),
        }
        match harness.state_provider_rx.recv().await.unwrap() {
            StateProviderEvent::GetLastCommittedLsn { reply } => {
                reply.send(Ok(7)).unwrap();
            }
            _ => panic!("state-changing data loss did not refresh progress"),
        }
        assert_eq!(
            loss_rx.await.unwrap().unwrap(),
            DataLossAction::StateChanged
        );
        assert_eq!(harness.state.current_progress(), 7);
        assert_eq!(harness.state.catch_up_capability(), 7);
        assert_eq!(harness.state.committed_lsn(), 7);
    }

    #[tokio::test]
    async fn configuration_epoch_preserves_accepted_suffix_but_data_loss_can_roll_back() {
        let mut harness = ActorHarness::start(Duration::from_secs(5)).await;
        harness.state.set_current_progress(5);
        harness.state.set_committed_lsn(4);
        for (epoch, expected_lsn) in [(Epoch::new(1, 2), 5), (Epoch::new(2, 3), 4)] {
            let (reply, receiver) = oneshot::channel();
            harness
                .control_tx
                .send(ReplicatorControlEvent::UpdateEpoch { epoch, reply })
                .await
                .unwrap();
            match harness.state_provider_rx.recv().await.unwrap() {
                StateProviderEvent::UpdateEpoch {
                    previous_epoch_last_lsn,
                    reply,
                    ..
                } => {
                    assert_eq!(previous_epoch_last_lsn, expected_lsn);
                    reply.send(Ok(())).unwrap();
                }
                _ => panic!("epoch update was not forwarded"),
            }
            receiver.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn data_loss_epoch_is_checked_by_actor_before_provider_callback() {
        let mut harness = ActorHarness::start(Duration::from_secs(5)).await;
        let (epoch_tx, epoch_rx) = oneshot::channel();
        harness
            .control_tx
            .send(ReplicatorControlEvent::UpdateEpoch {
                epoch: Epoch::new(1, 2),
                reply: epoch_tx,
            })
            .await
            .unwrap();
        match harness.state_provider_rx.recv().await.unwrap() {
            StateProviderEvent::UpdateEpoch { reply, .. } => reply.send(Ok(())).unwrap(),
            _ => panic!("epoch update was not forwarded"),
        }
        epoch_rx.await.unwrap().unwrap();

        let (loss_tx, loss_rx) = oneshot::channel();
        harness
            .control_tx
            .send(ReplicatorControlEvent::OnDataLoss {
                expected_epoch: Some(Epoch::new(1, 1)),
                reply: loss_tx,
            })
            .await
            .unwrap();
        let error = loss_rx.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("does not match actor epoch"));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), harness.state_provider_rx.recv())
                .await
                .is_err()
        );
    }

    fn three_replica_config() -> ReplicaSetConfig {
        ReplicaSetConfig {
            members: [2, 3]
                .into_iter()
                .map(|id| ReplicaInfo {
                    id,
                    instance_id: crate::types::ReplicaInstanceId::new(format!("actor-test-{id}")),
                    role: Role::ActiveSecondary,
                    status: ReplicaStatus::Up,
                    replicator_address: String::new(),
                    current_progress: 0,
                    catch_up_capability: 0,
                    must_catch_up: false,
                })
                .collect(),
            write_quorum: 2,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn write_expires_while_actor_waits_for_state_provider() {
        let timeout = Duration::from_millis(100);
        let mut harness = ActorHarness::start(timeout).await;
        let write_rx = harness.register_write().await;

        let (epoch_tx, epoch_rx) = oneshot::channel();
        harness
            .control_tx
            .send(ReplicatorControlEvent::UpdateEpoch {
                epoch: Epoch::new(1, 2),
                reply: epoch_tx,
            })
            .await
            .unwrap();

        let event = harness.state_provider_rx.recv().await.unwrap();
        let StateProviderEvent::UpdateEpoch { reply, .. } = event else {
            panic!("expected UpdateEpoch");
        };

        tokio::time::advance(timeout).await;
        assert!(matches!(
            write_rx.await.unwrap(),
            Err(KubericError::NoWriteQuorum)
        ));
        assert_eq!(harness.state.current_progress(), 1);
        assert_eq!(harness.state.committed_lsn(), 0);

        reply.send(Ok(())).unwrap();
        epoch_rx.await.unwrap().unwrap();

        let (close_tx, close_rx) = oneshot::channel();
        harness
            .control_tx
            .send(ReplicatorControlEvent::Close { reply: close_tx })
            .await
            .unwrap();
        close_rx.await.unwrap().unwrap();
        harness.task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn oldest_write_expires_under_continuous_registration_wakeups() {
        let timeout = Duration::from_millis(100);
        let harness = ActorHarness::start(timeout).await;
        let mut first_write_rx = harness.register_write().await;

        // Keep registering work as the clock crosses the first write's
        // deadline. Each registration notifies the scheduler, reproducing the
        // wakeup traffic that must not starve expiration.
        for _ in 0..100 {
            tokio::time::advance(Duration::from_millis(1)).await;
            drop(harness.register_write().await);
        }
        tokio::task::yield_now().await;

        assert!(matches!(
            first_write_rx.try_recv(),
            Ok(Err(KubericError::NoWriteQuorum))
        ));

        harness
            .control_tx
            .send(ReplicatorControlEvent::Abort)
            .await
            .unwrap();
        harness.task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn configuration_updates_do_not_regress_partition_committed_lsn() {
        let harness = ActorHarness::start(Duration::from_secs(1)).await;
        harness.state.set_committed_lsn(9);
        let config = ReplicaSetConfig {
            members: Vec::new(),
            write_quorum: 1,
        };

        let (catch_up_tx, catch_up_rx) = oneshot::channel();
        harness
            .control_tx
            .send(ReplicatorControlEvent::UpdateCatchUpConfiguration {
                current: config.clone(),
                previous: ReplicaSetConfig {
                    members: Vec::new(),
                    write_quorum: 0,
                },
                required_build_key: None,
                reply: catch_up_tx,
            })
            .await
            .unwrap();
        catch_up_rx.await.unwrap().unwrap();
        assert_eq!(harness.state.committed_lsn(), 9);

        let (current_tx, current_rx) = oneshot::channel();
        harness
            .control_tx
            .send(ReplicatorControlEvent::UpdateCurrentConfiguration {
                current: config,
                reply: current_tx,
            })
            .await
            .unwrap();
        current_rx.await.unwrap().unwrap();
        assert_eq!(harness.state.committed_lsn(), 9);

        harness
            .control_tx
            .send(ReplicatorControlEvent::Abort)
            .await
            .unwrap();
        harness.task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn promotion_seeds_progress_and_assigns_next_lsn() {
        let (control_tx, control_rx) = mpsc::channel(16);
        let (data_tx, data_rx) = mpsc::channel(16);
        let (state_provider_tx, _state_provider_rx) = mpsc::unbounded_channel();
        let state = Arc::new(PartitionState::new());
        state.set_current_progress(9);
        state.set_committed_lsn(8);

        let actor = WalReplicatorActor::with_quorum_timeout(1, Duration::from_secs(1));
        let actor_state = state.clone();
        let task = tokio::spawn(async move {
            actor
                .run(control_rx, data_rx, actor_state, state_provider_tx)
                .await;
        });

        let (role_tx, role_rx) = oneshot::channel();
        control_tx
            .send(ReplicatorControlEvent::ChangeRole {
                epoch: Epoch::new(1, 1),
                role: Role::Primary,
                reply: role_tx,
            })
            .await
            .unwrap();
        role_rx.await.unwrap().unwrap();

        let (config_tx, config_rx) = oneshot::channel();
        control_tx
            .send(ReplicatorControlEvent::UpdateCurrentConfiguration {
                current: ReplicaSetConfig {
                    members: Vec::new(),
                    write_quorum: 1,
                },
                reply: config_tx,
            })
            .await
            .unwrap();
        config_rx.await.unwrap().unwrap();

        let (write_tx, write_rx) = oneshot::channel();
        data_tx
            .send(ReplicateRequest {
                data: Bytes::from_static(b"after-promotion"),
                reply: write_tx,
            })
            .await
            .unwrap();
        assert_eq!(write_rx.await.unwrap().unwrap(), 10);
        assert_eq!(state.current_progress(), 10);
        assert_eq!(state.committed_lsn(), 10);

        control_tx
            .send(ReplicatorControlEvent::Abort)
            .await
            .unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn promoted_primary_waits_for_configuration_before_committing() {
        let (control_tx, control_rx) = mpsc::channel(16);
        let (data_tx, data_rx) = mpsc::channel(16);
        let (state_provider_tx, _state_provider_rx) = mpsc::unbounded_channel();
        let state = Arc::new(PartitionState::new());

        let actor = WalReplicatorActor::with_quorum_timeout(1, Duration::from_secs(1));
        let actor_state = state.clone();
        let task = tokio::spawn(async move {
            actor
                .run(control_rx, data_rx, actor_state, state_provider_tx)
                .await;
        });

        let (role_tx, role_rx) = oneshot::channel();
        control_tx
            .send(ReplicatorControlEvent::ChangeRole {
                epoch: Epoch::new(1, 1),
                role: Role::Primary,
                reply: role_tx,
            })
            .await
            .unwrap();
        role_rx.await.unwrap().unwrap();

        let (write_tx, mut write_rx) = oneshot::channel();
        data_tx
            .send(ReplicateRequest {
                data: Bytes::from_static(b"before-configuration"),
                reply: write_tx,
            })
            .await
            .unwrap();
        while state.current_progress() == 0 {
            tokio::task::yield_now().await;
        }
        assert!(write_rx.try_recv().is_err());
        assert_eq!(state.committed_lsn(), 0);

        let (config_tx, config_rx) = oneshot::channel();
        control_tx
            .send(ReplicatorControlEvent::UpdateCurrentConfiguration {
                current: ReplicaSetConfig {
                    members: Vec::new(),
                    write_quorum: 1,
                },
                reply: config_tx,
            })
            .await
            .unwrap();
        config_rx.await.unwrap().unwrap();

        assert_eq!(write_rx.await.unwrap().unwrap(), 1);
        assert_eq!(state.committed_lsn(), 1);

        control_tx
            .send(ReplicatorControlEvent::Abort)
            .await
            .unwrap();
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn demotion_fails_pending_write_before_expiration() {
        let harness = ActorHarness::start(Duration::from_secs(1)).await;
        let write_rx = harness.register_write().await;

        let (role_tx, role_rx) = oneshot::channel();
        harness
            .control_tx
            .send(ReplicatorControlEvent::ChangeRole {
                epoch: Epoch::new(1, 2),
                role: Role::ActiveSecondary,
                reply: role_tx,
            })
            .await
            .unwrap();
        role_rx.await.unwrap().unwrap();

        assert!(matches!(
            write_rx.await.unwrap(),
            Err(KubericError::NotPrimary)
        ));
        drop(harness.control_tx);
        drop(harness.data_tx);
        harness.task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn demotion_rejects_write_already_queued_in_data_channel() {
        let mut harness = ActorHarness::start(Duration::from_secs(1)).await;

        // Hold the actor inside a control branch while both a write and the
        // demotion are queued. The biased select must process demotion first,
        // then drain the write with NotPrimary rather than stranding it.
        let (epoch_tx, epoch_rx) = oneshot::channel();
        harness
            .control_tx
            .send(ReplicatorControlEvent::UpdateEpoch {
                epoch: Epoch::new(1, 2),
                reply: epoch_tx,
            })
            .await
            .unwrap();
        let event = harness.state_provider_rx.recv().await.unwrap();
        let StateProviderEvent::UpdateEpoch { reply, .. } = event else {
            panic!("expected UpdateEpoch");
        };

        let (write_tx, write_rx) = oneshot::channel();
        harness
            .data_tx
            .send(ReplicateRequest {
                data: Bytes::from_static(b"stale"),
                reply: write_tx,
            })
            .await
            .unwrap();
        let (role_tx, role_rx) = oneshot::channel();
        harness
            .control_tx
            .send(ReplicatorControlEvent::ChangeRole {
                epoch: Epoch::new(1, 3),
                role: Role::ActiveSecondary,
                reply: role_tx,
            })
            .await
            .unwrap();

        reply.send(Ok(())).unwrap();
        epoch_rx.await.unwrap().unwrap();
        role_rx.await.unwrap().unwrap();
        assert!(matches!(
            write_rx.await.unwrap(),
            Err(KubericError::NotPrimary)
        ));
        assert_eq!(harness.state.current_progress(), 0);

        drop(harness.control_tx);
        drop(harness.data_tx);
        harness.task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn close_fails_pending_write_before_expiration() {
        let harness = ActorHarness::start(Duration::from_secs(1)).await;
        let write_rx = harness.register_write().await;

        let (close_tx, close_rx) = oneshot::channel();
        harness
            .control_tx
            .send(ReplicatorControlEvent::Close { reply: close_tx })
            .await
            .unwrap();
        close_rx.await.unwrap().unwrap();

        assert!(matches!(write_rx.await.unwrap(), Err(KubericError::Closed)));
        harness.task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn abort_fails_pending_write_before_expiration() {
        let harness = ActorHarness::start(Duration::from_secs(1)).await;
        let write_rx = harness.register_write().await;

        harness
            .control_tx
            .send(ReplicatorControlEvent::Abort)
            .await
            .unwrap();

        assert!(matches!(write_rx.await.unwrap(), Err(KubericError::Closed)));
        harness.task.await.unwrap();
    }
}
