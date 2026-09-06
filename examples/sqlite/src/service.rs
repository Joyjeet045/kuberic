//! Lifecycle + StateProvider event loop for the SQLite service.
//!
//! Same two-channel pattern as kvstore: LifecycleEvent + StateProviderEvent.

use bytes::Bytes;
use kuberic_core::events::{LifecycleEvent, StateProviderEvent};
use kuberic_core::handles::StateReplicatorHandle;
use kuberic_core::replicator::WalReplicator;
use kuberic_core::types::{CancellationToken, Operation, OperationStream, Role};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::frames::WalFrameSet;
use crate::server::run_client_server;
use crate::state::SharedState;

#[derive(Debug, Clone, Default)]
pub enum DataLossBehavior {
    #[default]
    NoStateChange,
    StateChanged,
    Fail(String),
    Delay {
        duration: std::time::Duration,
        state_changed: bool,
    },
}

/// Handle a single state provider event.
async fn handle_state_provider_event(
    event: StateProviderEvent,
    state: &SharedState,
    data_loss_behavior: &DataLossBehavior,
) {
    match event {
        StateProviderEvent::UpdateEpoch {
            previous_epoch_last_lsn,
            reply,
            ..
        } => {
            let current_lsn = state.lock().await.last_applied_lsn;
            if previous_epoch_last_lsn > 0 && previous_epoch_last_lsn < current_lsn {
                info!(
                    previous_epoch_last_lsn,
                    current_lsn, "epoch updated — rolling back"
                );
                if let Err(e) = state
                    .lock()
                    .await
                    .rollback_to(previous_epoch_last_lsn)
                    .await
                {
                    warn!(error = %e, "rollback failed");
                }
            } else {
                info!(previous_epoch_last_lsn, current_lsn, "epoch updated");
            }
            let _ = reply.send(Ok(()));
        }
        StateProviderEvent::GetLastCommittedLsn { reply } => {
            let lsn = state.lock().await.committed_lsn;
            info!(lsn, "reporting last committed LSN");
            let _ = reply.send(Ok(lsn));
        }
        StateProviderEvent::GetCopyContext { reply } => {
            let lsn = state.lock().await.committed_lsn;
            let (tx, stream) = OperationStream::channel(1);
            let data = Bytes::from(lsn.to_string());
            let _ = tx.send(Operation::new(0, data, None)).await;
            drop(tx);
            info!(lsn, "sent copy context");
            let _ = reply.send(Ok(stream));
        }
        StateProviderEvent::GetCopyState {
            up_to_lsn,
            mut copy_context,
            reply,
        } => {
            let peer_lsn = if let Some(op) = copy_context.get_operation().await {
                String::from_utf8_lossy(&op.data)
                    .parse::<i64>()
                    .unwrap_or(0)
            } else {
                0
            };

            info!(peer_lsn, up_to_lsn, "producing copy state (DB snapshot)");

            let st = state.clone();
            let snapshot = match tokio::task::spawn_blocking(move || {
                let state = st.blocking_lock();
                state.snapshot_db()
            })
            .await
            {
                Ok(Ok(data)) => data,
                Ok(Err(e)) => {
                    warn!(error = %e, "failed to snapshot DB");
                    let _ = reply.send(Err(kuberic_core::KubericError::Internal(Box::new(e))));
                    return;
                }
                Err(e) => {
                    warn!(error = %e, "snapshot task panicked");
                    let _ = reply.send(Err(kuberic_core::KubericError::Internal(Box::new(
                        std::io::Error::other(format!("snapshot task panicked: {e}")),
                    ))));
                    return;
                }
            };

            let current_lsn = state.lock().await.last_applied_lsn;
            let (tx, stream) = OperationStream::channel(64);
            let _ = reply.send(Ok(stream));

            // Send snapshot in background (doesn't need state)
            tokio::spawn(async move {
                let _ = tx
                    .send(Operation::new(current_lsn, Bytes::from(snapshot), None))
                    .await;
                drop(tx);
                info!("copy state produced");
            });
        }
        StateProviderEvent::OnDataLoss { reply } => {
            let result = match data_loss_behavior {
                DataLossBehavior::NoStateChange => Ok(false),
                DataLossBehavior::StateChanged => Ok(true),
                DataLossBehavior::Fail(message) => {
                    Err(kuberic_core::KubericError::Internal(message.clone().into()))
                }
                DataLossBehavior::Delay {
                    duration,
                    state_changed,
                } => {
                    tokio::time::sleep(*duration).await;
                    Ok(*state_changed)
                }
            };
            match &result {
                Ok(state_changed) => {
                    info!(state_changed, "data loss callback completed");
                }
                Err(_) => {
                    warn!("data loss callback failed");
                }
            }
            let _ = reply.send(result);
        }
    }
}

/// Drain a copy or replication stream for the secondary.
async fn drain_stream(
    state: SharedState,
    mut stream: OperationStream,
    token: CancellationToken,
    label: &'static str,
) {
    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => {
                info!(label, "stream drain cancelled");
                break;
            }
            item = stream.get_operation() => {
                let Some(op) = item else { break };
                let lsn = op.lsn;

                if label == "copy" {
                    // Copy stream: full DB snapshot
                    let mut st = state.lock().await;
                    if let Err(e) = st.restore_from_snapshot(&op.data).await {
                        warn!(error = %e, "failed to restore snapshot");
                        continue;
                    }
                    st.last_applied_lsn = lsn;
                    st.committed_lsn = lsn;
                    info!(lsn, "restored DB from copy stream");
                    op.acknowledge();
                } else {
                    // Replication stream: WalFrameSet
                    match serde_json::from_slice::<WalFrameSet>(&op.data) {
                        Ok(frame_set) => {
                            if !frame_set.verify_checksum() {
                                warn!(lsn, "checksum mismatch, not acknowledging");
                                continue;
                            }
                            let mut st = state.lock().await;
                            if let Err(e) = st.persist_frame(lsn, &frame_set).await {
                                warn!(lsn, error = %e, "frame persist failed, not acknowledging");
                                continue;
                            }
                            op.acknowledge();
                        }
                        Err(e) => {
                            warn!(lsn, error = %e, "failed to deserialize WalFrameSet");
                        }
                    }
                }
            }
        }
    }
    info!(label, "stream drained");
}

/// Main service event loop.
pub async fn run_service(
    lifecycle_rx: mpsc::Receiver<LifecycleEvent>,
    state: SharedState,
    client_bind: String,
) {
    run_service_with_data_loss(
        lifecycle_rx,
        state,
        client_bind,
        DataLossBehavior::default(),
    )
    .await;
}

pub async fn run_service_with_data_loss(
    mut lifecycle_rx: mpsc::Receiver<LifecycleEvent>,
    state: SharedState,
    client_bind: String,
    data_loss_behavior: DataLossBehavior,
) {
    let mut partition = None;
    let mut replicator: Option<StateReplicatorHandle> = None;
    let mut state_provider_rx: Option<mpsc::UnboundedReceiver<StateProviderEvent>> = None;
    let mut copy_stream: Option<OperationStream> = None;
    let mut replication_stream: Option<OperationStream> = None;
    let mut token: Option<CancellationToken> = None;
    let mut bg_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let mut bg_token: Option<CancellationToken> = None;
    let mut client_server_handle: Option<tokio::task::JoinHandle<()>> = None;
    let mut client_server_shutdown: Option<CancellationToken> = None;
    let mut last_role = Role::Unknown;

    info!("sqlite service started, waiting for events");

    loop {
        tokio::select! {
            biased;

            Some(event) = lifecycle_rx.recv() => match event {
                LifecycleEvent::Open { ctx, reply } => {
                    info!("service opened — creating replicator");
                    let (sp_tx, sp_rx) = mpsc::unbounded_channel();
                    match WalReplicator::create(
                        ctx.replica_id,
                        &ctx.data_bind,
                        ctx.fault_tx.clone(),
                        sp_tx,
                    ).await {
                        Ok((handle, handles)) => {
                            partition = Some(handles.partition);
                            replicator = Some(handles.replicator);
                            copy_stream = handles.copy_stream;
                            replication_stream = handles.replication_stream;
                            state_provider_rx = Some(sp_rx);
                            token = Some(ctx.token);
                            let _ = reply.send(Ok(handle));
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "failed to create replicator");
                            let _ = reply.send(Err(e));
                        }
                    }
                }
                LifecycleEvent::ChangeRole { new_role, reply } => {
                    info!(?new_role, "role changed");

                    if new_role == Role::ActiveSecondary {
                        // Let copy drain finish
                        for h in bg_handles.drain(..) {
                            let _ = h.await;
                        }
                        // Apply committed frames after copy
                        {
                            let st = state.lock().await;
                            let lsn = st.last_applied_lsn;
                            drop(st);
                            let mut st = state.lock().await;
                            st.committed_lsn = lsn;
                            if let Err(e) = crate::framelog::FrameLog::save_meta(
                                &st.data_dir,
                                &crate::framelog::FrameLogMeta {
                                    committed_lsn: lsn,
                                    ..Default::default()
                                },
                            ).await {
                                warn!(error = %e, "meta save after copy failed");
                            }
                        }
                    } else {
                        if let Some(t) = bg_token.take() {
                            t.cancel();
                        }
                        for h in bg_handles.drain(..) {
                            let _ = h.await;
                        }
                    }

                    let t = CancellationToken::new();
                    bg_token = Some(t.clone());

                    match new_role {
                        Role::IdleSecondary => {
                            // Open frame log for secondary
                            {
                                let mut st = state.lock().await;
                                if let Err(e) = st.open_frame_log().await {
                                    warn!(error = %e, "failed to open frame log");
                                }
                            }
                            if let Some(cs) = copy_stream.take() {
                                let st = state.clone();
                                bg_handles.push(tokio::spawn(
                                    drain_stream(st, cs, t.clone(), "copy"),
                                ));
                            }
                        }
                        Role::ActiveSecondary => {
                            if let Some(rs) = replication_stream.take() {
                                let st = state.clone();
                                bg_handles.push(tokio::spawn(
                                    drain_stream(st, rs, t.clone(), "replication"),
                                ));
                            }
                        }
                        Role::Primary => {
                            // Apply unapplied frames, then open SQLite as primary
                            {
                                let mut st = state.lock().await;
                                let lsn = st.last_applied_lsn;
                                if let Err(e) = st.apply_committed_frames(lsn).await {
                                    warn!(error = %e, "applying frames before promotion failed");
                                }
                            }
                            // open_as_primary is blocking (rusqlite) — use spawn_blocking
                            let st = state.clone();
                            if let Err(e) = tokio::task::spawn_blocking(move || {
                                let mut st = st.blocking_lock();
                                st.open_as_primary()
                            })
                            .await
                            .unwrap_or_else(|e| Err(std::io::Error::other(e)))
                            {
                                warn!(error = %e, "failed to open as primary");
                            }
                            if client_server_handle.is_none() {
                                let srv_state = state.clone();
                                let p = partition.as_ref().unwrap().clone();
                                let r = replicator.as_ref().unwrap().clone();
                                let srv_token = token.as_ref().unwrap().clone();
                                let shutdown = CancellationToken::new();
                                let shutdown_cp = shutdown.clone();
                                let bind = client_bind.clone();

                                client_server_shutdown = Some(shutdown);
                                client_server_handle = Some(tokio::spawn(async move {
                                    run_client_server(
                                        bind, srv_state, p, r,
                                        srv_token, shutdown_cp,
                                    ).await;
                                }));
                            }
                        }
                        Role::None => {
                            // Permanent removal — stop client server immediately
                            if let Some(shutdown) = client_server_shutdown.take() {
                                shutdown.cancel();
                            }
                            if let Some(h) = client_server_handle.take() {
                                let _ = h.await;
                            }
                        }
                        Role::Unknown => {}
                    }

                    last_role = new_role;
                    let _ = reply.send(Ok(String::new()));
                }
                LifecycleEvent::Close { reply } => {
                    info!("service closing");
                    if let Some(token) = bg_token.take() {
                        token.cancel();
                    }
                    for h in bg_handles.drain(..) {
                        let _ = h.await;
                    }
                    if let Some(shutdown) = client_server_shutdown.take() {
                        shutdown.cancel();
                    }
                    if let Some(h) = client_server_handle.take() {
                        let _ = h.await;
                    }
                    let data_dir = state.lock().await.data_dir.clone();
                    state.lock().await.close();
                    if last_role == Role::None {
                        // Permanent removal — delete data directory
                        info!(?data_dir, "deleting data directory (decommissioned)");
                        let _ = tokio::fs::remove_dir_all(&data_dir).await;
                    }
                    let _ = reply.send(Ok(()));
                    break;
                }
                LifecycleEvent::Abort => {
                    if let Some(token) = bg_token.take() {
                        token.cancel();
                    }
                    if let Some(shutdown) = client_server_shutdown.take() {
                        shutdown.cancel();
                    }
                    state.lock().await.close();
                    break;
                }
            },

            Some(event) = async {
                match state_provider_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                handle_state_provider_event(event, &state, &data_loss_behavior).await;
            },

            else => break,
        }
    }
    info!("sqlite service exited");
}
