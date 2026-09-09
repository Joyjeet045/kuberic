use bytes::Bytes;
use kuberic_core::events::{LifecycleEvent, StateProviderEvent};
use kuberic_core::handles::StateReplicatorHandle;
use kuberic_core::replicator::{WalReplicator, WalReplicatorOptions};
use kuberic_core::types::{CancellationToken, Operation, OperationStream, Role};
use tokio::sync::mpsc;
use tracing::info;

use crate::server::run_client_server;
use crate::state::{CopyChunk, SharedState, drain_copy_stream, drain_stream};

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

async fn complete_copy(
    handles: &mut Vec<tokio::task::JoinHandle<std::io::Result<()>>>,
    failure: &mut Option<String>,
) -> kuberic_core::Result<()> {
    for handle in handles.drain(..) {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => *failure = Some(error.to_string()),
            Err(error) => *failure = Some(error.to_string()),
        }
    }
    match failure {
        Some(error) => Err(kuberic_core::KubericError::Internal(error.clone().into())),
        None => Ok(()),
    }
}

/// Handle a single state provider event.
///
/// This is the KV service's "state provider" — the replicator calls these
/// during copy, catchup, and reconfiguration. Matches SF's IStateProvider.
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
            // A6: Rollback uncommitted ops on epoch change.
            let mut state = state.write().await;
            state.generation += 1;
            let current_lsn = state.last_applied_lsn;
            if previous_epoch_last_lsn < current_lsn {
                info!(
                    previous_epoch_last_lsn,
                    current_lsn, "epoch updated — rolling back uncommitted ops"
                );
                if let Err(e) = state.rollback_to(previous_epoch_last_lsn).await {
                    tracing::warn!(error = %e, "rollback failed");
                    let _ = reply.send(Err(kuberic_core::KubericError::Internal(
                        e.to_string().into(),
                    )));
                    return;
                }
            } else {
                info!(previous_epoch_last_lsn, current_lsn, "epoch updated");
            }
            let _ = reply.send(Ok(()));
        }
        StateProviderEvent::GetLastCommittedLsn { reply } => {
            let lsn = state.read().await.last_applied_lsn;
            info!(lsn, "reporting last committed LSN");
            let _ = reply.send(Ok(lsn));
        }
        StateProviderEvent::GetCopyContext { reply } => {
            let lsn = state.read().await.last_applied_lsn;
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
            // Spawn to background — state serialization can be slow
            // for large datasets and must not block the event loop.
            let st = state.clone();
            tokio::spawn(async move {
                let peer_lsn = if let Some(op) = copy_context.get_operation().await {
                    String::from_utf8_lossy(&op.data)
                        .parse::<i64>()
                        .unwrap_or(0)
                } else {
                    0
                };

                info!(peer_lsn, up_to_lsn, "producing copy state");

                let applied = st.read().await.applied.clone();
                let snapshot = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    loop {
                        let notified = applied.notified();
                        tokio::pin!(notified);
                        notified.as_mut().enable();
                        let guard = st.read().await;
                        if guard.last_applied_lsn >= up_to_lsn {
                            break guard.snapshot_at(up_to_lsn).await;
                        }
                        drop(guard);
                        notified.await;
                    }
                })
                .await;
                let snapshot = match snapshot {
                    Ok(Ok(snapshot)) => snapshot,
                    result => {
                        let _ = reply.send(Err(kuberic_core::KubericError::Internal(
                            format!("cannot produce copy at LSN {up_to_lsn}: {result:?}").into(),
                        )));
                        return;
                    }
                };

                let (tx, stream) = OperationStream::channel(1);
                let _ = reply.send(Ok(stream));

                let data = serde_json::to_vec(&snapshot).unwrap();
                let chunks = data.chunks(256 * 1024);
                let count = chunks.len();
                for (index, data) in chunks.enumerate() {
                    let chunk = CopyChunk {
                        index,
                        data: data.to_vec(),
                        last: index + 1 == count,
                    };
                    let data = Bytes::from(serde_json::to_vec(&chunk).unwrap());
                    if tx
                        .send(Operation::new(up_to_lsn, data, None))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
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
                    tracing::warn!("data loss callback failed");
                }
            }
            let _ = reply.send(result);
        }
    }
}

/// Main service event loop. Processes lifecycle and state provider events
/// with biased select (lifecycle takes priority).
///
/// In the new API, the user creates the replicator in the Open handler
/// and returns a ReplicatorHandle to the runtime.
pub async fn run_service(
    lifecycle_rx: mpsc::Receiver<LifecycleEvent>,
    state: SharedState,
    client_bind: String,
) {
    run_service_with_options(
        lifecycle_rx,
        state,
        client_bind,
        WalReplicatorOptions::default(),
    )
    .await;
}

/// Run the service with explicit WAL replicator options.
pub async fn run_service_with_options(
    lifecycle_rx: mpsc::Receiver<LifecycleEvent>,
    state: SharedState,
    client_bind: String,
    replicator_options: WalReplicatorOptions,
) {
    run_service_with_options_and_data_loss(
        lifecycle_rx,
        state,
        client_bind,
        replicator_options,
        DataLossBehavior::default(),
    )
    .await;
}

pub async fn run_service_with_options_and_data_loss(
    mut lifecycle_rx: mpsc::Receiver<LifecycleEvent>,
    state: SharedState,
    client_bind: String,
    replicator_options: WalReplicatorOptions,
    data_loss_behavior: DataLossBehavior,
) {
    let mut partition = None;
    let mut replicator: Option<StateReplicatorHandle> = None;
    let mut state_provider_rx: Option<mpsc::UnboundedReceiver<StateProviderEvent>> = None;
    let mut copy_stream: Option<OperationStream> = None;
    let mut replication_stream: Option<OperationStream> = None;
    let mut token: Option<CancellationToken> = None;
    let mut bg_handles: Vec<tokio::task::JoinHandle<std::io::Result<()>>> = Vec::new();
    let mut copy_failure: Option<String> = None;
    let mut bg_token: Option<CancellationToken> = None;
    let mut client_server_handle: Option<tokio::task::JoinHandle<()>> = None;
    let mut client_server_shutdown: Option<CancellationToken> = None;
    let mut last_role = Role::Unknown;

    info!("kv service started, waiting for events");

    loop {
        tokio::select! {
            biased;

            Some(event) = lifecycle_rx.recv() => match event {
                LifecycleEvent::Open { ctx, reply } => {
                    info!("service opened — creating replicator");
                    // User creates the state provider channel
                    let (sp_tx, sp_rx) = mpsc::unbounded_channel();
                    // User creates the replicator, passes state_provider_tx
                    match WalReplicator::create_with_options(
                        ctx.replica_id,
                        &ctx.data_bind,
                        ctx.fault_tx.clone(),
                        sp_tx,
                        replicator_options.clone(),
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
                    if new_role == last_role {
                        let _ = reply.send(Ok(String::new()));
                        continue;
                    }
                    if matches!(new_role, Role::ActiveSecondary | Role::Primary)
                        && let Some(error) = &copy_failure
                    {
                        let _ = reply.send(Err(kuberic_core::KubericError::Internal(error.clone().into())));
                        continue;
                    }
                    state.write().await.generation += 1;

                    if new_role == Role::ActiveSecondary && last_role == Role::IdleSecondary {
                        // IdleSecondary → ActiveSecondary: let copy drain finish
                        if let Err(error) = complete_copy(&mut bg_handles, &mut copy_failure).await {
                            let _ = reply.send(Err(error));
                            continue;
                        }
                        // Checkpoint after copy completes
                        {
                            let mut guard = state.write().await;
                            guard.committed_lsn = guard.last_applied_lsn;
                            if let Err(e) = guard.checkpoint().await {
                                tracing::warn!(error = %e, "checkpoint after copy failed");
                                let _ = reply.send(Err(kuberic_core::KubericError::Internal(e.to_string().into())));
                                continue;
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
                            if let Some(cs) = copy_stream.take() {
                                let st = state.clone();
                                bg_handles.push(tokio::spawn(
                                    drain_copy_stream(st, cs, t.clone()),
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
                    if last_role == Role::None {
                        // Permanent removal — delete data directory
                        let dir = state.read().await.data_dir().to_path_buf();
                        info!(?dir, "deleting data directory (decommissioned)");
                        let _ = tokio::fs::remove_dir_all(&dir).await;
                    } else {
                        // Graceful close — checkpoint for fast recovery
                        let mut guard = state.write().await;
                        guard.committed_lsn = guard.last_applied_lsn;
                        if let Err(e) = guard.checkpoint().await {
                            tracing::warn!(error = %e, "checkpoint on close failed");
                        }
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
    info!("kv service exited");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failed_copy_cannot_succeed_when_completion_is_retried() {
        let directory =
            std::env::temp_dir().join(format!("kv-copy-failure-{:032x}", rand::random::<u128>()));
        let state = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::state::KvState::open(directory.clone())
                .await
                .unwrap(),
        ));
        let (sender, stream) = OperationStream::channel(1);
        drop(sender);
        let mut handles = vec![tokio::spawn(drain_copy_stream(
            state.clone(),
            stream,
            CancellationToken::new(),
        ))];
        let mut failure = None;
        assert!(complete_copy(&mut handles, &mut failure).await.is_err());
        assert!(handles.is_empty());
        assert!(complete_copy(&mut handles, &mut failure).await.is_err());
        assert_eq!(state.read().await.last_applied_lsn, 0);
        drop(state);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}
