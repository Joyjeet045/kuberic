use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::stream;
use kuberic_runtime::application::{OperationDataStream, StateProvider};
use kuberic_runtime::protocol::types::Epoch;
use kuberic_runtime::{Result, RuntimeError};

use crate::persistence::{KvPersistence, snapshot_stream};

pub struct KvStateProvider {
    persistence: Arc<KvPersistence>,
    copy_gate: CopyGate,
}

impl KvStateProvider {
    pub fn new(persistence: Arc<KvPersistence>, copy_gate: CopyGate) -> Self {
        Self {
            persistence,
            copy_gate,
        }
    }

    pub fn persistence(&self) -> &Arc<KvPersistence> {
        &self.persistence
    }
}

#[derive(Clone)]
pub struct CopyGate {
    path: Option<Arc<PathBuf>>,
    max_hold: Duration,
    io_lock: Arc<Mutex<()>>,
}

impl CopyGate {
    const MAX_HOLD: Duration = Duration::from_secs(120);

    pub fn disabled() -> Self {
        Self {
            path: None,
            max_hold: Self::MAX_HOLD,
            io_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn enabled(path: impl Into<PathBuf>) -> io::Result<Self> {
        Self::enabled_with_max_hold(path, Self::MAX_HOLD)
    }

    #[cfg(test)]
    fn enabled_with_timeout(path: impl Into<PathBuf>, max_hold: Duration) -> io::Result<Self> {
        Self::enabled_with_max_hold(path, max_hold)
    }

    fn enabled_with_max_hold(path: impl Into<PathBuf>, max_hold: Duration) -> io::Result<Self> {
        let gate = Self {
            path: Some(Arc::new(path.into())),
            max_hold,
            io_lock: Arc::new(Mutex::new(())),
        };
        gate.cleanup_or_schedule()?;
        Ok(gate)
    }

    pub fn hold(&self) -> io::Result<()> {
        let path = self.enabled_path()?;
        let deadline = unix_millis(
            SystemTime::now()
                .checked_add(self.max_hold)
                .ok_or_else(|| io::Error::other("copy gate expiry overflow"))?,
        )?;
        {
            let _guard = self
                .io_lock
                .lock()
                .map_err(|_| io::Error::other("copy gate lock poisoned"))?;
            write_deadline_atomically(path, deadline)?;
        }
        self.schedule_cleanup(deadline);
        Ok(())
    }

    pub fn release(&self) -> io::Result<()> {
        let Some(path) = self.path.as_deref() else {
            return Ok(());
        };
        let _guard = self
            .io_lock
            .lock()
            .map_err(|_| io::Error::other("copy gate lock poisoned"))?;
        remove_file_if_present(path)
    }

    #[cfg(test)]
    pub fn is_held(&self) -> bool {
        self.path.as_deref().is_some_and(|path| path.exists())
    }

    async fn wait_until_released(&self) -> Result<()> {
        if self.path.is_none() {
            return Ok(());
        }
        loop {
            let Some(deadline) = self.current_deadline().map_err(copy_gate_error)? else {
                return Ok(());
            };
            let now = unix_millis(SystemTime::now()).map_err(copy_gate_error)?;
            if now >= deadline {
                self.remove_if_deadline(deadline).map_err(copy_gate_error)?;
                return Ok(());
            }
            let remaining =
                Duration::from_millis(u64::try_from(deadline - now).unwrap_or(u64::MAX));
            tokio::time::sleep(remaining.min(Duration::from_millis(50))).await;
        }
    }

    fn enabled_path(&self) -> io::Result<&Path> {
        self.path
            .as_deref()
            .map(PathBuf::as_path)
            .ok_or_else(|| io::Error::other("live-test copy gate is disabled"))
    }

    fn cleanup_or_schedule(&self) -> io::Result<()> {
        let Some(deadline) = self.current_deadline()? else {
            return Ok(());
        };
        if unix_millis(SystemTime::now())? >= deadline {
            self.remove_if_deadline(deadline)?;
        } else {
            self.schedule_cleanup(deadline);
        }
        Ok(())
    }

    fn current_deadline(&self) -> io::Result<Option<u128>> {
        let Some(path) = self.path.as_deref() else {
            return Ok(None);
        };
        let _guard = self
            .io_lock
            .lock()
            .map_err(|_| io::Error::other("copy gate lock poisoned"))?;
        let contents = match std::fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        match contents.trim().parse::<u128>() {
            Ok(deadline) => Ok(Some(deadline)),
            Err(_) => {
                remove_file_if_present(path)?;
                Ok(None)
            }
        }
    }

    fn remove_if_deadline(&self, expected: u128) -> io::Result<()> {
        let Some(path) = self.path.as_deref() else {
            return Ok(());
        };
        let _guard = self
            .io_lock
            .lock()
            .map_err(|_| io::Error::other("copy gate lock poisoned"))?;
        let contents = match std::fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        match contents.trim().parse::<u128>() {
            Ok(deadline) if deadline != expected => Ok(()),
            Ok(_) | Err(_) => remove_file_if_present(path),
        }
    }

    fn schedule_cleanup(&self, deadline: u128) {
        let gate = self.clone();
        std::thread::spawn(move || {
            loop {
                let now = match unix_millis(SystemTime::now()) {
                    Ok(now) => now,
                    Err(_) => return,
                };
                if now >= deadline {
                    let _ = gate.remove_if_deadline(deadline);
                    return;
                }
                let remaining =
                    Duration::from_millis(u64::try_from(deadline - now).unwrap_or(u64::MAX));
                std::thread::sleep(remaining);
            }
        });
    }

    #[cfg(test)]
    fn persisted_deadline(&self) -> io::Result<Option<u128>> {
        self.current_deadline()
    }
}

fn unix_millis(time: SystemTime) -> io::Result<u128> {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .map_err(|error| io::Error::other(format!("system clock predates Unix epoch: {error}")))
}

fn write_deadline_atomically(path: &Path, deadline: u128) -> io::Result<()> {
    let mut temporary = path.as_os_str().to_os_string();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    writeln!(file, "{deadline}")?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn remove_file_if_present(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn copy_gate_error(error: io::Error) -> RuntimeError {
    RuntimeError::Application(format!("live-test copy gate failed: {error}"))
}

#[async_trait]
impl StateProvider for KvStateProvider {
    async fn update_epoch(&self, epoch: Epoch, _previous_epoch_last_lsn: i64) -> Result<()> {
        self.persistence.update_epoch(epoch)
    }

    async fn last_committed_lsn(&self) -> Result<i64> {
        use kuberic_runtime::engine::DurableState;
        Ok(self.persistence.durable_progress().await?.committed_lsn)
    }

    async fn get_copy_context(&self) -> Result<OperationDataStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn get_copy_state(
        &self,
        up_to_lsn: i64,
        mut copy_context: OperationDataStream,
    ) -> Result<OperationDataStream> {
        use futures::StreamExt;
        if copy_context.next().await.is_some() {
            return Err(RuntimeError::Application(
                "kvstore2 does not use a copy context".into(),
            ));
        }
        self.copy_gate.wait_until_released().await?;
        snapshot_stream(self.persistence.snapshot_at(up_to_lsn)?)
    }

    async fn on_data_loss(&self) -> Result<bool> {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use futures::{StreamExt, stream};
    use kuberic_runtime::application::Operation;
    use kuberic_runtime::engine::DurableState;

    use super::*;

    async fn collect_copy(mut copy: OperationDataStream) -> Vec<Bytes> {
        let mut chunks = Vec::new();
        while let Some(chunk) = copy.next().await {
            chunks.push(chunk.unwrap());
        }
        chunks
    }

    #[tokio::test]
    async fn provider_freezes_copy_before_post_boundary_write_and_replays_identically() {
        let directory = tempfile::tempdir().unwrap();
        let persistence = Arc::new(KvPersistence::open(directory.path()).unwrap());
        persistence
            .apply(Operation {
                lsn: 1,
                committed_lsn: 0,
                data: KvPersistence::encode_put("key".into(), "old".into()).unwrap(),
            })
            .await
            .unwrap();
        persistence.commit(1).await.unwrap();
        let provider = KvStateProvider::new(persistence.clone(), CopyGate::disabled());
        let frozen = provider
            .get_copy_state(1, Box::pin(stream::empty()))
            .await
            .unwrap();

        let post_boundary = Operation {
            lsn: 2,
            committed_lsn: 1,
            data: KvPersistence::encode_put("key".into(), "new".into()).unwrap(),
        };
        persistence.apply(post_boundary.clone()).await.unwrap();
        persistence.commit(2).await.unwrap();

        let first = collect_copy(frozen).await;
        let second = collect_copy(
            provider
                .get_copy_state(1, Box::pin(stream::empty()))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(first, second);
        let snapshot: BTreeMap<String, String> = serde_json::from_slice(&first.concat()).unwrap();
        assert_eq!(snapshot.get("key").map(String::as_str), Some("old"));

        let retained = persistence
            .get_replication_operations(2, 2)
            .await
            .unwrap()
            .map(|operation| operation.unwrap())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(retained, vec![post_boundary]);
    }

    #[tokio::test]
    async fn live_test_copy_gate_is_bounded_and_released_explicitly() {
        let directory = tempfile::tempdir().unwrap();
        let persistence = Arc::new(KvPersistence::open(directory.path()).unwrap());
        let gate = CopyGate::enabled(directory.path().join("copy-gate")).unwrap();
        gate.hold().unwrap();
        let provider = Arc::new(KvStateProvider::new(persistence, gate.clone()));
        let pending = tokio::spawn({
            let provider = provider.clone();
            async move { provider.get_copy_state(0, Box::pin(stream::empty())).await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!pending.is_finished());
        gate.release().unwrap();
        let mut stream = tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            Bytes::from_static(b"{}")
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn live_test_copy_gate_expiry_cleans_up_and_copy_continues() {
        let directory = tempfile::tempdir().unwrap();
        let persistence = Arc::new(KvPersistence::open(directory.path()).unwrap());
        let gate = CopyGate::enabled_with_timeout(
            directory.path().join("copy-gate"),
            Duration::from_millis(50),
        )
        .unwrap();
        gate.hold().unwrap();
        let provider = KvStateProvider::new(persistence, gate.clone());
        let mut stream = tokio::time::timeout(
            Duration::from_secs(1),
            provider.get_copy_state(0, Box::pin(stream::empty())),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            Bytes::from_static(b"{}")
        );
        assert!(!gate.is_held());
    }

    #[tokio::test]
    async fn disabled_copy_path_ignores_existing_sentinel() {
        let directory = tempfile::tempdir().unwrap();
        let sentinel = directory.path().join("copy-gate");
        std::fs::write(&sentinel, b"not-a-deadline").unwrap();
        let persistence = Arc::new(KvPersistence::open(directory.path().join("data")).unwrap());
        let provider = KvStateProvider::new(persistence, CopyGate::disabled());
        let mut copy = provider
            .get_copy_state(0, Box::pin(stream::empty()))
            .await
            .unwrap();
        assert_eq!(
            copy.next().await.unwrap().unwrap(),
            Bytes::from_static(b"{}")
        );
        assert!(sentinel.exists());
    }

    #[tokio::test]
    async fn armed_gate_expires_without_copy_activity() {
        let directory = tempfile::tempdir().unwrap();
        let gate = CopyGate::enabled_with_timeout(
            directory.path().join("copy-gate"),
            Duration::from_millis(50),
        )
        .unwrap();
        gate.hold().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while gate.is_held() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn restart_preserves_original_absolute_expiry() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("copy-gate");
        let first = CopyGate::enabled_with_timeout(&path, Duration::from_millis(240)).unwrap();
        first.hold().unwrap();
        let original_deadline = first.persisted_deadline().unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        let restarted = CopyGate::enabled_with_timeout(&path, Duration::from_millis(240)).unwrap();
        assert_eq!(
            restarted.persisted_deadline().unwrap(),
            Some(original_deadline)
        );
        tokio::time::timeout(Duration::from_millis(220), async {
            while restarted.is_held() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("restart extended the original arm-time expiry");
    }

    #[tokio::test]
    async fn cancelled_copy_wait_still_gets_background_cleanup() {
        let directory = tempfile::tempdir().unwrap();
        let persistence = Arc::new(KvPersistence::open(directory.path().join("data")).unwrap());
        let gate = CopyGate::enabled_with_timeout(
            directory.path().join("copy-gate"),
            Duration::from_millis(80),
        )
        .unwrap();
        gate.hold().unwrap();
        let provider = Arc::new(KvStateProvider::new(persistence, gate.clone()));
        let waiting =
            tokio::spawn(
                async move { provider.get_copy_state(0, Box::pin(stream::empty())).await },
            );
        tokio::time::sleep(Duration::from_millis(20)).await;
        waiting.abort();
        let _ = waiting.await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while gate.is_held() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn malformed_sentinel_is_removed_during_enabled_startup() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("copy-gate");
        std::fs::write(&path, b"malformed").unwrap();
        let gate = CopyGate::enabled(&path).unwrap();
        assert!(!gate.is_held());
        assert_eq!(gate.persisted_deadline().unwrap(), None);
    }

    #[test]
    fn expired_sentinel_is_removed_during_enabled_startup() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("copy-gate");
        std::fs::write(&path, b"0\n").unwrap();
        let gate = CopyGate::enabled(&path).unwrap();
        assert!(!gate.is_held());
        assert_eq!(gate.persisted_deadline().unwrap(), None);
    }

    #[tokio::test]
    async fn provider_empty_state_copy_uses_boundary_zero() {
        let directory = tempfile::tempdir().unwrap();
        let persistence = Arc::new(KvPersistence::open(directory.path()).unwrap());
        let provider = KvStateProvider::new(persistence, CopyGate::disabled());
        let chunks = collect_copy(
            provider
                .get_copy_state(0, Box::pin(stream::empty()))
                .await
                .unwrap(),
        )
        .await;
        let snapshot: BTreeMap<String, String> = serde_json::from_slice(&chunks.concat()).unwrap();
        assert!(snapshot.is_empty());
    }
}
