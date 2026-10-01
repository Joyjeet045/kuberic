use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use kuberic_agent::process::ApplicationStorageState;
use kuberic_protocol::types::{FaultType, ReplicaRole, ResourceUid};
use kuberic_runtime::application::{OpenContext, OpenMode, RoleChange, StateProvider};
use kuberic_runtime::replicator::{ReplicatorFactoryContext, ReplicatorSettings};
use kuberic_runtime::{
    Replicator, ReplicatorFactory, ReplicatorInterfaces, Result, RuntimeError,
    StatefulServiceReplica,
};
use tokio_util::sync::CancellationToken;

use crate::adapter::{PgReplicator, application_error, report_failure};
use crate::durable::{PgDurableIdentity, PgDurableStore, StorageMode};
use crate::instance::PgInstanceManager;

#[derive(Clone)]
pub struct PgServiceConfig {
    pub resource_uid: ResourceUid,
    pub application_root: PathBuf,
    pub pg_data: PathBuf,
    pub pg_bin: PathBuf,
    pub pg_port: u16,
    pub replication_address: String,
}

pub struct PgService {
    config: PgServiceConfig,
    instance: Arc<PgInstanceManager>,
    driver: OnceLock<Arc<PgReplicator>>,
    cancellation: CancellationToken,
    coordination_token: String,
}

impl PgService {
    /// Construction and storage classification never create or open application state.
    pub fn deferred(config: PgServiceConfig) -> Self {
        Self {
            instance: Arc::new(PgInstanceManager::new(
                config.pg_data.clone(),
                config.pg_bin.clone(),
                config.pg_port,
            )),
            config,
            driver: OnceLock::new(),
            cancellation: CancellationToken::new(),
            coordination_token: String::new(),
        }
    }

    pub fn storage_state(config: &PgServiceConfig) -> std::io::Result<ApplicationStorageState> {
        if is_empty(&config.application_root)? && is_empty(&config.pg_data)? {
            Ok(ApplicationStorageState::FreshEmpty)
        } else {
            Ok(ApplicationStorageState::Established)
        }
    }

    pub fn storage_paths(config: &PgServiceConfig) -> BTreeMap<String, PathBuf> {
        BTreeMap::from([
            (
                "postgres-application".into(),
                config.application_root.clone(),
            ),
            ("postgres-pgdata".into(), config.pg_data.clone()),
        ])
    }

    pub fn instance(&self) -> &Arc<PgInstanceManager> {
        &self.instance
    }

    pub fn with_coordination_token(mut self, token: String) -> Self {
        self.coordination_token = token;
        self
    }

    pub(crate) fn driver(&self) -> std::result::Result<&Arc<PgReplicator>, tonic::Status> {
        self.driver
            .get()
            .ok_or_else(|| tonic::Status::unavailable("PostgreSQL host is not open"))
    }

    pub fn abort_and_wait(&self) -> Result<()> {
        self.cancellation.cancel();
        self.instance.abort_owned().map_err(application_error)
    }

    #[cfg(feature = "testing")]
    pub fn native_driver(&self) -> &Arc<PgReplicator> {
        self.driver.get().expect("service is open")
    }
}

pub(crate) fn is_empty(path: &Path) -> std::io::Result<bool> {
    match std::fs::read_dir(path) {
        Ok(mut entries) => Ok(entries.next().transpose()?.is_none()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error),
    }
}

#[async_trait]
impl StatefulServiceReplica for PgService {
    async fn open(self: Arc<Self>, context: OpenContext) -> Result<Arc<dyn Replicator>> {
        if context
            .partition
            .get_partition_information()
            .partition_id
            .as_str()
            != self.config.resource_uid.as_str()
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        if self.driver.get().is_some() || self.cancellation.is_cancelled() {
            return Err(RuntimeError::Closed);
        }
        let empty = Self::storage_state(&self.config).map_err(application_error)?
            == ApplicationStorageState::FreshEmpty;
        let initializing = context.mode == OpenMode::New;
        let durable = match PgDurableStore::open(
            &self.config.application_root,
            PgDurableIdentity {
                resource_uid: self.config.resource_uid.clone(),
                replica: context.identity,
            },
            if initializing && empty {
                StorageMode::Fresh
            } else {
                StorageMode::Established
            },
        )
        .await
        {
            Ok(durable) => Arc::new(durable),
            Err(error) => {
                return Err(report_failure(&context.partition, FaultType::Permanent, error).await);
            }
        };
        let (fault_tx, mut fault_rx) = tokio::sync::mpsc::channel(8);
        let (generation_fault_tx, mut generation_fault_rx) = tokio::sync::mpsc::channel(8);
        self.instance.bind_fault_sink(generation_fault_tx);
        // The reporting task owns the partition lifetime; the driver must not
        // form a strong host -> driver -> partition -> host cycle.
        let partition = Arc::new(context.partition.clone());
        let driver = Arc::new(
            PgReplicator::new(
                self.instance.clone(),
                durable,
                fault_tx,
                Arc::downgrade(&partition),
                self.cancellation.clone(),
                crate::data_service::PgCoordination {
                    local_endpoint: self.config.replication_address.clone(),
                    bearer_token: self.coordination_token.clone(),
                },
                initializing,
            )
            .await,
        );
        self.driver
            .set(driver.clone())
            .map_err(|_| RuntimeError::Closed)?;
        let cancellation = self.cancellation.clone();
        let instance = Arc::downgrade(&self.instance);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    fault = generation_fault_rx.recv() => match fault {
                        Some(fault) => {
                            if let Some(instance) = instance.upgrade() {
                                instance.deliver_fault(fault, &partition).await;
                            }
                        }
                        None => break,
                    },
                    // Only the standalone instance API uses the untagged sink.
                    // Keep its receiver alive without forwarding stale notices.
                    _ = fault_rx.recv() => {}
                }
            }
        });
        let interfaces = context
            .partition
            .with_factory(Arc::new(PgReplicatorFactory(driver)))
            .create_replicator(None, None)
            .await?;
        debug_assert!(interfaces.state_replicator().is_none());
        Ok(interfaces.replicator())
    }

    async fn change_role(&self, role: ReplicaRole) -> Result<RoleChange> {
        Ok(RoleChange {
            service_address: matches!(role, ReplicaRole::Primary | ReplicaRole::ActiveSecondary)
                .then(|| format!("{}:{}", self.instance.listen_host(), self.instance.port())),
        })
    }

    async fn close(&self) -> Result<()> {
        let result = match self.driver.get() {
            Some(driver) => driver.close().await,
            None => Ok(()),
        };
        self.cancellation.cancel();
        // An interrupted open has no opened driver, but may own a process tree
        // and a retained abort failure. Always join cleanup and surface that result.
        let cleanup = self.instance.stop().await.map_err(application_error);
        match (result, cleanup) {
            (Err(error), Err(cleanup)) => Err(application_error(format!(
                "{error}; PostgreSQL cleanup: {cleanup}"
            ))),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    fn abort(&self) {
        if let Err(error) = self.abort_and_wait() {
            tracing::error!(%error, "PostgreSQL abort cleanup failed");
        }
    }
}

impl Drop for PgService {
    fn drop(&mut self) {
        self.abort();
    }
}

struct PgReplicatorFactory(Arc<PgReplicator>);

impl PgReplicatorFactory {
    fn interfaces(&self) -> ReplicatorInterfaces {
        ReplicatorInterfaces::primary(self.0.clone(), None)
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use crate::testing::{PgPod, TestDataDir, layout::SINGLE_REPLICA_DIRECTORY, native_identity};

    #[tokio::test]
    async fn postgres_custom_factory_returns_no_operation_or_copy_state_replicator() {
        let directory = TestDataDir::new("sf-api");
        let pod = PgPod::new(
            directory.path().join(SINGLE_REPLICA_DIRECTORY),
            native_identity(1, "api"),
        )
        .await;
        let factory = PgReplicatorFactory(pod.application.native_driver().clone());
        let interfaces = factory.interfaces();
        assert!(interfaces.state_replicator().is_none());
        assert!(interfaces.primary_replicator().is_some());
        let control: Arc<dyn Replicator> = pod.application.native_driver().clone();
        assert!(Arc::ptr_eq(&interfaces.replicator(), &control));
    }
}

#[async_trait]
impl ReplicatorFactory for PgReplicatorFactory {
    async fn create_replicator(
        &self,
        _context: ReplicatorFactoryContext,
        state_provider: Option<Arc<dyn StateProvider>>,
        _settings: ReplicatorSettings,
    ) -> Result<ReplicatorInterfaces> {
        if state_provider.is_some() {
            return Err(application_error(
                "PostgreSQL owns its replication and copy state",
            ));
        }
        Ok(self.interfaces())
    }
}
