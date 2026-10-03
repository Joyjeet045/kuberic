use super::*;
use kuberic_protocol::types::{ConfigurationId, SecondaryScaleDownCleanup};
use kuberic_runtime_internal::Result as ContractResult;
use kuberic_runtime_internal::authority::{
    AuthorityFence, BuildAuthority, BuildAuthorityStore, BuildProgressStore, DurableBuildProgress,
    DurableLocalWrite, RetiredAuthority,
};

pub(super) struct FaultAuthorityStore {
    pub(super) inner: Arc<SqliteStore>,
    pub(super) boundary: String,
    pub(super) interrupted: Mutex<Option<kuberic_agent::hosting::PendingWrite>>,
    pub(super) candidate_path: PathBuf,
}

#[async_trait]
impl ReplicaAuthorityStore for FaultAuthorityStore {
    async fn load(&self) -> ContractResult<Option<AdmittedAuthority>> {
        self.inner.load().await
    }
    async fn admit(&self, authority: &AdmittedAuthority) -> ContractResult<()> {
        self.inner.admit(authority).await
    }
    async fn load_secondary_removal(&self) -> ContractResult<Option<SecondaryRemovalPreparation>> {
        self.inner.load_secondary_removal().await
    }
    async fn record_secondary_removal(
        &self,
        preparation: &SecondaryRemovalPreparation,
    ) -> ContractResult<()> {
        if self.boundary == "prepare:closed-before-boundary" {
            assert!(self.inner.load_secondary_removal().await?.is_none());
            assert_eq!(preparation.boundary_lsn, 2);
            let pending = self
                .inner
                .load_state()
                .await
                .unwrap()
                .pending_effect
                .unwrap();
            assert_eq!(pending.stage, EffectStage::IntentCommitted);
            assert!(matches!(
                pending.effect.action,
                RuntimeEffectAction::PrepareSecondaryRemoval { .. }
            ));
            let write = self
                .inner
                .load_local_write(&OperationId::new("interrupted-local-write"))
                .await?
                .unwrap();
            assert_eq!(write.lsn, 2);
            assert_eq!(write.data.as_ref(), b"unacknowledged-before-preparation");
            let active = self.inner.load().await?.unwrap();
            assert_eq!(
                self.inner
                    .load_replication_progress(&active.fence())
                    .await?
                    .unwrap()
                    .verified_lsn,
                2
            );
            let interrupted = self.interrupted.lock().unwrap().take().unwrap();
            assert!(
                matches!(
                    tokio::time::timeout(
                        std::time::Duration::from_secs(1),
                        interrupted.committed()
                    )
                    .await
                    .unwrap(),
                    Err(RuntimeError::WriteClosed(
                        AccessStatus::ReconfigurationPending
                    ))
                ),
                "fenced client must not report success"
            );
            std::fs::write(
                &self.candidate_path,
                serde_json::to_vec(preparation).unwrap(),
            )
            .unwrap();
            std::fs::File::open(&self.candidate_path)
                .unwrap()
                .sync_all()
                .unwrap();
            std::process::exit(73);
        }
        self.inner.record_secondary_removal(preparation).await
    }
    async fn load_retirement_started(&self) -> ContractResult<Option<RetiredAuthority>> {
        self.inner.load_retirement_started().await
    }
    async fn record_retirement_started(&self, retired: &RetiredAuthority) -> ContractResult<()> {
        self.inner.record_retirement_started(retired).await?;
        if self.boundary == "retire:started-before-role-none" {
            assert_eq!(
                self.inner.load_retirement_started().await?,
                Some(retired.clone())
            );
            assert!(self.inner.load_retired_authority().await?.is_none());
            assert!(self.inner.load().await?.is_some());
            std::process::exit(73);
        }
        Ok(())
    }
    async fn load_retired_authority(&self) -> ContractResult<Option<RetiredAuthority>> {
        self.inner.load_retired_authority().await
    }
    async fn retire(&self, retired: &RetiredAuthority) -> ContractResult<()> {
        self.inner.retire(retired).await
    }
    async fn load_secondary_removal_commit(
        &self,
    ) -> ContractResult<Option<SecondaryScaleDownCleanup>> {
        self.inner.load_secondary_removal_commit().await
    }
    async fn record_secondary_removal_commit(
        &self,
        committed: &SecondaryScaleDownCleanup,
    ) -> ContractResult<()> {
        self.inner.record_secondary_removal_commit(committed).await
    }
}

#[async_trait]
impl ReplicationProgressStore for FaultAuthorityStore {
    async fn load_replication_progress(
        &self,
        fence: &AuthorityFence,
    ) -> ContractResult<Option<ReplicationProgress>> {
        self.inner.load_replication_progress(fence).await
    }
    async fn load_configuration_progress(
        &self,
        epoch: Epoch,
        id: &ConfigurationId,
    ) -> ContractResult<Option<ReplicationProgress>> {
        self.inner.load_configuration_progress(epoch, id).await
    }
    async fn record_replication_progress(
        &self,
        progress: &ReplicationProgress,
    ) -> ContractResult<()> {
        self.inner.record_replication_progress(progress).await
    }
}

#[async_trait]
impl LocalWriteJournal for FaultAuthorityStore {
    async fn load_local_write(
        &self,
        id: &OperationId,
    ) -> ContractResult<Option<DurableLocalWrite>> {
        self.inner.load_local_write(id).await
    }
    async fn load_local_writes(&self) -> ContractResult<Vec<DurableLocalWrite>> {
        self.inner.load_local_writes().await
    }
    async fn record_local_write(&self, write: &DurableLocalWrite) -> ContractResult<()> {
        self.inner.record_local_write(write).await
    }
    async fn reset_local_writes_after_data_loss(&self, lsn: i64) -> ContractResult<()> {
        self.inner.reset_local_writes_after_data_loss(lsn).await
    }
}

#[async_trait]
impl BuildAuthorityStore for FaultAuthorityStore {
    async fn load_build(&self, id: &OperationId) -> ContractResult<Option<BuildAuthority>> {
        self.inner.load_build(id).await
    }
    async fn admit_build(&self, authority: &BuildAuthority) -> ContractResult<()> {
        self.inner.admit_build(authority).await
    }
    async fn select_build(
        &self,
        authority: &BuildAuthority,
    ) -> ContractResult<kuberic_runtime_internal::authority::BuildSelection> {
        self.inner.select_build(authority).await
    }
    async fn load_build_selection(
        &self,
        target: &kuberic_protocol::types::ReplicaIdentity,
    ) -> ContractResult<Option<kuberic_runtime_internal::authority::BuildSelection>> {
        self.inner.load_build_selection(target).await
    }
}

#[async_trait]
impl BuildProgressStore for FaultAuthorityStore {
    async fn load_build_progress(
        &self,
        id: &OperationId,
    ) -> ContractResult<Option<DurableBuildProgress>> {
        self.inner.load_build_progress(id).await
    }
    async fn record_build_progress(&self, progress: &DurableBuildProgress) -> ContractResult<()> {
        self.inner.record_build_progress(progress).await
    }
    async fn record_selected_build_progress(
        &self,
        selection: &kuberic_runtime_internal::authority::BuildSelection,
        progress: &DurableBuildProgress,
    ) -> ContractResult<()> {
        self.inner
            .record_selected_build_progress(selection, progress)
            .await
    }
}
