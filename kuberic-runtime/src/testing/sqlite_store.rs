use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::authority::{BuildAuthorityStore, BuildProgressStore, LocalWriteJournal};
use crate::host::store::AgentStore;
use crate::protocol::command::EnsureReplicaBuild;
use crate::protocol::types::{BuildAuthority, OperationId};

use super::authority::{DurableBuildProgress, DurableLocalWrite};
use super::effects::{RuntimeEffect, RuntimeEffectResult};
use super::state::{AgentState, StorageIdentity};

pub struct SqliteStore {
    pub(super) inner: Arc<crate::host::sqlite_store::SqliteStore>,
}

impl SqliteStore {
    pub fn metadata_database_path(root: &Path) -> PathBuf {
        crate::host::sqlite_store::SqliteStore::metadata_database_path(root)
    }

    pub fn create_authorized(
        path: impl AsRef<Path>,
        state: AgentState,
    ) -> crate::host::Result<Self> {
        Ok(Self {
            inner: Arc::new(crate::host::sqlite_store::SqliteStore::create_authorized(
                path,
                super::convert(state),
            )?),
        })
    }

    pub fn open_existing(
        path: impl AsRef<Path>,
        identity: Option<&StorageIdentity>,
    ) -> crate::host::Result<Self> {
        let identity = identity.map(super::convert);
        Ok(Self {
            inner: Arc::new(crate::host::sqlite_store::SqliteStore::open_existing(
                path,
                identity.as_ref(),
            )?),
        })
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    pub async fn load_state(&self) -> crate::host::Result<AgentState> {
        self.inner.load_state().await.map(super::convert)
    }

    pub async fn journal_build(
        &self,
        command: &EnsureReplicaBuild,
    ) -> crate::host::Result<EnsureReplicaBuild> {
        self.inner.journal_build(command).await
    }

    pub async fn load_local_writes(&self) -> crate::host::Result<Vec<DurableLocalWrite>> {
        Ok(super::convert(self.inner.load_local_writes().await?))
    }

    pub async fn load_local_write(
        &self,
        id: &OperationId,
    ) -> crate::host::Result<Option<DurableLocalWrite>> {
        Ok(super::convert(self.inner.load_local_write(id).await?))
    }

    pub async fn load_build(
        &self,
        id: &OperationId,
    ) -> crate::host::Result<Option<BuildAuthority>> {
        Ok(self.inner.load_build(id).await?)
    }

    pub async fn load_build_progress(
        &self,
        id: &OperationId,
    ) -> crate::host::Result<Option<DurableBuildProgress>> {
        Ok(super::convert(self.inner.load_build_progress(id).await?))
    }

    pub async fn begin_effect(&self, effect: &RuntimeEffect) -> crate::host::Result<()> {
        self.inner.begin_effect(&super::convert(effect)).await?;
        Ok(())
    }

    pub async fn mark_effect_applied(
        &self,
        effect: &RuntimeEffect,
        result: &super::effects::RuntimeEffectResult,
    ) -> crate::host::Result<()> {
        self.inner
            .mark_effect_applied(&super::convert(effect), &super::convert(result))
            .await
    }

    pub async fn complete_effect(&self, result: &RuntimeEffectResult) -> crate::host::Result<()> {
        self.inner.complete_effect(&super::convert(result)).await
    }
}
