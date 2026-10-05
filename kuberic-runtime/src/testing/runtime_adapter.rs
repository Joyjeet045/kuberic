use std::sync::Arc;

use super::effects::{RuntimeEffect, RuntimeEffectResult};
use super::hosting::PodRuntime;
use super::sqlite_store::SqliteStore;

pub struct RuntimeAdapter {
    inner: crate::host::runtime_adapter::RuntimeAdapter<
        crate::host::sqlite_store::SqliteStore,
        crate::host::hosting::PodRuntime,
    >,
}

impl RuntimeAdapter {
    pub fn new(store: Arc<SqliteStore>, runtime: Arc<PodRuntime>) -> Self {
        Self {
            inner: crate::host::runtime_adapter::RuntimeAdapter::new(
                store.inner.clone(),
                runtime.inner.clone(),
            ),
        }
    }

    pub async fn execute(&self, effect: RuntimeEffect) -> crate::host::Result<RuntimeEffectResult> {
        self.inner
            .execute(super::convert(effect))
            .await
            .map(super::convert)
    }

    pub async fn resume_pending(&self) -> crate::host::Result<Option<RuntimeEffectResult>> {
        self.inner.resume_pending().await.map(super::convert)
    }
}
