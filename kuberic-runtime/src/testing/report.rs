use std::sync::Arc;

use super::hosting::PodRuntime;
use super::sqlite_store::SqliteStore;
use crate::control::proto;

pub struct AgentReporter {
    inner: crate::host::report::AgentReporter<crate::host::sqlite_store::SqliteStore>,
}

impl AgentReporter {
    pub fn new(store: Arc<SqliteStore>) -> Self {
        Self {
            inner: crate::host::report::AgentReporter::new(store.inner.clone()),
        }
    }

    pub async fn report(
        &self,
        runtime: &PodRuntime,
    ) -> crate::host::Result<proto::AgentStatusReport> {
        self.inner.report(&runtime.inner.report_runtime()).await
    }
}
