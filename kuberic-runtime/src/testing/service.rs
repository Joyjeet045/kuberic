use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use super::hosting::PodRuntime;
use super::sqlite_store::SqliteStore;
use crate::protocol::types::ProcessSessionId;

pub struct AgentService<S = SqliteStore, R = PodRuntime> {
    inner: crate::host::service::AgentService<
        crate::host::sqlite_store::SqliteStore,
        crate::host::hosting::PodRuntime,
    >,
    marker: PhantomData<(S, R)>,
}

impl<S, R> Clone for AgentService<S, R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            marker: PhantomData,
        }
    }
}

pub struct SessionRegistry {
    local: ProcessSessionId,
}

impl SessionRegistry {
    pub fn local_session(&self) -> &ProcessSessionId {
        &self.local
    }
}

impl AgentService {
    pub fn new(
        store: Arc<SqliteStore>,
        runtime: Arc<PodRuntime>,
        reporter: Arc<PodRuntime>,
        bearer_token: impl Into<Arc<str>>,
    ) -> crate::host::Result<Self> {
        Ok(Self {
            inner: crate::host::service::AgentService::new(
                store.inner.clone(),
                runtime.inner.clone(),
                reporter.inner.clone(),
                bearer_token,
            )?,
            marker: PhantomData,
        })
    }

    pub fn sessions(&self) -> SessionRegistry {
        SessionRegistry {
            local: self.inner.sessions().local_session().clone(),
        }
    }

    pub async fn reconstruct_runtime(&self) -> crate::host::Result<()> {
        self.inner.reconstruct_runtime().await
    }

    pub async fn serve(
        self,
        control: SocketAddr,
        replication: SocketAddr,
        ready: tokio::sync::watch::Sender<bool>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> crate::host::Result<()> {
        self.inner
            .serve(control, replication, ready, shutdown)
            .await
    }
}
