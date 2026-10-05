use std::pin::Pin;
use std::sync::Arc;

use super::copy::{BuildConfiguration, PrepareCopyRequest};
use super::effects::{RuntimeEffect, RuntimeEffectResult, RuntimeSnapshot};
use super::sqlite_store::SqliteStore;
use crate::application::OpenMode;
use crate::control::proto;
use crate::protocol::types::*;
use crate::{Result, StatefulServiceReplica};
use futures::Stream;

pub struct PodRuntime {
    pub(super) inner: Arc<crate::host::hosting::PodRuntime>,
}

impl PodRuntime {
    pub fn new<A: StatefulServiceReplica + 'static>(
        identity: ReplicaIdentity,
        application: Arc<A>,
        store: Arc<SqliteStore>,
    ) -> Self {
        Self {
            inner: Arc::new(crate::host::hosting::PodRuntime::new(
                identity,
                application,
                store.inner.clone(),
            )),
        }
    }

    pub fn abort(&self) {
        self.inner.abort();
    }

    pub fn bind_replica_session(
        &self,
        resource: ResourceUid,
        session: ProcessSessionId,
    ) -> Result<()> {
        self.inner.bind_replica_session(resource, session)
    }

    pub async fn authorize_build(
        &self,
        id: OperationId,
        target: ReplicaIdentity,
        configuration: BuildConfiguration,
    ) -> Result<BuildAuthority> {
        self.inner
            .authorize_build(id, target, configuration.into_inner())
            .await
    }

    pub async fn reconstruct(
        &self,
        mode: OpenMode,
        role: ReplicaRole,
        read: AccessStatus,
        write: AccessStatus,
        transition: Option<(ReplicaRole, bool, bool)>,
    ) -> Result<()> {
        self.inner
            .reconstruct(mode, role, read, write, transition)
            .await
    }

    pub async fn apply_effect(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        self.inner
            .apply_effect(super::convert(effect))
            .await
            .map(super::convert)
    }

    pub async fn snapshot(&self) -> RuntimeSnapshot {
        super::convert(self.inner.snapshot().await)
    }

    pub async fn cancel_configuration_work(&self) -> Result<()> {
        self.inner.cancel_configuration_work().await
    }

    pub async fn cancel_outbound_build(&self, id: &OperationId) -> Result<()> {
        self.inner.cancel_outbound_build(id).await
    }

    pub async fn primary_replicator(&self) -> Result<Arc<dyn crate::PrimaryReplicator>> {
        self.inner.primary_replicator().await
    }

    pub async fn register_peer_session(
        &self,
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    ) -> Result<()> {
        self.inner.register_peer_session(identity, session).await
    }

    pub async fn repair_peer(&self, identity: ReplicaIdentity, progress: i64) -> Result<()> {
        self.inner.repair_peer(identity, progress).await
    }

    pub async fn partition_report(&self) -> PartitionReportSnapshot {
        let report = self.inner.partition_report().await;
        PartitionReportSnapshot {
            information: report.information,
            read_status: report.read_status,
            write_status: report.write_status,
            load_metrics: report.load_metrics,
            reported_fault: report.reported_fault,
        }
    }

    pub fn data_plane(&self) -> RuntimeDataPlane {
        RuntimeDataPlane(self.inner.data_plane())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReportSnapshot {
    pub information: PartitionInformation,
    pub read_status: AccessStatus,
    pub write_status: AccessStatus,
    pub load_metrics: Vec<LoadMetric>,
    pub reported_fault: Option<FaultType>,
}

#[derive(Clone)]
pub struct RuntimeDataPlane(crate::host::hosting::RuntimeDataPlane);

pub struct PreparedCopy {
    pub authority: BuildAuthority,
    pub items: Pin<Box<dyn Stream<Item = Result<proto::CopyItem>> + Send>>,
}

pub struct PendingReplication {
    pub received: proto::ReplicationAck,
    inner: crate::host::hosting::PendingReplication,
}

impl PendingReplication {
    pub async fn applied(self) -> Result<proto::ReplicationAck> {
        self.inner.applied().await
    }
}

#[derive(Debug)]
pub enum OutboundReplication {
    Replication(proto::ReplicationItem),
    Copy(proto::CopyItem),
    Build(super::transport::ReplicaEndpoint),
    Remove(ReplicaId),
    Evict(ReplicaIdentity),
}

impl OutboundReplication {
    fn from_inner(value: crate::host::hosting::OutboundReplication) -> Self {
        match value {
            crate::host::hosting::OutboundReplication::Replication(item) => Self::Replication(item),
            crate::host::hosting::OutboundReplication::Copy(item) => Self::Copy(item),
            crate::host::hosting::OutboundReplication::Build(endpoint) => {
                Self::Build(super::transport::ReplicaEndpoint {
                    identity: endpoint.identity,
                    replication_address: endpoint.replication_address,
                    build_id: endpoint.build_id,
                })
            }
            crate::host::hosting::OutboundReplication::Remove(id) => Self::Remove(id),
            crate::host::hosting::OutboundReplication::Evict(identity) => Self::Evict(identity),
        }
    }
}

impl RuntimeDataPlane {
    pub async fn next_outbound(&self) -> Option<OutboundReplication> {
        self.0
            .next_outbound()
            .await
            .map(OutboundReplication::from_inner)
    }

    pub async fn prepare_copy(&self, request: PrepareCopyRequest) -> Result<PreparedCopy> {
        let prepared = self.0.prepare_copy(request.into_inner()).await?;
        Ok(PreparedCopy {
            authority: prepared.authority,
            items: prepared.items,
        })
    }

    pub async fn receive_replication(
        &self,
        item: proto::ReplicationItem,
    ) -> Result<PendingReplication> {
        let inner = self.0.receive_replication(item).await?;
        Ok(PendingReplication {
            received: inner.received.clone(),
            inner,
        })
    }

    pub async fn receive_copy_item(&self, item: proto::CopyItem) -> Result<proto::CopyAck> {
        self.0.receive_copy_item(item).await
    }

    pub async fn accept_acknowledgement(&self, ack: proto::ReplicationAck) -> Result<()> {
        self.0.accept_acknowledgement(ack).await
    }

    pub async fn accept_copy_acknowledgement(&self, ack: proto::CopyAck) -> Result<()> {
        self.0.accept_copy_acknowledgement(ack).await
    }
}
