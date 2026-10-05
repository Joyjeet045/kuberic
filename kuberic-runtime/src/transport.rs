use crate::protocol::types::{ConfigurationId, Epoch, OperationId, ReplicaId, ReplicaIdentity};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ReplicationItem {
    pub(crate) sender: ReplicaIdentity,
    pub(crate) receiver: ReplicaIdentity,
    pub(crate) epoch: Epoch,
    pub(crate) previous_configuration_id: Option<ConfigurationId>,
    pub(crate) current_configuration_id: ConfigurationId,
    pub(crate) lsn: i64,
    pub(crate) committed_lsn: i64,
    pub(crate) data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ReplicationAck {
    pub(crate) sender: ReplicaIdentity,
    pub(crate) receiver: ReplicaIdentity,
    pub(crate) epoch: Epoch,
    pub(crate) previous_configuration_id: Option<ConfigurationId>,
    pub(crate) current_configuration_id: ConfigurationId,
    pub(crate) received_lsn: i64,
    pub(crate) applied_lsn: i64,
    pub(crate) committed_lsn: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CopyItem {
    pub(crate) build_id: OperationId,
    pub(crate) sender: ReplicaIdentity,
    pub(crate) receiver: ReplicaIdentity,
    pub(crate) epoch: Epoch,
    pub(crate) current_configuration_id: ConfigurationId,
    pub(crate) sequence: u64,
    pub(crate) lsn: i64,
    pub(crate) committed_lsn: i64,
    pub(crate) replication_boundary_lsn: i64,
    #[serde(default)]
    pub(crate) catch_up_boundary_lsn: Option<i64>,
    pub(crate) final_item: bool,
    pub(crate) snapshot_chunk: bool,
    pub(crate) data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CopyAck {
    pub(crate) build_id: OperationId,
    pub(crate) sender: ReplicaIdentity,
    pub(crate) receiver: ReplicaIdentity,
    pub(crate) epoch: Epoch,
    pub(crate) current_configuration_id: ConfigurationId,
    pub(crate) sequence: u64,
    pub(crate) durable_lsn: i64,
    pub(crate) replication_boundary_lsn: i64,
    #[serde(default)]
    pub(crate) catch_up_boundary_lsn: Option<i64>,
    pub(crate) final_item: bool,
    pub(crate) snapshot_chunk: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ReplicaEndpoint {
    pub(crate) build_id: OperationId,
    pub(crate) identity: ReplicaIdentity,
    pub(crate) replication_address: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OutboundOperation {
    Replication(ReplicationItem),
    Copy(CopyItem),
    Build(ReplicaEndpoint),
    Remove(ReplicaId),
    Evict(ReplicaIdentity),
}
