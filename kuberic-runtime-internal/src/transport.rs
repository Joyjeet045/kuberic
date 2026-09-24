use bytes::Bytes;
use kuberic_protocol::types::{ConfigurationId, Epoch, OperationId, ReplicaId, ReplicaIdentity};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationItem {
    pub sender: ReplicaIdentity,
    pub receiver: ReplicaIdentity,
    pub epoch: Epoch,
    pub previous_configuration_id: Option<ConfigurationId>,
    pub current_configuration_id: ConfigurationId,
    pub lsn: i64,
    pub committed_lsn: i64,
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationAck {
    pub sender: ReplicaIdentity,
    pub receiver: ReplicaIdentity,
    pub epoch: Epoch,
    pub previous_configuration_id: Option<ConfigurationId>,
    pub current_configuration_id: ConfigurationId,
    pub received_lsn: i64,
    pub applied_lsn: i64,
    pub committed_lsn: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CopyItem {
    pub build_id: OperationId,
    pub sender: ReplicaIdentity,
    pub receiver: ReplicaIdentity,
    pub epoch: Epoch,
    pub current_configuration_id: ConfigurationId,
    pub sequence: u64,
    pub lsn: i64,
    pub committed_lsn: i64,
    pub replication_boundary_lsn: i64,
    pub final_item: bool,
    pub snapshot_chunk: bool,
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CopyAck {
    pub build_id: OperationId,
    pub sender: ReplicaIdentity,
    pub receiver: ReplicaIdentity,
    pub epoch: Epoch,
    pub current_configuration_id: ConfigurationId,
    pub sequence: u64,
    pub durable_lsn: i64,
    pub replication_boundary_lsn: i64,
    pub final_item: bool,
    pub snapshot_chunk: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaEndpoint {
    pub build_id: OperationId,
    pub identity: ReplicaIdentity,
    pub replication_address: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundOperation {
    Replication(ReplicationItem),
    Copy(CopyItem),
    Build(ReplicaEndpoint),
    Remove(ReplicaId),
}
