//! Versioned transport contracts for the level-triggered Kuberic stack.
//!
//! Generated protobuf types remain transport-only. Validation and conversion
//! establish canonical `kuberic-protocol` authority before callers use them.

pub mod convert;

pub mod proto {
    tonic::include_proto!("kuberic.level.v1");
}

pub use convert::{
    CopyAcknowledgement, CopyEnvelope, ExecuteEnvelope, ReplicationAcknowledgement,
    ReplicationEnvelope, WireError, ensure_supported_version, normalize_agent_status_report,
    normalize_copy_ack, normalize_copy_item, normalize_execute_request, normalize_replication_ack,
    normalize_replication_item, validate_agent_status_report, validate_copy_ack,
    validate_copy_item, validate_execute_request, validate_replication_ack,
    validate_replication_item,
};
