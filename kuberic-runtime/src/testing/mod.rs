//! Opt-in isolated replica fixtures for application conformance tests.
//!
//! Fixture records are detached, serializable descriptions. The opaque fixture
//! hosts and stores cannot be extracted from a running production host, and
//! none of these records can be passed to production admission APIs. Private
//! attachment, registration, lifecycle, and authority capabilities stay private.

pub mod authority;
pub mod copy;
pub mod effects;
pub mod hosting;
pub mod report;
pub mod runtime_adapter;
pub mod service;
pub mod session;
pub mod sqlite_store;
pub mod state;
pub mod transport;

use serde::{Serialize, de::DeserializeOwned};

fn convert<T: Serialize, U: DeserializeOwned>(value: T) -> U {
    serde_json::from_value(serde_json::to_value(value).expect("serialize fixture record"))
        .expect("fixture record matches the durable host format")
}

pub use transport::{
    ControlOutput, DeliveryId, InProcessTransport, Message, PumpReport, TransportError,
    TransportEvent,
};

pub async fn describe_peer(
    runtime: &hosting::PodRuntime,
    replica: crate::replicator::ReplicaInformation,
) -> crate::Result<()> {
    crate::host::testing::describe_peer(&runtime.inner, replica).await
}

pub async fn execute_build(
    runtime: &hosting::PodRuntime,
    replica: crate::replicator::ReplicaInformation,
) -> crate::Result<()> {
    crate::host::testing::execute_build(&runtime.inner, replica).await
}
