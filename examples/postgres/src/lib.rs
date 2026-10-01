pub mod access;
pub mod adapter;
pub mod build;
pub mod config;
pub mod data_service;
pub mod durable;
mod generation;
pub mod instance;
pub mod monitor;
pub mod native;
mod owned_command;
mod owned_process;
#[doc(hidden)]
pub mod process_supervisor;
pub mod service;

pub use service::{PgService, PgServiceConfig};

pub mod proto {
    tonic::include_proto!("pgdata.v2");
}

#[cfg(feature = "testing")]
pub mod testing;
