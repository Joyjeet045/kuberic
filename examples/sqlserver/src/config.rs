use std::num::NonZeroU32;

use crate::error::ContractError;
use crate::types::{PinnedImage, SecretRef};

pub const SUPPORTED_ENGINE_MAJOR: u16 = 16;
pub const SUPPORTED_REPLICA_COUNT: u8 = 3;
pub const SUPPORTED_DATABASE_COUNT: u8 = 1;
pub const SUPPORTED_REQUIRED_SECONDARIES: u8 = 1;

/// Rendered form of [`SUPPORTED_REPLICA_COUNT`] for error messages.
///
/// [`ContractError::UnsupportedProfile::expected`] is a `&'static str`, so the
/// rendered value is declared next to the numeric constant to keep the two from
/// drifting apart. `supported_profile_constants_agree` asserts that they match.
pub const SUPPORTED_REPLICA_COUNT_TEXT: &str = "3";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Edition {
    Developer,
    Enterprise,
    Standard,
    Express,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterType {
    External,
    None,
    Wsfc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverMode {
    External,
    Manual,
    Automatic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvailabilityMode {
    SynchronousCommit,
    AsynchronousCommit,
    ConfigurationOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedingMode {
    Automatic,
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationMode {
    ObserveOnly,
    Enabled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlServerSupportConfig {
    pub engine_major: u16,
    pub edition: Edition,
    pub image: PinnedImage,
    pub eula_accepted: bool,
    pub cluster_type: ClusterType,
    pub failover_mode: FailoverMode,
    pub availability_mode: AvailabilityMode,
    pub seeding_mode: SeedingMode,
    pub replica_count: u8,
    pub database_count: u8,
    pub required_synchronized_secondaries_to_commit: u8,
    pub external_write_lease_seconds: Option<NonZeroU32>,
    pub mutation_mode: MutationMode,
    pub observer_credentials: SecretRef,
    pub mutation_credentials: Option<SecretRef>,
    pub endpoint_certificate: SecretRef,
}

impl SqlServerSupportConfig {
    pub fn validate(&self) -> Result<(), ContractError> {
        require(
            "engine major version",
            self.engine_major == SUPPORTED_ENGINE_MAJOR,
            "16 (SQL Server 2022)",
            self.engine_major,
        )?;
        require(
            "edition",
            matches!(self.edition, Edition::Developer | Edition::Enterprise),
            "Developer or Enterprise",
            format!("{:?}", self.edition),
        )?;
        require(
            "EULA acceptance",
            self.eula_accepted,
            "true",
            self.eula_accepted,
        )?;
        require(
            "cluster type",
            self.cluster_type == ClusterType::External,
            "EXTERNAL",
            format!("{:?}", self.cluster_type),
        )?;
        require(
            "failover mode",
            self.failover_mode == FailoverMode::External,
            "EXTERNAL",
            format!("{:?}", self.failover_mode),
        )?;
        require(
            "availability mode",
            self.availability_mode == AvailabilityMode::SynchronousCommit,
            "SYNCHRONOUS_COMMIT",
            format!("{:?}", self.availability_mode),
        )?;
        require(
            "seeding mode",
            self.seeding_mode == SeedingMode::Automatic,
            "AUTOMATIC",
            format!("{:?}", self.seeding_mode),
        )?;
        require(
            "replica count",
            self.replica_count == SUPPORTED_REPLICA_COUNT,
            SUPPORTED_REPLICA_COUNT_TEXT,
            self.replica_count,
        )?;
        require(
            "database count",
            self.database_count == SUPPORTED_DATABASE_COUNT,
            "1",
            self.database_count,
        )?;
        require(
            "required synchronized secondaries",
            self.required_synchronized_secondaries_to_commit == SUPPORTED_REQUIRED_SECONDARIES,
            "1",
            self.required_synchronized_secondaries_to_commit,
        )?;
        require(
            "external write lease",
            self.external_write_lease_seconds.is_some(),
            "a positive duration",
            self.external_write_lease_seconds
                .map_or_else(|| "unset".to_string(), |seconds| seconds.to_string()),
        )?;

        match (self.mutation_mode, &self.mutation_credentials) {
            (MutationMode::ObserveOnly, Some(_)) => {
                return Err(ContractError::UnsupportedProfile {
                    field: "mutation credentials",
                    expected: "unset while mutation mode is ObserveOnly",
                    actual: "configured".to_string(),
                });
            }
            (MutationMode::Enabled, None) => {
                return Err(ContractError::MissingField {
                    field: "mutation credentials",
                });
            }
            (MutationMode::Enabled, Some(credentials))
                if credentials == &self.observer_credentials =>
            {
                return Err(ContractError::UnsupportedProfile {
                    field: "mutation credentials",
                    expected: "a Secret key distinct from observation credentials",
                    actual: "same Secret key".to_string(),
                });
            }
            _ => {}
        }

        Ok(())
    }
}

fn require(
    field: &'static str,
    condition: bool,
    expected: &'static str,
    actual: impl ToString,
) -> Result<(), ContractError> {
    if condition {
        Ok(())
    } else {
        Err(ContractError::UnsupportedProfile {
            field,
            expected,
            actual: actual.to_string(),
        })
    }
}
