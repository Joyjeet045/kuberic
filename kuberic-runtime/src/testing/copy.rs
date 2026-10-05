use crate::application::OperationDataStream;
use crate::protocol::types::{ConfigurationDescriptor, OperationId, ReplicaIdentity};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildConfiguration {
    Current,
    Bootstrap(ConfigurationDescriptor),
}

pub struct PrepareCopyRequest {
    pub build_id: OperationId,
    pub target: ReplicaIdentity,
    pub configuration: BuildConfiguration,
    pub copy_context: OperationDataStream,
}

impl BuildConfiguration {
    pub(super) fn into_inner(self) -> crate::replicator::copy::BuildConfiguration {
        match self {
            Self::Current => crate::replicator::copy::BuildConfiguration::Current,
            Self::Bootstrap(configuration) => {
                crate::replicator::copy::BuildConfiguration::Bootstrap(configuration)
            }
        }
    }
}

impl PrepareCopyRequest {
    pub(super) fn into_inner(self) -> crate::replicator::copy::PrepareCopyRequest {
        crate::replicator::copy::PrepareCopyRequest {
            build_id: self.build_id,
            target: self.target,
            configuration: self.configuration.into_inner(),
            copy_context: self.copy_context,
        }
    }
}
