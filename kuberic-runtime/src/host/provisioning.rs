//! Authorized fresh-store provisioning.

#[cfg(test)]
use std::path::Path;

use crate::protocol::command::InitializeAgentStore;
use crate::protocol::types::{
    PodUid, ProvisioningIntent, PvcUid, ReplicaIdentity, ReplicaInstanceId, ResourceUid,
    TransitionIntent, TransitionKind, derive_agent_generation, derive_initialization_id,
};

use crate::host::Result;
use crate::host::state::{SCHEMA_VERSION, StorageIdentity};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservedStorageIdentity {
    pub(crate) resource_uid: ResourceUid,
    pub(crate) pod_uid: PodUid,
    pub(crate) pvc_uid: PvcUid,
    pub(crate) instance_id: ReplicaInstanceId,
}

pub(crate) enum InitializationAuthority<'a> {
    Bootstrap(&'a TransitionIntent),
    Replacement(&'a ProvisioningIntent),
    ScaleUp(&'a ProvisioningIntent),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
pub(crate) enum StorePresence {
    FreshUninitialized,
    Established,
    UnsafeMissingEstablished,
}

#[cfg(test)]
pub(crate) fn inspect_store(path: &Path, established_required: bool) -> StorePresence {
    if path.is_file() {
        StorePresence::Established
    } else if established_required {
        StorePresence::UnsafeMissingEstablished
    } else {
        StorePresence::FreshUninitialized
    }
}

pub(crate) fn authorize_initialization(
    command: &InitializeAgentStore,
    observed: &ObservedStorageIdentity,
    authority: InitializationAuthority<'_>,
) -> Result<StorageIdentity> {
    if command.resource_uid != observed.resource_uid
        || command.expected_pod_uid != observed.pod_uid
        || command.expected_pvc_uid != observed.pvc_uid
        || command.expected_instance_id != observed.instance_id
    {
        return Err(crate::host::HostError::InitializationNotAuthorized(
            "resource, Pod, PVC, or replica incarnation does not match observation".into(),
        ));
    }

    let local_identity = ReplicaIdentity {
        replica_id: command.local_replica_id,
        instance_id: command.expected_instance_id.clone(),
        agent_generation: command.assigned_agent_generation.clone(),
    };
    let derived_initialization = derive_initialization_id(
        &command.resource_uid,
        command.local_replica_id,
        &command.expected_pod_uid,
        &command.expected_pvc_uid,
    );
    if command.initialization_id != derived_initialization
        || command.assigned_agent_generation != derive_agent_generation(&command.initialization_id)
    {
        return Err(crate::host::HostError::InitializationNotAuthorized(
            "initialization ID or durable generation is not derived from exact storage identity"
                .into(),
        ));
    }

    match authority {
        InitializationAuthority::Bootstrap(transition) => {
            if transition.kind != TransitionKind::Bootstrap
                || transition.previous_configuration_id.is_some()
                || transition.effective_policy != command.effective_policy
                || transition.current_configuration != command.bootstrap_configuration
                || !transition
                    .current_configuration
                    .members
                    .iter()
                    .any(|member| member.identity == local_identity)
            {
                return Err(crate::host::HostError::InitializationNotAuthorized(
                    "command does not match the persisted Bootstrap transition".into(),
                ));
            }
        }
        InitializationAuthority::Replacement(provisioning) => {
            if provisioning.replacement().is_none()
                || provisioning.replica_id() != command.local_replica_id
                || provisioning.instance_id() != command.expected_instance_id
                || provisioning.pod_uid != command.expected_pod_uid
                || provisioning.pvc_uid != command.expected_pvc_uid
                || provisioning.initialization_id(&command.resource_uid)
                    != command.initialization_id
                || provisioning.assigned_agent_generation(&command.resource_uid)
                    != command.assigned_agent_generation
            {
                return Err(crate::host::HostError::InitializationNotAuthorized(
                    "command does not match persisted replacement provisioning".into(),
                ));
            }
        }
        InitializationAuthority::ScaleUp(provisioning) => {
            crate::protocol::validation::validate_scale_up_provisioning(provisioning).map_err(
                |error| crate::host::HostError::InitializationNotAuthorized(error.to_string()),
            )?;
            let scale_up = provisioning.scale_up().ok_or_else(|| {
                crate::host::HostError::InitializationNotAuthorized(
                    "missing persisted scale-up provisioning authority".into(),
                )
            })?;
            let primary = scale_up
                .previous_configuration
                .members
                .iter()
                .find(|member| {
                    member.identity.replica_id == scale_up.previous_configuration.primary_id
                })
                .expect("validated previous configuration has one primary");
            if command.provisioning.as_ref() != Some(provisioning)
                || scale_up.resource_uid != command.resource_uid
                || provisioning.replica_id() != command.local_replica_id
                || provisioning.instance_id() != command.expected_instance_id
                || provisioning.pod_uid != command.expected_pod_uid
                || provisioning.pvc_uid != command.expected_pvc_uid
                || provisioning.initialization_id(&command.resource_uid)
                    != command.initialization_id
                || provisioning.assigned_agent_generation(&command.resource_uid)
                    != command.assigned_agent_generation
                || command.bootstrap_configuration != scale_up.previous_configuration
                || command.effective_policy != scale_up.current_policy
                || primary.role != crate::protocol::types::ReplicaRole::Primary
            {
                return Err(crate::host::HostError::InitializationNotAuthorized(
                    "command does not match exact persisted scale-up provisioning authority".into(),
                ));
            }
        }
    }

    Ok(StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: command.resource_uid.clone(),
        pod_uid: command.expected_pod_uid.clone(),
        pvc_uid: command.expected_pvc_uid.clone(),
        initialization_id: command.initialization_id.clone(),
        local_identity,
        effective_policy: command.effective_policy.clone(),
    })
}

pub(crate) fn validate_established_identity(
    identity: &StorageIdentity,
    observed: &ObservedStorageIdentity,
    replica_id: crate::protocol::types::ReplicaId,
) -> Result<()> {
    if identity.resource_uid != observed.resource_uid
        || identity.pod_uid != observed.pod_uid
        || identity.pvc_uid != observed.pvc_uid
        || identity.local_identity.replica_id != replica_id
        || identity.local_identity.instance_id != observed.instance_id
    {
        return Err(crate::host::HostError::IdentityMismatch(
            "established resource, Pod, PVC, or replica incarnation differs from this process"
                .into(),
        ));
    }
    Ok(())
}
