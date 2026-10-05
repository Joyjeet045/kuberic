//! Durable agent status reporting.

use std::collections::BTreeMap;
use std::sync::Arc;

use kuberic_protocol::types::{AccessStatus, FaultType, ReplicaRole};
use kuberic_wire::proto;

use crate::Result;
use crate::hosting::PodRuntime;
use crate::session::ProcessSession;
use crate::store::AgentStore;

pub struct AgentReporter<S> {
    store: Arc<S>,
    session: ProcessSession,
}

impl<S: AgentStore> AgentReporter<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            session: ProcessSession::new(),
        }
    }

    pub fn session(&self) -> &ProcessSession {
        &self.session
    }

    pub async fn report(&self, runtime: &PodRuntime) -> Result<proto::AgentStatusReport> {
        for attempt in 0..100 {
            match runtime.observe_progress().await {
                Ok(()) | Err(kuberic_runtime::RuntimeError::ReconfigurationPending) => break,
                Err(kuberic_runtime::RuntimeError::OperationCancelled) if attempt < 99 => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        }
        let durable = self.store.load_state().await?;
        let snapshot = runtime.snapshot().await;
        if durable.pending_effect.is_none()
            && durable.reconfiguration.is_none()
            && (snapshot.read_status != durable.read_status
                || snapshot.write_status != durable.write_status)
        {
            for attempt in 0..100 {
                match runtime
                    .reconcile_durable_access(durable.read_status, durable.write_status)
                    .await
                {
                    Ok(())
                    | Err(
                        kuberic_runtime::RuntimeError::ReconfigurationPending
                        | kuberic_runtime::RuntimeError::NotOpen
                        | kuberic_runtime::RuntimeError::Closed,
                    ) => break,
                    Err(kuberic_runtime::RuntimeError::OperationCancelled) if attempt < 99 => {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        let partition = runtime.partition_report().await;
        self.store
            .record_partition_reports(partition.load_metrics.clone(), partition.reported_fault)
            .await?;
        for _ in 0..3 {
            let state = self.store.load_state().await?;
            let snapshot = runtime.snapshot().await;
            let catch_up_capability = if snapshot.open && snapshot.role != ReplicaRole::None {
                Some(runtime.catch_up_capability().await?)
            } else {
                None
            };
            let confirmed_snapshot = runtime.snapshot().await;
            let confirmed_state = self.store.load_state().await?;
            if state != confirmed_state
                || !same_report_fence(&snapshot, &confirmed_snapshot)
                || !snapshot_matches_state(&confirmed_snapshot, &confirmed_state)
            {
                continue;
            }
            return Ok(build_report(
                &self.session,
                confirmed_state,
                confirmed_snapshot,
                catch_up_capability,
                partition.reported_fault,
            ));
        }
        Err(crate::AgentError::EffectConflict(
            "agent authority changed while constructing a status report".into(),
        ))
    }
}

fn build_report(
    session: &ProcessSession,
    state: crate::state::AgentState,
    snapshot: kuberic_runtime_internal::effects::RuntimeSnapshot,
    catch_up_capability: Option<i64>,
    reported_fault: Option<FaultType>,
) -> proto::AgentStatusReport {
    let mut builds = snapshot
        .builds
        .into_iter()
        .map(|build| (build.authority.build_id.clone(), build))
        .collect::<BTreeMap<_, _>>();
    for (build_id, command) in &state.build_commands {
        let retained_scale_up_completion = state
            .scale_up_evidence
            .as_ref()
            .is_some_and(|evidence| &evidence.intent().build_id == build_id);
        if !snapshot.live_builds_only
            && ((command.authority.is_none()
                && !state.retired_builds.contains(build_id)
                && !state.abandoned_builds.contains(build_id))
                || retained_scale_up_completion)
            && let Some(progress) = state.build_progress.get(build_id)
        {
            builds.entry(build_id.clone()).or_insert_with(|| {
                kuberic_runtime_internal::effects::BuildPostcondition {
                    authority: progress.authority.clone(),
                    last_sequence: progress.last_sequence,
                    durable_lsn: progress.durable_lsn,
                    completed: progress.completed,
                    catch_up_boundary_lsn: progress.catch_up_boundary_lsn,
                }
            });
        }
    }
    let pending_operation_id = state
        .reconfiguration
        .as_ref()
        .map(|record| record.command.operation_id.to_string())
        .or_else(|| {
            state
                .pending_effect
                .as_ref()
                .map(|effect| effect.effect.operation_id.to_string())
        })
        .unwrap_or_default();
    let retained_removal = state
        .retired_authority
        .as_ref()
        .map(|r| &r.report.operation_id)
        .or_else(|| {
            state
                .prepared_secondary_removal
                .as_ref()
                .filter(|p| {
                    state.current_configuration.as_ref() == Some(&p.intent.previous_configuration)
                })
                .map(|p| &p.operation_id)
        });
    proto::AgentStatusReport {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        replication_address: snapshot.replication_address.clone().unwrap_or_default(),
        resource_uid: state.identity.resource_uid.to_string(),
        identity: Some(state.identity.local_identity.clone().into()),
        process_session_id: session.id().to_string(),
        report_sequence: session.next_report_sequence(),
        role: role_to_proto(snapshot.role) as i32,
        write_status: access_to_proto(snapshot.write_status) as i32,
        epoch: Some(state.highest_epoch.into()),
        previous_configuration: state.previous_configuration.map(Into::into),
        current_configuration: state.current_configuration.map(Into::into),
        current_progress: snapshot.current_progress,
        verified_replication_lsn: snapshot.verified_replication_lsn,
        committed_lsn: snapshot.committed_lsn,
        catch_up_capability,
        storage_state: proto::AgentStorageState::Initialized as i32,
        pod_uid: state.identity.pod_uid.to_string(),
        pvc_uid: state.identity.pvc_uid.to_string(),
        storage_error: String::new(),
        healthy: reported_fault != Some(FaultType::Permanent),
        replica_id: state.identity.local_identity.replica_id.value(),
        read_status: access_to_proto(snapshot.read_status) as i32,
        current_configuration_quorum_progress: snapshot.current_configuration_quorum_progress,
        catch_up_boundary: snapshot.catch_up_boundary,
        catch_up_complete: snapshot.catch_up_complete,
        deactivated_lsn: state
            .deactivation
            .as_ref()
            .map(|deactivation| deactivation.deactivated_lsn),
        deactivation_epoch: state
            .deactivation
            .as_ref()
            .map(|deactivation| deactivation.epoch.into()),
        load_metrics: state
            .load_metrics
            .into_iter()
            .map(|metric| proto::LoadMetric {
                name: metric.name,
                value: metric.value,
            })
            .collect(),
        reported_fault: fault_to_proto(state.reported_fault) as i32,
        pending_operation_id,
        pending_configuration: state
            .reconfiguration
            .map(|record| kuberic_wire::configuration_command_to_proto(record.command)),
        retained_operation_id: retained_removal
            .map(|id| id.to_string())
            .unwrap_or_else(|| {
                state.retained_command.as_ref().map_or_else(
                    || {
                        state
                            .retained_result
                            .as_ref()
                            .map_or_else(String::new, |result| result.operation_id.to_string())
                    },
                    |result| result.command.operation_id.to_string(),
                )
            }),
        builds: builds
            .into_values()
            .map(|build| proto::BuildStatus {
                build_id: build.authority.build_id.to_string(),
                target: Some(build.authority.target.into()),
                last_sequence: build.last_sequence,
                replication_boundary_lsn: build.authority.replication_boundary_lsn,
                durable_lsn: build.durable_lsn,
                completed: build.completed,
                catch_up_boundary_lsn: build.catch_up_boundary_lsn,
            })
            .collect(),
        prepared_switchover: state.prepared_switchover.map(Into::into),
        prepared_secondary_removal: state.prepared_secondary_removal.map(Into::into),
        secondary_removal_evidence: state.secondary_removal_evidence.map(Into::into),
        retired_replica: state.retired_authority.map(|r| r.report.into()),
        accepted_secondary_removal: state.accepted_secondary_removal.map(Into::into),
        scale_up_intent: state
            .scale_up_evidence
            .map(|evidence| evidence.intent().clone().into()),
    }
}

fn snapshot_matches_state(
    snapshot: &kuberic_runtime_internal::effects::RuntimeSnapshot,
    state: &crate::state::AgentState,
) -> bool {
    let authority_matches = match snapshot.authority.as_ref() {
        Some(authority) => {
            authority.previous_configuration == state.previous_configuration
                && Some(&authority.current_configuration) == state.current_configuration.as_ref()
                && authority.scale_up == state.scale_up_evidence
        }
        None => state.previous_configuration.is_none() && state.current_configuration.is_none(),
    };
    let access_matches = |projected, desired| {
        projected == desired
            || (snapshot.live_builds_only
                && projected == AccessStatus::ReconfigurationPending
                && desired == AccessStatus::Granted)
    };
    snapshot.role == state.role
        && access_matches(snapshot.read_status, state.read_status)
        && access_matches(snapshot.write_status, state.write_status)
        && authority_matches
        && snapshot.retired_authority == state.retired_authority
}

fn same_report_fence(
    before: &kuberic_runtime_internal::effects::RuntimeSnapshot,
    after: &kuberic_runtime_internal::effects::RuntimeSnapshot,
) -> bool {
    before.identity == after.identity
        && before.open == after.open
        && before.replication_address == after.replication_address
        && before.role == after.role
        && before.role_transition == after.role_transition
        && before.read_status == after.read_status
        && before.write_status == after.write_status
        && before.authority == after.authority
        && before.prepared_secondary_removal == after.prepared_secondary_removal
        && before.retired_authority == after.retired_authority
        && before.accepted_secondary_removal == after.accepted_secondary_removal
        && before.live_builds_only == after.live_builds_only
}

fn role_to_proto(role: ReplicaRole) -> proto::ReplicaRole {
    match role {
        ReplicaRole::Primary => proto::ReplicaRole::Primary,
        ReplicaRole::ActiveSecondary => proto::ReplicaRole::ActiveSecondary,
        ReplicaRole::IdleSecondary => proto::ReplicaRole::IdleSecondary,
        ReplicaRole::None => proto::ReplicaRole::None,
    }
}

fn access_to_proto(status: AccessStatus) -> proto::AccessStatus {
    match status {
        AccessStatus::Granted => proto::AccessStatus::Granted,
        AccessStatus::ReconfigurationPending => proto::AccessStatus::ReconfigurationPending,
        AccessStatus::NotPrimary => proto::AccessStatus::NotPrimary,
        AccessStatus::NoWriteQuorum => proto::AccessStatus::NoWriteQuorum,
    }
}

fn fault_to_proto(fault: Option<FaultType>) -> proto::FaultType {
    match fault {
        None => proto::FaultType::Unknown,
        Some(FaultType::Transient) => proto::FaultType::Transient,
        Some(FaultType::Permanent) => proto::FaultType::Permanent,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kuberic_protocol::command::EnsureReplicaBuild;
    use kuberic_protocol::types::*;
    use kuberic_runtime_internal::authority::DurableBuildProgress;

    #[test]
    fn custom_completion_cannot_be_resurrected_from_a_previous_process_journal() {
        let identity = ReplicaIdentity {
            replica_id: ReplicaId::new(1),
            instance_id: ReplicaInstanceId::new("source"),
            agent_generation: AgentGeneration::new("generation"),
        };
        let target = ReplicaIdentity {
            replica_id: ReplicaId::new(2),
            ..identity.clone()
        };
        let configuration = ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            identity.replica_id,
            vec![ConfigurationMember {
                identity: identity.clone(),
                role: ReplicaRole::Primary,
            }],
            1,
        );
        let id = OperationId::new("completed-old-session");
        let authority = BuildAuthority {
            build_id: id.clone(),
            kind: BuildAuthorityKind::Provisioning,
            source: identity.clone(),
            target: target.clone(),
            current_configuration: configuration,
            replication_boundary_lsn: 10,
        };
        let mut state = crate::state::AgentState::new(crate::state::StorageIdentity {
            schema_version: crate::state::SCHEMA_VERSION,
            resource_uid: ResourceUid::new("resource"),
            local_identity: identity.clone(),
            pod_uid: PodUid::new("source"),
            pvc_uid: PvcUid::new("data"),
            initialization_id: InitializationId::new("init"),
            effective_policy: EffectivePolicy::fixed(1, 30).unwrap(),
        });
        state.build_commands.insert(
            id.clone(),
            EnsureReplicaBuild {
                operation_id: id.clone(),
                local_replica_id: identity.replica_id,
                expected_instance_id: identity.instance_id.clone(),
                expected_agent_generation: identity.agent_generation.clone(),
                target,
                authority: None,
                source_session_id: None,
                retire: false,
            },
        );
        state.build_progress.insert(
            id,
            DurableBuildProgress {
                authority,
                last_sequence: 5,
                durable_lsn: 10,
                completed: true,
                catch_up_boundary_lsn: Some(10),
            },
        );
        let mut snapshot = crate::hosting::empty_snapshot(identity);
        snapshot.live_builds_only = true;
        let fresh = ProcessSession::new();
        assert!(
            build_report(&fresh, state.clone(), snapshot.clone(), None, None)
                .builds
                .is_empty()
        );
        snapshot.live_builds_only = false;
        assert_eq!(
            build_report(&fresh, state, snapshot, None, None)
                .builds
                .len(),
            1
        );
    }

    #[test]
    fn report_fence_allows_progress_but_not_authority_changes() {
        let identity = ReplicaIdentity {
            replica_id: ReplicaId::new(1),
            instance_id: ReplicaInstanceId::new("source"),
            agent_generation: AgentGeneration::new("generation"),
        };
        let mut before = crate::hosting::empty_snapshot(identity);
        before.current_progress = 10;
        before.verified_replication_lsn = Some(10);
        before.committed_lsn = 9;
        before.current_configuration_quorum_progress = 8;
        before.catch_up_boundary = Some(10);
        before.catch_up_complete = false;

        let mut after = before.clone();
        after.current_progress = 11;
        after.verified_replication_lsn = Some(11);
        after.committed_lsn = 10;
        after.current_configuration_quorum_progress = 10;
        after.catch_up_boundary = Some(11);
        after.catch_up_complete = true;
        assert!(same_report_fence(&before, &after));

        after.write_status = AccessStatus::Granted;
        assert!(!same_report_fence(&before, &after));
    }
}
