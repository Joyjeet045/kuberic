//! Durable agent status reporting.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::authority::AdmittedAuthority;
use crate::control::proto;
use crate::protocol::types::{AccessStatus, FaultType, ReplicaRole};

use crate::host::Result;
use crate::host::hosting::ReportRuntime;
use crate::host::observation::{DurableAgentObservation, ReportObservation};
use crate::host::session::ProcessSession;
use crate::host::store::AgentStore;

pub(crate) struct AgentReporter<S> {
    store: Arc<S>,
    session: ProcessSession,
}

impl<S: AgentStore> AgentReporter<S> {
    pub(crate) fn new(store: Arc<S>) -> Self {
        Self {
            store,
            session: ProcessSession::new(),
        }
    }

    pub(crate) fn session(&self) -> &ProcessSession {
        &self.session
    }

    pub(crate) async fn report(&self, runtime: &ReportRuntime) -> Result<proto::AgentStatusReport> {
        for _ in 0..3 {
            let partition = runtime.partition_report().await;
            let state: DurableAgentObservation = self.store.load_state().await?.into();
            let authority = self.store.load_admitted_authority().await?;
            let snapshot = runtime.observation().await;
            let confirmed_snapshot = runtime.observation().await;
            let confirmed_state: DurableAgentObservation = self.store.load_state().await?.into();
            let confirmed_authority = self.store.load_admitted_authority().await?;
            let confirmed_partition = runtime.partition_report().await;
            let durable_stable = state == confirmed_state && authority == confirmed_authority;
            let fence_stable = same_report_fence(&snapshot, &confirmed_snapshot);
            let ownership_matches = snapshot_matches_state(
                &confirmed_snapshot,
                &confirmed_state,
                confirmed_authority.as_ref(),
            );
            if !durable_stable
                || !fence_stable
                || !ownership_matches
                || partition.revision != confirmed_partition.revision
            {
                continue;
            }
            return Ok(build_report(
                &self.session,
                confirmed_state,
                confirmed_snapshot,
                confirmed_partition.reported_fault,
            ));
        }
        Err(crate::host::HostError::DurableEffectConflict(
            "agent authority changed while constructing a status report".into(),
        ))
    }
}

fn build_report(
    session: &ProcessSession,
    state: DurableAgentObservation,
    snapshot: ReportObservation,
    reported_fault: Option<FaultType>,
) -> proto::AgentStatusReport {
    let state = state.into_state();
    let mut builds = snapshot
        .engine
        .builds
        .iter()
        .cloned()
        .map(|build| (build.authority.build_id.clone(), build))
        .collect::<BTreeMap<_, _>>();
    for (build_id, command) in &state.build_commands {
        let retained_scale_up_completion = state
            .scale_up_evidence
            .as_ref()
            .is_some_and(|evidence| &evidence.intent().build_id == build_id);
        if !snapshot.host.live_builds_only
            && ((command.authority.is_none()
                && !state.retired_builds.contains(build_id)
                && !state.abandoned_builds.contains(build_id))
                || retained_scale_up_completion)
            && let Some(progress) = state.build_progress.get(build_id)
        {
            builds
                .entry(build_id.clone())
                .or_insert_with(|| crate::effects::BuildPostcondition {
                    authority: progress.authority.clone(),
                    last_sequence: progress.last_sequence,
                    durable_lsn: progress.durable_lsn,
                    completed: progress.completed,
                    catch_up_boundary_lsn: progress.catch_up_boundary_lsn,
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
        protocol_version: crate::protocol::PROTOCOL_VERSION,
        replication_address: snapshot
            .host
            .replication_address
            .clone()
            .unwrap_or_default(),
        resource_uid: state.identity.resource_uid.to_string(),
        identity: Some(state.identity.local_identity.clone().into()),
        process_session_id: session.id().to_string(),
        report_sequence: session.next_report_sequence(),
        role: role_to_proto(snapshot.host.role) as i32,
        write_status: access_to_proto(snapshot.host.write_status) as i32,
        epoch: Some(state.highest_epoch.into()),
        previous_configuration: state.previous_configuration.map(Into::into),
        current_configuration: state.current_configuration.map(Into::into),
        current_progress: snapshot.engine.current_progress,
        verified_replication_lsn: snapshot.engine.verified_replication_lsn,
        committed_lsn: snapshot.engine.committed_lsn,
        catch_up_capability: snapshot.engine.catch_up_capability,
        storage_state: proto::AgentStorageState::Initialized as i32,
        pod_uid: state.identity.pod_uid.to_string(),
        pvc_uid: state.identity.pvc_uid.to_string(),
        storage_error: String::new(),
        healthy: reported_fault != Some(FaultType::Permanent),
        replica_id: state.identity.local_identity.replica_id.value(),
        read_status: access_to_proto(snapshot.host.read_status) as i32,
        current_configuration_quorum_progress: snapshot
            .engine
            .current_configuration_quorum_progress,
        catch_up_boundary: snapshot.engine.catch_up_boundary,
        catch_up_complete: snapshot.engine.catch_up_complete,
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
            .map(|record| crate::control::configuration_command_to_proto(record.command)),
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
    snapshot: &ReportObservation,
    state: &DurableAgentObservation,
    authority: Option<&AdmittedAuthority>,
) -> bool {
    let authority_matches = match snapshot.host.authority.as_ref() {
        Some(projected) => {
            authority == Some(projected)
                && projected.local_identity == state.identity.local_identity
                && projected.previous_configuration == state.previous_configuration
                && Some(&projected.current_configuration) == state.current_configuration.as_ref()
                && projected.scale_up == state.scale_up_evidence
                && projected.secondary_removal == state.secondary_removal_evidence
        }
        None => authority.is_none(),
    };
    let access_matches = |projected, desired| {
        projected == desired
            || (projected != AccessStatus::Granted && desired != AccessStatus::Granted)
            || (snapshot
                .host
                .pending_access
                .as_ref()
                .is_some_and(|pending| {
                    pending.desired == (state.read_status, state.write_status)
                        && pending.authority == snapshot.host.authority
                        && pending.configuration_generation
                            == snapshot.host.configuration_generation
                        && pending
                            .active_access_generation
                            .unwrap_or(pending.access_generation)
                            == snapshot.host.access_generation
                        && pending.peer_sessions == snapshot.host.peer_sessions
                        && pending.engine_fence == snapshot.engine.fence
                })
                && projected == AccessStatus::ReconfigurationPending
                && desired == AccessStatus::Granted)
    };
    let engine_matches_host = match (
        snapshot.engine.fence.as_ref(),
        snapshot.host.authority.as_ref(),
    ) {
        (Some(fence), Some(authority)) => {
            fence.configuration.as_ref().is_some_and(|configuration| {
                configuration.local_identity == authority.local_identity
                    && configuration.previous_configuration == authority.previous_configuration
                    && configuration.current_configuration == authority.current_configuration
                    && configuration.switchover_handoff == authority.switchover_handoff
                    && configuration.secondary_removal == authority.secondary_removal
                    && configuration.scale_up == authority.scale_up
            })
        }

        (Some(fence), None) => fence.configuration.is_none(),
        (None, _) => !snapshot.host.engine_required,
    };
    let generation_linked = if !snapshot.host.engine_required {
        snapshot.engine.host_generation.is_none()
    } else {
        snapshot.engine.host_generation == snapshot.host.engine_host_generation
            && snapshot.host.engine_host_generation.is_some()
    };
    snapshot.host.role == state.role
        && access_matches(snapshot.host.read_status, state.read_status)
        && access_matches(snapshot.host.write_status, state.write_status)
        && authority_matches
        && engine_matches_host
        && generation_linked
        && snapshot.engine.retired_authority == state.retired_authority
}

fn same_report_fence(before: &ReportObservation, after: &ReportObservation) -> bool {
    before.same_fence(after)
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
    use crate::authority::DurableBuildProgress;
    use crate::protocol::command::EnsureReplicaBuild;
    use crate::protocol::types::*;

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
        let mut state = crate::host::state::AgentState::new(crate::host::state::StorageIdentity {
            schema_version: crate::host::state::SCHEMA_VERSION,
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
        let mut snapshot = crate::host::hosting::empty_snapshot(identity);
        snapshot.live_builds_only = true;
        let fresh = ProcessSession::new();
        assert!(
            build_report(&fresh, state.clone().into(), snapshot.clone().into(), None,)
                .builds
                .is_empty()
        );
        snapshot.live_builds_only = false;
        assert_eq!(
            build_report(&fresh, state.into(), snapshot.into(), None)
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
        let mut before = crate::host::hosting::empty_snapshot(identity);
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
        let before: ReportObservation = before.into();
        let mut after: ReportObservation = after.into();
        assert!(same_report_fence(&before, &after));
        after.host.configuration_generation = 1;
        assert!(!same_report_fence(&before, &after));
        after.host.configuration_generation = 0;
        after.host.access_generation = 1;
        assert!(!same_report_fence(&before, &after));
        after.host.access_generation = 0;
        after.host.peer_sessions.push((
            before.host.identity.clone(),
            ProcessSessionId::new("peer-session"),
        ));
        assert!(!same_report_fence(&before, &after));
        after.host.peer_sessions.clear();
        after.engine.fence = Some(crate::replicator::ManagedOperationFence {
            configuration: None,
            engine_session_id: "engine-session".into(),
            engine_generation: 1,
        });
        assert!(!same_report_fence(&before, &after));
        after.engine.fence = None;
        assert!(same_report_fence(&before, &after));
        after.host.write_status = AccessStatus::Granted;
        assert!(!same_report_fence(&before, &after));
    }

    #[test]
    fn exact_pending_access_is_reportable_but_stale_mismatch_is_not() {
        let identity = ReplicaIdentity {
            replica_id: ReplicaId::new(1),
            instance_id: ReplicaInstanceId::new("source"),
            agent_generation: AgentGeneration::new("generation"),
        };
        let mut state = crate::host::state::AgentState::new(crate::host::state::StorageIdentity {
            schema_version: crate::host::state::SCHEMA_VERSION,
            resource_uid: ResourceUid::new("resource"),
            local_identity: identity.clone(),
            pod_uid: PodUid::new("source"),
            pvc_uid: PvcUid::new("data"),
            initialization_id: InitializationId::new("init"),
            effective_policy: EffectivePolicy::fixed(1, 30).unwrap(),
        });
        state.read_status = AccessStatus::Granted;
        state.write_status = AccessStatus::Granted;
        let mut observation: ReportObservation =
            crate::host::hosting::empty_snapshot(identity).into();
        observation.host.read_status = AccessStatus::ReconfigurationPending;
        observation.host.write_status = AccessStatus::ReconfigurationPending;
        observation.host.engine_required = true;
        observation.host.engine_host_generation = Some(0);
        let engine_fence = crate::replicator::ManagedOperationFence {
            configuration: None,
            engine_session_id: "engine-session".into(),
            engine_generation: 1,
        };
        observation.engine.fence = Some(engine_fence.clone());
        observation.engine.host_generation = Some(0);
        assert!(!snapshot_matches_state(
            &observation,
            &state.clone().into(),
            None
        ));
        observation.host.pending_access =
            Some(crate::host::observation::PendingAccessObservation {
                desired: (AccessStatus::Granted, AccessStatus::Granted),
                authority: None,
                configuration_generation: 0,
                access_generation: 0,
                active_access_generation: None,
                peer_sessions: Vec::new(),
                engine_fence: Some(engine_fence),
            });
        assert!(snapshot_matches_state(
            &observation,
            &state.clone().into(),
            None
        ));
        observation.host.access_generation = 1;
        assert!(!snapshot_matches_state(&observation, &state.into(), None));
    }

    #[test]
    fn report_authority_requires_exact_switchover_and_local_identity() {
        let identity = ReplicaIdentity {
            replica_id: ReplicaId::new(1),
            instance_id: ReplicaInstanceId::new("source"),
            agent_generation: AgentGeneration::new("generation"),
        };
        let target = ReplicaIdentity {
            replica_id: ReplicaId::new(2),
            instance_id: ReplicaInstanceId::new("target"),
            agent_generation: AgentGeneration::new("generation"),
        };
        let previous = ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            identity.replica_id,
            vec![ConfigurationMember {
                identity: identity.clone(),
                role: ReplicaRole::Primary,
            }],
            1,
        );
        let current = ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            target.replica_id,
            vec![
                ConfigurationMember {
                    identity: identity.clone(),
                    role: ReplicaRole::ActiveSecondary,
                },
                ConfigurationMember {
                    identity: target.clone(),
                    role: ReplicaRole::Primary,
                },
            ],
            2,
        );
        let handoff = SwitchoverHandoff {
            preparation_generation: 1,
            preparation_operation_id: OperationId::new("preparation"),
            request_id: SwitchoverRequestId::new("request"),
            source: identity.clone(),
            target,
            starting_configuration_id: previous.configuration_id.clone(),
            starting_epoch: previous.epoch,
            handoff_lsn: 7,
        };
        let mut state = crate::host::state::AgentState::new(crate::host::state::StorageIdentity {
            schema_version: crate::host::state::SCHEMA_VERSION,
            resource_uid: ResourceUid::new("resource"),
            local_identity: identity.clone(),
            pod_uid: PodUid::new("source"),
            pvc_uid: PvcUid::new("data"),
            initialization_id: InitializationId::new("init"),
            effective_policy: EffectivePolicy::fixed(2, 30).unwrap(),
        });
        state.previous_configuration = Some(previous);
        state.current_configuration = Some(current);
        state.prepared_switchover = Some(handoff.clone());
        let authority = AdmittedAuthority {
            local_identity: identity.clone(),
            transition_kind: Some(crate::protocol::types::TransitionKind::PlannedSwitchover),
            previous_configuration: state.previous_configuration.clone(),
            current_configuration: state.current_configuration.clone().unwrap(),
            switchover_handoff: Some(handoff.clone()),
            secondary_removal: None,
            scale_up: None,
        };
        let mut observation: ReportObservation =
            crate::host::hosting::empty_snapshot(identity).into();
        observation.host.authority = Some(authority.clone());
        assert!(snapshot_matches_state(
            &observation,
            &state.clone().into(),
            Some(&authority)
        ));

        observation
            .host
            .authority
            .as_mut()
            .unwrap()
            .switchover_handoff = None;
        assert!(!snapshot_matches_state(
            &observation,
            &state.clone().into(),
            Some(&authority)
        ));
        observation.host.authority = Some(authority);
        observation
            .host
            .authority
            .as_mut()
            .unwrap()
            .local_identity
            .replica_id = ReplicaId::new(9);
        let durable = observation.host.authority.as_ref().unwrap().clone();
        assert!(!snapshot_matches_state(
            &observation,
            &state.into(),
            Some(&durable)
        ));
    }
}
