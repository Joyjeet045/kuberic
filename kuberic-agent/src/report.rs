//! Durable agent status reporting.

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
        let partition = runtime.partition_report().await;
        self.store
            .record_partition_reports(partition.load_metrics.clone(), partition.reported_fault)
            .await?;
        for _ in 0..3 {
            let state = self.store.load_state().await?;
            let snapshot = runtime.snapshot().await;
            let catch_up_capability = if snapshot.open {
                Some(runtime.catch_up_capability().await?)
            } else {
                None
            };
            let confirmed_snapshot = runtime.snapshot().await;
            let confirmed_state = self.store.load_state().await?;
            if state != confirmed_state
                || snapshot != confirmed_snapshot
                || !snapshot_matches_state(&snapshot, &state)
            {
                continue;
            }
            return Ok(build_report(
                &self.session,
                state,
                snapshot,
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
    proto::AgentStatusReport {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
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
        retained_operation_id: state.retained_command.as_ref().map_or_else(
            || {
                state
                    .retained_result
                    .as_ref()
                    .map_or_else(String::new, |result| result.operation_id.to_string())
            },
            |result| result.command.operation_id.to_string(),
        ),
        builds: snapshot
            .builds
            .into_iter()
            .map(|build| proto::BuildStatus {
                build_id: build.authority.build_id.to_string(),
                target: Some(build.authority.target.into()),
                last_sequence: build.last_sequence,
                durable_lsn: build.durable_lsn,
                completed: build.completed,
            })
            .collect(),
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
        }
        None => state.previous_configuration.is_none() && state.current_configuration.is_none(),
    };
    snapshot.role == state.role
        && snapshot.read_status == state.read_status
        && snapshot.write_status == state.write_status
        && authority_matches
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
