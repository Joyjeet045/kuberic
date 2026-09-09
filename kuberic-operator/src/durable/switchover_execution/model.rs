use crate::crd::{
    DURABLE_OPERATION_VERSION, DurableOperationKind, DurableOperationPhase, DurableOperationStatus,
    StablePartitionSnapshotStatus, StableReplicaRoleStatus, StableReplicaSnapshotStatus,
};
use crate::durable::ACTION_DEADLINE_SECONDS;

pub fn admit_direct_switchover(
    operation_authority: &str,
    previous_snapshot: StablePartitionSnapshotStatus,
    target_primary_id: i64,
    accepted_unix_seconds: i64,
) -> Result<DurableOperationStatus, String> {
    if operation_authority.is_empty() {
        return Err("direct switchover operation authority is empty".to_string());
    }
    validate_snapshot(&previous_snapshot)?;
    if previous_snapshot.primary_id == target_primary_id {
        return Err("switchover target is already primary".to_string());
    }
    if !previous_snapshot
        .members
        .iter()
        .any(|member| member.id == target_primary_id)
    {
        return Err(format!(
            "switchover target replica {target_primary_id} is not in the stable snapshot"
        ));
    }

    let mut target_snapshot = previous_snapshot.clone();
    target_snapshot.epoch.configuration_number = target_snapshot
        .epoch
        .configuration_number
        .checked_add(1)
        .ok_or_else(|| "switchover epoch overflow".to_string())?;
    target_snapshot.primary_id = target_primary_id;
    for member in &mut target_snapshot.members {
        member.role = if member.id == target_primary_id {
            StableReplicaRoleStatus::Primary
        } else {
            StableReplicaRoleStatus::ActiveSecondary
        };
    }

    let operation_id = format!(
        "{operation_authority}:switchover:v{DURABLE_OPERATION_VERSION}:{}-{}:{target_primary_id}",
        previous_snapshot.epoch.data_loss_number, previous_snapshot.epoch.configuration_number,
    );
    let operation = DurableOperationStatus {
        execution_id: format!("{operation_id}:execution-1"),
        operation_id,
        version: DURABLE_OPERATION_VERSION,
        kind: DurableOperationKind::Switchover,
        phase: DurableOperationPhase::Revoke,
        old_primary_id: previous_snapshot.primary_id,
        target_primary_id,
        add_mode: None,
        remove_mode: None,
        target_replica_id: None,
        target_instance_id: None,
        target_pod_name: None,
        target_pod_uid: None,
        remove_target_replicator_address: None,
        remove_target_agent_generation: None,
        retired_instance_id: None,
        previous_snapshot: previous_snapshot.into(),
        target_snapshot,
        committed_snapshot: None,
        minimum_committed_replicas: None,
        frozen_lsn: None,
        next_secondary_index: 0,
        phase_deadline_unix_seconds: next_deadline(accepted_unix_seconds)?,
        pending_action: None,
        last_error: None,
        failover: None,
        add_intent: None,
        remove_intent: None,
        remove_commit_evidence: None,
        remove_cleanup: None,
        removal_disposition: None,
    };
    DirectSwitchoverDefinition::from_initial(&operation)?;
    Ok(operation)
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectSwitchoverDefinition {
    pub execution_id: String,
    pub operation_id: String,
    pub previous_snapshot: StablePartitionSnapshotStatus,
    pub target_snapshot: StablePartitionSnapshotStatus,
    pub old_primary_id: i64,
    pub target_primary_id: i64,
    pub initial_deadline_unix_seconds: i64,
}

impl DirectSwitchoverDefinition {
    pub fn from_initial(operation: &DurableOperationStatus) -> Result<Self, String> {
        if operation.version != DURABLE_OPERATION_VERSION {
            return Err(format!(
                "unsupported direct switchover operation version {}",
                operation.version
            ));
        }
        if operation.kind != DurableOperationKind::Switchover {
            return Err("direct switchover input is not a switchover operation".to_string());
        }
        if operation.phase != DurableOperationPhase::Revoke
            || operation.pending_action.is_some()
            || operation.frozen_lsn.is_some()
            || operation.next_secondary_index != 0
        {
            return Err("direct switchover input is not immutable admission state".to_string());
        }
        if operation.operation_id.is_empty()
            || operation.execution_id != format!("{}:execution-1", operation.operation_id)
        {
            return Err("direct switchover operation identity is invalid".to_string());
        }
        if operation.committed_snapshot.is_some()
            || operation.minimum_committed_replicas.is_some()
            || operation.add_mode.is_some()
            || operation.remove_mode.is_some()
            || operation.target_replica_id.is_some()
            || operation.target_instance_id.is_some()
            || operation.target_pod_name.is_some()
            || operation.target_pod_uid.is_some()
            || operation.remove_target_replicator_address.is_some()
            || operation.remove_target_agent_generation.is_some()
            || operation.retired_instance_id.is_some()
            || operation.last_error.is_some()
            || operation.failover.is_some()
            || operation.add_intent.is_some()
            || operation.remove_intent.is_some()
            || operation.remove_commit_evidence.is_some()
            || operation.remove_cleanup.is_some()
            || operation.removal_disposition.is_some()
        {
            return Err("direct switchover admission contains unrelated mutable state".to_string());
        }
        let previous_snapshot = operation
            .previous_snapshot
            .cloned()
            .ok_or_else(|| "direct switchover input has no previous snapshot".to_string())?;
        validate_snapshot(&previous_snapshot)?;
        validate_snapshot(&operation.target_snapshot)?;
        if previous_snapshot.primary_id != operation.old_primary_id {
            return Err(
                "direct switchover old primary does not match admission snapshot".to_string(),
            );
        }
        if operation.target_snapshot.primary_id != operation.target_primary_id {
            return Err("direct switchover target does not match target snapshot".to_string());
        }
        if operation.old_primary_id == operation.target_primary_id {
            return Err("direct switchover target is already primary".to_string());
        }
        if previous_snapshot.members.len() != operation.target_snapshot.members.len() {
            return Err("direct switchover membership changed at admission".to_string());
        }
        let expected_configuration_number = previous_snapshot
            .epoch
            .configuration_number
            .checked_add(1)
            .ok_or_else(|| "direct switchover admission epoch overflows".to_string())?;
        if operation.target_snapshot.epoch.data_loss_number
            != previous_snapshot.epoch.data_loss_number
            || operation.target_snapshot.epoch.configuration_number != expected_configuration_number
            || operation.target_snapshot.write_quorum != previous_snapshot.write_quorum
        {
            return Err("direct switchover target epoch or quorum is invalid".to_string());
        }
        for (previous, target) in previous_snapshot
            .members
            .iter()
            .zip(&operation.target_snapshot.members)
        {
            if target.id != previous.id || target.instance_id != previous.instance_id {
                return Err(format!(
                    "direct switchover replica {} incarnation changed at admission",
                    previous.id
                ));
            }
            let expected_role = if target.id == operation.target_primary_id {
                StableReplicaRoleStatus::Primary
            } else {
                StableReplicaRoleStatus::ActiveSecondary
            };
            if target.role != expected_role {
                return Err(format!(
                    "direct switchover replica {} target role is invalid",
                    target.id
                ));
            }
        }

        Ok(Self {
            execution_id: operation.execution_id.clone(),
            operation_id: operation.operation_id.clone(),
            previous_snapshot,
            target_snapshot: operation.target_snapshot.clone(),
            old_primary_id: operation.old_primary_id,
            target_primary_id: operation.target_primary_id,
            initial_deadline_unix_seconds: operation.phase_deadline_unix_seconds,
        })
    }

    pub fn member(&self, id: i64) -> Result<&StableReplicaSnapshotStatus, String> {
        member(&self.previous_snapshot, id)
    }

    pub fn normal_epoch_distribution_ids(&self) -> Vec<i64> {
        sorted_ids_excluding(
            &self.previous_snapshot,
            &[self.old_primary_id, self.target_primary_id],
        )
    }

    pub fn compensation_epoch_distribution_ids(&self) -> Vec<i64> {
        sorted_ids_excluding(&self.previous_snapshot, &[self.old_primary_id])
    }

    pub fn compensation_snapshot(&self) -> StablePartitionSnapshotStatus {
        let mut snapshot = self.previous_snapshot.clone();
        snapshot.epoch = self.target_snapshot.epoch.clone();
        snapshot.primary_id = self.old_primary_id;
        for member in &mut snapshot.members {
            member.role = if member.id == self.old_primary_id {
                StableReplicaRoleStatus::Primary
            } else {
                StableReplicaRoleStatus::ActiveSecondary
            };
        }
        snapshot
    }
}

pub fn next_deadline(observed_at_unix_seconds: i64) -> Result<i64, String> {
    observed_at_unix_seconds
        .checked_add(ACTION_DEADLINE_SECONDS)
        .ok_or_else(|| "direct switchover action deadline overflows unix time".to_string())
}

fn member(
    snapshot: &StablePartitionSnapshotStatus,
    id: i64,
) -> Result<&StableReplicaSnapshotStatus, String> {
    snapshot
        .members
        .iter()
        .find(|member| member.id == id)
        .ok_or_else(|| format!("direct switchover replica {id} is not in the snapshot"))
}

fn sorted_ids_excluding(snapshot: &StablePartitionSnapshotStatus, excluded: &[i64]) -> Vec<i64> {
    let mut ids = snapshot
        .members
        .iter()
        .filter_map(|member| (!excluded.contains(&member.id)).then_some(member.id))
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

fn validate_snapshot(snapshot: &StablePartitionSnapshotStatus) -> Result<(), String> {
    if !(2..=crate::crd::KUBERIC_MAX_REPLICAS as usize).contains(&snapshot.members.len()) {
        return Err(format!(
            "direct switchover snapshot member count {} is outside 2..={}",
            snapshot.members.len(),
            crate::crd::KUBERIC_MAX_REPLICAS
        ));
    }
    let expected_quorum = u32::try_from(snapshot.members.len() / 2 + 1)
        .map_err(|_| "direct switchover snapshot quorum overflows u32".to_string())?;
    if snapshot.write_quorum != expected_quorum {
        return Err("direct switchover snapshot write quorum is not a majority".to_string());
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut instances = std::collections::BTreeSet::new();
    let mut primary_count = 0;
    for member in &snapshot.members {
        if !ids.insert(member.id) {
            return Err(format!(
                "direct switchover snapshot repeats replica {}",
                member.id
            ));
        }
        if member.instance_id.is_empty() || !instances.insert(member.instance_id.as_str()) {
            return Err(
                "direct switchover snapshot has an empty or repeated incarnation".to_string(),
            );
        }
        if member.role == StableReplicaRoleStatus::Primary {
            primary_count += 1;
            if member.id != snapshot.primary_id {
                return Err("direct switchover snapshot role disagrees with primary id".to_string());
            }
        } else if member.id == snapshot.primary_id {
            return Err("direct switchover primary id is not marked primary".to_string());
        }
    }
    if primary_count != 1 {
        return Err("direct switchover snapshot must contain exactly one primary".to_string());
    }
    Ok(())
}

pub fn same_topology(
    actual: &StablePartitionSnapshotStatus,
    expected: &StablePartitionSnapshotStatus,
) -> bool {
    actual.epoch == expected.epoch
        && actual.primary_id == expected.primary_id
        && actual.write_quorum == expected.write_quorum
        && actual.members.len() == expected.members.len()
        && actual
            .members
            .iter()
            .zip(&expected.members)
            .all(|(actual, expected)| {
                actual.id == expected.id
                    && actual.instance_id == expected.instance_id
                    && actual.role == expected.role
            })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::EpochStatus;

    fn snapshot(
        primary_id: i64,
        count: i64,
        configuration_number: i64,
    ) -> StablePartitionSnapshotStatus {
        StablePartitionSnapshotStatus {
            epoch: EpochStatus {
                data_loss_number: 1,
                configuration_number,
            },
            primary_id,
            members: (1..=count)
                .map(|id| StableReplicaSnapshotStatus {
                    id,
                    instance_id: format!("instance-{id}"),
                    role: if id == primary_id {
                        StableReplicaRoleStatus::Primary
                    } else {
                        StableReplicaRoleStatus::ActiveSecondary
                    },
                    election_metadata: None,
                })
                .collect(),
            write_quorum: u32::try_from(count / 2 + 1).unwrap(),
        }
    }

    #[test]
    fn distribution_orders_are_deterministic() {
        let previous = snapshot(1, 4, 7);
        let target = snapshot(3, 4, 8);
        let definition = DirectSwitchoverDefinition {
            execution_id: "execution".to_string(),
            operation_id: "operation".to_string(),
            previous_snapshot: previous,
            target_snapshot: target,
            old_primary_id: 1,
            target_primary_id: 3,
            initial_deadline_unix_seconds: 10,
        };
        assert_eq!(definition.normal_epoch_distribution_ids(), vec![2, 4]);
        assert_eq!(
            definition.compensation_epoch_distribution_ids(),
            vec![2, 3, 4]
        );
        let compensated = definition.compensation_snapshot();
        assert_eq!(compensated.primary_id, 1);
        assert_eq!(compensated.epoch.configuration_number, 8);
    }

    #[test]
    fn direct_switchover_rejects_old_operation_versions() {
        let mut operation = admit_direct_switchover("set-uid", snapshot(1, 2, 7), 2, 100).unwrap();
        operation.version = DURABLE_OPERATION_VERSION + 1;
        assert!(DirectSwitchoverDefinition::from_initial(&operation).is_err());
    }

    #[test]
    fn direct_admission_constructs_the_exact_target_epoch_and_roles() {
        let operation = admit_direct_switchover("set-uid", snapshot(1, 4, 7), 3, 100).unwrap();
        assert_eq!(operation.old_primary_id, 1);
        assert_eq!(operation.target_primary_id, 3);
        assert_eq!(operation.target_snapshot.epoch.configuration_number, 8);
        assert_eq!(
            operation
                .target_snapshot
                .members
                .iter()
                .find(|member| member.id == 3)
                .unwrap()
                .role,
            StableReplicaRoleStatus::Primary
        );
        assert!(DirectSwitchoverDefinition::from_initial(&operation).is_ok());
    }

    #[test]
    fn direct_switchover_admission_preserves_exact_product_range_and_distinct_target() {
        for member_count in [2, crate::crd::KUBERIC_MAX_REPLICAS as i64] {
            assert!(
                admit_direct_switchover("set-uid", snapshot(1, member_count, 7), 2, 100).is_ok()
            );
        }
        assert!(admit_direct_switchover("set-uid", snapshot(1, 1, 7), 1, 100).is_err());
        assert!(
            admit_direct_switchover(
                "set-uid",
                snapshot(1, crate::crd::KUBERIC_MAX_REPLICAS as i64 + 1, 7),
                2,
                100,
            )
            .is_err()
        );
        let same_primary =
            admit_direct_switchover("set-uid", snapshot(1, 2, 7), 1, 100).unwrap_err();
        assert!(same_primary.contains("already primary"));
        let missing_target =
            admit_direct_switchover("set-uid", snapshot(1, 2, 7), 3, 100).unwrap_err();
        assert!(missing_target.contains("not in the stable snapshot"));
    }
}
