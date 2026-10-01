use super::*;
use kuberic_protocol::types::{
    ReplicaRole, SecondaryRemovalPreparation, SecondaryRemovalStage, SecondaryRemovalWitness,
    SecondaryScaleDownCleanup, SecondaryScaleDownIntent,
};
use kuberic_protocol::validation::{
    validate_secondary_removal_preparation, validate_secondary_scale_down_cleanup,
};

impl CustomReplicatorHost {
    pub(super) async fn prepare_removal(
        &self,
        intent: SecondaryScaleDownIntent,
        process_session_id: ProcessSessionId,
        report_sequence: u64,
    ) -> Result<()> {
        let host = self.host()?;
        let mut preparation = SecondaryRemovalPreparation {
            operation_id: intent
                .command_operation_id(SecondaryRemovalStage::Prepare, &intent.primary),
            intent,
            process_session_id,
            report_sequence,
            boundary_lsn: 0,
        };
        validate_secondary_removal_preparation(&preparation)?;
        let authority = self
            .state
            .read()
            .await
            .authority
            .clone()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if preparation.intent.primary != host.identity
            || host.replica_session.get().map(|(_, session)| session)
                != Some(&preparation.process_session_id)
            || host.state.read().await.fallback_snapshot.role != ReplicaRole::Primary
            || authority.current_configuration != preparation.intent.previous_configuration
            || authority.previous_configuration.is_some()
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        if let Some(previous) = host
            .default_dependencies
            .replica_authority_store
            .load_secondary_removal()
            .await?
        {
            if previous.intent == preparation.intent {
                if previous.process_session_id != preparation.process_session_id
                    || previous.report_sequence != preparation.report_sequence
                {
                    return Err(RuntimeError::AuthorityNotAdmitted);
                }
                let mut state = self.state.write().await;
                state.committed_lsn = previous.boundary_lsn;
                state.prepared_secondary_removal = Some(previous);
                return Ok(());
            }
        }
        self.set_access(
            AccessStatus::ReconfigurationPending,
            AccessStatus::ReconfigurationPending,
        )
        .await?;
        preparation.boundary_lsn = self.control.current_progress().await?;
        self.primary
            .wait_for_catch_up_quorum(ReplicaSetQuorumMode::All)
            .await?;
        host.default_dependencies
            .replica_authority_store
            .record_secondary_removal(&preparation)
            .await?;
        self.refresh().await?;
        let mut state = self.state.write().await;
        state.committed_lsn = preparation.boundary_lsn;
        state.prepared_secondary_removal = Some(preparation);
        Ok(())
    }

    pub(super) async fn observe_removal(&self, witness: SecondaryRemovalWitness) -> Result<()> {
        let host = self.host()?;
        let authority = self
            .state
            .read()
            .await
            .authority
            .clone()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let evidence = authority
            .secondary_removal
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let stage = if authority.previous_configuration.is_some() {
            SecondaryRemovalStage::PreviousCurrent
        } else {
            SecondaryRemovalStage::CurrentOnly
        };
        if witness.resource_uid != evidence.preparation.intent.resource_uid
            || self.sessions.read().await.get(&witness.identity)
                != Some(&witness.process_session_id)
            || witness.identity == host.identity
            || witness.epoch != authority.current_configuration.epoch
            || witness.current_configuration_id != authority.current_configuration.configuration_id
            || witness.previous_configuration_id
                != authority
                    .previous_configuration
                    .as_ref()
                    .map(|c| c.configuration_id.clone())
            || !authority
                .current_configuration
                .members
                .iter()
                .any(|m| m.identity == witness.identity && m.role == witness.role)
            || witness.verified_replication_lsn < evidence.preparation.boundary_lsn
            || witness.write_status == AccessStatus::Granted
            || witness.pending_operation_id.is_some()
            || witness.report_sequence == 0
            || witness.retained_operation_id.as_ref()
                != Some(
                    &evidence
                        .preparation
                        .intent
                        .command_operation_id(stage, &witness.identity),
                )
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        let mut witnesses = self.removal_witnesses.write().await;
        if witnesses
            .get(&witness.identity)
            .is_some_and(|old| old.report_sequence >= witness.report_sequence && old != &witness)
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        witnesses.insert(witness.identity.clone(), witness);
        Ok(())
    }

    pub(super) async fn accept_removal(
        &self,
        committed: SecondaryScaleDownCleanup,
        historical: bool,
    ) -> Result<()> {
        validate_secondary_scale_down_cleanup(&committed)?;
        let host = self.host()?;
        let authority = self
            .state
            .read()
            .await
            .authority
            .clone()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let intent = &committed.evidence.preparation.intent;
        let lifecycle = host.state.read().await.fallback_snapshot.clone();
        if authority.previous_configuration.is_some()
            || authority.current_configuration != intent.current_configuration
            || authority.secondary_removal.as_ref() != Some(&committed.evidence)
            || host
                .default_dependencies
                .replica_authority_store
                .load()
                .await?
                .as_ref()
                != Some(&authority)
            || self.control.current_progress().await? < committed.evidence.preparation.boundary_lsn
            || lifecycle.role != authority.local_role()
            || lifecycle.role_transition.is_some()
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        if historical {
            if host.state.read().await.fallback_snapshot.role != ReplicaRole::ActiveSecondary
                || self.state.read().await.write_status == AccessStatus::Granted
            {
                return Err(RuntimeError::AuthorityNotAdmitted);
            }
        } else {
            if host
                .default_dependencies
                .replica_authority_store
                .load_secondary_removal_commit()
                .await?
                .as_ref()
                == Some(&committed)
            {
                self.state.write().await.accepted_secondary_removal = Some(committed);
                return Ok(());
            }
            for witness in &committed.current_only_write_quorum {
                if witness.identity == host.identity {
                    if host.replica_session.get().map(|(_, session)| session)
                        != Some(&witness.process_session_id)
                    {
                        return Err(RuntimeError::AuthorityNotAdmitted);
                    }
                } else if self.removal_witnesses.read().await.get(&witness.identity)
                    != Some(witness)
                    || self.sessions.read().await.get(&witness.identity)
                        != Some(&witness.process_session_id)
                {
                    return Err(RuntimeError::AuthorityNotAdmitted);
                }
            }
            if authority.primary_identity() == &host.identity {
                self.primary
                    .wait_for_catch_up_quorum(ReplicaSetQuorumMode::All)
                    .await?;
            }
            host.default_dependencies
                .replica_authority_store
                .record_secondary_removal_commit(&committed)
                .await?;
        }
        self.state.write().await.accepted_secondary_removal = Some(committed);
        Ok(())
    }

    pub(super) async fn observe_removal_progress(
        &self,
        witness: SecondaryRemovalWitness,
        committed: SecondaryScaleDownCleanup,
    ) -> Result<()> {
        validate_secondary_scale_down_cleanup(&committed)?;
        let host = self.host()?;
        let authority = self
            .state
            .read()
            .await
            .authority
            .clone()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let intent = &committed.evidence.preparation.intent;
        if authority.previous_configuration.is_some()
            || authority.current_configuration != intent.current_configuration
            || authority.secondary_removal.as_ref() != Some(&committed.evidence)
            || witness.resource_uid != intent.resource_uid
            || witness.epoch != intent.current_configuration.epoch
            || witness.current_configuration_id != intent.current_configuration.configuration_id
            || witness.previous_configuration_id.is_some()
            || witness.report_sequence == 0
            || witness.pending_operation_id.is_some()
            || witness.verified_replication_lsn < committed.evidence.preparation.boundary_lsn
            || self.sessions.read().await.get(&witness.identity)
                != Some(&witness.process_session_id)
            || witness.identity == host.identity
            || !intent
                .current_configuration
                .members
                .iter()
                .any(|m| m.identity == witness.identity && m.role == witness.role)
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        // Fresh reports can confirm session presence, never replace the custom
        // replicator's own catch-up proof or mutate committed quorum progress.
        Ok(())
    }
}
