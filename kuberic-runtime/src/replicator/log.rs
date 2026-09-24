use std::collections::BTreeSet;

use bytes::Bytes;
use kuberic_protocol::types::{Epoch, OperationId, ReplicaIdentity, ReplicaRole};
use kuberic_runtime_internal::transport::{ReplicationAck, ReplicationItem};
use tokio::sync::oneshot;

use crate::application::{ClientWrite, Lsn, Operation};
use crate::authority::AdmittedAuthority;
use crate::replicator::queue::ReplicationQueue;
use crate::{Result, RuntimeError};

use super::quorum::QuorumTracker;

#[derive(Debug)]
pub(crate) struct PreparedWrite {
    pub lsn: Lsn,
    pub items: Vec<ReplicationItem>,
    pub completion: oneshot::Receiver<Result<Lsn>>,
}

#[derive(Debug)]
pub(crate) struct ReplicationLog {
    local_identity: ReplicaIdentity,
    authority: Option<AdmittedAuthority>,
    open: bool,
    role: ReplicaRole,
    epoch: Epoch,
    next_lsn: Lsn,
    pending_local_write: Option<PendingLocalWrite>,
    queue: ReplicationQueue,
    quorum: QuorumTracker,
}

#[derive(Debug, Clone)]
struct PendingLocalWrite {
    operation_id: OperationId,
    lsn: Lsn,
    data: Bytes,
}

impl ReplicationLog {
    pub fn new(local_identity: ReplicaIdentity) -> Self {
        Self {
            local_identity,
            authority: None,
            open: false,
            role: ReplicaRole::None,
            epoch: Epoch::default(),
            next_lsn: 0,
            pending_local_write: None,
            queue: ReplicationQueue::default(),
            quorum: QuorumTracker::default(),
        }
    }

    fn configure(&mut self, authority: AdmittedAuthority, local_progress: Lsn) -> Result<()> {
        if authority.local_identity != self.local_identity {
            return Err(RuntimeError::AuthorityMismatch(
                "admitted local identity differs from runtime identity".to_string(),
            ));
        }
        if self.authority.as_ref() != Some(&authority) {
            self.pending_local_write = None;
        }
        self.next_lsn = self.next_lsn.max(local_progress);
        self.quorum.configure(authority.clone(), local_progress)?;
        self.authority = Some(authority);
        Ok(())
    }

    fn reserve_write_inner(&mut self, write: &ClientWrite) -> Result<Lsn> {
        let authority = self
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if !self.open {
            return Err(RuntimeError::NotOpen);
        }
        if self.epoch != authority.current_configuration.epoch {
            return Err(RuntimeError::AuthorityMismatch(
                "write authority predates the replicator epoch".into(),
            ));
        }
        if self.role != ReplicaRole::Primary {
            return Err(RuntimeError::NotPrimary);
        }
        if authority.primary_identity() != &self.local_identity {
            return Err(RuntimeError::NotPrimary);
        }
        if let Some(pending) = &self.pending_local_write {
            if pending.operation_id == write.operation_id && pending.data == write.data {
                return Ok(pending.lsn);
            }
            return Err(RuntimeError::LocalWritePending(
                pending.operation_id.to_string(),
            ));
        }
        let lsn = self.next_lsn + 1;
        self.pending_local_write = Some(PendingLocalWrite {
            operation_id: write.operation_id.clone(),
            lsn,
            data: write.data.clone(),
        });
        Ok(lsn)
    }

    fn restore_write_reservation_inner(&mut self, write: &ClientWrite, lsn: Lsn) -> Result<()> {
        if let Some(pending) = &self.pending_local_write {
            if pending.operation_id == write.operation_id
                && pending.lsn == lsn
                && pending.data == write.data
            {
                return Ok(());
            }
            return Err(RuntimeError::LocalWritePending(
                pending.operation_id.to_string(),
            ));
        }
        self.pending_local_write = Some(PendingLocalWrite {
            operation_id: write.operation_id.clone(),
            lsn,
            data: write.data.clone(),
        });
        self.next_lsn = self.next_lsn.max(lsn - 1);
        Ok(())
    }

    fn ensure_local_write_registered_inner(
        &mut self,
        operation: &Operation,
    ) -> Result<PreparedWrite> {
        if !self.open || self.role != ReplicaRole::Primary {
            return Err(RuntimeError::NotPrimary);
        }
        if self
            .authority
            .as_ref()
            .map(|authority| authority.current_configuration.epoch)
            != Some(self.epoch)
        {
            return Err(RuntimeError::AuthorityMismatch(
                "write registration belongs to a fenced epoch".into(),
            ));
        }
        if let Some(pending) = self.pending_local_write.as_ref()
            && (pending.lsn != operation.lsn || pending.data != operation.data)
        {
            return Err(RuntimeError::Application(
                "local write differs from its reserved operation".to_string(),
            ));
        }
        self.queue.push(operation.clone());
        self.quorum.record_local_progress(operation.lsn)?;
        let completion = if operation.lsn <= self.quorum.committed_lsn() {
            let (sender, receiver) = oneshot::channel();
            let _ = sender.send(Ok(operation.lsn));
            receiver
        } else {
            self.quorum.register_write(operation.lsn)?
        };
        let items = self.replication_items(operation)?;
        self.queue.truncate_committed(self.quorum.committed_lsn());
        self.next_lsn = self.next_lsn.max(operation.lsn);
        self.pending_local_write = None;
        Ok(PreparedWrite {
            lsn: operation.lsn,
            items,
            completion,
        })
    }

    fn replication_items(&self, operation: &Operation) -> Result<Vec<ReplicationItem>> {
        let authority = self
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let targets = authority
            .current_configuration
            .members
            .iter()
            .chain(
                authority
                    .previous_configuration
                    .iter()
                    .flat_map(|configuration| configuration.members.iter()),
            )
            .map(|member| member.identity.clone())
            .filter(|identity| identity != &self.local_identity)
            .collect::<BTreeSet<_>>();
        Ok(targets
            .into_iter()
            .map(|receiver| ReplicationItem {
                sender: self.local_identity.clone(),
                receiver,
                epoch: authority.current_configuration.epoch,
                previous_configuration_id: authority
                    .previous_configuration
                    .as_ref()
                    .map(|configuration| configuration.configuration_id.clone()),
                current_configuration_id: authority.current_configuration.configuration_id.clone(),
                lsn: operation.lsn,
                committed_lsn: self.quorum.committed_lsn(),
                data: operation.data.clone(),
            })
            .collect())
    }

    fn acknowledge_inner(&mut self, acknowledgement: &ReplicationAck) -> Result<()> {
        if acknowledgement.epoch != self.epoch {
            return Err(RuntimeError::AuthorityMismatch(
                "acknowledgement predates the replicator epoch".into(),
            ));
        }
        self.quorum.acknowledge(acknowledgement)
    }

    fn record_local_progress_inner(&mut self, lsn: Lsn) -> Result<()> {
        self.next_lsn = self.next_lsn.max(lsn);
        self.quorum.record_local_progress(lsn)
    }

    fn committed_lsn_inner(&self) -> Lsn {
        self.quorum.committed_lsn()
    }

    fn ready_commit_lsn_inner(&self) -> Option<Lsn> {
        self.quorum.ready_commit_lsn()
    }

    fn finalize_commit_inner(&mut self, committed_lsn: Lsn) -> Result<()> {
        self.quorum.finalize_commit(committed_lsn)?;
        self.queue.truncate_committed(committed_lsn);
        Ok(())
    }

    fn restore_committed_write_inner(&mut self, operation: &Operation) -> Result<()> {
        if let Some(pending) = self.pending_local_write.as_ref()
            && (pending.lsn != operation.lsn || pending.data != operation.data)
        {
            return Err(RuntimeError::Application(
                "committed write conflicts with the active reservation".to_string(),
            ));
        }
        self.pending_local_write = None;
        self.next_lsn = self.next_lsn.max(operation.lsn);
        self.quorum.record_local_progress(operation.lsn)?;
        self.quorum.restore_committed_lsn(operation.lsn);
        self.queue.truncate_committed(operation.lsn);
        Ok(())
    }

    fn current_configuration_quorum_progress_inner(&self) -> Lsn {
        self.quorum.current_configuration_quorum_progress()
    }

    fn catch_up_boundary_inner(&self) -> Option<Lsn> {
        self.quorum.catch_up_boundary()
    }

    fn retained_operations_from_inner(&self, from_lsn: Lsn) -> Vec<Operation> {
        self.queue.operations_from(from_lsn)
    }

    fn catch_up_complete_inner(&self) -> bool {
        self.quorum.catch_up_complete()
    }

    fn fence_client_writes_inner(&mut self) {
        self.quorum.fail_pending();
    }
}

impl ReplicationLog {
    pub(crate) fn open(&mut self) -> Result<()> {
        if self.open {
            return Err(RuntimeError::Application("replicator already open".into()));
        }
        self.open = true;
        Ok(())
    }

    pub(crate) fn change_role(&mut self, epoch: Epoch, role: ReplicaRole) -> Result<()> {
        if !self.open {
            return Err(RuntimeError::NotOpen);
        }
        self.update_epoch(epoch)?;
        self.role = role;
        if role != ReplicaRole::Primary {
            self.fence_client_writes_inner();
        }
        Ok(())
    }

    pub(crate) fn update_epoch(&mut self, epoch: Epoch) -> Result<()> {
        if epoch < self.epoch {
            return Err(RuntimeError::AuthorityMismatch(
                "replicator epoch cannot regress".to_string(),
            ));
        }
        if epoch != self.epoch {
            self.fence_client_writes_inner();
        }
        self.epoch = epoch;
        Ok(())
    }

    pub(crate) fn admit_authority(
        &mut self,
        authority: AdmittedAuthority,
        local_progress: Lsn,
    ) -> Result<()> {
        self.update_epoch(authority.current_configuration.epoch)?;
        self.configure(authority, local_progress)
    }

    pub(crate) fn current_progress(&self) -> Lsn {
        self.quorum.highest_lsn()
    }

    pub(crate) fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub(crate) fn catch_up_capability(&self, current_progress: Lsn) -> Lsn {
        self.queue
            .first_lsn()
            .unwrap_or_else(|| current_progress.max(self.current_progress()))
    }

    pub(crate) fn committed_lsn(&self) -> Lsn {
        self.committed_lsn_inner()
    }

    pub(crate) fn record_local_progress(&mut self, lsn: Lsn) -> Result<()> {
        self.record_local_progress_inner(lsn)
    }

    pub(crate) fn record_build_handoff_progress(
        &mut self,
        identity: ReplicaIdentity,
        lsn: Lsn,
    ) -> Result<()> {
        self.quorum.record_build_handoff_progress(identity, lsn)
    }

    pub(crate) fn close(&mut self) -> Result<()> {
        self.fence_client_writes_inner();
        self.queue.clear();
        self.open = false;
        self.role = ReplicaRole::None;
        Ok(())
    }

    pub(crate) fn abort(&mut self) {
        self.fence_client_writes_inner();
        self.queue.clear();
        self.open = false;
        self.role = ReplicaRole::None;
    }

    pub(crate) fn reset_progress_after_data_loss(
        &mut self,
        current_progress: Lsn,
        committed_lsn: Lsn,
    ) {
        self.pending_local_write = None;
        self.next_lsn = current_progress;
        self.queue.clear();
        self.quorum.reset_progress_after_data_loss(
            self.local_identity.clone(),
            current_progress,
            committed_lsn,
        );
    }
}

impl ReplicationLog {
    pub(crate) fn reserve_write(&mut self, write: &ClientWrite) -> Result<Lsn> {
        self.reserve_write_inner(write)
    }

    pub(crate) fn restore_write_reservation(
        &mut self,
        write: &ClientWrite,
        lsn: Lsn,
    ) -> Result<()> {
        self.restore_write_reservation_inner(write, lsn)
    }

    pub(crate) fn ensure_local_write_registered(
        &mut self,
        operation: &Operation,
    ) -> Result<PreparedWrite> {
        self.ensure_local_write_registered_inner(operation)
    }

    pub(crate) fn acknowledge(&mut self, acknowledgement: &ReplicationAck) -> Result<()> {
        self.acknowledge_inner(acknowledgement)
    }

    pub(crate) fn ready_commit_lsn(&self) -> Option<Lsn> {
        self.ready_commit_lsn_inner()
    }

    pub(crate) fn finalize_commit(&mut self, committed_lsn: Lsn) -> Result<()> {
        self.finalize_commit_inner(committed_lsn)
    }

    pub(crate) fn restore_committed_write(&mut self, operation: &Operation) -> Result<()> {
        self.restore_committed_write_inner(operation)
    }

    pub(crate) fn current_configuration_quorum_progress(&self) -> Lsn {
        self.current_configuration_quorum_progress_inner()
    }

    pub(crate) fn catch_up_boundary(&self) -> Option<Lsn> {
        self.catch_up_boundary_inner()
    }

    pub(crate) fn retained_operations_from(&self, from_lsn: Lsn) -> Vec<Operation> {
        self.retained_operations_from_inner(from_lsn)
    }

    pub(crate) fn catch_up_complete(&self) -> bool {
        self.catch_up_complete_inner()
    }

    pub(crate) fn all_caught_up(&self, lsn: Lsn) -> bool {
        self.quorum.all_caught_up(lsn)
    }

    pub(crate) fn fence_client_writes(&mut self) {
        self.fence_client_writes_inner();
    }
}
