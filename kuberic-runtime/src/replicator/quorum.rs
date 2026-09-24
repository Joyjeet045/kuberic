use std::collections::{BTreeMap, BTreeSet};

use kuberic_protocol::types::{ConfigurationDescriptor, ReplicaIdentity, ReplicaRole};
use kuberic_runtime_internal::transport::ReplicationAck;
use tokio::sync::oneshot;

use crate::application::Lsn;
use crate::authority::AdmittedAuthority;
use crate::{Result, RuntimeError};

#[derive(Debug, Default)]
pub struct QuorumTracker {
    authority: Option<AdmittedAuthority>,
    progress: BTreeMap<ReplicaIdentity, Lsn>,
    pending: BTreeMap<Lsn, Vec<oneshot::Sender<Result<Lsn>>>>,
    highest_lsn: Lsn,
    committed_lsn: Lsn,
    catch_up_boundary: Option<Lsn>,
    must_catch_up: BTreeSet<ReplicaIdentity>,
}

impl QuorumTracker {
    pub fn configure(&mut self, authority: AdmittedAuthority, local_progress: Lsn) -> Result<()> {
        authority.validate()?;
        let same_fence = self.authority.as_ref().is_some_and(|existing| {
            existing.current_configuration.configuration_id
                == authority.current_configuration.configuration_id
                && existing
                    .previous_configuration
                    .as_ref()
                    .map(|configuration| &configuration.configuration_id)
                    == authority
                        .previous_configuration
                        .as_ref()
                        .map(|configuration| &configuration.configuration_id)
        });
        if self.authority.is_some() && !same_fence {
            for (_, senders) in std::mem::take(&mut self.pending) {
                for sender in senders {
                    let _ = sender.send(Err(RuntimeError::AuthorityMismatch(
                        "authority changed before the write committed".to_string(),
                    )));
                }
            }
            self.progress.clear();
        }
        let members = authority_members(&authority);
        self.progress
            .retain(|identity, _| members.contains(identity));
        self.progress
            .entry(authority.local_identity.clone())
            .and_modify(|progress| *progress = (*progress).max(local_progress))
            .or_insert(local_progress);
        self.highest_lsn = self.highest_lsn.max(local_progress);
        if !same_fence {
            self.catch_up_boundary = authority
                .previous_configuration
                .as_ref()
                .map(|_| self.highest_lsn);
            self.must_catch_up = derive_must_catch_up(&authority);
        }
        self.authority = Some(authority);
        Ok(())
    }

    pub fn register_write(&mut self, lsn: Lsn) -> Result<oneshot::Receiver<Result<Lsn>>> {
        if self.authority.is_none() {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        self.highest_lsn = self.highest_lsn.max(lsn);
        let (sender, receiver) = oneshot::channel();
        self.pending.entry(lsn).or_default().push(sender);
        Ok(receiver)
    }

    pub fn record_local_progress(&mut self, lsn: Lsn) -> Result<()> {
        let authority = self
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        self.progress
            .entry(authority.local_identity.clone())
            .and_modify(|progress| *progress = (*progress).max(lsn))
            .or_insert(lsn);
        self.highest_lsn = self.highest_lsn.max(lsn);
        Ok(())
    }

    pub fn record_build_handoff_progress(
        &mut self,
        identity: ReplicaIdentity,
        lsn: Lsn,
    ) -> Result<()> {
        let authority = self
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if !authority.contains_member(&identity) {
            return Err(RuntimeError::AuthorityMismatch(
                "durable progress belongs to a replica outside authority".to_string(),
            ));
        }
        self.progress
            .entry(identity)
            .and_modify(|progress| *progress = (*progress).max(lsn))
            .or_insert(lsn);
        self.highest_lsn = self.highest_lsn.max(lsn);
        Ok(())
    }

    pub fn acknowledge(&mut self, acknowledgement: &ReplicationAck) -> Result<()> {
        let authority = self
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        authority.validate_acknowledgement(acknowledgement)?;
        self.progress
            .entry(acknowledgement.receiver.clone())
            .and_modify(|progress| {
                *progress = (*progress).max(acknowledgement.applied_lsn);
            })
            .or_insert(acknowledgement.applied_lsn);
        Ok(())
    }

    pub fn current_configuration_quorum_progress(&self) -> Lsn {
        self.authority.as_ref().map_or(0, |authority| {
            quorum_progress(&authority.current_configuration, &self.progress)
        })
    }

    pub fn committed_lsn(&self) -> Lsn {
        self.committed_lsn
    }

    pub fn highest_lsn(&self) -> Lsn {
        self.highest_lsn
    }

    pub fn catch_up_boundary(&self) -> Option<Lsn> {
        self.catch_up_boundary
    }

    pub fn fail_pending(&mut self) {
        for (_, senders) in std::mem::take(&mut self.pending) {
            for sender in senders {
                let _ = sender.send(Err(RuntimeError::WriteClosed(
                    kuberic_protocol::types::AccessStatus::ReconfigurationPending,
                )));
            }
        }
    }

    pub fn catch_up_complete(&self) -> bool {
        let Some(boundary) = self.catch_up_boundary else {
            return true;
        };
        let quorum_progress = self.current_configuration_quorum_progress();
        quorum_progress >= boundary
            && self.must_catch_up.iter().all(|identity| {
                self.progress.get(identity).copied().unwrap_or(0) >= quorum_progress
            })
    }

    pub(crate) fn all_caught_up(&self, lsn: Lsn) -> bool {
        self.authority.as_ref().is_some_and(|authority| {
            authority
                .current_configuration
                .members
                .iter()
                .all(|member| self.progress.get(&member.identity).copied().unwrap_or(0) >= lsn)
        })
    }

    pub fn ready_commit_lsn(&self) -> Option<Lsn> {
        let authority = self.authority.as_ref()?;
        self.pending
            .keys()
            .copied()
            .filter(|lsn| client_commit_ready(authority, &self.progress, *lsn))
            .max()
    }

    pub fn finalize_commit(&mut self, committed_lsn: Lsn) -> Result<()> {
        let ready = self.ready_commit_lsn().ok_or_else(|| {
            RuntimeError::Application("no quorum-ready write can be finalized".to_string())
        })?;
        if committed_lsn > ready {
            return Err(RuntimeError::Application(
                "application commit exceeds quorum-ready progress".to_string(),
            ));
        }
        let completed = self
            .pending
            .keys()
            .copied()
            .take_while(|lsn| *lsn <= committed_lsn)
            .collect::<Vec<_>>();
        for lsn in completed {
            if let Some(senders) = self.pending.remove(&lsn) {
                for sender in senders {
                    let _ = sender.send(Ok(lsn));
                }
            }
        }
        self.committed_lsn = self.committed_lsn.max(committed_lsn);
        Ok(())
    }

    pub fn restore_committed_lsn(&mut self, committed_lsn: Lsn) {
        let completed = self
            .pending
            .keys()
            .copied()
            .take_while(|lsn| *lsn <= committed_lsn)
            .collect::<Vec<_>>();
        for lsn in completed {
            if let Some(senders) = self.pending.remove(&lsn) {
                for sender in senders {
                    let _ = sender.send(Ok(lsn));
                }
            }
        }
        self.highest_lsn = self.highest_lsn.max(committed_lsn);
        self.committed_lsn = self.committed_lsn.max(committed_lsn);
    }

    pub fn reset_progress_after_data_loss(
        &mut self,
        local_identity: ReplicaIdentity,
        current_progress: Lsn,
        committed_lsn: Lsn,
    ) {
        self.fail_pending();
        self.progress.clear();
        self.progress.insert(local_identity, current_progress);
        self.highest_lsn = current_progress;
        self.committed_lsn = committed_lsn;
        self.catch_up_boundary = None;
        self.must_catch_up.clear();
    }
}

fn authority_members(authority: &AdmittedAuthority) -> BTreeSet<ReplicaIdentity> {
    authority
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
        .collect()
}

fn derive_must_catch_up(authority: &AdmittedAuthority) -> BTreeSet<ReplicaIdentity> {
    let Some(previous) = authority.previous_configuration.as_ref() else {
        return BTreeSet::new();
    };
    let current_primary = authority
        .current_configuration
        .members
        .iter()
        .find(|member| {
            member.identity.replica_id == authority.current_configuration.primary_id
                && member.role == ReplicaRole::Primary
        })
        .expect("validated configuration has one primary");
    let was_same_primary = previous.members.iter().any(|member| {
        member.identity == current_primary.identity && member.role == ReplicaRole::Primary
    });
    if was_same_primary {
        BTreeSet::new()
    } else {
        BTreeSet::from([current_primary.identity.clone()])
    }
}

fn quorum_progress(
    configuration: &ConfigurationDescriptor,
    progress: &BTreeMap<ReplicaIdentity, Lsn>,
) -> Lsn {
    let mut values = configuration
        .members
        .iter()
        .map(|member| progress.get(&member.identity).copied().unwrap_or(0))
        .collect::<Vec<_>>();
    values.sort_unstable_by(|left, right| right.cmp(left));
    values
        .get(configuration.write_quorum.saturating_sub(1) as usize)
        .copied()
        .unwrap_or(0)
}

fn client_commit_ready(
    authority: &AdmittedAuthority,
    progress: &BTreeMap<ReplicaIdentity, Lsn>,
    lsn: Lsn,
) -> bool {
    has_quorum(&authority.current_configuration, progress, lsn)
        && authority
            .previous_configuration
            .as_ref()
            .is_none_or(|previous| has_quorum(previous, progress, lsn))
}

fn has_quorum(
    configuration: &ConfigurationDescriptor,
    progress: &BTreeMap<ReplicaIdentity, Lsn>,
    lsn: Lsn,
) -> bool {
    configuration
        .members
        .iter()
        .filter(|member| progress.get(&member.identity).copied().unwrap_or(0) >= lsn)
        .count()
        >= configuration.write_quorum as usize
}
