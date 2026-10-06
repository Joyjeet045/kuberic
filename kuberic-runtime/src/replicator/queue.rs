use std::collections::BTreeMap;

use bytes::Bytes;

use crate::application::{Lsn, Operation};

#[derive(Debug, Default)]
pub(super) struct ReplicationQueue {
    operations: BTreeMap<Lsn, Bytes>,
}

impl ReplicationQueue {
    pub(super) fn push(&mut self, operation: Operation) {
        self.operations.insert(operation.lsn, operation.data);
    }

    pub(super) fn operations_from(&self, from_lsn: Lsn) -> Vec<Operation> {
        self.operations
            .range(from_lsn..)
            .map(|(&lsn, data)| Operation {
                lsn,
                committed_lsn: 0,
                data: data.clone(),
            })
            .collect()
    }

    pub(super) fn first_lsn(&self) -> Option<Lsn> {
        self.operations.first_key_value().map(|(&lsn, _)| lsn)
    }

    pub(super) fn truncate_committed(&mut self, committed_lsn: Lsn) {
        self.operations = self.operations.split_off(&(committed_lsn + 1));
    }

    pub(super) fn clear(&mut self) {
        self.operations.clear();
    }
}
