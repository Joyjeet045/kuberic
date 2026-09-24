//! Ephemeral process-session identity.

use std::sync::atomic::{AtomicU64, Ordering};

use kuberic_protocol::types::ProcessSessionId;

pub struct ProcessSession {
    id: ProcessSessionId,
    report_sequence: AtomicU64,
}

impl ProcessSession {
    pub fn new() -> Self {
        Self {
            id: ProcessSessionId::new(uuid::Uuid::new_v4().to_string()),
            report_sequence: AtomicU64::new(0),
        }
    }

    pub fn id(&self) -> &ProcessSessionId {
        &self.id
    }

    pub fn next_report_sequence(&self) -> u64 {
        self.report_sequence.fetch_add(1, Ordering::AcqRel) + 1
    }
}

impl Default for ProcessSession {
    fn default() -> Self {
        Self::new()
    }
}
