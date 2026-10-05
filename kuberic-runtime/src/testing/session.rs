use crate::protocol::types::ProcessSessionId;

#[derive(Default)]
pub struct ProcessSession(crate::host::session::ProcessSession);

impl ProcessSession {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn id(&self) -> &ProcessSessionId {
        self.0.id()
    }

    pub fn next_report_sequence(&self) -> u64 {
        self.0.next_report_sequence()
    }
}
