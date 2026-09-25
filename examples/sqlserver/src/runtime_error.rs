use std::error::Error;
use std::fmt;

use crate::{ObservationFailure, ObservationFailureKind};

/// Diagnostics contain only adapter-owned text, never driver messages or credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeError {
    pub kind: ObservationFailureKind,
    pub stage: &'static str,
    pub message: &'static str,
    pub server_code: Option<u32>,
}

impl RuntimeError {
    pub fn new(kind: ObservationFailureKind, stage: &'static str, message: &'static str) -> Self {
        Self {
            kind,
            stage,
            message,
            server_code: None,
        }
    }

    pub fn into_failure(self, observed_at_unix_millis: u64) -> ObservationFailure {
        ObservationFailure {
            kind: self.kind,
            message: self.to_string(),
            observed_at_unix_millis,
        }
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.stage, self.message)?;
        if let Some(code) = self.server_code {
            write!(f, " (SQL Server error {code})")?;
        }
        Ok(())
    }
}

impl Error for RuntimeError {}
