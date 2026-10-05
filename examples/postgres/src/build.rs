use crate::native::PgReplicationEvidence;
use std::path::Path;

use kuberic_runtime::protocol::types::{BuildAuthority, ProcessSessionId, ResourceUid};
use serde::{Deserialize, Serialize};

use crate::instance::PgError;
use crate::monitor::parse_pg_lsn;

pub const BUILD_PROTOCOL_VERSION: u32 = 2;
pub const MAX_ENVELOPE_BYTES: usize = 65536;
pub const MAX_BUILDS: usize = 16;
const MAX_HISTORY: usize = 64;

#[cfg(feature = "testing")]
pub struct BuildGate {
    pub stage: PgBuildStage,
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimelineFork {
    pub timeline: u32,
    pub end_lsn: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgLineage {
    pub system_identifier: String,
    pub timeline: u32,
    pub history: Vec<TimelineFork>,
    pub history_text: String,
}

impl PgLineage {
    pub fn validate(&self) -> Result<(), String> {
        if self
            .system_identifier
            .parse::<u64>()
            .ok()
            .is_none_or(|id| id == 0)
            || self.system_identifier.len() > 20
            || self.timeline == 0
            || self.history.len() > MAX_HISTORY
            || (self.timeline == 1) != self.history.is_empty()
            || self.history_text.len() > 16384
        {
            return Err("invalid PostgreSQL lineage".into());
        }
        if Self::parse_history(&self.history_text).map_err(|e| e.to_string())? != self.history {
            return Err("timeline history text differs from parsed lineage".into());
        }
        let mut timeline = 0;
        let mut boundary = 0;
        for fork in &self.history {
            if fork.timeline <= timeline
                || fork.timeline >= self.timeline
                || fork.end_lsn <= 0
                || fork.end_lsn < boundary
            {
                return Err("unordered or duplicate PostgreSQL history".into());
            }
            timeline = fork.timeline;
            boundary = fork.end_lsn;
        }
        Ok(())
    }

    pub fn can_rewind_from(&self, source: &Self) -> bool {
        self.system_identifier == source.system_identifier
            && (self == source
                || source
                    .history
                    .iter()
                    .any(|fork| fork.timeline == self.timeline)
                    && source.history.starts_with(&self.history))
    }

    pub async fn read(
        data: &Path,
        system_identifier: String,
        timeline: u32,
    ) -> Result<Self, PgError> {
        let mut history_text = String::new();
        if timeline > 1 {
            let path = data.join("pg_wal").join(format!("{timeline:08X}.history"));
            let metadata = tokio::fs::metadata(&path)
                .await
                .map_err(|e| PgError::Configuration(format!("timeline history: {e}")))?;
            if metadata.len() > 16384 {
                return Err(PgError::Configuration("oversized timeline history".into()));
            }
            history_text = tokio::fs::read_to_string(path)
                .await
                .map_err(|e| PgError::Configuration(format!("timeline history: {e}")))?;
        }
        let history = Self::parse_history(&history_text)?;
        let lineage = Self {
            system_identifier,
            timeline,
            history,
            history_text,
        };
        lineage.validate().map_err(PgError::Configuration)?;
        Ok(lineage)
    }

    fn parse_history(text: &str) -> Result<Vec<TimelineFork>, PgError> {
        let mut history = Vec::new();
        for line in text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
        {
            let mut fields = line.split_whitespace();
            let timeline = fields
                .next()
                .and_then(|v| v.parse().ok())
                .ok_or_else(|| PgError::Configuration("invalid history timeline".into()))?;
            let end_lsn = parse_pg_lsn(
                fields
                    .next()
                    .ok_or_else(|| PgError::Configuration("missing history boundary".into()))?,
            )?;
            history.push(TimelineFork { timeline, end_lsn });
        }
        Ok(history)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgBuildRequest {
    pub version: u32,
    pub resource_uid: ResourceUid,
    pub authority: BuildAuthority,
    pub source_session: ProcessSessionId,
    pub target_session: ProcessSessionId,
    pub source_endpoint: String,
    pub source_host: String,
    pub source_port: u16,
    pub lineage: PgLineage,
}

impl PgBuildRequest {
    pub fn validate(&self) -> Result<(), String> {
        self.authority.validate().map_err(|e| e.to_string())?;
        self.lineage.validate()?;
        if self.version != BUILD_PROTOCOL_VERSION
            || self.source_port == 0
            || self.authority.source == self.authority.target
            || self.authority.build_id.is_empty()
            || self.authority.current_configuration.members.len() > 32
        {
            return Err("invalid native build envelope".into());
        }
        for value in [
            self.resource_uid.as_str(),
            self.authority.build_id.as_str(),
            self.source_session.as_str(),
            self.target_session.as_str(),
            self.source_endpoint.as_str(),
            self.source_host.as_str(),
            self.authority.source.instance_id.as_str(),
            self.authority.source.agent_generation.as_str(),
            self.authority.target.instance_id.as_str(),
            self.authority.target.agent_generation.as_str(),
        ] {
            if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
                return Err("missing or oversized native build identity/endpoint".into());
            }
        }
        if !self.source_endpoint.starts_with("http://")
            || self.source_host.parse::<std::net::IpAddr>().is_err()
        {
            return Err("native build requires an explicit source IP and HTTP endpoint".into());
        }
        Ok(())
    }

    pub fn same_work(&self, other: &Self) -> bool {
        self.authority == other.authority
            && self.resource_uid == other.resource_uid
            && self.lineage == other.lineage
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PgBuildMethod {
    Fresh,
    Rewind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PgBuildStage {
    Intent,
    Copying,
    Installed,
    Recovering,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgBuildProgress {
    pub request: PgBuildRequest,
    pub stage: PgBuildStage,
    pub method: PgBuildMethod,
    pub sequence: u64,
    pub evidence: Option<PgReplicationEvidence>,
}

impl PgBuildProgress {
    pub fn advance(&mut self, stage: PgBuildStage) -> Result<(), String> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or("native build sequence overflow")?;
        self.stage = stage;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), String> {
        self.request.validate()?;
        if self.sequence == 0 {
            return Err("missing native build sequence".into());
        }
        if let Some(evidence) = &self.evidence {
            evidence.validate()?;
            if evidence.engine != "postgres-physical"
                || evidence.system_identifier != self.request.lineage.system_identifier
                || evidence.timeline_id != self.request.lineage.timeline
                || !evidence.in_recovery
            {
                return Err("native build lineage differs from frozen source".into());
            }
        }
        if self.stage == PgBuildStage::Complete && !self.recovered() {
            return Err("native build is not durably replayed at its frozen boundary".into());
        }
        Ok(())
    }

    pub fn recovered(&self) -> bool {
        let boundary = self.request.authority.replication_boundary_lsn;
        self.evidence.as_ref().is_some_and(|e| {
            e.engine == "postgres-physical"
                && e.system_identifier == self.request.lineage.system_identifier
                && e.timeline_id == self.request.lineage.timeline
                && e.in_recovery
                && e.flush_lsn >= boundary
                && e.replay_lsn.is_some_and(|replay| replay >= boundary)
        })
    }
}

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    let bytes = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    if bytes.len() > MAX_ENVELOPE_BYTES {
        return Err("oversized native envelope".into());
    }
    Ok(bytes)
}

pub fn decode<T: serde::de::DeserializeOwned + Serialize>(bytes: &[u8]) -> Result<T, String> {
    if bytes.is_empty() || bytes.len() > MAX_ENVELOPE_BYTES {
        return Err("invalid native envelope length".into());
    }
    let value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    if encode(&value)? != bytes {
        return Err("native envelope must use canonical encoding".into());
    }
    Ok(value)
}
