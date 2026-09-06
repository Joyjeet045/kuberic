//! Secondary-side persistence: frames.log + meta.json.
//!
//! Received WalFrameSets are persisted to frames.log (length-prefixed binary)
//! before ACK. Applied frames are tracked via committed_lsn in meta.json.

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, info, warn};

use crate::frames::WalFrameSet;
use kuberic_core::types::Lsn;

/// Persisted metadata for crash recovery.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct FrameLogMeta {
    pub committed_lsn: Lsn,
    #[serde(default)]
    pub confirmed_wal_offset: u64,
    #[serde(default)]
    pub confirmed_wal_salt: Option<(u32, u32)>,
}

/// A single entry in frames.log: one WalFrameSet at one LSN.
#[derive(Serialize, Deserialize, Debug)]
pub struct FrameLogEntry {
    pub lsn: Lsn,
    pub frame_set: WalFrameSet,
}

/// Secondary's durable frame log.
pub struct FrameLog {
    #[allow(dead_code)]
    data_dir: PathBuf,
    file: tokio::fs::File,
}

impl FrameLog {
    /// Open or create frames.log in the given directory.
    pub async fn open(data_dir: &Path) -> io::Result<Self> {
        tokio::fs::create_dir_all(data_dir).await?;
        let path = data_dir.join("frames.log");
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .await?;
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            file,
        })
    }

    /// Append a WalFrameSet entry. Fsyncs for durability before returning.
    /// Format: [4-byte length (LE)][JSON payload][4-byte CRC32 (LE)]
    pub async fn append(&mut self, lsn: Lsn, frame_set: &WalFrameSet) -> io::Result<()> {
        let entry = FrameLogEntry {
            lsn,
            frame_set: frame_set.clone(),
        };
        let payload = serde_json::to_vec(&entry).map_err(io::Error::other)?;
        let len = payload.len() as u32;
        let crc = crc32fast::hash(&payload);

        self.file.write_all(&len.to_le_bytes()).await?;
        self.file.write_all(&payload).await?;
        self.file.write_all(&crc.to_le_bytes()).await?;
        self.file.flush().await?;
        self.file.sync_data().await?;

        debug!(lsn, payload_len = len, "frame log entry appended");
        Ok(())
    }

    /// Read all valid entries from frames.log.
    pub async fn read_all(data_dir: &Path) -> io::Result<Vec<FrameLogEntry>> {
        let path = data_dir.join("frames.log");
        let mut file = match tokio::fs::File::open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(e),
        };

        let mut entries = Vec::new();
        loop {
            // Read length prefix
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            // Read payload
            let mut payload = vec![0u8; len];
            match file.read_exact(&mut payload).await {
                Ok(_) => {}
                Err(_) => {
                    warn!("truncated frame log entry, stopping");
                    break;
                }
            }

            // Read and verify CRC
            let mut crc_buf = [0u8; 4];
            match file.read_exact(&mut crc_buf).await {
                Ok(_) => {}
                Err(_) => {
                    warn!("truncated CRC in frame log, stopping");
                    break;
                }
            }
            let stored_crc = u32::from_le_bytes(crc_buf);
            let computed_crc = crc32fast::hash(&payload);
            if stored_crc != computed_crc {
                warn!(
                    stored_crc,
                    computed_crc, "CRC mismatch in frame log, stopping"
                );
                break;
            }

            match serde_json::from_slice::<FrameLogEntry>(&payload) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    warn!(error = %e, "invalid frame log entry, stopping");
                    break;
                }
            }
        }

        Ok(entries)
    }

    /// Truncate frames.log to only entries with LSN <= max_lsn.
    pub async fn truncate_to(data_dir: &Path, max_lsn: Lsn) -> io::Result<()> {
        let entries = Self::read_all(data_dir).await?;
        let kept: Vec<_> = entries.into_iter().filter(|e| e.lsn <= max_lsn).collect();
        let kept_len = kept.len();

        let mut buf = Vec::new();
        for entry in &kept {
            let payload = serde_json::to_vec(entry).map_err(io::Error::other)?;
            let len = payload.len() as u32;
            let crc = crc32fast::hash(&payload);
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(&payload);
            buf.extend_from_slice(&crc.to_le_bytes());
        }

        let path = data_dir.join("frames.log");
        let tmp_path = data_dir.join("frames.log.tmp");

        tokio::task::spawn_blocking(move || -> io::Result<()> {
            use std::io::Write;
            {
                let mut file = std::fs::File::create(&tmp_path)?;
                file.write_all(&buf)?;
                file.sync_all()?;
            }
            std::fs::rename(&tmp_path, &path)
        })
        .await
        .map_err(io::Error::other)??;

        info!(max_lsn, kept = kept_len, "frame log truncated");
        Ok(())
    }

    /// Load committed_lsn from meta.json.
    pub async fn load_meta(data_dir: &Path) -> io::Result<FrameLogMeta> {
        let path = data_dir.join("meta.json");
        match tokio::fs::read_to_string(&path).await {
            Ok(contents) => {
                let meta: FrameLogMeta =
                    serde_json::from_str(&contents).map_err(io::Error::other)?;
                info!(committed_lsn = meta.committed_lsn, "loaded meta.json");
                Ok(meta)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                info!("no meta.json found, starting fresh");
                Ok(FrameLogMeta::default())
            }
            Err(e) => Err(e),
        }
    }

    /// Save committed_lsn to meta.json (atomic write).
    pub async fn save_meta(data_dir: &Path, meta: &FrameLogMeta) -> io::Result<()> {
        let path = data_dir.join("meta.json");
        let tmp = data_dir.join("meta.json.tmp");
        let json = serde_json::to_string(meta).map_err(io::Error::other)?;
        let committed_lsn = meta.committed_lsn;

        tokio::task::spawn_blocking(move || -> io::Result<()> {
            use std::io::Write;
            {
                let mut file = std::fs::File::create(&tmp)?;
                file.write_all(json.as_bytes())?;
                file.sync_all()?;
            }
            std::fs::rename(&tmp, &path)
        })
        .await
        .map_err(io::Error::other)??;

        debug!(committed_lsn, "saved meta.json");
        Ok(())
    }
}
