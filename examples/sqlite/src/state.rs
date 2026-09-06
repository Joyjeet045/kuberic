//! SqliteState: manages SQLite database for both primary and secondary roles.
//!
//! Primary: opens SQLite in WAL mode, captures WAL frames after commits.
//! Secondary: applies received page data directly to the DB file.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use kuberic_core::types::Lsn;
use rusqlite::{Connection, OpenFlags};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::framelog::{FrameLog, FrameLogMeta};
use crate::frames::WalFrameSet;

const WAL_HEADER_BYTES: u64 = 32;

/// The SQLite database state.
pub struct SqliteState {
    /// SQLite connection (only on primary).
    conn: Option<Connection>,
    /// Path to the SQLite database file.
    pub db_path: PathBuf,
    /// Data directory containing db, frames.log, meta.json.
    pub data_dir: PathBuf,
    /// SQLite page size (typically 4096).
    pub page_size: u32,
    /// Current offset in WAL file for frame reading (primary).
    wal_read_offset: u64,
    wal_salt: Option<(u32, u32)>,
    confirmed_wal_offset: u64,
    confirmed_wal_salt: Option<(u32, u32)>,
    /// Last LSN applied to the database.
    pub last_applied_lsn: Lsn,
    /// Last committed LSN (confirmed by quorum).
    pub committed_lsn: Lsn,
    /// Secondary's frame log.
    frame_log: Option<FrameLog>,
}

pub type SharedState = Arc<Mutex<SqliteState>>;

impl SqliteState {
    /// Create a new SqliteState. Does not open the database yet.
    pub async fn open(data_dir: PathBuf) -> io::Result<Self> {
        tokio::fs::create_dir_all(&data_dir).await?;
        let db_path = data_dir.join("db.sqlite");

        // Load metadata if exists
        let meta = FrameLog::load_meta(&data_dir).await?;

        Ok(Self {
            conn: None,
            db_path,
            data_dir,
            page_size: 4096,
            wal_read_offset: 0,
            wal_salt: None,
            confirmed_wal_offset: meta.confirmed_wal_offset,
            confirmed_wal_salt: meta.confirmed_wal_salt,
            last_applied_lsn: meta.committed_lsn,
            committed_lsn: meta.committed_lsn,
            frame_log: None,
        })
    }

    /// Open SQLite as primary: WAL mode, single connection, no auto-checkpoint.
    pub fn open_as_primary(&mut self) -> io::Result<()> {
        self.discard_unconfirmed_wal()?;

        let conn = Connection::open_with_flags(
            &self.db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| io::Error::other(format!("failed to open SQLite: {e}")))?;

        // Enable WAL mode
        conn.pragma_update(None, "journal_mode", "wal")
            .map_err(|e| io::Error::other(format!("failed to set WAL mode: {e}")))?;

        // Disable auto-checkpoint — we control checkpointing
        conn.pragma_update(None, "wal_autocheckpoint", 0)
            .map_err(|e| io::Error::other(format!("failed to disable auto-checkpoint: {e}")))?;

        // Read page size
        let page_size: u32 = conn
            .pragma_query_value(None, "page_size", |row| row.get(0))
            .map_err(|e| io::Error::other(format!("failed to read page_size: {e}")))?;
        self.page_size = page_size;

        info!(
            db = %self.db_path.display(),
            page_size,
            "SQLite opened as primary (WAL mode)"
        );

        self.conn = Some(conn);
        Ok(())
    }

    /// Open frame log for secondary role.
    pub async fn open_frame_log(&mut self) -> io::Result<()> {
        self.frame_log = Some(FrameLog::open(&self.data_dir).await?);
        Ok(())
    }

    /// Execute a write SQL statement (primary only).
    /// Returns (rows_affected, last_insert_rowid).
    pub fn execute_sql(
        &self,
        sql: &str,
        params: &[rusqlite::types::Value],
    ) -> Result<(usize, i64), rusqlite::Error> {
        let conn = self.conn.as_ref().expect("not open as primary");
        let rows = conn.execute(sql, rusqlite::params_from_iter(params))?;
        let rowid = conn.last_insert_rowid();
        Ok((rows, rowid))
    }

    /// Execute a batch of SQL statements in a transaction (primary only).
    /// Returns rows_affected per statement.
    pub fn execute_batch_sql(
        &mut self,
        statements: &[String],
    ) -> Result<Vec<usize>, rusqlite::Error> {
        let conn = self.conn.as_mut().expect("not open as primary");
        let tx = conn.transaction()?;
        let mut results = Vec::with_capacity(statements.len());
        for sql in statements {
            let rows = tx.execute(sql, [])?;
            results.push(rows);
        }
        tx.commit()?;
        Ok(results)
    }

    /// Execute a SELECT query (primary only).
    /// Returns (column_names, rows) where each row is a Vec of serde_json::Value.
    pub fn query_sql(
        &self,
        sql: &str,
        params: &[rusqlite::types::Value],
    ) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>), rusqlite::Error> {
        let conn = self.conn.as_ref().expect("not open as primary");
        let mut stmt = conn.prepare(sql)?;
        let columns: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let col_count = columns.len();

        let rows = stmt
            .query_map(rusqlite::params_from_iter(params), |row| {
                let mut values = Vec::with_capacity(col_count);
                for i in 0..col_count {
                    let val: rusqlite::types::Value = row.get(i)?;
                    values.push(sqlite_value_to_json(val));
                }
                Ok(values)
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok((columns, rows))
    }

    /// Capture new WAL frames after a write operation (primary only).
    /// Returns None if no new frames were written.
    pub fn capture_wal_frames(&mut self) -> io::Result<Option<(WalFrameSet, u64)>> {
        let wal_path = self.wal_path();

        let salt = crate::frames::read_wal_salt(&wal_path)?;
        if salt != self.wal_salt {
            debug!(?salt, previous = ?self.wal_salt, "WAL generation changed — rewinding capture");
            self.wal_read_offset = 0;
            self.wal_salt = salt;
        }

        // Check page size from WAL if needed
        if self.wal_read_offset == 0
            && let Some(ps) = crate::frames::read_wal_page_size(&wal_path)?
            && ps != self.page_size
        {
            warn!(
                wal_page_size = ps,
                db_page_size = self.page_size,
                "page size mismatch"
            );
        }

        let (frames, db_size_pages, new_offset) =
            crate::frames::read_wal_frames(&wal_path, self.page_size, self.wal_read_offset)?;

        if frames.is_empty() {
            return Ok(None);
        }

        self.wal_read_offset = new_offset;

        let checksum = WalFrameSet::compute_checksum(&frames);
        let frame_set = WalFrameSet {
            frames,
            db_size_pages,
            checksum,
        };

        debug!(
            num_frames = frame_set.frames.len(),
            db_size_pages, "captured WAL frames"
        );

        Ok(Some((frame_set, new_offset)))
    }

    pub fn wal_read_offset(&self) -> u64 {
        self.wal_read_offset
    }

    pub fn rewind_capture(&mut self, offset: u64) {
        self.wal_read_offset = offset;
    }

    pub async fn mark_confirmed(&mut self, lsn: Lsn, wal_offset: u64) -> io::Result<()> {
        self.last_applied_lsn = lsn;
        self.committed_lsn = lsn;
        self.confirmed_wal_offset = wal_offset;
        self.confirmed_wal_salt = self.wal_salt;

        FrameLog::save_meta(
            &self.data_dir,
            &FrameLogMeta {
                committed_lsn: lsn,
                confirmed_wal_offset: wal_offset,
                confirmed_wal_salt: self.wal_salt,
            },
        )
        .await
    }

    fn discard_unconfirmed_wal(&mut self) -> io::Result<()> {
        let wal_path = self.wal_path();
        let wal_len = match std::fs::metadata(&wal_path) {
            Ok(m) => m.len(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                self.wal_read_offset = 0;
                self.wal_salt = None;
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        let salt = crate::frames::read_wal_salt(&wal_path)?;

        if salt != self.confirmed_wal_salt {
            if wal_len > WAL_HEADER_BYTES {
                warn!(
                    wal_len,
                    "discarding WAL from an unconfirmed generation — no frame in \
                     it reached write quorum"
                );
            }
            let _ = std::fs::remove_file(&wal_path);
            let _ = std::fs::remove_file(self.shm_path());
            self.wal_read_offset = 0;
            self.wal_salt = None;
            self.confirmed_wal_offset = 0;
            self.confirmed_wal_salt = None;
            return Ok(());
        }

        self.wal_salt = salt;

        if wal_len > self.confirmed_wal_offset {
            let file = std::fs::OpenOptions::new().write(true).open(&wal_path)?;
            file.set_len(self.confirmed_wal_offset)?;
            file.sync_all()?;
            drop(file);
            let _ = std::fs::remove_file(self.shm_path());
            warn!(
                discarded_bytes = wal_len - self.confirmed_wal_offset,
                confirmed_wal_offset = self.confirmed_wal_offset,
                "discarded WAL frames that never reached write quorum"
            );
        }

        self.wal_read_offset = self.confirmed_wal_offset;
        Ok(())
    }

    /// Apply a WalFrameSet to the database file (secondary).
    /// Writes page data directly at the correct file offsets.
    pub fn apply_frames(&mut self, frame_set: &WalFrameSet) -> io::Result<()> {
        use std::io::{Seek, SeekFrom, Write};

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&self.db_path)?;

        // Extend file if needed
        if frame_set.db_size_pages > 0 {
            let target_size = frame_set.db_size_pages as u64 * self.page_size as u64;
            let current_size = file.metadata()?.len();
            if target_size > current_size {
                file.set_len(target_size)?;
            }
        }

        for frame in &frame_set.frames {
            let offset = (frame.page_number as u64 - 1) * self.page_size as u64;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&frame.data)?;
        }

        file.sync_data()?;

        debug!(
            num_frames = frame_set.frames.len(),
            db_size_pages = frame_set.db_size_pages,
            "applied frames to DB file"
        );

        Ok(())
    }

    /// Persist a frame set to the frame log (secondary, before ACK).
    pub async fn persist_frame(&mut self, lsn: Lsn, frame_set: &WalFrameSet) -> io::Result<()> {
        if let Some(ref mut log) = self.frame_log {
            log.append(lsn, frame_set).await?;
        }
        if lsn > self.last_applied_lsn {
            self.last_applied_lsn = lsn;
        }
        Ok(())
    }

    /// Apply committed frames from frame log to DB file (secondary).
    pub async fn apply_committed_frames(&mut self, up_to_lsn: Lsn) -> io::Result<()> {
        let entries = FrameLog::read_all(&self.data_dir).await?;
        let mut applied = 0u64;
        for entry in &entries {
            if entry.lsn <= up_to_lsn && entry.lsn > self.committed_lsn {
                self.apply_frames(&entry.frame_set)?;
                applied += 1;
            }
        }
        if applied > 0 {
            self.committed_lsn = up_to_lsn;
            FrameLog::save_meta(
                &self.data_dir,
                &FrameLogMeta {
                    committed_lsn: self.committed_lsn,
                    confirmed_wal_offset: self.confirmed_wal_offset,
                    confirmed_wal_salt: self.confirmed_wal_salt,
                },
            )
            .await?;
            info!(
                committed_lsn = self.committed_lsn,
                applied, "applied committed frames"
            );
        }
        Ok(())
    }

    /// Rollback: truncate frame log beyond target LSN.
    pub async fn rollback_to(&mut self, target_lsn: Lsn) -> io::Result<()> {
        if target_lsn < self.committed_lsn {
            warn!(
                target_lsn,
                committed_lsn = self.committed_lsn,
                "rollback past committed data — need full copy"
            );
            // Signal data loss needed
            return Ok(());
        }

        FrameLog::truncate_to(&self.data_dir, target_lsn).await?;
        self.last_applied_lsn = target_lsn;
        info!(target_lsn, "rollback complete");
        Ok(())
    }

    /// Produce a full snapshot of the database for copy protocol.
    pub fn snapshot_db(&self) -> io::Result<Vec<u8>> {
        if let Some(ref conn) = self.conn {
            // Checkpoint WAL into DB file so the snapshot has all data
            let _ = conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)");
        }
        std::fs::read(&self.db_path)
    }

    /// Replace DB file with a full snapshot (secondary, during copy).
    pub async fn restore_from_snapshot(&mut self, data: &[u8]) -> io::Result<()> {
        // Close any existing connection
        self.conn = None;
        tokio::fs::write(&self.db_path, data).await?;

        // Remove any stale WAL/SHM files
        let wal = self.data_dir.join("db.sqlite-wal");
        let shm = self.data_dir.join("db.sqlite-shm");
        let _ = tokio::fs::remove_file(&wal).await;
        let _ = tokio::fs::remove_file(&shm).await;

        self.wal_read_offset = 0;
        self.wal_salt = None;
        self.confirmed_wal_offset = 0;
        self.confirmed_wal_salt = None;

        info!(size = data.len(), "restored DB from snapshot");
        Ok(())
    }

    /// Close the SQLite connection.
    pub fn close(&mut self) {
        if let Some(conn) = self.conn.take() {
            let _ = conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)");
            drop(conn);
            info!("SQLite connection closed");
        }
    }

    fn wal_path(&self) -> PathBuf {
        let mut wal = self.db_path.clone();
        let name = wal.file_name().unwrap().to_str().unwrap().to_string();
        wal.set_file_name(format!("{}-wal", name));
        wal
    }

    fn shm_path(&self) -> PathBuf {
        let mut shm = self.db_path.clone();
        let name = shm.file_name().unwrap().to_str().unwrap().to_string();
        shm.set_file_name(format!("{}-shm", name));
        shm
    }
}

fn sqlite_value_to_json(val: rusqlite::types::Value) -> serde_json::Value {
    match val {
        rusqlite::types::Value::Null => serde_json::Value::Null,
        rusqlite::types::Value::Integer(i) => serde_json::Value::Number(i.into()),
        rusqlite::types::Value::Real(f) => {
            serde_json::json!(f)
        }
        rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
        rusqlite::types::Value::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            serde_json::Value::String(hex)
        }
    }
}
