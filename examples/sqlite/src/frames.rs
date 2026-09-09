//! WalFrameSet types and SQLite WAL file reader.
//!
//! The replication unit is a WalFrameSet: a set of WAL frames from one
//! committed transaction. Each frame is a (page_number, page_data) pair.
//! We serialize via serde_json for simplicity (matching kvstore).

use serde::{Deserialize, Serialize};

/// A single WAL frame: one page of data.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WalFrame {
    pub page_number: u32,
    pub data: Vec<u8>,
}

/// A set of WAL frames from one committed transaction.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WalFrameSet {
    pub frames: Vec<WalFrame>,
    pub db_size_pages: u32,
    pub checksum: u32,
}

impl WalFrameSet {
    /// Compute CRC32 over all frame data.
    pub fn compute_checksum(frames: &[WalFrame]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        for frame in frames {
            hasher.update(&frame.page_number.to_le_bytes());
            hasher.update(&frame.data);
        }
        hasher.finalize()
    }

    /// Verify the checksum matches the frame data.
    pub fn verify_checksum(&self) -> bool {
        Self::compute_checksum(&self.frames) == self.checksum
    }
}

// SQLite WAL file format constants
const WAL_HEADER_SIZE: u64 = 32;
const FRAME_HEADER_SIZE: u64 = 24;

/// Split the raw WAL bytes of one transaction into replicable frames.
///
/// `wal_offset` is where `bytes` begins in the WAL file, so frame boundaries
/// are located against the file rather than the slice.
pub fn frames_from_wal_bytes(wal_offset: u64, bytes: &[u8], page_size: u32) -> Vec<WalFrame> {
    if page_size == 0 {
        return Vec::new();
    }
    let frame_size = FRAME_HEADER_SIZE + page_size as u64;
    let end = wal_offset + bytes.len() as u64;
    let mut header_at = WAL_HEADER_SIZE;
    if wal_offset > WAL_HEADER_SIZE {
        let past = wal_offset - WAL_HEADER_SIZE;
        let frames_before = past.div_ceil(frame_size);
        header_at = WAL_HEADER_SIZE + frames_before * frame_size;
    }

    let mut frames = Vec::new();
    while header_at + frame_size <= end {
        let at = (header_at - wal_offset) as usize;
        let page_number =
            u32::from_be_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]]);
        let data_at = at + FRAME_HEADER_SIZE as usize;
        frames.push(WalFrame {
            page_number,
            data: bytes[data_at..data_at + page_size as usize].to_vec(),
        });
        header_at += frame_size;
    }
    frames
}
