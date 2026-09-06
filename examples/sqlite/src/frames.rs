//! WalFrameSet types and SQLite WAL file reader.
//!
//! The replication unit is a WalFrameSet: a set of WAL frames from one
//! committed transaction. Each frame is a (page_number, page_data) pair.
//! We serialize via serde_json for simplicity (matching kvstore).

use serde::{Deserialize, Serialize};
use std::io;
use std::path::Path;

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

/// Read the page size from the WAL header.
/// Returns None if WAL file doesn't exist or is too small.
pub fn read_wal_page_size(wal_path: &Path) -> io::Result<Option<u32>> {
    use std::io::Read;
    let mut file = match std::fs::File::open(wal_path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut header = [0u8; 32];
    match file.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    // Page size is at offset 8, big-endian u32
    let page_size = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);
    Ok(Some(page_size))
}

pub fn read_wal_salt(wal_path: &Path) -> io::Result<Option<(u32, u32)>> {
    use std::io::Read;
    let mut file = match std::fs::File::open(wal_path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut header = [0u8; 32];
    match file.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let salt1 = u32::from_be_bytes([header[16], header[17], header[18], header[19]]);
    let salt2 = u32::from_be_bytes([header[20], header[21], header[22], header[23]]);
    Ok(Some((salt1, salt2)))
}

/// Read new WAL frames starting from `offset` in the WAL file.
/// Returns the frames and the new offset (for tracking position).
///
/// WAL format: 32-byte header, then frames of (24-byte header + page_size data).
/// Frame header: page_number (4B BE), db_size (4B BE), salt1, salt2, cksum1, cksum2.
/// db_size > 0 indicates a commit frame.
pub fn read_wal_frames(
    wal_path: &Path,
    page_size: u32,
    from_offset: u64,
) -> io::Result<(Vec<WalFrame>, u32, u64)> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = match std::fs::File::open(wal_path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((vec![], 0, from_offset)),
        Err(e) => return Err(e),
    };

    let file_len = file.metadata()?.len();
    let frame_size = FRAME_HEADER_SIZE + page_size as u64;

    // Start from WAL header if offset is 0
    let start = if from_offset == 0 {
        WAL_HEADER_SIZE
    } else {
        from_offset
    };

    if start >= file_len {
        return Ok((vec![], 0, start));
    }

    file.seek(SeekFrom::Start(start))?;

    let mut frames = Vec::new();
    let mut db_size_pages: u32 = 0;
    let mut pos = start;

    while pos + frame_size <= file_len {
        let mut frame_header = [0u8; 24];
        file.read_exact(&mut frame_header)?;

        let page_number = u32::from_be_bytes([
            frame_header[0],
            frame_header[1],
            frame_header[2],
            frame_header[3],
        ]);
        let db_size = u32::from_be_bytes([
            frame_header[4],
            frame_header[5],
            frame_header[6],
            frame_header[7],
        ]);

        let mut page_data = vec![0u8; page_size as usize];
        file.read_exact(&mut page_data)?;

        frames.push(WalFrame {
            page_number,
            data: page_data,
        });

        if db_size > 0 {
            db_size_pages = db_size;
        }

        pos += frame_size;
    }

    Ok((frames, db_size_pages, pos))
}
