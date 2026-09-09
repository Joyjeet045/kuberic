use std::ffi::CString;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use libsqlite3_sys as ffi;
use sqlite_commit_barrier::{
    BarrierError, CommitBarrier, Transaction, WAL_HEADER_SIZE, register, register_with_parent,
};

mod failing_vfs;

const PAGE: u32 = 512;
const FRAME_HEADER: usize = 24;
const MARKER: u8 = 0xab;

#[derive(Default)]
struct Recorder {
    published: AtomicUsize,
    abandoned: AtomicUsize,
    reject: bool,
    seen: Mutex<Vec<Vec<u8>>>,
}

impl Recorder {
    fn rejecting() -> Self {
        Self {
            reject: true,
            ..Self::default()
        }
    }
}

impl CommitBarrier for Recorder {
    fn publish(&self, transaction: &Transaction<'_>) -> Result<(), BarrierError> {
        self.published.fetch_add(1, Ordering::SeqCst);
        self.seen.lock().unwrap().push(transaction.frames.to_vec());
        if self.reject {
            return Err(BarrierError::new("no quorum"));
        }
        Ok(())
    }

    fn abandon(&self, _error: &str) {
        self.abandoned.fetch_add(1, Ordering::SeqCst);
    }
}

fn wal_header() -> Vec<u8> {
    let mut header = vec![0u8; WAL_HEADER_SIZE];
    header[0..4].copy_from_slice(&0x377f_0682u32.to_be_bytes());
    header[8..12].copy_from_slice(&PAGE.to_be_bytes());
    header
}

fn commit_frame() -> Vec<u8> {
    let mut frame = vec![0u8; FRAME_HEADER + PAGE as usize];
    frame[0..4].copy_from_slice(&1u32.to_be_bytes());
    frame[4..8].copy_from_slice(&1u32.to_be_bytes());
    frame[FRAME_HEADER..].fill(MARKER);
    frame
}

struct OpenFile {
    buffer: Vec<u8>,
}

impl OpenFile {
    fn open(vfs_name: &str, path: &std::path::Path) -> Self {
        let name = CString::new(vfs_name).unwrap();
        let path = CString::new(path.to_str().unwrap()).unwrap();
        unsafe {
            let vfs = ffi::sqlite3_vfs_find(name.as_ptr());
            assert!(!vfs.is_null(), "vfs not registered");
            let mut buffer = vec![0u8; (*vfs).szOsFile as usize];
            let file = buffer.as_mut_ptr() as *mut ffi::sqlite3_file;
            let mut out_flags = 0;
            let rc = ((*vfs).xOpen.unwrap())(
                vfs,
                path.as_ptr(),
                file,
                ffi::SQLITE_OPEN_WAL | ffi::SQLITE_OPEN_CREATE | ffi::SQLITE_OPEN_READWRITE,
                &mut out_flags,
            );
            assert_eq!(rc, ffi::SQLITE_OK, "xOpen failed");
            Self { buffer }
        }
    }

    fn handle(&mut self) -> *mut ffi::sqlite3_file {
        self.buffer.as_mut_ptr() as *mut ffi::sqlite3_file
    }

    fn write(&mut self, offset: i64, data: &[u8]) -> i32 {
        let file = self.handle();
        unsafe {
            ((*(*file).pMethods).xWrite.unwrap())(
                file,
                data.as_ptr() as *const std::ffi::c_void,
                data.len() as i32,
                offset,
            )
        }
    }

    fn close(&mut self) {
        let file = self.handle();
        unsafe {
            ((*(*file).pMethods).xClose.unwrap())(file);
        }
    }

    fn sync(&mut self) -> i32 {
        let file = self.handle();
        unsafe { ((*(*file).pMethods).xSync.unwrap())(file, ffi::SQLITE_SYNC_NORMAL) }
    }
}

#[test]
fn a_local_write_failure_after_quorum_abandons_the_replica() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("staged.wal");
    let barrier = Arc::new(Recorder::default());
    failing_vfs::register("failing-parent");
    register_with_parent("staging-failing", "failing-parent", barrier.clone()).unwrap();

    let mut file = OpenFile::open("staging-failing", &path);
    assert_eq!(file.write(0, &wal_header()), ffi::SQLITE_OK);

    failing_vfs::WRITES_FAIL.store(1, Ordering::SeqCst);
    let rc = file.write(WAL_HEADER_SIZE as i64, &commit_frame());
    failing_vfs::WRITES_FAIL.store(0, Ordering::SeqCst);
    file.close();

    assert_eq!(
        barrier.published.load(Ordering::SeqCst),
        1,
        "the transaction must have reached quorum first"
    );
    assert_eq!(
        barrier.abandoned.load(Ordering::SeqCst),
        1,
        "a quorum-confirmed transaction lost locally must abandon the replica"
    );
    assert_ne!(rc, ffi::SQLITE_OK, "the write must not report success");
}

#[test]
fn a_commit_frame_reaches_the_barrier_before_it_reaches_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("staged.wal");
    let barrier = Arc::new(Recorder::rejecting());
    register("staging-reject", barrier.clone()).unwrap();

    let mut file = OpenFile::open("staging-reject", &path);
    assert_eq!(file.write(0, &wal_header()), ffi::SQLITE_OK);

    let rc = file.write(WAL_HEADER_SIZE as i64, &commit_frame());

    assert_eq!(
        barrier.published.load(Ordering::SeqCst),
        1,
        "a complete commit frame must reach the barrier without waiting for a sync"
    );
    assert_eq!(
        rc,
        ffi::SQLITE_IOERR_WRITE,
        "a rejected commit must fail the write that completed it"
    );

    let far = (WAL_HEADER_SIZE + (FRAME_HEADER + PAGE as usize) * 8) as i64;
    file.write(far, &[0u8; 16]);
    file.close();

    let on_disk = std::fs::read(&path).unwrap_or_default();
    assert!(
        !on_disk.contains(&MARKER),
        "rejected commit bytes reached the file"
    );
}

#[test]
fn an_accepted_commit_displaced_by_a_discontiguous_write_is_published_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("staged.wal");
    let barrier = Arc::new(Recorder::default());
    register("staging-accept", barrier.clone()).unwrap();

    let mut file = OpenFile::open("staging-accept", &path);
    assert_eq!(file.write(0, &wal_header()), ffi::SQLITE_OK);
    assert_eq!(
        file.write(WAL_HEADER_SIZE as i64, &commit_frame()),
        ffi::SQLITE_OK
    );

    let far = (WAL_HEADER_SIZE + (FRAME_HEADER + PAGE as usize) * 8) as i64;
    assert_eq!(file.write(far, &[7u8; 16]), ffi::SQLITE_OK);
    assert_eq!(
        file.sync(),
        ffi::SQLITE_OK,
        "a sync after publication must delegate cleanly"
    );
    file.close();

    assert_eq!(barrier.published.load(Ordering::SeqCst), 1);
    let seen = barrier.seen.lock().unwrap();
    assert!(
        seen[0].ends_with(&[MARKER; 16]),
        "the barrier must see the commit frame payload"
    );

    let on_disk = std::fs::read(&path).unwrap();
    assert!(
        on_disk.contains(&MARKER),
        "an accepted commit must reach the file"
    );
}

#[test]
fn staged_bytes_without_a_commit_frame_flush_without_the_barrier() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("staged.wal");
    let barrier = Arc::new(Recorder::default());
    register("staging-partial", barrier.clone()).unwrap();

    let mut file = OpenFile::open("staging-partial", &path);
    assert_eq!(file.write(0, &wal_header()), ffi::SQLITE_OK);

    let mut partial = commit_frame();
    partial[4..8].copy_from_slice(&0u32.to_be_bytes());
    assert_eq!(file.write(WAL_HEADER_SIZE as i64, &partial), ffi::SQLITE_OK);

    let far = (WAL_HEADER_SIZE + (FRAME_HEADER + PAGE as usize) * 8) as i64;
    assert_eq!(file.write(far, &[7u8; 16]), ffi::SQLITE_OK);
    file.close();

    assert_eq!(
        barrier.published.load(Ordering::SeqCst),
        0,
        "a non-commit frame must not consume a quorum round trip"
    );
    let on_disk = std::fs::read(&path).unwrap();
    assert!(on_disk.contains(&MARKER));
}
