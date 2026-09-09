use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::ptr;
use std::sync::Arc;

use libsqlite3_sys as ffi;

use crate::wal::{StageOutcome, WalStage, parse_wal_header};
use crate::{CommitBarrier, Transaction};

pub(crate) struct VfsAppData {
    pub(crate) parent: *mut ffi::sqlite3_vfs,
    pub(crate) barrier: Arc<dyn CommitBarrier>,
}

#[repr(C)]
pub(crate) struct BarrierFile {
    base: ffi::sqlite3_file,
    stage: *mut WalStage,
    barrier: *const c_void,
    parent_vfs: *mut ffi::sqlite3_vfs,
}

struct ParentFile(*mut ffi::sqlite3_file);

impl ParentFile {
    unsafe fn methods(&self) -> &ffi::sqlite3_io_methods {
        unsafe { &*(*self.0).pMethods }
    }

    unsafe fn opened(&self) -> bool {
        unsafe { !(*self.0).pMethods.is_null() }
    }

    unsafe fn close(&self) -> c_int {
        unsafe { (self.methods().xClose.expect("xClose"))(self.0) }
    }

    unsafe fn read(&self, out: &mut [u8], offset: u64) -> c_int {
        unsafe {
            (self.methods().xRead.expect("xRead"))(
                self.0,
                out.as_mut_ptr() as *mut c_void,
                out.len() as c_int,
                offset as i64,
            )
        }
    }

    unsafe fn write(&self, offset: u64, data: &[u8]) -> c_int {
        unsafe {
            (self.methods().xWrite.expect("xWrite"))(
                self.0,
                data.as_ptr() as *const c_void,
                data.len() as c_int,
                offset as i64,
            )
        }
    }

    unsafe fn truncate(&self, size: i64) -> c_int {
        unsafe { (self.methods().xTruncate.expect("xTruncate"))(self.0, size) }
    }

    unsafe fn sync(&self, flags: c_int) -> c_int {
        unsafe { (self.methods().xSync.expect("xSync"))(self.0, flags) }
    }

    unsafe fn file_size(&self, out: *mut i64) -> c_int {
        unsafe { (self.methods().xFileSize.expect("xFileSize"))(self.0, out) }
    }

    unsafe fn lock(&self, level: c_int) -> c_int {
        unsafe { (self.methods().xLock.expect("xLock"))(self.0, level) }
    }

    unsafe fn unlock(&self, level: c_int) -> c_int {
        unsafe { (self.methods().xUnlock.expect("xUnlock"))(self.0, level) }
    }

    unsafe fn check_reserved_lock(&self, out: *mut c_int) -> c_int {
        unsafe {
            (self
                .methods()
                .xCheckReservedLock
                .expect("xCheckReservedLock"))(self.0, out)
        }
    }

    unsafe fn file_control(&self, op: c_int, arg: *mut c_void) -> c_int {
        unsafe { (self.methods().xFileControl.expect("xFileControl"))(self.0, op, arg) }
    }

    unsafe fn sector_size(&self) -> c_int {
        unsafe { (self.methods().xSectorSize.expect("xSectorSize"))(self.0) }
    }

    unsafe fn device_characteristics(&self) -> c_int {
        unsafe {
            (self
                .methods()
                .xDeviceCharacteristics
                .expect("xDeviceCharacteristics"))(self.0)
        }
    }
}

impl BarrierFile {
    unsafe fn parent(file: *mut ffi::sqlite3_file) -> ParentFile {
        unsafe {
            ParentFile((file as *mut u8).add(size_of::<BarrierFile>()) as *mut ffi::sqlite3_file)
        }
    }

    unsafe fn stage<'a>(file: *mut ffi::sqlite3_file) -> Option<&'a mut WalStage> {
        unsafe {
            let this = file as *mut BarrierFile;
            if (*this).stage.is_null() {
                None
            } else {
                Some(&mut *(*this).stage)
            }
        }
    }

    unsafe fn barrier<'a>(file: *mut ffi::sqlite3_file) -> &'a Arc<dyn CommitBarrier> {
        unsafe {
            let this = file as *mut BarrierFile;
            &*((*this).barrier as *const Arc<dyn CommitBarrier>)
        }
    }
}

unsafe fn write_through(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let Some(stage) = BarrierFile::stage(file) else {
            return ffi::SQLITE_OK;
        };
        if stage.is_empty() {
            return ffi::SQLITE_OK;
        }
        let (start, bytes) = stage.take();
        BarrierFile::parent(file).write(start, &bytes)
    }
}

unsafe fn flush_stage(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        match BarrierFile::stage(file) {
            Some(stage) if stage.commit_pages().is_some() => publish_stage(file),
            _ => write_through(file),
        }
    }
}

/// A commit frame is published as soon as it is complete, because SQLite only
/// syncs the WAL when `synchronous=FULL`.
unsafe fn publish_if_complete(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        match BarrierFile::stage(file) {
            Some(stage) if stage.commit_pages().is_some() => publish_stage(file),
            _ => ffi::SQLITE_OK,
        }
    }
}

unsafe fn publish_stage(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let Some(stage) = BarrierFile::stage(file) else {
            return ffi::SQLITE_OK;
        };
        if stage.is_empty() {
            return ffi::SQLITE_OK;
        }
        let Some(pages) = stage.commit_pages() else {
            return write_through(file);
        };
        let layout = stage.layout();
        let barrier = BarrierFile::barrier(file).clone();
        let transaction = Transaction {
            wal_offset: stage.start(),
            frames: stage.bytes(),
            page_size: layout.map(|layout| layout.page_size).unwrap_or_default(),
            database_pages: pages,
        };
        match barrier.publish(&transaction) {
            Ok(()) => {
                let rc = write_through(file);
                if rc != ffi::SQLITE_OK {
                    let message = format!(
                        "a replicated transaction could not be stored locally: {}",
                        describe_error(rc)
                    );
                    tracing::error!(message, "local publication failed after quorum");
                    barrier.abandon(&message);
                }
                rc
            }
            Err(error) => {
                tracing::warn!(%error, "commit barrier rejected a transaction");
                stage.take();
                ffi::SQLITE_IOERR_WRITE
            }
        }
    }
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        if let Some(stage) = BarrierFile::stage(file) {
            stage.take();
            let this = file as *mut BarrierFile;
            drop(Box::from_raw((*this).stage));
            (*this).stage = ptr::null_mut();
        }
        let parent = BarrierFile::parent(file);
        if !parent.opened() {
            return ffi::SQLITE_OK;
        }
        parent.close()
    }
}

unsafe extern "C" fn x_read(
    file: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amount: c_int,
    offset: i64,
) -> c_int {
    unsafe {
        let parent = BarrierFile::parent(file);
        let out = std::slice::from_raw_parts_mut(buf as *mut u8, amount as usize);
        let Some(stage) = BarrierFile::stage(file) else {
            return parent.read(out, offset as u64);
        };
        if stage.is_empty() {
            return parent.read(out, offset as u64);
        }
        if stage.read_overlay(offset as u64, out) {
            return ffi::SQLITE_OK;
        }
        let mut backing = vec![0u8; amount as usize];
        let rc = parent.read(&mut backing, offset as u64);
        if rc != ffi::SQLITE_OK && rc != ffi::SQLITE_IOERR_SHORT_READ {
            return rc;
        }
        stage.read_overlay(offset as u64, &mut backing);
        out.copy_from_slice(&backing);
        rc
    }
}

unsafe extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amount: c_int,
    offset: i64,
) -> c_int {
    unsafe {
        let data = std::slice::from_raw_parts(buf as *const u8, amount as usize);
        let Some(stage) = BarrierFile::stage(file) else {
            return BarrierFile::parent(file).write(offset as u64, data);
        };
        if stage.layout().is_none()
            && offset == 0
            && let Some(layout) = parse_wal_header(data)
        {
            stage.set_layout(layout);
        }
        if stage.accept(offset as u64, data) == StageOutcome::Buffered {
            return publish_if_complete(file);
        }
        let rc = flush_stage(file);
        if rc != ffi::SQLITE_OK {
            return rc;
        }
        let Some(stage) = BarrierFile::stage(file) else {
            return BarrierFile::parent(file).write(offset as u64, data);
        };
        stage.accept(offset as u64, data);
        publish_if_complete(file)
    }
}

unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: i64) -> c_int {
    unsafe {
        if let Some(stage) = BarrierFile::stage(file) {
            stage.discard_from(size as u64);
        }
        BarrierFile::parent(file).truncate(size)
    }
}

unsafe extern "C" fn x_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    unsafe {
        let rc = publish_stage(file);
        if rc != ffi::SQLITE_OK {
            return rc;
        }
        BarrierFile::parent(file).sync(flags)
    }
}

unsafe extern "C" fn x_file_size(file: *mut ffi::sqlite3_file, size: *mut i64) -> c_int {
    unsafe {
        let rc = BarrierFile::parent(file).file_size(size);
        if rc != ffi::SQLITE_OK {
            return rc;
        }
        if let Some(stage) = BarrierFile::stage(file)
            && !stage.is_empty()
        {
            *size = (*size).max(stage.end() as i64);
        }
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_lock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe { BarrierFile::parent(file).lock(level) }
}

unsafe extern "C" fn x_unlock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe { BarrierFile::parent(file).unlock(level) }
}

unsafe extern "C" fn x_check_reserved_lock(file: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    unsafe { BarrierFile::parent(file).check_reserved_lock(out) }
}

unsafe extern "C" fn x_file_control(
    file: *mut ffi::sqlite3_file,
    op: c_int,
    arg: *mut c_void,
) -> c_int {
    unsafe { BarrierFile::parent(file).file_control(op, arg) }
}

unsafe extern "C" fn x_sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe { BarrierFile::parent(file).sector_size() }
}

unsafe extern "C" fn x_device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe { BarrierFile::parent(file).device_characteristics() }
}

static IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 1,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_file_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_unlock),
    xCheckReservedLock: Some(x_check_reserved_lock),
    xFileControl: Some(x_file_control),
    xSectorSize: Some(x_sector_size),
    xDeviceCharacteristics: Some(x_device_characteristics),
    xShmMap: None,
    xShmLock: None,
    xShmBarrier: None,
    xShmUnmap: None,
    xFetch: None,
    xUnfetch: None,
};

unsafe extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    unsafe {
        let app = (*vfs).pAppData as *mut VfsAppData;
        let parent = (*app).parent;
        let this = file as *mut BarrierFile;
        (*this).base.pMethods = ptr::null();
        (*this).stage = ptr::null_mut();
        (*this).barrier = &(*app).barrier as *const Arc<dyn CommitBarrier> as *const c_void;
        (*this).parent_vfs = parent;

        let open = (*parent).xOpen.expect("xOpen");
        let rc = open(parent, name, BarrierFile::parent(file).0, flags, out_flags);
        if rc != ffi::SQLITE_OK {
            return rc;
        }
        if flags & ffi::SQLITE_OPEN_WAL != 0 {
            (*this).stage = Box::into_raw(Box::new(WalStage::default()));
        }
        (*this).base.pMethods = &IO_METHODS;
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_delete(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    sync_dir: c_int,
) -> c_int {
    unsafe {
        let parent = (*((*vfs).pAppData as *mut VfsAppData)).parent;
        ((*parent).xDelete.expect("xDelete"))(parent, name, sync_dir)
    }
}

unsafe extern "C" fn x_access(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    unsafe {
        let parent = (*((*vfs).pAppData as *mut VfsAppData)).parent;
        ((*parent).xAccess.expect("xAccess"))(parent, name, flags, out)
    }
}

unsafe extern "C" fn x_full_pathname(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    out_len: c_int,
    out: *mut c_char,
) -> c_int {
    unsafe {
        let parent = (*((*vfs).pAppData as *mut VfsAppData)).parent;
        ((*parent).xFullPathname.expect("xFullPathname"))(parent, name, out_len, out)
    }
}

unsafe extern "C" fn x_randomness(
    vfs: *mut ffi::sqlite3_vfs,
    len: c_int,
    out: *mut c_char,
) -> c_int {
    unsafe {
        let parent = (*((*vfs).pAppData as *mut VfsAppData)).parent;
        ((*parent).xRandomness.expect("xRandomness"))(parent, len, out)
    }
}

unsafe extern "C" fn x_sleep(vfs: *mut ffi::sqlite3_vfs, micros: c_int) -> c_int {
    unsafe {
        let parent = (*((*vfs).pAppData as *mut VfsAppData)).parent;
        ((*parent).xSleep.expect("xSleep"))(parent, micros)
    }
}

unsafe extern "C" fn x_current_time(vfs: *mut ffi::sqlite3_vfs, out: *mut f64) -> c_int {
    unsafe {
        let parent = (*((*vfs).pAppData as *mut VfsAppData)).parent;
        ((*parent).xCurrentTime.expect("xCurrentTime"))(parent, out)
    }
}

unsafe extern "C" fn x_get_last_error(
    vfs: *mut ffi::sqlite3_vfs,
    len: c_int,
    out: *mut c_char,
) -> c_int {
    unsafe {
        let parent = (*((*vfs).pAppData as *mut VfsAppData)).parent;
        ((*parent).xGetLastError.expect("xGetLastError"))(parent, len, out)
    }
}

pub(crate) fn register(
    name: &str,
    parent_name: Option<&str>,
    barrier: Arc<dyn CommitBarrier>,
) -> Result<(), crate::Error> {
    let name = CString::new(name).map_err(|_| crate::Error::InvalidName)?;
    let parent_name = parent_name
        .map(|parent| CString::new(parent).map_err(|_| crate::Error::InvalidName))
        .transpose()?;
    unsafe {
        if !ffi::sqlite3_vfs_find(name.as_ptr()).is_null() {
            return Err(crate::Error::AlreadyRegistered);
        }
        let parent = match &parent_name {
            Some(parent) => ffi::sqlite3_vfs_find(parent.as_ptr()),
            None => ffi::sqlite3_vfs_find(ptr::null()),
        };
        if parent.is_null() {
            return Err(match parent_name {
                Some(parent) => crate::Error::UnknownParent(parent.to_string_lossy().into_owned()),
                None => crate::Error::NoDefaultVfs,
            });
        }
        let app = Box::into_raw(Box::new(VfsAppData { parent, barrier }));
        let vfs = Box::into_raw(Box::new(ffi::sqlite3_vfs {
            iVersion: 1,
            szOsFile: size_of::<BarrierFile>() as c_int + (*parent).szOsFile,
            mxPathname: (*parent).mxPathname,
            pNext: ptr::null_mut(),
            zName: name.into_raw(),
            pAppData: app as *mut c_void,
            xOpen: Some(x_open),
            xDelete: Some(x_delete),
            xAccess: Some(x_access),
            xFullPathname: Some(x_full_pathname),
            xDlOpen: None,
            xDlError: None,
            xDlSym: None,
            xDlClose: None,
            xRandomness: Some(x_randomness),
            xSleep: Some(x_sleep),
            xCurrentTime: Some(x_current_time),
            xGetLastError: Some(x_get_last_error),
            xCurrentTimeInt64: None,
            xSetSystemCall: None,
            xGetSystemCall: None,
            xNextSystemCall: None,
        }));
        let rc = ffi::sqlite3_vfs_register(vfs, 0);
        if rc != ffi::SQLITE_OK {
            return Err(crate::Error::Register(rc));
        }
    }
    Ok(())
}

pub(crate) fn is_registered(name: &str) -> bool {
    let Ok(name) = CString::new(name) else {
        return false;
    };
    unsafe { !ffi::sqlite3_vfs_find(name.as_ptr()).is_null() }
}

pub(crate) fn describe_error(code: c_int) -> String {
    unsafe {
        let text = ffi::sqlite3_errstr(code);
        if text.is_null() {
            return format!("sqlite error {code}");
        }
        CStr::from_ptr(text).to_string_lossy().into_owned()
    }
}
