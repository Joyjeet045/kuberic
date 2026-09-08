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

impl BarrierFile {
    unsafe fn real(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
        unsafe { (file as *mut u8).add(size_of::<BarrierFile>()) as *mut ffi::sqlite3_file }
    }

    unsafe fn methods(file: *mut ffi::sqlite3_file) -> *const ffi::sqlite3_io_methods {
        unsafe { (*BarrierFile::real(file)).pMethods }
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

unsafe fn real_write(file: *mut ffi::sqlite3_file, offset: i64, data: &[u8]) -> c_int {
    unsafe {
        let real = BarrierFile::real(file);
        let write = (*BarrierFile::methods(file)).xWrite.expect("xWrite");
        write(
            real,
            data.as_ptr() as *const c_void,
            data.len() as c_int,
            offset,
        )
    }
}

unsafe fn flush_stage(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let Some(stage) = BarrierFile::stage(file) else {
            return ffi::SQLITE_OK;
        };
        if stage.is_empty() {
            return ffi::SQLITE_OK;
        }
        let (start, bytes) = stage.take();
        real_write(file, start as i64, &bytes)
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
            return flush_stage(file);
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
                let (start, bytes) = stage.take();
                real_write(file, start as i64, &bytes)
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
        let real = BarrierFile::real(file);
        if (*real).pMethods.is_null() {
            return ffi::SQLITE_OK;
        }
        let close = (*(*real).pMethods).xClose.expect("xClose");
        close(real)
    }
}

unsafe extern "C" fn x_read(
    file: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amount: c_int,
    offset: i64,
) -> c_int {
    unsafe {
        let real = BarrierFile::real(file);
        let read = (*BarrierFile::methods(file)).xRead.expect("xRead");
        let Some(stage) = BarrierFile::stage(file) else {
            return read(real, buf, amount, offset);
        };
        if stage.is_empty() {
            return read(real, buf, amount, offset);
        }
        let out = std::slice::from_raw_parts_mut(buf as *mut u8, amount as usize);
        let fully_staged = stage.read_overlay(offset as u64, out);
        if fully_staged {
            return ffi::SQLITE_OK;
        }
        let mut backing = vec![0u8; amount as usize];
        let rc = read(real, backing.as_mut_ptr() as *mut c_void, amount, offset);
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
            return real_write(file, offset, data);
        };
        if stage.layout().is_none()
            && offset == 0
            && let Some(layout) = parse_wal_header(data)
        {
            stage.set_layout(layout);
        }
        if stage.accept(offset as u64, data) == StageOutcome::Buffered {
            return ffi::SQLITE_OK;
        }
        let rc = flush_stage(file);
        if rc != ffi::SQLITE_OK {
            return rc;
        }
        let Some(stage) = BarrierFile::stage(file) else {
            return real_write(file, offset, data);
        };
        stage.accept(offset as u64, data);
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: i64) -> c_int {
    unsafe {
        if let Some(stage) = BarrierFile::stage(file) {
            stage.discard_from(size as u64);
        }
        let real = BarrierFile::real(file);
        let truncate = (*BarrierFile::methods(file)).xTruncate.expect("xTruncate");
        truncate(real, size)
    }
}

unsafe extern "C" fn x_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    unsafe {
        let rc = publish_stage(file);
        if rc != ffi::SQLITE_OK {
            return rc;
        }
        let real = BarrierFile::real(file);
        let sync = (*BarrierFile::methods(file)).xSync.expect("xSync");
        sync(real, flags)
    }
}

unsafe extern "C" fn x_file_size(file: *mut ffi::sqlite3_file, size: *mut i64) -> c_int {
    unsafe {
        let real = BarrierFile::real(file);
        let file_size = (*BarrierFile::methods(file)).xFileSize.expect("xFileSize");
        let rc = file_size(real, size);
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
    unsafe {
        let real = BarrierFile::real(file);
        ((*BarrierFile::methods(file)).xLock.expect("xLock"))(real, level)
    }
}

unsafe extern "C" fn x_unlock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe {
        let real = BarrierFile::real(file);
        ((*BarrierFile::methods(file)).xUnlock.expect("xUnlock"))(real, level)
    }
}

unsafe extern "C" fn x_check_reserved_lock(file: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    unsafe {
        let real = BarrierFile::real(file);
        ((*BarrierFile::methods(file))
            .xCheckReservedLock
            .expect("xCheckReservedLock"))(real, out)
    }
}

unsafe extern "C" fn x_file_control(
    file: *mut ffi::sqlite3_file,
    op: c_int,
    arg: *mut c_void,
) -> c_int {
    unsafe {
        let real = BarrierFile::real(file);
        ((*BarrierFile::methods(file))
            .xFileControl
            .expect("xFileControl"))(real, op, arg)
    }
}

unsafe extern "C" fn x_sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let real = BarrierFile::real(file);
        ((*BarrierFile::methods(file))
            .xSectorSize
            .expect("xSectorSize"))(real)
    }
}

unsafe extern "C" fn x_device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let real = BarrierFile::real(file);
        ((*BarrierFile::methods(file))
            .xDeviceCharacteristics
            .expect("xDeviceCharacteristics"))(real)
    }
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

        let real = BarrierFile::real(file);
        let open = (*parent).xOpen.expect("xOpen");
        let rc = open(parent, name, real, flags, out_flags);
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

pub(crate) fn register(name: &str, barrier: Arc<dyn CommitBarrier>) -> Result<(), crate::Error> {
    let name = CString::new(name).map_err(|_| crate::Error::InvalidName)?;
    unsafe {
        if !ffi::sqlite3_vfs_find(name.as_ptr()).is_null() {
            return Err(crate::Error::AlreadyRegistered);
        }
        let parent = ffi::sqlite3_vfs_find(ptr::null());
        if parent.is_null() {
            return Err(crate::Error::NoDefaultVfs);
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
