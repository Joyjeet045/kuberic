use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::ffi::{CString, c_char, c_int, c_void};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

use libsqlite3_sys as ffi;

#[repr(C)]
struct FailFile {
    base: ffi::sqlite3_file,
    real: *mut ffi::sqlite3_file,
    real_size: usize,
}

/// Set while a test wants writes to fail, so the shared VFS stays usable.
pub static WRITES_FAIL: AtomicUsize = AtomicUsize::new(0);

fn real_layout(size: usize) -> Layout {
    Layout::from_size_align(size, 16).expect("layout")
}

unsafe fn real(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    unsafe { (*(file as *mut FailFile)).real }
}

unsafe fn methods(file: *mut ffi::sqlite3_file) -> &'static ffi::sqlite3_io_methods {
    unsafe { &*(*real(file)).pMethods }
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let this = file as *mut FailFile;
        let target = (*this).real;
        let rc = if (*target).pMethods.is_null() {
            ffi::SQLITE_OK
        } else {
            (methods(file).xClose.expect("xClose"))(target)
        };
        dealloc(target as *mut u8, real_layout((*this).real_size));
        (*this).real = ptr::null_mut();
        rc
    }
}

unsafe extern "C" fn x_read(
    file: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amount: c_int,
    offset: i64,
) -> c_int {
    unsafe { (methods(file).xRead.expect("xRead"))(real(file), buf, amount, offset) }
}

unsafe extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amount: c_int,
    offset: i64,
) -> c_int {
    if WRITES_FAIL.load(Ordering::SeqCst) != 0 {
        return ffi::SQLITE_IOERR_WRITE;
    }
    unsafe { (methods(file).xWrite.expect("xWrite"))(real(file), buf, amount, offset) }
}

unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: i64) -> c_int {
    unsafe { (methods(file).xTruncate.expect("xTruncate"))(real(file), size) }
}

unsafe extern "C" fn x_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    unsafe { (methods(file).xSync.expect("xSync"))(real(file), flags) }
}

unsafe extern "C" fn x_file_size(file: *mut ffi::sqlite3_file, out: *mut i64) -> c_int {
    unsafe { (methods(file).xFileSize.expect("xFileSize"))(real(file), out) }
}

unsafe extern "C" fn x_lock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe { (methods(file).xLock.expect("xLock"))(real(file), level) }
}

unsafe extern "C" fn x_unlock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe { (methods(file).xUnlock.expect("xUnlock"))(real(file), level) }
}

unsafe extern "C" fn x_check_reserved_lock(file: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    unsafe {
        (methods(file)
            .xCheckReservedLock
            .expect("xCheckReservedLock"))(real(file), out)
    }
}

unsafe extern "C" fn x_file_control(
    file: *mut ffi::sqlite3_file,
    op: c_int,
    arg: *mut c_void,
) -> c_int {
    unsafe { (methods(file).xFileControl.expect("xFileControl"))(real(file), op, arg) }
}

unsafe extern "C" fn x_sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe { (methods(file).xSectorSize.expect("xSectorSize"))(real(file)) }
}

unsafe extern "C" fn x_device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        (methods(file)
            .xDeviceCharacteristics
            .expect("xDeviceCharacteristics"))(real(file))
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

unsafe fn parent_of(vfs: *mut ffi::sqlite3_vfs) -> *mut ffi::sqlite3_vfs {
    unsafe { (*vfs).pAppData as *mut ffi::sqlite3_vfs }
}

unsafe extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    unsafe {
        let parent = parent_of(vfs);
        let size = (*parent).szOsFile as usize;
        let target = alloc_zeroed(real_layout(size)) as *mut ffi::sqlite3_file;
        let this = file as *mut FailFile;
        (*this).base.pMethods = ptr::null();
        (*this).real = target;
        (*this).real_size = size;
        let rc = ((*parent).xOpen.expect("xOpen"))(parent, name, target, flags, out_flags);
        if rc != ffi::SQLITE_OK {
            return rc;
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
        let parent = parent_of(vfs);
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
        let parent = parent_of(vfs);
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
        let parent = parent_of(vfs);
        ((*parent).xFullPathname.expect("xFullPathname"))(parent, name, out_len, out)
    }
}

unsafe extern "C" fn x_randomness(
    vfs: *mut ffi::sqlite3_vfs,
    len: c_int,
    out: *mut c_char,
) -> c_int {
    unsafe {
        let parent = parent_of(vfs);
        ((*parent).xRandomness.expect("xRandomness"))(parent, len, out)
    }
}

unsafe extern "C" fn x_sleep(vfs: *mut ffi::sqlite3_vfs, micros: c_int) -> c_int {
    unsafe {
        let parent = parent_of(vfs);
        ((*parent).xSleep.expect("xSleep"))(parent, micros)
    }
}

unsafe extern "C" fn x_current_time(vfs: *mut ffi::sqlite3_vfs, out: *mut f64) -> c_int {
    unsafe {
        let parent = parent_of(vfs);
        ((*parent).xCurrentTime.expect("xCurrentTime"))(parent, out)
    }
}

unsafe extern "C" fn x_get_last_error(
    vfs: *mut ffi::sqlite3_vfs,
    len: c_int,
    out: *mut c_char,
) -> c_int {
    unsafe {
        let parent = parent_of(vfs);
        ((*parent).xGetLastError.expect("xGetLastError"))(parent, len, out)
    }
}

/// Registers a pass-through VFS whose writes fail while [`WRITES_FAIL`] is set.
pub fn register(name: &str) {
    let name = CString::new(name).expect("vfs name");
    unsafe {
        if !ffi::sqlite3_vfs_find(name.as_ptr()).is_null() {
            return;
        }
        let parent = ffi::sqlite3_vfs_find(ptr::null());
        assert!(!parent.is_null(), "no default vfs");
        let vfs = Box::into_raw(Box::new(ffi::sqlite3_vfs {
            iVersion: 1,
            szOsFile: size_of::<FailFile>() as c_int,
            mxPathname: (*parent).mxPathname,
            pNext: ptr::null_mut(),
            zName: name.into_raw(),
            pAppData: parent as *mut c_void,
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
        assert_eq!(
            ffi::sqlite3_vfs_register(vfs, 0),
            ffi::SQLITE_OK,
            "failing vfs registration"
        );
    }
}
