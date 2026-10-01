use std::io::{self, Read};
use std::path::PathBuf;
use std::process::{Command, Stdio};

use rustix::process::{Pid, WaitOptions, getpid, set_child_subreaper, wait};

pub(crate) const ARGUMENT: &str = "--internal-postgres-supervisor";
pub(crate) const CLEAR_PGDATA_ARGUMENT: &str = "--internal-postgres-clear-pgdata";
pub(crate) const START: u8 = 1;
pub(crate) const STOP: u8 = 2;

pub(crate) fn executable() -> io::Result<PathBuf> {
    let executable = std::env::current_exe()?;
    let mut directory = executable
        .parent()
        .ok_or_else(|| io::Error::other("missing executable directory"))?;
    // Cargo integration tests use the package binary beside deps/.
    if cfg!(feature = "testing") && directory.file_name().is_some_and(|name| name == "deps") {
        directory = directory.parent().unwrap();
    }
    Ok(directory.join("postgres-replicated"))
}

fn clear_pgdata(mut arguments: impl Iterator<Item = std::ffi::OsString>) -> io::Result<()> {
    let data = PathBuf::from(
        arguments
            .next()
            .ok_or_else(|| io::Error::other("missing PGDATA deletion path"))?,
    );
    #[cfg(feature = "testing")]
    let gate = match (arguments.next(), arguments.next()) {
        (None, None) => None,
        (Some(entry), Some(marker)) => Some((entry, PathBuf::from(marker))),
        _ => return Err(io::Error::other("incomplete PGDATA deletion gate")),
    };
    if arguments.next().is_some() {
        return Err(io::Error::other("unexpected PGDATA deletion argument"));
    }
    // This re-exec is owned by the same generation registry as backup/rewind.
    // Its owner must reap it before releasing build authority, including Drop.
    std::fs::create_dir_all(&data)?;
    for entry in std::fs::read_dir(&data)? {
        let entry = entry?;
        let directory = entry.file_type()?.is_dir();
        #[cfg(feature = "testing")]
        if let Some((name, marker)) = &gate
            && entry.file_name() == *name
        {
            std::fs::write(marker, std::process::id().to_string())?;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            while marker.try_exists()? {
                if std::time::Instant::now() >= deadline {
                    return Err(io::Error::other("PGDATA deletion gate timed out"));
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        if directory {
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    std::fs::File::open(data)?.sync_all()
}

/// Internal re-exec entry point; run before creating threads or a Tokio runtime.
#[doc(hidden)]
pub fn run_if_requested() -> io::Result<bool> {
    let mut arguments = std::env::args_os().skip(1);
    match arguments.next() {
        Some(argument) if argument == CLEAR_PGDATA_ARGUMENT => {
            clear_pgdata(arguments)?;
            return Ok(true);
        }
        Some(argument) if argument == ARGUMENT => {}
        _ => return Ok(false),
    }
    let postgres = arguments
        .next()
        .ok_or_else(|| io::Error::other("missing supervised executable"))?;
    // These handlers belong only to the re-exec, never the application host.
    // Caught handlers reset on exec, preserving PostgreSQL's signal semantics.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let _entered = runtime.enter();
    let _interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let _terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    set_child_subreaper(Some(getpid()))?;
    // No launch occurs before the owner retains our pidfd. STOP completes an
    // undispatched run normally, rather than killing its ownership root.
    let mut message = [0];
    io::stdin().read_exact(&mut message)?;
    let mut launcher = match message[0] {
        START => {
            let child = Command::new(postgres)
                .args(arguments)
                .stdin(Stdio::null())
                .spawn()?;
            Some(Pid::from_raw(child.id() as i32).expect("newly spawned launcher"))
        }
        STOP => None,
        _ => return Err(io::Error::other("invalid supervisor start command")),
    };
    let mut status = launcher.is_none().then_some(0i32);
    // This isolated process owns exactly one launch lineage, including orphans
    // from double forks and setsid. Only ECHILD proves successful completion.
    loop {
        match wait(WaitOptions::empty()) {
            Ok(Some((pid, waited))) => {
                if launcher == Some(pid) && (waited.exited() || waited.signaled()) {
                    status = Some(waited.as_raw());
                    launcher = None;
                }
            }
            Ok(None) | Err(rustix::io::Errno::INTR) => {}
            Err(rustix::io::Errno::CHILD) => {
                // The socket separates the launcher's status from stdout/stderr
                // and from our own exit, which certifies complete tree reaping.
                let bytes = status
                    .ok_or_else(|| io::Error::other("supervisor lost launcher exit status"))?
                    .to_ne_bytes();
                if rustix::io::write(io::stdin(), &bytes)? != bytes.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "supervisor completion was not written",
                    ));
                }
                return Ok(true);
            }
            Err(error) => return Err(error.into()),
        }
    }
}
