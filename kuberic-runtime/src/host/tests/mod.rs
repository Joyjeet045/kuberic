use std::path::PathBuf;

pub(crate) fn tempdir() -> std::io::Result<tempfile::TempDir> {
    let target = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target");
    std::fs::create_dir_all(&target)?;
    tempfile::tempdir_in(target)
}

#[cfg(kuberic_workspace_tests)]
mod coordinator;
#[cfg(all(feature = "testing", kuberic_workspace_tests))]
mod crash_boundaries;
mod recovery;
#[cfg(all(feature = "testing", kuberic_workspace_tests))]
mod runtime;
#[cfg(all(feature = "testing", kuberic_workspace_tests))]
mod service;
mod store;
mod transport;
