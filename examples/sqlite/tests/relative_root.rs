//! This target intentionally has one test: cwd is process-global. Keeping the
//! test here isolates it from the library's parallel tests and other targets,
//! without launching any child processes from the test.

use std::path::{Path, PathBuf};

use sqlite_replicated::{RecoveryState, SqlitePersistence};

struct RestoreWorkingDirectory(PathBuf);

impl Drop for RestoreWorkingDirectory {
    fn drop(&mut self) {
        std::env::set_current_dir(&self.0).expect("restore caller working directory");
    }
}

#[test]
fn single_component_relative_root_can_be_created_and_reopened() {
    let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/sqlite-relative-root");
    std::fs::create_dir_all(&scratch).unwrap();
    let directory = tempfile::tempdir_in(scratch).unwrap();
    let _restore = RestoreWorkingDirectory(std::env::current_dir().unwrap());
    std::env::set_current_dir(directory.path()).unwrap();

    let state = SqlitePersistence::open(PathBuf::from("data")).unwrap();
    assert_eq!(state.recovery_state().unwrap(), RecoveryState::Healthy);
    let snapshot = state.snapshot(0).unwrap();
    assert!(Path::new("data/state-v2.json").is_file());
    drop(state);

    let reopened = SqlitePersistence::open(PathBuf::from("data")).unwrap();
    assert_eq!(reopened.recovery_state().unwrap(), RecoveryState::Healthy);
    assert_eq!(reopened.snapshot(0).unwrap(), snapshot);
    assert_eq!(
        std::fs::metadata(reopened.materialize_committed().unwrap())
            .unwrap()
            .len(),
        0
    );
}
