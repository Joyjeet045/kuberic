use std::path::PathBuf;

use serde_json::{Value, json};
use sqlserver_replicated::runtime_config::ObserverConfig;

pub struct Fixture {
    directory: tempfile::TempDir,
    pub document: Value,
}

impl Fixture {
    pub fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let username = directory.path().join("username");
        let password = directory.path().join("password");
        std::fs::write(&username, "unit-test-observer").unwrap();
        std::fs::write(&password, "unit-test-only-not-a-real-credential").unwrap();
        Self {
            document: json!({
                "host": "localhost",
                "availability_group": "test-ag",
                "expected_server_name": "sql-0",
                "replica_id": "replica-0",
                "incarnation": "pod-uid-0",
                "observer_username_file": username,
                "observer_password_file": password
            }),
            directory,
        }
    }

    pub fn config(&self) -> ObserverConfig {
        ObserverConfig::from_json(&serde_json::to_vec(&self.document).unwrap()).unwrap()
    }

    pub fn config_path(&self) -> PathBuf {
        let path = self.directory.path().join("observer.json");
        std::fs::write(&path, serde_json::to_vec(&self.document).unwrap()).unwrap();
        path
    }
}
