use std::fs;
use std::path::Path;
use std::process::{Command, Output};

#[test]
#[ignore = "build-only: requires Cargo, cached dependencies and workspace sources; run scripts/check_runtime_public_api.sh"]
fn external_applications_can_use_contracts_but_cannot_forge_host_capabilities() {
    let runtime = Path::new(env!("CARGO_MANIFEST_DIR"));
    let target = runtime.parent().unwrap().join("target");
    fs::create_dir_all(&target).unwrap();
    let fixture = tempfile::tempdir_in(&target).unwrap();
    fs::create_dir(fixture.path().join("scratch")).unwrap();
    fs::write(
        fixture.path().join("Cargo.toml"),
        format!(
            r#"[package]
name = "runtime-public-api-fixture"
version = "0.0.0"
edition = "2024"

[workspace]

[dependencies]
kuberic-runtime = {{ path = {:?}, features = ["testing"] }}

[[bin]]
name = "public"
path = "public.rs"

[[bin]]
name = "private"
path = "private.rs"
"#,
            runtime,
        ),
    )
    .unwrap();
    fs::copy(
        runtime.parent().unwrap().join("Cargo.lock"),
        fixture.path().join("Cargo.lock"),
    )
    .unwrap();
    fs::write(
        fixture.path().join("public.rs"),
        include_str!("fixtures/public_api.rs"),
    )
    .unwrap();
    fs::write(
        fixture.path().join("private.rs"),
        include_str!("fixtures/private_api.rs"),
    )
    .unwrap();

    let check = |binary: &str| -> Output {
        Command::new(env!("CARGO"))
            .args(["check", "--offline", "--quiet", "--bin", binary])
            .arg("--manifest-path")
            .arg(fixture.path().join("Cargo.toml"))
            .arg("--target-dir")
            .arg(&target)
            .env("TMPDIR", fixture.path().join("scratch"))
            .output()
            .expect("check external application")
    };
    let supported = check("public");
    assert!(
        supported.status.success(),
        "supported runtime API failed: {}",
        String::from_utf8_lossy(&supported.stderr),
    );

    let forbidden = check("private");
    let diagnostics = String::from_utf8_lossy(&forbidden.stderr);
    assert!(
        !forbidden.status.success(),
        "host capability fixture compiled"
    );
    for expected in [
        "module `authority` is private",
        "module `capabilities` is private",
        "module `effects` is private",
        "module `receipts` is private",
        "module `runtime` is private",
        "module `transport` is private",
        "module `copy` is private",
        "module `quorum` is private",
        "module `sender` is private",
        "struct `ReplicatorAttachment` is private",
        "struct `ReplicatorCreationReservation` is private",
        "struct `DefaultReplicatorDependencies` is private",
        "trait `ManagedReplicatorLifecycle` is private",
        "trait `ManagedReplicatorDataPlane` is private",
        "trait `ReplicatorRegistration` is private",
        "trait `PartitionAccessView` is private",
        "associated function `new` is private",
        "field `default_dependencies` of struct `ReplicatorFactoryContext` is private",
        "cannot construct `ReplicatorInterfaces` with struct literal syntax due to private fields",
        "module `command` is private",
        "module `coordinator` is private",
        "module `hosting` is private",
        "module `provisioning` is private",
        "no `recovery` in `host`",
        "module `report` is private",
        "module `runtime_adapter` is private",
        "module `service` is private",
        "module `session` is private",
        "module `sqlite_store` is private",
        "module `state` is private",
        "module `store` is private",
        "module `testing` is private",
        "field `inner` of struct `PodRuntime` is private",
        "field `inner` of struct `SqliteStore` is private",
    ] {
        assert!(
            diagnostics.contains(expected),
            "missing {expected:?}:\n{diagnostics}"
        );
    }
}
