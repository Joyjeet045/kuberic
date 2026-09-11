fn library_dependency_lines(manifest: &str) -> Vec<&str> {
    let mut library_dependencies = false;
    manifest
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                library_dependencies = trimmed == "[dependencies]"
                    || (trimmed.starts_with("[target.") && trimmed.ends_with(".dependencies]"));
                return false;
            }
            library_dependencies
        })
        .collect()
}

fn manifest_is_runtime_neutral(manifest: &str) -> bool {
    let dependencies = library_dependency_lines(manifest);
    if dependencies.is_empty() {
        return false;
    }

    const ASYNC_RUNTIMES: [&str; 4] = ["tokio", "async-std", "smol", "glommio"];
    !dependencies.into_iter().any(|line| {
        let line = line.split('#').next().unwrap_or_default();
        let Some((name, declaration)) = line.split_once('=') else {
            return false;
        };
        let name = name
            .trim()
            .trim_matches('"')
            .strip_suffix(".workspace")
            .unwrap_or_else(|| name.trim().trim_matches('"'));
        ASYNC_RUNTIMES.contains(&name)
            || ASYNC_RUNTIMES
                .iter()
                .any(|runtime| declaration.contains(&format!("package = \"{runtime}\"")))
    })
}

#[test]
fn library_dependencies_are_runtime_neutral() {
    assert!(manifest_is_runtime_neutral(include_str!("../Cargo.toml")));

    let test_only_runtime = r#"
[dependencies]
serde.workspace = true

[dev-dependencies]
tokio.workspace = true

[features]
runtime-test = ["tokio/test-util"]
"#;
    assert!(manifest_is_runtime_neutral(test_only_runtime));

    let library_runtime = r#"
[dependencies]
serde.workspace = true
tokio = { workspace = true, features = ["rt"] }

[dev-dependencies]
pretty_assertions = "1"
"#;
    assert!(!manifest_is_runtime_neutral(library_runtime));

    let target_runtime = r#"
[dependencies]
serde.workspace = true

[target.'cfg(unix)'.dependencies]
tokio.workspace = true
"#;
    assert!(!manifest_is_runtime_neutral(target_runtime));

    let renamed_runtime = r#"
[dependencies]
runtime = { package = "tokio", version = "1" }
"#;
    assert!(!manifest_is_runtime_neutral(renamed_runtime));
    assert!(!manifest_is_runtime_neutral(
        "[dev-dependencies]\ntokio.workspace = true\n"
    ));
}
