#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
temporary=$(mktemp -d)
trap 'rm -rf -- "$temporary"' EXIT

new_git_repo() {
    local directory=$1
    mkdir -p "$directory/scripts" "$directory/kuberic-core"
    cp "$repo_root/scripts/check_level_triggered_scope.sh" "$directory/scripts/"
    (
        cd "$directory"
        git init -q -b main
        git config user.email test@example.invalid
        git config user.name "Guard Test"
        printf 'baseline\n' > kuberic-core/file.rs
        git add kuberic-core/file.rs
        git commit -q -m baseline
    )
}

expect_scope_failure() {
    local directory=$1
    local base=$2
    if (cd "$directory" && scripts/check_level_triggered_scope.sh "$base"); then
        echo "Scope guard accepted a protected v1 change in $directory." >&2
        exit 1
    fi
}

committed="$temporary/committed"
new_git_repo "$committed"
(
    cd "$committed"
    base=$(git rev-parse HEAD)
    git switch -q -c feature
    mkdir new-stack
    git mv kuberic-core/file.rs new-stack/file.rs
    git commit -q -m rename
    printf '%s\n' "$base" > "$temporary/committed-base"
)
expect_scope_failure "$committed" "$(cat "$temporary/committed-base")"

staged="$temporary/staged"
new_git_repo "$staged"
(
    cd "$staged"
    mkdir new-stack
    git mv kuberic-core/file.rs "new-stack/quoted file.rs"
)
expect_scope_failure "$staged" HEAD

unstaged="$temporary/unstaged"
new_git_repo "$unstaged"
printf 'changed\n' >> "$unstaged/kuberic-core/file.rs"
expect_scope_failure "$unstaged" HEAD

untracked="$temporary/untracked"
new_git_repo "$untracked"
printf 'new\n' > "$untracked/kuberic-core/untracked file.rs"
expect_scope_failure "$untracked" HEAD

# SQLite is the same package, now migrated in place rather than protected v1.
sqlite_scope="$temporary/sqlite-scope"
new_git_repo "$sqlite_scope"
mkdir -p "$sqlite_scope/examples/sqlite/src"
printf 'pub fn sqlite() {}\n' > "$sqlite_scope/examples/sqlite/src/lib.rs"
(cd "$sqlite_scope" && scripts/check_level_triggered_scope.sh HEAD)
(cd "$sqlite_scope" && git add examples/sqlite/src/lib.rs && scripts/check_level_triggered_scope.sh HEAD)

new_cargo_repo() {
    local directory=$1
    mkdir -p "$directory/scripts" "$directory/kuberic-protocol/src" \
        "$directory/kuberic-core/src"
    cp "$repo_root/scripts/check_level_triggered_dependencies.sh" "$directory/scripts/"
    cat > "$directory/Cargo.toml" <<'EOF'
[workspace]
resolver = "2"
members = ["kuberic-protocol"]
EOF
    cat > "$directory/kuberic-protocol/Cargo.toml" <<'EOF'
[package]
name = "kuberic-protocol"
version = "0.1.0"
edition = "2024"
EOF
    printf 'pub fn protected() {}\n' > "$directory/kuberic-core/src/types.rs"
}

expect_dependency_failure() {
    local directory=$1
    if (cd "$directory" && scripts/check_level_triggered_dependencies.sh); then
        echo "Dependency guard accepted a protected v1 import in $directory." >&2
        exit 1
    fi
}

direct="$temporary/direct"
new_cargo_repo "$direct"
cat >> "$direct/Cargo.toml" <<'EOF'
exclude = ["kuberic-core"]
EOF
cat >> "$direct/kuberic-protocol/Cargo.toml" <<'EOF'
[dependencies]
kuberic-core = { path = "../kuberic-core" }
EOF
cat > "$direct/kuberic-core/Cargo.toml" <<'EOF'
[package]
name = "kuberic-core"
version = "0.1.0"
edition = "2024"
EOF
printf 'pub fn protocol() {}\n' > "$direct/kuberic-protocol/src/lib.rs"
expect_dependency_failure "$direct"

path_attribute="$temporary/path-attribute"
new_cargo_repo "$path_attribute"
cat > "$path_attribute/kuberic-protocol/src/lib.rs" <<'EOF'
#[path = "../../kuberic-core/src/types.rs"]
mod protected;
EOF
expect_dependency_failure "$path_attribute"

multiline_include="$temporary/multiline-include"
new_cargo_repo "$multiline_include"
cat > "$multiline_include/kuberic-protocol/src/lib.rs" <<'EOF'
const PROTECTED: &str = include_str!(
    "../../kuberic-core/src/types.rs"
);
EOF
expect_dependency_failure "$multiline_include"

nested_include="$temporary/nested-include"
new_cargo_repo "$nested_include"
cat > "$nested_include/kuberic-protocol/src/lib.rs" <<'EOF'
const PROTECTED: &str = include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../kuberic-core/src/types.rs"
));
EOF
expect_dependency_failure "$nested_include"

cfg_attr_path="$temporary/cfg-attr-path"
new_cargo_repo "$cfg_attr_path"
cat > "$cfg_attr_path/kuberic-protocol/src/lib.rs" <<'EOF'
#[cfg_attr(all(), path = "../../kuberic-core/src/types.rs")]
mod protected;
EOF
expect_dependency_failure "$cfg_attr_path"

symlink_import="$temporary/symlink-import"
new_cargo_repo "$symlink_import"
printf 'pub fn protocol() {}\n' > "$symlink_import/kuberic-protocol/src/lib.rs"
ln -s ../../kuberic-core/src/types.rs \
    "$symlink_import/kuberic-protocol/src/protected.rs"
expect_dependency_failure "$symlink_import"

new_sqlite_repo() {
    local directory=$1
    new_cargo_repo "$directory"
    mkdir -p "$directory/examples"
    mv "$directory/kuberic-protocol" "$directory/examples/sqlite"
    sed -i 's/"kuberic-protocol"/"examples\/sqlite"/' "$directory/Cargo.toml"
    sed -i 's/name = "kuberic-protocol"/name = "sqlite-replicated"/' \
        "$directory/examples/sqlite/Cargo.toml"
    printf 'pub fn sqlite() {}\n' > "$directory/examples/sqlite/src/lib.rs"
}

sqlite_dependency="$temporary/sqlite-dependency"
new_sqlite_repo "$sqlite_dependency"
(cd "$sqlite_dependency" && scripts/check_level_triggered_dependencies.sh)
cat >> "$sqlite_dependency/Cargo.toml" <<'EOF'
exclude = ["kuberic-core"]
EOF
cat > "$sqlite_dependency/kuberic-core/Cargo.toml" <<'EOF'
[package]
name = "kuberic-core"
version = "0.1.0"
edition = "2024"
EOF
printf 'pub fn classic() {}\n' > "$sqlite_dependency/kuberic-core/src/lib.rs"
cat >> "$sqlite_dependency/examples/sqlite/Cargo.toml" <<'EOF'
[dependencies]
kuberic-core = { path = "../../kuberic-core" }
EOF
expect_dependency_failure "$sqlite_dependency"

# Reject both classic runtime and operator dependencies on the migrated package.
mv "$sqlite_dependency/kuberic-core" "$sqlite_dependency/kuberic-operator"
sed -i 's/kuberic-core/kuberic-operator/g' \
    "$sqlite_dependency/Cargo.toml" \
    "$sqlite_dependency/examples/sqlite/Cargo.toml" \
    "$sqlite_dependency/kuberic-operator/Cargo.toml"
expect_dependency_failure "$sqlite_dependency"

sqlite_source="$temporary/sqlite-source"
new_sqlite_repo "$sqlite_source"
cat > "$sqlite_source/examples/sqlite/src/lib.rs" <<'EOF'
include!("../../../kuberic-core/src/types.rs");
EOF
expect_dependency_failure "$sqlite_source"

sqlite_symlink="$temporary/sqlite-symlink"
new_sqlite_repo "$sqlite_symlink"
ln -s ../../../kuberic-core/src/types.rs "$sqlite_symlink/examples/sqlite/src/classic.rs"
expect_dependency_failure "$sqlite_symlink"

echo "Level-triggered guard regression tests passed."
