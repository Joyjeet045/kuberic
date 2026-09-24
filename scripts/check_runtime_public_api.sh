#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

doc_root="target/doc/kuberic_runtime"
allowlist="scripts/runtime_public_api.allowlist"
fixture="scripts/fixtures/runtime-api-leak"
inventory=$(mktemp)
fixture_output=$(mktemp)
trap 'rm -f -- "$inventory" "$fixture_output" "$fixture/Cargo.lock"; rm -rf -- "$fixture/target"' EXIT

rm -rf -- "$doc_root"
cargo doc -p kuberic-runtime --no-deps --quiet

find "$doc_root" -type f -name '*.html' -printf 'page:%P\n' |
    sort -u > "$inventory"

if ! diff -u "$allowlist" "$inventory"; then
    echo "kuberic-runtime documented public API differs from the reviewed allowlist." >&2
    exit 1
fi

if cargo check --manifest-path "$fixture/Cargo.toml" --quiet >"$fixture_output" 2>&1; then
    echo "The external application fixture unexpectedly reached agent-owned runtime APIs." >&2
    exit 1
fi

if ! grep -q 'managed_replicator' "$fixture_output" ||
    ! grep -q 'no associated function or constant named `new`' "$fixture_output" ||
    ! grep -q 'RuntimeHostToken: Default' "$fixture_output" ||
    ! grep -q 'method `register_managed` is private' "$fixture_output" ||
    ! grep -q 'field `default_dependencies` of struct `ReplicatorFactoryContext` is private' "$fixture_output" ||
    ! grep -q 'module `authority` is private' "$fixture_output"; then
    cat "$fixture_output" >&2
    echo "The external fixture failed for an unexpected reason." >&2
    exit 1
fi

cargo test -p kuberic-runtime --test public_api_inventory --quiet

echo "kuberic-runtime documented and source-public APIs match their allowlists; tested managed, authority, and host-construction paths are unreachable from safe application code."
