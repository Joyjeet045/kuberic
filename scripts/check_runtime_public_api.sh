#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

doc_root="target/doc/kuberic_runtime"
allowlist="scripts/runtime_public_api.allowlist"
fixture="scripts/fixtures/runtime-api-leak"
scratch="target/runtime-api-check-$$"
mkdir -p target
mkdir "$scratch"
inventory="$scratch/inventory"
fixture_output="$scratch/fixture-output"
trap 'rm -f -- "$fixture/Cargo.lock"; rm -rf -- "$scratch" "$fixture/target"' EXIT

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

if ! grep -q 'method `prepare_attachment` is private' "$fixture_output" ||
    ! grep -q 'cannot construct `ReplicatorInterfaces` with struct literal syntax due to private fields' "$fixture_output" ||
    ! grep -q 'cannot construct `kuberic_runtime::replicator::ReplicatorAttachment` with struct literal syntax due to private fields' "$fixture_output" ||
    ! grep -q 'RuntimeHostToken: Default' "$fixture_output" ||
    ! grep -q 'method `disarm` is private' "$fixture_output" ||
    ! grep -q 'field `default_dependencies` of struct `ReplicatorFactoryContext` is private' "$fixture_output" ||
    ! grep -q 'module `authority` is private' "$fixture_output"; then
    cat "$fixture_output" >&2
    echo "The external fixture failed for an unexpected reason." >&2
    exit 1
fi

cargo test -p kuberic-runtime --test public_api_inventory --quiet

echo "kuberic-runtime documented and source-public APIs match their allowlists; tested private-capability extraction, forging, attachment, guard, authority, and host-construction paths are unreachable from safe application code."
