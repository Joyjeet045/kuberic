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

for expected in \
    'struct `ReplicatorAttachment` is private' \
    'struct `ReplicatorCreationReservation` is private' \
    'trait `ReplicatorRegistration` is private' \
    'module `authority` is private'; do
    if ! grep -Fq "$expected" "$fixture_output"; then
        cat "$fixture_output" >&2
        echo "The external fixture failed for an unexpected reason: $expected" >&2
        exit 1
    fi
done

cargo test -p kuberic-runtime --all-features \
    --test public_api_inventory --quiet
cargo test -p kuberic-runtime --all-features \
    --test public_api_privacy --quiet -- --ignored

echo "kuberic-runtime documented and source-public APIs match their allowlists; tested private-capability extraction, forging, attachment, guard, authority, and host-construction paths are unreachable from safe application code."
