#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
manifest="${KUBERIC_EXTERNAL_MANIFEST:-scripts/external-dependencies.json}"
dependency_dir="${KUBERIC_EXTERNAL_DIR:-.kuberic-cache/external}"
[[ "$manifest" == /* ]] || manifest="$PWD/$manifest"
[[ "$dependency_dir" == /* ]] || dependency_dir="$PWD/$dependency_dir"
readonly manifest dependency_dir
readonly stamp="$dependency_dir/.manifest.sha256"

usage() {
    echo 'Usage: external_dependencies.sh prepare|verify|path NAME|version NAME' >&2
    exit 2
}

validate_manifest() {
    jq -e '
        .schemaVersion == 1 and
        (.dependencies | type == "array" and length > 0) and
        all(.dependencies[];
            (.name | type == "string" and test("^[a-z0-9][a-z0-9-]*$")) and
            (.transport == "https" or .transport == "oci") and
            (.source | type == "string" and length > 0) and
            ((.transport == "https" and (.source | startswith("https://"))) or
             (.transport == "oci" and (.source | startswith("oci://")))) and
            (.version | type == "string" and test("^[A-Za-z0-9][A-Za-z0-9._+-]*$")) and
            (.sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
            (.file | type == "string" and test("^[A-Za-z0-9][A-Za-z0-9._-]*$"))) and
        (([.dependencies[].name] | unique | length) == (.dependencies | length)) and
        (([.dependencies[].file] | unique | length) == (.dependencies | length))
    ' "$manifest" >/dev/null || {
        echo "Invalid external dependency manifest: $manifest" >&2
        exit 2
    }
}

manifest_hash() {
    sha256sum "$manifest" | cut -d ' ' -f 1
}

dependency_records() {
    jq -r '.dependencies[] | [.name, .transport, .source, .version, .sha256, .file] | @tsv' "$manifest"
}

verify_files() {
    local failed=0
    local name transport source version sha256 file artifact
    while IFS=$'\t' read -r name transport source version sha256 file; do
        artifact="$dependency_dir/$file"
        if [[ ! -f "$artifact" ]]; then
            echo "Missing external dependency '$name': $artifact" >&2
            failed=1
        elif ! printf '%s  %s\n' "$sha256" "$artifact" | sha256sum --check --status; then
            echo "Checksum mismatch for external dependency '$name': $artifact" >&2
            failed=1
        fi
    done < <(dependency_records)
    ((failed == 0))
}

verify_stamp() {
    local expected
    expected=$(manifest_hash)
    [[ -f "$stamp" ]] && [[ "$(cat "$stamp")" == "$expected" ]] || {
        echo "The prepared dependency bundle does not match $manifest." >&2
        return 1
    }
}

prepare() {
    mkdir -p -- "$dependency_dir"
    local name transport source version sha256 file artifact
    while IFS=$'\t' read -r name transport source version sha256 file; do
        artifact="$dependency_dir/$file"
        echo "Preparing $name $version"
        case "$transport" in
            https)
                bash scripts/download.sh "$source" "$sha256" "$artifact"
                ;;
            oci)
                bash scripts/download.sh "$source" "$sha256" "$artifact" "$version"
                ;;
            *)
                echo "Unsupported transport '$transport' for '$name'." >&2
                exit 2
                ;;
        esac
    done < <(dependency_records)
    verify_files
    local temporary_stamp
    temporary_stamp=$(mktemp "$dependency_dir/.manifest.sha256.tmp.XXXXXX")
    manifest_hash > "$temporary_stamp"
    mv -- "$temporary_stamp" "$stamp"
    echo "Prepared external dependencies in $dependency_dir"
}

verify() {
    if ! verify_files || ! verify_stamp; then
        echo 'Run: just prepare-external-dependencies' >&2
        exit 1
    fi
    echo "Verified external dependencies in $dependency_dir"
}

dependency_path() {
    [[ "$#" == 1 ]] || usage
    local name=$1
    local match_count file sha256 artifact
    match_count=$(jq --arg name "$name" '[.dependencies[] | select(.name == $name)] | length' "$manifest")
    [[ "$match_count" == 1 ]] || {
        echo "Unknown external dependency: $name" >&2
        exit 2
    }
    verify_stamp || {
        echo 'Run: just prepare-external-dependencies' >&2
        exit 1
    }
    file=$(jq -r --arg name "$name" '.dependencies[] | select(.name == $name) | .file' "$manifest")
    sha256=$(jq -r --arg name "$name" '.dependencies[] | select(.name == $name) | .sha256' "$manifest")
    artifact="$dependency_dir/$file"
    [[ -f "$artifact" ]] &&
        printf '%s  %s\n' "$sha256" "$artifact" | sha256sum --check --status || {
        echo "External dependency '$name' is missing or corrupt: $artifact" >&2
        echo 'Run: just prepare-external-dependencies' >&2
        exit 1
    }
    printf '%s\n' "$artifact"
}

dependency_version() {
    [[ "$#" == 1 ]] || usage
    local name=$1
    local match_count
    match_count=$(jq --arg name "$name" '[.dependencies[] | select(.name == $name)] | length' "$manifest")
    [[ "$match_count" == 1 ]] || {
        echo "Unknown external dependency: $name" >&2
        exit 2
    }
    jq -r --arg name "$name" '.dependencies[] | select(.name == $name) | .version' "$manifest"
}

[[ -f "$manifest" ]] || {
    echo "External dependency manifest not found: $manifest" >&2
    exit 2
}
command -v jq >/dev/null || {
    echo 'jq is required to manage external dependencies.' >&2
    exit 2
}
validate_manifest

case "${1:-}" in
    prepare)
        [[ "$#" == 1 ]] || usage
        prepare
        ;;
    verify)
        [[ "$#" == 1 ]] || usage
        verify
        ;;
    path)
        shift
        dependency_path "$@"
        ;;
    version)
        shift
        dependency_version "$@"
        ;;
    *)
        usage
        ;;
esac
