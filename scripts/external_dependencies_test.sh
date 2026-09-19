#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
temporary=$(mktemp -d)
trap 'rm -rf -- "$temporary"' EXIT
export download_source="$temporary/source file"
export download_calls="$temporary/calls"
destination="$temporary/cache/artifact file"
printf 'verified artifact\n' > "$download_source"
sha256=$(sha256sum "$download_source" | cut -d ' ' -f 1)

curl() {
    printf 'download\n' >> "$download_calls"
    [[ "${download_fail:-0}" == 0 ]] || return 22
    cp -- "$download_source" "${@: -1}"
}
export -f curl

bash scripts/download.sh https://example.invalid/artifact "$sha256" "$destination"
cmp "$download_source" "$destination"
[[ "$(wc -l < "$download_calls")" == 1 ]]

rm -- "$download_source"
bash scripts/download.sh https://example.invalid/artifact "$sha256" "$destination"
[[ "$(wc -l < "$download_calls")" == 1 ]]

printf 'corrupt cache\n' > "$destination"
printf 'verified artifact\n' > "$download_source"
bash scripts/download.sh https://example.invalid/artifact "$sha256" "$destination"
cmp "$download_source" "$destination"
[[ "$(wc -l < "$download_calls")" == 2 ]]

printf 'untrusted artifact\n' > "$download_source"
if bash scripts/download.sh https://example.invalid/artifact "$sha256" "$temporary/rejected"; then
    echo 'Accepted an incorrect checksum.' >&2
    exit 1
fi
[[ ! -e "$temporary/rejected" ]]
[[ -z "$(find "$temporary" -name '*.tmp.*' -print)" ]]

export download_fail=1
if bash scripts/download.sh https://example.invalid/artifact "$sha256" "$temporary/failed"; then
    echo 'Accepted a failed download.' >&2
    exit 1
fi
[[ ! -e "$temporary/failed" ]]
[[ -z "$(find "$temporary" -name '*.tmp.*' -print)" ]]

export chart_calls="$temporary/chart-calls"
helm() {
    [[ "$1" == pull && "$3" == --version && "$4" == v1.9.1 && "$5" == --destination ]]
    printf 'download\n' >> "$chart_calls"
    cp -- "$download_source" "$6/gateway-helm-v1.9.1.tgz"
}
timeout() {
    shift 2
    "$@"
}
export -f helm timeout

printf 'verified artifact\n' > "$download_source"
chart="$temporary/cache/gateway-helm-v1.9.1.tgz"
bash scripts/download.sh oci://example.invalid/gateway-helm "$sha256" "$chart" v1.9.1
cmp "$download_source" "$chart"
rm -- "$download_source"
bash scripts/download.sh oci://example.invalid/gateway-helm "$sha256" "$chart" v1.9.1
[[ "$(wc -l < "$chart_calls")" == 1 ]]

printf 'untrusted chart\n' > "$download_source"
if bash scripts/download.sh oci://example.invalid/gateway-helm "$sha256" "$temporary/rejected-chart/gateway-helm-v1.9.1.tgz" v1.9.1; then
    echo 'Accepted an incorrect chart checksum.' >&2
    exit 1
fi
[[ ! -e "$temporary/rejected-chart/gateway-helm-v1.9.1.tgz" ]]
[[ -z "$(find "$temporary" -name '*.tmp.*' -print)" ]]

export dependency_calls="$temporary/dependency-calls"
curl() {
    printf 'download\n' >> "$dependency_calls"
    cp -- "$download_source" "${@: -1}"
}
export -f curl

printf 'prepared dependency\n' > "$download_source"
dependency_sha256=$(sha256sum "$download_source" | cut -d ' ' -f 1)
dependency_manifest="$temporary/external-dependencies.json"
dependency_dir="$temporary/external"
cat > "$dependency_manifest" <<EOF
{
  "schemaVersion": 1,
  "dependencies": [
    {
      "name": "test-artifact",
      "transport": "https",
      "source": "https://example.invalid/test-artifact",
      "version": "v1",
      "sha256": "$dependency_sha256",
      "file": "test-artifact-v1.txt"
    }
  ]
}
EOF
export KUBERIC_EXTERNAL_MANIFEST="$dependency_manifest"
export KUBERIC_EXTERNAL_DIR="$dependency_dir"

if bash scripts/external_dependencies.sh verify; then
    echo 'Verified an unprepared dependency bundle.' >&2
    exit 1
fi
bash scripts/external_dependencies.sh prepare
bash scripts/external_dependencies.sh verify
[[ "$(bash scripts/external_dependencies.sh path test-artifact)" == "$dependency_dir/test-artifact-v1.txt" ]]
[[ "$(bash scripts/external_dependencies.sh version test-artifact)" == v1 ]]
cmp "$download_source" "$dependency_dir/test-artifact-v1.txt"
[[ "$(wc -l < "$dependency_calls")" == 1 ]]

bash scripts/external_dependencies.sh prepare
[[ "$(wc -l < "$dependency_calls")" == 1 ]]

jq '.description = "manifest digest changed"' "$dependency_manifest" > "$temporary/updated-manifest.json"
mv -- "$temporary/updated-manifest.json" "$dependency_manifest"
if bash scripts/external_dependencies.sh verify; then
    echo 'Accepted a bundle prepared from a stale manifest.' >&2
    exit 1
fi
bash scripts/external_dependencies.sh prepare
[[ "$(wc -l < "$dependency_calls")" == 1 ]]
bash scripts/external_dependencies.sh verify

printf 'corrupt dependency\n' > "$dependency_dir/test-artifact-v1.txt"
if bash scripts/external_dependencies.sh verify; then
    echo 'Accepted a corrupt prepared dependency.' >&2
    exit 1
fi
bash scripts/external_dependencies.sh prepare
cmp "$download_source" "$dependency_dir/test-artifact-v1.txt"
[[ "$(wc -l < "$dependency_calls")" == 2 ]]

if grep -Eq 'https://|oci://|scripts/download\.sh|external_dependencies\.sh prepare' scripts/gateway_kind.sh; then
    echo 'Gateway installation contains an implicit external dependency download.' >&2
    exit 1
fi

echo 'Download and prepared dependency checksum, caching, stale-lock, and cleanup tests passed.'
