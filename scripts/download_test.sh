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

echo 'HTTPS and OCI checksum, cache reuse, corruption recovery, and failure cleanup tests passed.'