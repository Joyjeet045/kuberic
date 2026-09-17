#!/usr/bin/env bash
set -euo pipefail

[[ "$#" == 3 || "$#" == 4 ]] || { echo 'Usage: download.sh URL SHA256 DESTINATION [CHART_VERSION]' >&2; exit 2; }
url=$1
sha256=$2
destination=$3
[[ "$sha256" =~ ^[[:xdigit:]]{64}$ ]] || { echo 'Expected a SHA-256 digest.' >&2; exit 2; }

if [[ -f "$destination" ]] && printf '%s  %s\n' "$sha256" "$destination" | sha256sum --check --status; then
    exit 0
fi

mkdir -p -- "$(dirname "$destination")"
temporary=$(mktemp -d "${destination}.tmp.XXXXXX")
trap 'rm -rf -- "$temporary"' EXIT
artifact="$temporary/$(basename "$destination")"
case "$url" in
    https://*)
        curl --fail --location --proto '=https' --proto-redir '=https' \
            --retry 3 --connect-timeout 20 --max-time 180 "$url" -o "$artifact"
        ;;
    oci://*)
        timeout --kill-after=5s 180s helm pull "$url" --version "${4:?Set a chart version}" \
            --destination "$temporary"
        ;;
    *) echo 'Only HTTPS and OCI downloads are supported.' >&2; exit 2 ;;
esac
printf '%s  %s\n' "$sha256" "$artifact" | sha256sum --check
mv -- "$artifact" "$destination"