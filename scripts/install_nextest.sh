#!/usr/bin/env bash
set -euo pipefail

readonly NEXTEST_VERSION="0.9.146"
readonly LINUX_X86_64_SHA256="682c21b777c333e96fd532e114d3a5a894e0729ab88d94c0a9f20f8419695428"

install_dir="${CARGO_HOME:-${HOME}/.cargo}/bin"
binary="${install_dir}/cargo-nextest"

if [[ -x "${binary}" ]] && [[ "$("${binary}" nextest --version | head -n 1)" == "cargo-nextest ${NEXTEST_VERSION} "* ]]; then
  exit 0
fi

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)
    archive_url="https://get.nexte.st/${NEXTEST_VERSION}/linux"
    archive_sha256="${LINUX_X86_64_SHA256}"
    ;;
  *)
    echo "unsupported platform for pinned cargo-nextest: $(uname -s)-$(uname -m)" >&2
    exit 2
    ;;
esac

mkdir -p "${install_dir}"
archive="$(mktemp)"
trap 'rm -f "${archive}"' EXIT

curl --fail --location --silent --show-error "${archive_url}" --output "${archive}"
printf '%s  %s\n' "${archive_sha256}" "${archive}" | sha256sum --check --status
tar xzf "${archive}" -C "${install_dir}" cargo-nextest

"${binary}" nextest --version
