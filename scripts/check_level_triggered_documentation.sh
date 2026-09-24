#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

generated_crd=$(mktemp)
trap 'rm -f -- "$generated_crd"' EXIT

cargo run -p kuberic-controller --bin crdgen --quiet > "$generated_crd"
if ! diff -u kuberic-controller/deploy/crd.json "$generated_crd"; then
    echo "The checked-in level-triggered CRD differs from the generated API." >&2
    exit 1
fi

grep -Fqx 'apiVersion: operator.kuberic.io/v1alpha1' examples/kvstore2/deploy/sample.yaml
grep -Fqx 'kind: KubericSet' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  replicas: 3' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  image: localhost/kvstore2:level-triggered-v1' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  failoverDelaySeconds: 10' examples/kvstore2/deploy/sample.yaml

python3 <<'PY'
import pathlib
import re
import sys
import urllib.parse

root = pathlib.Path.cwd()
documents = [
    root / "README.md",
    root / "docs/features/kuberic/level-triggered-operator.md",
    root / "docs/proposal/level-triggered-operator-design.md",
    root / "examples/kvstore2/README.md",
    root / "kuberic-agent/README.md",
    root / "kuberic-controller/README.md",
    root / "kuberic-protocol/README.md",
    root / "kuberic-runtime/README.md",
    root / "kuberic-wire/README.md",
]
link = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
failures = []

for document in documents:
    if not document.is_file():
        failures.append(f"{document.relative_to(root)} does not exist")
        continue
    for line_number, line in enumerate(document.read_text().splitlines(), 1):
        for raw_target in link.findall(line):
            target = raw_target.strip()
            if target.startswith("<") and target.endswith(">"):
                target = target[1:-1]
            target = target.split(maxsplit=1)[0]
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            path = urllib.parse.unquote(target.split("#", 1)[0].split("?", 1)[0])
            if not path:
                continue
            resolved = (document.parent / path).resolve()
            if not resolved.exists():
                failures.append(
                    f"{document.relative_to(root)}:{line_number}: missing {target}"
                )

if failures:
    print("Level-triggered documentation link failures:", file=sys.stderr)
    for failure in failures:
        print(f"- {failure}", file=sys.stderr)
    raise SystemExit(1)
PY

scripts/check_runtime_public_api.sh

echo "Level-triggered documentation links, generated CRD, and runtime API boundaries are current."
