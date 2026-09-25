#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

generated_crd=$(cargo run -p kuberic-controller --bin crdgen --quiet)
if ! diff -u kuberic-controller/deploy/crd.json <(printf '%s\n' "$generated_crd"); then
    echo "The checked-in level-triggered CRD differs from the generated API." >&2
    exit 1
fi

grep -Fqx 'apiVersion: operator.kuberic.io/v1alpha1' examples/kvstore2/deploy/sample.yaml
grep -Fqx 'kind: KubericSet' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  replicas: 3' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  image: localhost/kvstore2:level-triggered-v1' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  failoverDelaySeconds: 10' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  # switchover:' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  #   requestId: move-to-replica-2-001' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  #   targetReplicaId: 2' examples/kvstore2/deploy/sample.yaml

python3 - "$@" <<'PY'
import json
import pathlib
import re
import sys
import urllib.parse

root = pathlib.Path.cwd()
schema = json.loads((root / "kuberic-controller/deploy/crd.json").read_text())
properties = schema["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
spec = properties["spec"]
request = spec["properties"]["switchover"]
assert "switchover" not in spec["required"], "Switchover must remain opt-in"
assert set(request["required"]) == {"requestId", "targetReplicaId"}
assert set(request["properties"]) == {"requestId", "targetReplicaId"}
assert request["properties"]["requestId"]["type"] == "string"
assert request["properties"]["requestId"]["minLength"] == 1
assert request["properties"]["targetReplicaId"]["type"] == "integer"
assert request["properties"]["targetReplicaId"]["minimum"] == 1
status = properties["status"]["properties"]
receipt = status["lastSwitchover"]["properties"]
assert set(receipt) == {
    "requestId", "requestedTargetReplicaId", "acceptedTarget", "resultingPrimary", "outcome"
}
assert set(receipt["outcome"]["enum"]) == {
    "requestedTargetCompleted", "oldPrimaryRestored", "oldPrimaryCompensated", "rejected", "unsafe"
}
transition = status["transition"]["properties"]
assert "plannedSwitchover" in transition["kind"]["enum"]
intent = transition["switchover"]["properties"]
assert set(intent) == {
    "preparationGeneration", "requestId", "source", "target", "requestedConfiguration", "resolution", "handoff"
}
assert set(intent["resolution"]["enum"]) == {
    "requestedTarget", "restoringOldPrimary", "compensatingOldPrimary", "unsafe"
}
assert set(intent["handoff"]["properties"]) == {
    "preparationGeneration", "preparationOperationId", "requestId", "source", "target",
    "startingConfigurationId", "startingEpoch", "handoffLsn"
}

sample = (root / "examples/kvstore2/deploy/sample.yaml").read_text()
active_sample = "\n".join(line for line in sample.splitlines() if not line.lstrip().startswith("#"))
assert "switchover:" not in active_sample, "Installation must not submit a switchover"
request_example = "\n".join(
    line.replace("  # ", "  ", 1) if line.startswith(
        ("  # switchover:", "  #   requestId:", "  #   targetReplicaId:")
    ) else line
    for line in sample.splitlines()
    if not line.lstrip().startswith("#") or line.startswith(
        ("  # switchover:", "  #   requestId:", "  #   targetReplicaId:")
    )
)
guide = root / "docs/features/kuberic/level-triggered-operator.md"
examples = re.findall(r"```yaml\n(.*?)\n```", guide.read_text(), re.DOTALL)
assert active_sample in examples, "Guide bootstrap example differs from sample"
assert request_example in examples, "Guide request example differs from opt-in sample"

documents = [
    root / "README.md",
    root / "docs/features/kuberic/level-triggered-operator.md",
    root / "docs/proposal/level-triggered-operator-design.md",
    root / "docs/proposal/v1-retirement-plan.md",
    root / "examples/kvstore2/README.md",
    root / "kuberic-agent/README.md",
    root / "kuberic-controller/README.md",
    root / "kuberic-protocol/README.md",
    root / "kuberic-runtime/README.md",
    root / "kuberic-wire/README.md",
]
documents.extend(root / path for path in sys.argv[1:])
link = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
failures = []

def heading_anchors(document):
    anchors = set()
    counts = {}
    content = re.sub(r"```.*?```", "", document.read_text(), flags=re.DOTALL)
    for heading in re.findall(r"^#{1,6}\s+(.+?)\s*#*\s*$", content, re.MULTILINE):
        slug = re.sub(r"[^\w\- ]", "", heading.lower()).replace(" ", "-")
        count = counts.get(slug, 0)
        counts[slug] = count + 1
        anchors.add(f"{slug}-{count}" if count else slug)
    return anchors

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
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            path = urllib.parse.unquote(target.split("#", 1)[0].split("?", 1)[0])
            resolved = (document.parent / path).resolve() if path else document
            if not resolved.exists():
                failures.append(
                    f"{document.relative_to(root)}:{line_number}: missing {target}"
                )
            elif "#" in target and resolved.suffix == ".md":
                fragment = urllib.parse.unquote(target.split("#", 1)[1])
                if fragment and fragment not in heading_anchors(resolved):
                    failures.append(
                        f"{document.relative_to(root)}:{line_number}: missing heading {target}"
                    )

if failures:
    print("Level-triggered documentation link failures:", file=sys.stderr)
    for failure in failures:
        print(f"- {failure}", file=sys.stderr)
    raise SystemExit(1)
PY

scripts/check_runtime_public_api.sh

echo "Level-triggered documentation links, request examples, generated CRD, and runtime API boundaries are current."
