#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

grep -Fqx 'apiVersion: operator.kuberic.io/v1alpha1' examples/kvstore2/deploy/sample.yaml
grep -Fqx 'kind: KubericSet' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  replicas: 3' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  # A singleton has no redundancy; raise replicas to restore one fresh ordinal at a time.' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  # Scale-up creates/freezes the PVC before the Pod and retries failed candidates fresh.' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  # The test-only live copy gate is intentionally absent from this user manifest.' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  image: localhost/kvstore2:level-triggered-v1' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  failoverDelaySeconds: 10' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  # switchover:' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  #   requestId: move-to-replica-2-001' examples/kvstore2/deploy/sample.yaml
grep -Fqx '  #   targetReplicaId: 2' examples/kvstore2/deploy/sample.yaml

python3 - "$@" <<'PY'
import difflib
import json
import pathlib
import re
import subprocess
import sys
import urllib.parse

root = pathlib.Path.cwd()
checked_crd = (root / "kuberic-controller/deploy/crd.json").read_bytes()
generated_crd = subprocess.run(
    ["cargo", "run", "-p", "kuberic-controller", "--bin", "crdgen", "--quiet"],
    check=True, stdout=subprocess.PIPE,
).stdout
if checked_crd != generated_crd:
    print("The checked-in level-triggered CRD differs from the generated API.", file=sys.stderr)
    sys.stderr.writelines(difflib.unified_diff(
        checked_crd.decode().splitlines(keepends=True),
        generated_crd.decode().splitlines(keepends=True),
        fromfile="checked-in CRD", tofile="generated CRD",
    ))
    raise SystemExit(1)
crd_guard = 350_000
assert len(checked_crd) < crd_guard
assert len(checked_crd) <= 345_000
crd_headroom = crd_guard - len(checked_crd)
schema = json.loads(checked_crd)
properties = schema["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
spec = properties["spec"]
assert set(spec["properties"]) == {"replicas", "image", "failoverDelaySeconds", "switchover"}
assert spec["properties"]["replicas"]["type"] == "integer"
assert spec["properties"]["replicas"]["minimum"] == 1
request = spec["properties"]["switchover"]
assert "switchover" not in spec["required"], "Switchover must remain opt-in"
assert set(request["required"]) == {"requestId", "targetReplicaId"}
assert set(request["properties"]) == {"requestId", "targetReplicaId"}
assert request["properties"]["requestId"]["type"] == "string"
assert request["properties"]["requestId"]["minLength"] == 1
assert request["properties"]["targetReplicaId"]["type"] == "integer"
assert request["properties"]["targetReplicaId"]["minimum"] == 1
status = properties["status"]["properties"]
scale_up = status["transition"]["properties"]["scaleUp"]["properties"]
assert set(scale_up) == {
    "operationId", "resourceUid", "specGeneration", "desiredReplicas",
    "previousConfiguration", "currentConfiguration", "previousPolicy", "currentPolicy",
    "primary", "target", "buildId", "snapshotBoundaryLsn", "catchUpBoundaryLsn",
}
failover = status["transition"]["properties"]["scaleUpFailover"]["properties"]
assert set(failover["finalElection"]["properties"]) == {
    "selectedPrimaryReplicaId", "witnesses", "previousReadQuorum", "currentReadQuorum",
}
final_witness = failover["finalElection"]["properties"]["witnesses"]["items"]["properties"]
assert set(final_witness) == {
    "replicaId", "processSessionId", "reportSequence", "currentProgress",
    "committedLsn", "deactivatedLsn", "fenceOperationId",
}
receipt_failover = status["lastScaleUp"]["properties"]["failoverEvidence"]["properties"]
assert set(receipt_failover) == {
    "provisionalPrimaryReplicaId", "previousReadQuorum", "currentReadQuorum",
    "finalElection",
}
allocation_schema = status["scaleUpAllocation"]
allocation = allocation_schema["properties"]
assert set(allocation) == {
    "resourceUid", "specGeneration", "desiredReplicas", "previousConfigurationId",
    "acceptedConfigurationId", "targetReplicaId", "operationId", "podUid", "pvcUid",
    "previousOperationId", "scaffoldingRequested", "cancellationStarted",
}
assert set(allocation_schema["required"]) == {
    "resourceUid", "specGeneration", "desiredReplicas", "previousConfigurationId",
    "acceptedConfigurationId", "targetReplicaId", "operationId",
}
assert allocation["targetReplicaId"]["minimum"] == 1
assert set(status["scaleUpCleanup"]["properties"]) == {
    "provisioning", "target", "resources",
}
assert set(status["lastScaleUp"]["properties"]) == {
    "intent", "acceptedConfiguration", "failoverEvidence", "failoverSafeLsn",
    "currentOnlyWriteQuorum",
}
for field in ("scaleUpAllocation", "scaleUpCleanup", "lastScaleUp",
              "scaleUpAdmissionStarted"):
    assert field not in properties["status"]["required"], f"{field} must remain optional"
receipt = status["lastSwitchover"]["properties"]
assert set(receipt) == {
    "requestId", "requestedTargetReplicaId", "acceptedTarget", "resultingPrimary", "outcome"
}
assert set(receipt["outcome"]["enum"]) == {
    "requestedTargetCompleted", "oldPrimaryRestored", "oldPrimaryCompensated", "rejected", "unsafe"
}
transition = status["transition"]["properties"]
assert "plannedSwitchover" in transition["kind"]["enum"]
assert "secondaryScaleDown" in transition["kind"]["enum"]
removal = transition["secondaryScaleDown"]["properties"]
assert set(removal) == {
    "operationId", "resourceUid", "specGeneration", "desiredReplicas",
    "previousConfiguration", "currentConfiguration", "previousPolicy", "currentPolicy",
    "primary", "target", "cleanup"
}
assert removal["desiredReplicas"]["minimum"] == 1
assert removal["specGeneration"]["minimum"] == 1
assert set(transition["secondaryRemovalEvidence"]["properties"]) == {
    "preparation", "previousReadQuorum", "reducedWriteQuorum"
}
assert set(status["secondaryScaleDownCleanup"]["properties"]) == {
    "evidence", "currentOnlyWriteQuorum", "retirement"
}
assert set(status["lastSecondaryRemoval"]["properties"]) == {
    "evidence", "currentOnlyWriteQuorum"
}, "Completed removal proof must not retain retirement/deletion authority"
for field in ("secondaryScaleDownCleanup", "lastSecondaryRemoval",
              "pendingReplacementCleanup", "lastReplacement"):
    assert field not in properties["status"]["required"], f"{field} must remain optional"
for field in ("pendingReplacementCleanup", "lastReplacement"):
    assert set(status[field]["properties"]) == {"resourceUid", "resources", "target"}
for resource in ("endpoint", "pod", "pvc"):
    identity = removal["cleanup"]["properties"][resource]["properties"]
    assert set(identity) == {"present", "absent"}
    assert set(identity["present"]["required"]) == {"name", "uid"}
    assert set(identity["absent"]["required"]) == {"name"}
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
service = (root / "examples/kvstore2/deploy/service.yaml").read_text()
assert "testing.kuberic.io/live-copy-gate" not in sample
assert "KUBERIC_LIVE_TEST_COPY_GATE_ADDRESS" not in sample
assert "18080" not in service
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
guide_text = guide.read_text()
assert "operator.kuberic.io/scale-up-allocation-operation" in guide_text
assert "same-name PVC with missing or mismatched" in guide_text
assert f"**{len(checked_crd):,} bytes**" in guide_text
assert f"**{crd_headroom:,} bytes**" in guide_text
status_size_run = subprocess.run(
    [
        "cargo", "test", "-p", "kuberic-protocol",
        "representative_scale_up_status_variants", "--", "--nocapture",
    ],
    check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
)
status_sizes = {
    (phase, int(members)): int(size)
    for phase, members, size in re.findall(
        r"scale-up-status phase=(\S+) members=(\d+) bytes=(\d+)",
        status_size_run.stdout,
    )
}
carried_failover_18 = status_sizes[("carried-failover", 18)]
assert (
    f"largest current 18-member sample is carried failover at "
    f"**{carried_failover_18:,} bytes**"
) in guide_text
examples = re.findall(r"```yaml\n(.*?)\n```", guide_text, re.DOTALL)
assert active_sample in examples, "Guide bootstrap example differs from sample"
assert request_example in examples, "Guide request example differs from opt-in sample"
assert active_sample.replace("  replicas: 3", "  replicas: 2") in examples, \
    "Guide scale-down example must only lower the sample replica count"
patches = [json.loads(value) for value in re.findall(r"-p '(\{.*\})'", guide_text)]
for replicas in (2, 1):
    assert {"spec": {"replicas": replicas}} in patches, \
        f"Missing spec-only scale-down patch for {replicas} replicas"
assert "PVC object deletion" in sample and "PVC object deletion" in guide_text

protocol = (root / "kuberic-protocol/src/lib.rs").read_text()
store = (root / "kuberic-agent/src/state.rs").read_text()
assert re.search(r"pub const PROTOCOL_VERSION: u32 = 8;", protocol)
assert re.search(r"pub const SCHEMA_VERSION: u32 = 3;", store)
assert "Protocol version 8" in guide_text and "schema 3" in guide_text
assert "Protocol version 8" in (root / "kuberic-wire/README.md").read_text()
assert "Protocol 8" in sample and "schema 3" in sample
current_requirement_documents = [
    root / "docs/Dev.md",
    root / "docs/features/kuberic/level-triggered-operator.md",
    root / "kuberic-agent/README.md",
    root / "kuberic-protocol/README.md",
]
for document in current_requirement_documents:
    text = document.read_text()
    normalized = " ".join(text.split()).lower()
    assert "protocol 8" in normalized or "protocol-8" in normalized, \
        f"{document.relative_to(root)}: current deployment requirement must name protocol 8"
    assert "schema 3" in normalized or "schema-3" in normalized, \
        f"{document.relative_to(root)}: current deployment requirement must name schema 3"
    for stale in (
        "fresh protocol-7/schema-3",
        "fresh coordinated protocol-7/schema-3",
        "fresh deployment for protocol 7 / schema 3",
        "the protocol 7 / schema 3 fresh-deployment contract",
        "requests protocol-7 sequential scale-up",
    ):
        assert stale not in normalized, \
            f"{document.relative_to(root)}: stale current protocol-7 requirement: {stale}"
diagnostics = (root / "kuberic-agent/src/process.rs").read_text()
assert "pub retired: bool" in diagnostics
assert "state.retired_authority.is_some() || snapshot.retired_authority.is_some()" in diagnostics
assert "`retired: true`" in guide_text and "Older diagnostic responses can omit" in guide_text

recipes = (root / "justfile").read_text()
workflow = (root / ".github/workflows/level-triggered-CI.yml").read_text()
live_tests = (root / "kuberic-level-tests/src/level_triggered_k8s.rs").read_text()
matrix = re.search(r"expanded\+=\(([^)]+)\)", recipes).group(1).split()
for selector, test in (
    ("scale-down", "scale_down"),
    ("scale-down-adversarial", "scale_down_adversarial"),
    ("scale-up", "scale_up"),
    ("scale-up-multi", "scale_up_multi"),
    ("scale-up-adversarial", "scale_up_adversarial"),
):
    assert selector in matrix
    assert f'{selector}) test_name="level_triggered_k8s::{test}"' in recipes
    assert re.search(rf"fn {test}\(", live_tests)
    if selector.startswith(("scale-down", "scale-up")):
        for document in (guide, root / "examples/kvstore2/README.md",
                         root / "docs/features/kuberic/testing.md"):
            assert f"just level-triggered-kind-test {selector}" in document.read_text()
assert "just level-triggered-kind-test scale-down" in workflow
assert "just level-triggered-kind-test all" in workflow
assert "just level-triggered-kind-test scale-up" in workflow
assert "cargo test -p kvstore --test reconciler test_reconciler_scale_up -- --exact" in workflow
assert "test_scale_up_replays_writes_buffered_during_copy -- --exact" in workflow
assert "expanded+=(scale-up scale-up-multi scale-up-adversarial)" in recipes
for document in (guide, root / "docs/features/kuberic/testing.md"):
    assert "just level-triggered-kind-test scale-up-full" in document.read_text()
for document in (guide, root / "docs/features/kuberic/testing.md"):
    text = " ".join(re.sub(r"[`*>]", "", document.read_text()).split()).lower()
    for claim in ("test-only", "disabled by default", "diagnostic port 18080",
                  "not a crd field", "supported user api"):
        assert claim in text, f"{document.relative_to(root)}: missing live gate boundary: {claim}"

documents = [
    root / "README.md",
    root / "docs/Dev.md",
    root / "docs/features/kuberic/testing.md",
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

assert {"sequential-scale-up", "provisioning-copy-and-admission",
        "cancellation-failover-and-cleanup", "secondary-scale-down",
        "authority-and-cleanup", "availability-and-restart",
        "tests-and-diagnostics"} <= heading_anchors(guide)
for document in (root / "README.md", root / "examples/kvstore2/README.md",
                 root / "docs/proposal/v1-retirement-plan.md"):
    assert "level-triggered-operator.md#secondary-scale-down" in document.read_text()
    assert "level-triggered-operator.md#sequential-scale-up" in document.read_text()

def prose(text):
    # Check claims across Markdown wrapping/emphasis, not incidental line layout.
    return " ".join(re.sub(r"[`*>]", "", text).split()).lower()

summary_documents = [
    root / "README.md", guide, root / "docs/proposal/v1-retirement-plan.md",
    root / "kuberic-protocol/README.md", root / "kuberic-controller/README.md",
    root / "kuberic-runtime/README.md", root / "kuberic-agent/README.md",
]
for document in summary_documents:
    text = prose(document.read_text())
    for claim in ("sf-inspired secondary scale-down using pc/cc quorum principles",
                  "kuberic-specific target/minimum coupling", "deterministic selection",
                  "write closure", "sequential cleanup", "kubernetes resource deletion",
                  "independently configurable"):
        assert claim in text, f"{document.relative_to(root)}: missing narrowed claim: {claim}"
    for claim in ("sequential scale-up", "protocol 8", "schema 3"):
        assert claim in text, f"{document.relative_to(root)}: missing scale-up claim: {claim}"

scale_up_text = prose(guide_text.split("## Sequential Scale-Up\n", 1)[1]
                     .split("\n## Secondary Scale-Down", 1)[0])
for claim in ("service fabric-inspired replica-add semantics",
              "classic kuberic.io/v1 reconciliation remains unchanged",
              "one member at a time", "first missing positive logical id",
              "fresh allocation operation", "creates the pvc",
              "only then creates the pod", "catchupboundarylsn",
              "without quorum credit", "independent write-quorum requirements",
              "scaleupcommitteddegraded", "endpoint → pod → pvc",
              "same-name/different-uid", "scaleupstable",
              "provisional newer-epoch primary", "fresh deactivation/progress reports",
              "exact safe lsn"):
    assert claim in scale_up_text, f"Scale-up guide missing contract: {claim}"
assert "maximum replica-count budget" in scale_up_text

removal_text = prose(guide_text.split("## Secondary Scale-Down\n", 1)[1]
                    .split("\n## Planned Switchover", 1)[0])
for claim in ("kuberic policy choice", "not general sf semantics",
              "retained-quorum preflight", "before freezing intent",
              "stable accepted current-only authority", "fresh exact sessions",
              "scaledownretainedreadquorumunavailable", "bounded requeues",
              "existing writable service", "no transition",
              "cleanup-provenance limitation", "exact original pvc provenance",
              "reconstructable before intent admission", "list omission is not absence",
              "frozen or reconstructable", "not a claim of physical storage erasure",
              "every retained member", "completed local acceptance", "indefinite outage"):
    assert claim in removal_text, f"Scale-down guide missing contract: {claim}"
# Restrict legacy wording checks to the scaling section: unrelated SF interface
# documentation and the historical proposal are not scaling-parity claims.
assert not re.search(r"(?:service fabric|sf)[ -]aligned", removal_text), \
    "Describe the precise SF-inspired subset rather than broad SF alignment"
assert not re.search(r"following the (?:selected )?service fabric target/minimum", removal_text)

retirement = root / "docs/proposal/v1-retirement-plan.md"
assert "deferred-scale-down-follow-ups" in heading_anchors(retirement)
assert "v1-retirement-plan.md#deferred-scale-down-follow-ups" in guide_text
retirement_text = prose(retirement.read_text())
for claim in ("sequential scale-up and secondary scale-down",
              "production v2 controller configuration",
              "classic kuberic.io/v1 remains unchanged",
              "pvc-before-pod", "post-enumeration catch-up boundary",
              "protocol 8 and agent schema 3", "primary-removal behavior"):
    assert claim in retirement_text, f"Retirement plan missing scale-up status: {claim}"
assert "outstanding scale-up" not in retirement_text
assert "scale-up | remains absent" not in retirement_text
followups = prose(retirement.read_text().split("### Deferred scale-down follow-ups\n", 1)[1]
                  .split("\n## Workstream 3", 1)[0])
for claim in ("priority", "why deferred", "durable per-member kubernetes resource provenance",
              "context-bound preparation/retirement", "replication proof",
              "cleanupobligation", "serialized status-size tests", "replica-count budget",
              "compatibility/api-breaking", "mechanical refactors",
              "candidate-selection helper", "neutral exact-cleanup helpers",
              "static command-binding predicates", "independent target/minimum",
              "plb/placement-aware", "frozen-primary recovery", "overlapping cleanup",
              "multi-member removal", "durable primary-agent coordinator",
              "keep desired-count/target policy in the evaluator/controller",
              "keep local journals out of cr status", "scale-up operational budget",
              "maximum replica count", "completion slo", "throughput target",
              "do not use opaque schemas, hash-only receipts, or ttl evidence deletion"):
    assert claim in followups, f"Deferred follow-ups missing contract: {claim}"

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

echo "Level-triggered links, examples, scale-down/scale-up contracts, live selector boundaries, protocol/schema versions, CRD/status size guards, and runtime API boundaries are current."
