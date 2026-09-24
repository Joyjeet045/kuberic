#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

metadata=$(mktemp)
trap 'rm -f -- "$metadata"' EXIT
cargo metadata --format-version 1 --no-deps > "$metadata"

python3 - "$metadata" <<'PY'
import json
import pathlib
import re
import sys

metadata_path = pathlib.Path(sys.argv[1])
metadata = json.loads(metadata_path.read_text())
workspace_root = pathlib.Path(metadata["workspace_root"]).resolve()
new_packages = {
    "kuberic-protocol",
    "kuberic-wire",
    "kuberic-runtime",
    "kuberic-runtime-internal",
    "kuberic-agent",
    "kuberic-controller",
    "kvstore2",
    "kuberic-level-tests",
}
protected_packages = {
    "kuberic-core",
    "kuberic-operator",
    "kvstore",
    "sqlite-replicated",
    "postgres-replicated",
    "kuberic-tests",
}

violations = []
for package in metadata["packages"]:
    if package["name"] not in new_packages:
        continue
    for dependency in package["dependencies"]:
        if dependency["name"] in protected_packages:
            violations.append(
                f'{package["name"]} depends on protected package {dependency["name"]}'
            )

protected_roots = [
    (workspace_root / "kuberic-core").resolve(),
    (workspace_root / "kuberic-operator").resolve(),
    (workspace_root / "examples" / "kvstore").resolve(),
    (workspace_root / "examples" / "sqlite").resolve(),
    (workspace_root / "examples" / "postgres").resolve(),
    (workspace_root / "kuberic-tests").resolve(),
]
protected_tokens = {
    "kuberic-core",
    "kuberic-operator",
    "examples/kvstore",
    "examples/sqlite",
    "examples/postgres",
    "kuberic-tests",
}
attribute = re.compile(r'#\s*\[(.*?)\]', re.DOTALL)
path_assignment = re.compile(r'\bpath\s*=\s*"([^"]+)"')
include_start = re.compile(r'(?:include|include_str|include_bytes)!\s*\(')
string_literal = re.compile(r'"([^"]+)"')

def is_protected(path):
    resolved = path.resolve()
    return any(
        resolved == root or root in resolved.parents
        for root in protected_roots
    )

def include_expressions(text):
    for match in include_start.finditer(text):
        depth = 1
        index = match.end()
        quote = None
        escaped = False
        line_comment = False
        block_comment_depth = 0
        while index < len(text):
            character = text[index]
            following = text[index + 1] if index + 1 < len(text) else ""
            if line_comment:
                if character == "\n":
                    line_comment = False
                index += 1
                continue
            if block_comment_depth:
                if character == "/" and following == "*":
                    block_comment_depth += 1
                    index += 2
                elif character == "*" and following == "/":
                    block_comment_depth -= 1
                    index += 2
                else:
                    index += 1
                continue
            if quote is not None:
                if escaped:
                    escaped = False
                elif character == "\\":
                    escaped = True
                elif character == quote:
                    quote = None
                index += 1
                continue
            if character == "/" and following == "/":
                line_comment = True
                index += 2
                continue
            if character == "/" and following == "*":
                block_comment_depth = 1
                index += 2
                continue
            if character in {'"', "'"}:
                quote = character
                index += 1
                continue
            if character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
                if depth == 0:
                    yield text[match.end():index]
                    break
            index += 1
        else:
            raise ValueError("unterminated include expression")

for package in metadata["packages"]:
    if package["name"] not in new_packages:
        continue
    package_root = pathlib.Path(package["manifest_path"]).parent
    for candidate in package_root.rglob("*"):
        if candidate.is_symlink() and is_protected(candidate):
            violations.append(
                f"{package['name']} links protected source via {candidate}"
            )
    for source in package_root.rglob("*.rs"):
        text = source.read_text(errors="replace")
        for body in attribute.findall(text):
            for relative in path_assignment.findall(body):
                if is_protected(source.parent / relative):
                    violations.append(
                        f"{package['name']} imports protected source via {source}: {relative}"
                    )
        try:
            expressions = list(include_expressions(text))
        except ValueError as error:
            violations.append(f"{package['name']} has {error} in {source}")
            expressions = []
        for expression in expressions:
            literals = string_literal.findall(expression)
            if any(is_protected(source.parent / relative) for relative in literals):
                violations.append(
                    f"{package['name']} includes protected source via {source}"
                )
            normalized = expression.replace("\\\\", "/")
            if any(token in normalized for token in protected_tokens):
                violations.append(
                    f"{package['name']} references protected path in include expression {source}"
                )

if violations:
    print("Level-triggered dependency boundary violations:", file=sys.stderr)
    for violation in violations:
        print(f"- {violation}", file=sys.stderr)
    raise SystemExit(1)
PY

echo "Level-triggered crates have no protected v1 dependencies."
