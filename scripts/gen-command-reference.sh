#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${GOG_BIN:-$ROOT_DIR/bin/gog}"
OUT="${1:-}"
PY="${PYTHON:-python3}"

if ! command -v "$PY" >/dev/null 2>&1; then
  PY="python"
fi
if [ ! -x "$BIN" ]; then
  make -C "$ROOT_DIR" build >/dev/null
fi

schema_file="$(mktemp "${TMPDIR:-/tmp}/gog-schema-XXXXXX.json")"
trap 'rm -f "$schema_file"' EXIT
"$BIN" schema --json >"$schema_file"

generate() {
  "$PY" - "$schema_file" <<'PY'
import json
import sys

schema_path = sys.argv[1]
with open(schema_path, "r", encoding="utf-8") as f:
    schema = json.load(f)

root = schema.get("command") or {}
lines = [
    "# Command Reference",
    "",
    "Generated from `gog schema --json`.",
    "",
]


def first_line(value):
    return (value or "").strip().splitlines()[0] if (value or "").strip() else ""


def walk(command):
    path = command.get("path") or command.get("name") or ""
    usage = command.get("usage") or ""
    summary = first_line(command.get("help"))
    prefix = path.removeprefix("gog ").strip()
    suffix = usage
    if prefix and usage.startswith(prefix):
        suffix = usage[len(prefix):].strip()
    label = path if not suffix else f"{path} {suffix}"
    if label:
        if summary:
            lines.append(f"- `{label}` - {summary}")
        else:
            lines.append(f"- `{label}`")
    for child in command.get("subcommands") or []:
        walk(child)


walk(root)
print("\n".join(lines))
PY
}

if [ -n "$OUT" ]; then
  generate >"$OUT"
else
  generate
fi
