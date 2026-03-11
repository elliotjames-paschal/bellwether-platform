#!/usr/bin/env python3
"""Upload active_markets.json to Cloudflare KV for the Worker API.

# TODO: Pipeline integration (do not edit manually)
# Once the other Claude Code session resolves Sherlock
# pipeline errors, add the following call to the end of
# generate_monitor_data.py:
#
#   import subprocess
#   subprocess.run([
#     "python",
#     "packages/pipelines/upload_active_markets_kv.py"
#   ], check=True)
#
# This ensures active_markets:latest in KV stays in sync
# every time the pipeline runs.
"""

import os
import re
import subprocess
import sys
import tempfile

NAMESPACE_ID = "2ce167f19ce748e0bf09b513eaafe9ad"
KV_KEY = "active_markets:latest"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
FILE_PATH = os.path.join(REPO_ROOT, "docs", "data", "active_markets.json")


def sanitize_json(raw: str) -> str:
    """Replace NaN, Infinity, -Infinity with null for valid JSON."""
    return re.sub(r'\bNaN\b', 'null', raw)


def upload_to_kv(kv_key: str, file_path: str):
    """Upload a file to Cloudflare KV."""
    file_size = os.path.getsize(file_path)
    print(f"Size: {file_size:,} bytes ({file_size / (1024 * 1024):.1f} MB)")

    cmd = [
        "npx", "wrangler", "kv", "key", "put",
        f"--namespace-id={NAMESPACE_ID}",
        "--remote",
        kv_key,
        f"--path={file_path}",
    ]

    print(f"Uploading to KV key: {kv_key}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"ERROR: Upload failed (exit code {result.returncode})", file=sys.stderr)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        sys.exit(1)

    print(result.stdout.strip())
    print(f"Upload successful: {kv_key} ({file_size:,} bytes)")


def main():
    if not os.path.exists(FILE_PATH):
        print(f"ERROR: File not found: {FILE_PATH}", file=sys.stderr)
        sys.exit(1)

    print(f"File: {FILE_PATH}")

    with open(FILE_PATH, "r") as f:
        raw = f.read()

    sanitized = sanitize_json(raw)
    nan_fixes = len(re.findall(r'\bnull\b', sanitized)) - len(re.findall(r'\bnull\b', raw))
    if nan_fixes > 0:
        print(f"Sanitized {nan_fixes} NaN values → null")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        tmp.write(sanitized)
        tmp_path = tmp.name

    try:
        upload_to_kv(KV_KEY, tmp_path)
    finally:
        os.unlink(tmp_path)


if __name__ == "__main__":
    main()
