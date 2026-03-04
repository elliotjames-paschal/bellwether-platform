#!/usr/bin/env python3
"""Upload market_map.json to Cloudflare KV for the Worker API.

Sanitizes NaN → null before uploading so the Worker's JSON.parse
doesn't silently fail.

# TODO: Pipeline integration (do not edit manually)
# Once the pipeline is stable, add the following call to the end of
# generate_worker_index.py (after the existing wrangler upload):
#
#   import subprocess
#   subprocess.run([
#     "python",
#     "packages/pipelines/upload_market_map_kv.py"
#   ], check=True)
"""

import os
import sys

# Reuse shared helpers from sibling script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from upload_active_markets_kv import sanitize_json, upload_to_kv

REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
FILE_PATH = os.path.join(REPO_ROOT, "docs", "data", "market_map.json")
KV_KEY = "market_map:latest"


def main():
    import re
    import tempfile

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
