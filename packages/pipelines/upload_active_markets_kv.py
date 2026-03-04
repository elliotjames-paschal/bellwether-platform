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
import subprocess
import sys

NAMESPACE_ID = "2ce167f19ce748e0bf09b513eaafe9ad"
KV_KEY = "active_markets:latest"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
FILE_PATH = os.path.join(REPO_ROOT, "docs", "data", "active_markets.json")


def main():
    if not os.path.exists(FILE_PATH):
        print(f"ERROR: File not found: {FILE_PATH}", file=sys.stderr)
        sys.exit(1)

    file_size = os.path.getsize(FILE_PATH)
    print(f"File: {FILE_PATH}")
    print(f"Size: {file_size:,} bytes ({file_size / (1024 * 1024):.1f} MB)")

    cmd = [
        "npx", "wrangler", "kv", "key", "put",
        f"--namespace-id={NAMESPACE_ID}",
        "--remote",
        KV_KEY,
        f"--path={FILE_PATH}",
    ]

    print(f"\nUploading to KV key: {KV_KEY}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"ERROR: Upload failed (exit code {result.returncode})", file=sys.stderr)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        sys.exit(1)

    print(result.stdout.strip())
    print(f"\nUpload successful: {KV_KEY} ({file_size:,} bytes)")


if __name__ == "__main__":
    main()
