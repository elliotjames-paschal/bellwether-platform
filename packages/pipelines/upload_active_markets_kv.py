#!/usr/bin/env python3
"""Upload active_markets.json to Cloudflare KV for the Worker API.

Uses the Cloudflare REST API directly (no wrangler/Node.js required),
which allows this to run on Sherlock and other environments without npm.

Before uploading, slims the payload to only the fields used by the worker
(/api/markets/search and /api/markets/top) to stay under the 25MB KV limit.
The full active_markets.json on disk is left untouched.

Requires: CLOUDFLARE_API_TOKEN environment variable.
"""

import json
import os
import re
import sys
import urllib.error
import urllib.request

ACCOUNT_ID = "1459befd055e568b62e4c9d789745486"
NAMESPACE_ID = "2ce167f19ce748e0bf09b513eaafe9ad"
KV_KEY = "active_markets:latest"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
FILE_PATH = os.path.join(REPO_ROOT, "docs", "data", "active_markets.json")

# Fields used by worker-v2.js formatMarketResult + search/filter logic
WORKER_FIELDS = {"ticker", "key", "label", "category", "total_volume", "has_both", "platform"}


def sanitize_json(raw: str) -> str:
    """Replace NaN with null for valid JSON."""
    return re.sub(r'\bNaN\b', 'null', raw)


def upload_to_kv(kv_key: str, file_path: str):
    """Upload a file to Cloudflare KV via REST API (no wrangler required)."""
    api_token = os.environ.get("CLOUDFLARE_API_TOKEN")
    if not api_token:
        print("ERROR: CLOUDFLARE_API_TOKEN not set", file=sys.stderr)
        sys.exit(1)

    with open(file_path, "rb") as f:
        payload = f.read()

    file_size = len(payload)
    print(f"Size: {file_size:,} bytes ({file_size / (1024 * 1024):.1f} MB)")
    print(f"Uploading to KV key: {kv_key}")

    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT_ID}"
        f"/storage/kv/namespaces/{NAMESPACE_ID}/values/{kv_key}"
    )
    req = urllib.request.Request(
        url,
        data=payload,
        method="PUT",
        headers={
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/octet-stream",
        },
    )

    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read().decode())
            if not result.get("success"):
                print(f"ERROR: KV upload failed: {result}", file=sys.stderr)
                sys.exit(1)
        print(f"Upload successful: {kv_key} ({file_size:,} bytes)")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"ERROR: HTTP {e.code}: {body}", file=sys.stderr)
        sys.exit(1)


def main():
    if not os.path.exists(FILE_PATH):
        print(f"ERROR: File not found: {FILE_PATH}", file=sys.stderr)
        sys.exit(1)

    print(f"File: {FILE_PATH}")
    original_size = os.path.getsize(FILE_PATH)
    print(f"Original size: {original_size:,} bytes ({original_size / (1024 * 1024):.1f} MB)")

    with open(FILE_PATH, "r") as f:
        raw = sanitize_json(f.read())

    data = json.loads(raw)
    markets = data.get("markets", [])

    # Slim to only fields the worker needs, keeping full file on disk untouched
    slimmed = {"markets": [{k: m[k] for k in WORKER_FIELDS if k in m} for m in markets]}
    payload = json.dumps(slimmed, separators=(",", ":")).encode("utf-8")

    print(f"Slimmed size: {len(payload):,} bytes ({len(payload) / (1024 * 1024):.1f} MB)")
    print(f"Markets: {len(slimmed['markets'])}")

    import tempfile
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".json", delete=False) as tmp:
        tmp.write(payload)
        tmp_path = tmp.name

    try:
        upload_to_kv(KV_KEY, tmp_path)
    finally:
        os.unlink(tmp_path)


if __name__ == "__main__":
    main()
