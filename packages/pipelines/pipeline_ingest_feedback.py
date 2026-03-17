#!/usr/bin/env python3
"""
Pipeline Step: Ingest Human Feedback from Google Sheet

Fetches the published Google Sheet CSV containing user-submitted market feedback,
parses rows since last_ingested_timestamp, normalizes feedback types, resolves
market BWR keys to market_ids, deduplicates, and writes to data/human_labels.json.

Reads: Google Sheet CSV (HTTP), tickers_postprocessed.json
Writes: data/human_labels.json
"""

import sys
import csv
import json
import hashlib
import io
import argparse
from pathlib import Path
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, atomic_write_json

# --- Paths ---
HUMAN_LABELS_FILE = DATA_DIR / "human_labels.json"
TICKERS_FILE = DATA_DIR / "tickers_postprocessed.json"

# Google Sheet published CSV URL
FEEDBACK_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vRPiDl8J5hruzzB3_CR83cDz1xrVob9XAgZn_cyfulKX4e3oBGmSbUvP_Ax4hSoesSDoDJXffWtqvjI"
    "/pub?output=csv"
)

# Default timestamp from CLAUDE.md — all rows before this were already
# reviewed and applied manually, so we skip them.
DEFAULT_LAST_INGESTED = "2026-02-12T22:41:28.460Z"


def generate_batch_id() -> str:
    """Generate a batch ID from current UTC timestamp.

    Format: batch_YYYYMMDD_HHMMSS
    """
    return datetime.now(timezone.utc).strftime("batch_%Y%m%d_%H%M%S")

# --- Label type normalization ---
LABEL_TYPE_MAP = {
    "same-event": "same_event_same_rules",
    "same-event:same-rules": "same_event_same_rules",
    "same-event:different-rules": "same_event_different_rules",
    "different-event": "different_event",
    "not-political": "not_political",
    "wrong-category": "wrong_category",
    "other": "other",
}


def normalize_label_type(raw_type: str) -> str:
    """Normalize frontend feedback type to canonical form.

    >>> normalize_label_type("same-event:same-rules")
    'same_event_same_rules'
    >>> normalize_label_type("not-political")
    'not_political'
    >>> normalize_label_type("SAME-EVENT")
    'same_event_same_rules'
    >>> normalize_label_type("unknown-type")
    'other'
    """
    normalized = raw_type.strip().lower()
    return LABEL_TYPE_MAP.get(normalized, "other")


def compute_label_id(timestamp: str, market_ids: list) -> str:
    """Deterministic label ID from timestamp + sorted market IDs.

    >>> compute_label_id("2026-01-01T00:00:00Z", ["abc", "def"])
    'hl_' + hashlib.sha256("2026-01-01T00:00:00Z|abc|def".encode()).hexdigest()[:12]
    """
    sorted_ids = sorted(str(mid) for mid in market_ids)
    key = timestamp + "|" + "|".join(sorted_ids)
    return "hl_" + hashlib.sha256(key.encode()).hexdigest()[:12]


def parse_markets_json(markets_str: str) -> list:
    """Parse the Markets (JSON) column into structured market objects.

    Each market has: key (BWR ticker or market key), label, platform, category.
    Returns list of dicts. Returns empty list on parse failure.
    """
    if not markets_str or not markets_str.strip():
        return []
    try:
        markets = json.loads(markets_str)
        if isinstance(markets, list):
            return markets
        return []
    except (json.JSONDecodeError, TypeError):
        return []


def resolve_market_ids(market_keys: list, ticker_lookup: dict) -> list:
    """Resolve BWR ticker keys to market_ids using the ticker lookup.

    Args:
        market_keys: List of BWR ticker strings from the feedback
        ticker_lookup: Dict mapping BWR ticker -> list of market_id strings

    Returns:
        List of resolved market_id strings. Keys that can't be resolved
        are included as-is (they may be raw market IDs already).
    """
    resolved = []
    for key in market_keys:
        if key in ticker_lookup:
            # Expand all market_ids for this ticker (e.g. Kalshi + Polymarket)
            resolved.extend(ticker_lookup[key])
        else:
            # Try prefix matching for grouped ticker keys (e.g.
            # "BWR-FED-HOLD-FFR-SPECIFIC_MEETING-ANY-DEC2026" should match
            # all specific variants like "...-0BPS-DEC2026", "...-25BPS-...")
            # The frontend submits keys with "ANY" as a wildcard for the
            # specificity segment; find all tickers sharing the same prefix
            # and suffix.
            prefix_matches = []
            if key.startswith("BWR-") and "-ANY-" in key:
                parts = key.split("-ANY-", 1)
                prefix, suffix = parts[0], parts[1]
                for tk in ticker_lookup:
                    if tk.startswith(prefix + "-") and tk.endswith("-" + suffix):
                        prefix_matches.extend(ticker_lookup[tk])
            if prefix_matches:
                resolved.extend(prefix_matches)
            else:
                # Key might already be a market_id, or unresolvable
                resolved.append(key)
    return resolved


def build_ticker_lookup(tickers_data: dict) -> dict:
    """Build multi-path lookup from tickers data.

    Indexes by:
      - BWR ticker string (e.g. "BWR-DEM-CONTROL-HOUSE-...")
      - Raw market_id (e.g. "CONTROLH-2026-D")
      - Frontend key format: "kalshi_{market_id}" or "polymarket_{market_id}"

    Returns dict mapping key -> list of market_id strings.
    """
    lookup = {}
    for t in tickers_data.get("tickers", []):
        ticker_str = t.get("ticker", "")
        market_id = str(t.get("market_id", ""))
        platform = t.get("platform", "")
        if ticker_str and market_id:
            lookup.setdefault(ticker_str, []).append(market_id)
        if market_id:
            # Direct market_id lookup (identity)
            lookup.setdefault(market_id, []).append(market_id)
            # Frontend key format: "kalshi_TICKER" or "polymarket_SLUG"
            if platform:
                prefixed = f"{platform.lower()}_{market_id}"
                lookup.setdefault(prefixed, []).append(market_id)
    return lookup


def fetch_feedback_csv(url: str) -> list:
    """Fetch CSV from published Google Sheet URL.

    Returns list of row dicts with keys: Timestamp, Feedback Type,
    Description, Market Count, Markets (JSON).

    Raises URLError on network failure.
    """
    req = Request(url, headers={"User-Agent": "Bellwether-Pipeline/1.0"})
    with urlopen(req, timeout=30) as response:
        content = response.read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(content))
    # Strip whitespace from header names (Google Sheets may add trailing spaces)
    if reader.fieldnames:
        reader.fieldnames = [name.strip() for name in reader.fieldnames]
    rows = list(reader)
    return rows


def load_human_labels() -> dict:
    """Load existing human_labels.json or create empty structure."""
    if HUMAN_LABELS_FILE.exists():
        with open(HUMAN_LABELS_FILE) as f:
            return json.load(f)
    return {
        "schema_version": 1,
        "updated_at": None,
        "last_ingested_timestamp": DEFAULT_LAST_INGESTED,
        "labels": [],
    }


def load_tickers_data() -> dict:
    """Load tickers_postprocessed.json."""
    if not TICKERS_FILE.exists():
        return {"tickers": []}
    with open(TICKERS_FILE) as f:
        return json.load(f)


def parse_timestamp(ts_str: str) -> datetime:
    """Parse timestamp string to datetime for comparison.

    Handles both ISO format and Google Sheets format.
    """
    # Try ISO format first
    for fmt in [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ]:
        try:
            return datetime.strptime(ts_str.strip(), fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse timestamp: {ts_str!r}")


def ingest_new_rows(csv_rows: list, existing_data: dict, tickers_data: dict, batch_id: str = None) -> tuple:
    """Process CSV rows newer than last_ingested_timestamp.

    Args:
        csv_rows: List of row dicts from fetch_feedback_csv
        existing_data: Current human_labels.json content
        tickers_data: Tickers data for resolving market IDs

    Returns:
        (new_labels, latest_timestamp_str) tuple.
        new_labels is list of label dicts to append.
        latest_timestamp_str is the timestamp of the most recent processed row.
    """
    last_ts_str = existing_data.get("last_ingested_timestamp", DEFAULT_LAST_INGESTED)
    try:
        last_ts = parse_timestamp(last_ts_str)
    except ValueError:
        last_ts = parse_timestamp(DEFAULT_LAST_INGESTED)

    # Build lookup for dedup
    existing_ids = {label["label_id"] for label in existing_data.get("labels", [])}

    # Build ticker lookup for resolving keys
    ticker_lookup = build_ticker_lookup(tickers_data)

    new_labels = []
    latest_ts = last_ts

    for row in csv_rows:
        # Normalize column names — Google Sheets CSV may have trailing spaces
        row = {k.strip(): v for k, v in row.items()}

        ts_str = row.get("Timestamp", "").strip()
        if not ts_str:
            continue

        try:
            row_ts = parse_timestamp(ts_str)
        except ValueError:
            continue

        # Skip rows at or before the last ingested timestamp
        if row_ts <= last_ts:
            continue

        # Parse markets JSON
        markets_raw = parse_markets_json(row.get("Markets (JSON)", ""))
        if not markets_raw:
            continue

        # Extract market keys and platform-specific IDs
        market_keys = [m.get("key", "") for m in markets_raw if m.get("key")]
        platforms = list({m.get("platform", "Unknown") for m in markets_raw})

        # Also extract platform IDs submitted by the frontend (richer payload)
        all_resolvable = list(market_keys)
        for m in markets_raw:
            for field in ("pm_market_id", "k_ticker", "ticker"):
                val = m.get(field)
                if val and val not in all_resolvable:
                    all_resolvable.append(val)

        if not all_resolvable:
            continue

        # Resolve to market IDs and deduplicate
        market_ids = resolve_market_ids(all_resolvable, ticker_lookup)
        seen = set()
        market_ids = [x for x in market_ids if not (x in seen or seen.add(x))]

        # Compute deterministic label ID
        label_id = compute_label_id(ts_str, market_ids)

        # Skip if already ingested
        if label_id in existing_ids:
            continue

        # Normalize feedback type
        raw_type = row.get("Feedback Type", "other")
        label_type = normalize_label_type(raw_type)

        label = {
            "label_id": label_id,
            "source": "google_sheet",
            "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "original_timestamp": ts_str,
            "label_type": label_type,
            "market_ids": market_ids,
            "market_keys": market_keys,
            "platforms": platforms,
            "description": row.get("Description", "").strip(),
            "status": "pending",
            "applied_at": None,
            "applied_action": None,
            "ingested_batch_id": batch_id,
        }

        new_labels.append(label)
        existing_ids.add(label_id)

        if row_ts > latest_ts:
            latest_ts = row_ts

    # Format latest timestamp back to string
    if latest_ts > last_ts:
        latest_ts_str = latest_ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    else:
        latest_ts_str = last_ts_str

    return new_labels, latest_ts_str


def main():
    parser = argparse.ArgumentParser(description="Ingest human feedback from Google Sheet")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing")
    parser.add_argument("--csv-file", type=str, help="Read from local CSV file instead of Google Sheet")
    parser.add_argument("--batch-id", type=str, default=None,
                        help="Batch ID for traceability (auto-generated if not provided)")
    args = parser.parse_args()

    batch_id = args.batch_id or generate_batch_id()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Ingesting human feedback (batch: {batch_id})...")

    # Load existing data
    existing_data = load_human_labels()
    last_ts = existing_data.get("last_ingested_timestamp", DEFAULT_LAST_INGESTED)
    print(f"  Last ingested: {last_ts}")
    print(f"  Existing labels: {len(existing_data.get('labels', []))}")

    # Load tickers for resolution
    tickers_data = load_tickers_data()
    print(f"  Tickers loaded: {len(tickers_data.get('tickers', []))}")

    # Fetch CSV
    if args.csv_file:
        print(f"  Reading from local file: {args.csv_file}")
        csv_path = Path(args.csv_file)
        if not csv_path.exists():
            print(f"  ERROR: CSV file not found: {args.csv_file}")
            sys.exit(1)
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            csv_rows = list(reader)
    else:
        print(f"  Fetching from Google Sheet...")
        try:
            csv_rows = fetch_feedback_csv(FEEDBACK_CSV_URL)
        except URLError as e:
            print(f"  ERROR: Failed to fetch CSV: {e}")
            sys.exit(1)

    print(f"  Total CSV rows: {len(csv_rows)}")

    # Ingest new rows
    new_labels, latest_ts = ingest_new_rows(csv_rows, existing_data, tickers_data, batch_id=batch_id)

    if not new_labels:
        print("  No new feedback rows to ingest.")
        return

    print(f"  New labels to ingest: {len(new_labels)}")
    for label in new_labels:
        print(f"    {label['label_id']}: {label['label_type']} ({len(label['market_ids'])} markets)")

    if args.dry_run:
        print("  [DRY RUN] Not writing changes.")
        return

    # Append new labels and update metadata
    existing_data["labels"].extend(new_labels)
    existing_data["last_ingested_timestamp"] = latest_ts
    existing_data["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    existing_data["last_batch_id"] = batch_id
    atomic_write_json(HUMAN_LABELS_FILE, existing_data, indent=2, ensure_ascii=False)
    print(f"  Wrote {len(new_labels)} new labels to {HUMAN_LABELS_FILE.name}")
    print(f"  Updated last_ingested_timestamp to {latest_ts}")


if __name__ == "__main__":
    main()
