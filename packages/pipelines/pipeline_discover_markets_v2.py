#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Discover Markets V2 - Native APIs
================================================================================

Part of Bellwether V2 Pipeline

This script replaces Dome API with native Kalshi + Polymarket APIs.

APPROACH:
  Kalshi (event ticker filtering):
    - Load political event tickers from data/kalshi_political_event_tickers.json
      (maintained by pipeline_classify_kalshi_events.py)
    - GET /markets (paginate with cursor), filtering each page to political
      event tickers immediately (never holds all markets in memory)

  Polymarket (tag-based discovery):
    - Load political tags from data/polymarket_political_tags.json
      (maintained by pipeline_refresh_political_tags.py)
    - For each political tag slug: GET /markets?tag={slug}
    - Deduplicate by condition_id across all tags

OUTPUT:
  - data/new_markets_discovered.csv (new markets not yet in index)

Usage:
    python pipeline_discover_markets_v2.py [--active-only] [--sample N]

Options:
    --active-only   Only fetch active/open Kalshi markets
    --sample N      Only keep N markets per platform (for testing)

================================================================================
"""

import json
import time
import sys
import os
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional

# =============================================================================
# CONFIGURATION
# =============================================================================

# API Base URLs
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
POLYMARKET_API_BASE = "https://gamma-api.polymarket.com"

# Output paths
from config import DATA_DIR
KALSHI_POLITICAL_TICKERS_FILE = DATA_DIR / "kalshi_political_event_tickers.json"
NEW_MARKETS_CSV = DATA_DIR / "new_markets_discovered.csv"
INDEX_FILE = DATA_DIR / "market_id_index.json"

# Rate limiting
KALSHI_RATE_LIMIT = 0.1  # 10 req/sec max
POLYMARKET_RATE_LIMIT = 0.1
MAX_RETRIES = 3


def log(msg: str):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# =============================================================================
# KALSHI API FUNCTIONS
# =============================================================================

def fetch_kalshi_markets(status: Optional[str] = None, limit: int = 1000) -> list:
    """
    Fetch all markets from Kalshi native API.

    Args:
        status: Filter by status ('open', 'closed', 'settled') or None for all
        limit: Results per page (max 1000)

    Returns:
        List of market dicts with ALL fields
    """
    all_markets = []
    cursor = None
    page = 0

    log(f"Fetching Kalshi markets (status={status or 'all'})...")

    while True:
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        if status:
            params["status"] = status

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    f"{KALSHI_API_BASE}/markets",
                    params=params,
                    headers={"Accept": "application/json"},
                    timeout=30
                )

                if response.status_code == 200:
                    data = response.json()
                    markets = data.get("markets", [])
                    all_markets.extend(markets)

                    cursor = data.get("cursor")
                    page += 1

                    log(f"  Page {page}: {len(markets)} markets (total: {len(all_markets)})")

                    if not cursor:
                        return all_markets

                    time.sleep(KALSHI_RATE_LIMIT)
                    break

                elif response.status_code == 429:
                    wait = 10 * (2 ** attempt)
                    log(f"  Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                else:
                    log(f"  Error {response.status_code}: {response.text[:200]}")
                    if attempt == MAX_RETRIES - 1:
                        return all_markets
                    time.sleep(5)

            except Exception as e:
                log(f"  Exception: {e}")
                if attempt == MAX_RETRIES - 1:
                    return all_markets
                time.sleep(5)

    return all_markets


def load_kalshi_political_event_tickers() -> set:
    """
    Load political event tickers from kalshi_political_event_tickers.json.

    Returns:
        Set of event ticker strings classified as political
    """
    if not KALSHI_POLITICAL_TICKERS_FILE.exists():
        log(f"ERROR: {KALSHI_POLITICAL_TICKERS_FILE} not found. "
            "Run pipeline_classify_kalshi_events.py first.")
        return set()

    with open(KALSHI_POLITICAL_TICKERS_FILE, "r") as f:
        all_tickers = json.load(f)

    political = {k for k, v in all_tickers.items() if v.get("is_political")}
    log(f"Loaded {len(political)} political event tickers (of {len(all_tickers)} total)")
    return political


def fetch_kalshi_political_markets(
    status: Optional[str] = None,
    existing_ids: Optional[set] = None,
    limit: int = 1000,
) -> dict:
    """
    Fetch Kalshi markets, filtering to political event tickers during pagination.

    Each page of results is filtered immediately so non-political markets are
    never accumulated in memory. Markets whose ticker is already in
    existing_ids are also skipped.

    Returns:
        Dict with 'markets' list (processed CSV-format dicts) and count metadata
    """
    political_tickers = load_kalshi_political_event_tickers()
    if not political_tickers:
        return {
            "markets": [],
            "political_event_tickers": 0,
            "total_markets_scanned": 0,
        }

    if existing_ids is None:
        existing_ids = set()

    political_markets = []
    political_event_set = set()
    total_scanned = 0
    cursor = None
    page = 0

    log(f"Fetching Kalshi political markets (status={status or 'all'})...")

    done = False
    while not done:
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        if status:
            params["status"] = status

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    f"{KALSHI_API_BASE}/markets",
                    params=params,
                    headers={"Accept": "application/json"},
                    timeout=30,
                )

                if response.status_code == 200:
                    data = response.json()
                    markets = data.get("markets", [])
                    total_scanned += len(markets)

                    # Filter this page immediately — discard non-political
                    for m in markets:
                        event_ticker = m.get("event_ticker")
                        ticker = m.get("ticker")
                        if event_ticker in political_tickers:
                            if ticker and ticker in existing_ids:
                                continue
                            political_markets.append(
                                process_kalshi_market_native(m)
                            )
                            if event_ticker:
                                political_event_set.add(event_ticker)

                    cursor = data.get("cursor")
                    page += 1

                    if page % 10 == 0 or not cursor:
                        log(f"  Page {page}: scanned {total_scanned}, "
                            f"kept {len(political_markets)} political")

                    if not cursor:
                        done = True

                    time.sleep(KALSHI_RATE_LIMIT)
                    break

                elif response.status_code == 429:
                    wait = 10 * (2 ** attempt)
                    log(f"  Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                else:
                    log(f"  Error {response.status_code}: {response.text[:200]}")
                    if attempt == MAX_RETRIES - 1:
                        done = True
                    time.sleep(5)

            except Exception as e:
                log(f"  Exception: {e}")
                if attempt == MAX_RETRIES - 1:
                    done = True
                time.sleep(5)

    log(f"Filtered to {len(political_markets)} political markets "
        f"({len(political_event_set)} events) from {total_scanned} scanned")

    return {
        "markets": political_markets,
        "political_event_tickers": len(political_tickers),
        "political_events_found": len(political_event_set),
        "total_markets_scanned": total_scanned,
    }


# =============================================================================
# POLYMARKET API FUNCTIONS
# =============================================================================

def fetch_markets_for_tag(tag_slug: str, limit: int = 100) -> list:
    """
    Fetch all markets for a single Polymarket tag slug.

    Args:
        tag_slug: The tag slug to filter by (e.g. "elections", "us-politics")
        limit: Results per page (max 100)

    Returns:
        List of market dicts
    """
    markets = []
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset, "tag": tag_slug}

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    f"{POLYMARKET_API_BASE}/markets",
                    params=params,
                    headers={"Accept": "application/json"},
                    timeout=30
                )

                if response.status_code == 200:
                    page_markets = response.json()

                    if not page_markets:
                        return markets

                    markets.extend(page_markets)

                    if len(page_markets) < limit:
                        return markets

                    offset += limit
                    time.sleep(POLYMARKET_RATE_LIMIT)
                    break

                elif response.status_code == 429:
                    wait = 10 * (2 ** attempt)
                    log(f"    Rate limited on tag={tag_slug}, waiting {wait}s...")
                    time.sleep(wait)
                else:
                    if attempt == MAX_RETRIES - 1:
                        return markets
                    time.sleep(5)

            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    return markets
                time.sleep(5)

    return markets


def fetch_polymarket_political_markets(existing_ids: Optional[set] = None) -> list:
    """
    Fetch Polymarket markets by iterating through political tag slugs.

    Loads political tags from polymarket_political_tags.json, fetches all
    markets for each tag via GET /markets?tag={slug}, deduplicates
    by condition_id, and skips markets already in existing_ids.

    Returns:
        List of processed market dicts (CSV-format)
    """
    political_tags_file = DATA_DIR / "polymarket_political_tags.json"

    if not political_tags_file.exists():
        log(f"ERROR: {political_tags_file} not found. Run pipeline_refresh_political_tags.py first.")
        return []

    with open(political_tags_file, "r") as f:
        political_tags = json.load(f)

    log(f"Loaded {len(political_tags)} political tags")

    if existing_ids is None:
        existing_ids = set()

    # Deduplicate markets by condition_id across all tags
    seen_condition_ids = set()
    processed_markets = []
    tags_with_markets = 0

    for i, tag in enumerate(political_tags):
        tag_slug = tag.get("slug", "")
        if not tag_slug:
            continue

        tag_markets = fetch_markets_for_tag(tag_slug)

        for market in tag_markets:
            condition_id = market.get("conditionId") or market.get("condition_id")
            if not condition_id or condition_id in seen_condition_ids:
                continue
            seen_condition_ids.add(condition_id)
            # Skip markets already in the index
            slug = market.get("slug")
            if condition_id in existing_ids or (slug and slug in existing_ids):
                continue
            processed_markets.append(process_polymarket_market_native(market))

        if tag_markets:
            tags_with_markets += 1

        if (i + 1) % 50 == 0 or (i + 1) == len(political_tags):
            log(f"  Tags processed: {i + 1}/{len(political_tags)}, "
                f"unique markets: {len(processed_markets)}")

    log(f"  Tags with markets: {tags_with_markets}/{len(political_tags)}")
    log(f"  Total unique political markets: {len(processed_markets)}")

    return processed_markets


# =============================================================================
# CONVERT TO CSV (for downstream pipeline compatibility)
# =============================================================================

# Polymarket tags that indicate electoral markets
ELECTORAL_TAGS = {
    "Elections", "US Election", "US Elections", "World Elections",
    "elections", "us-elections", "world-elections",
}


def process_kalshi_market_native(market: dict) -> dict:
    """Convert native Kalshi API market to pipeline CSV format."""
    status = market.get("status", "")
    result = market.get("result", "")

    # Map result to winning_outcome
    winning_outcome = None
    if result:
        winning_outcome = result.capitalize() if result in ("yes", "no") else result

    return {
        "platform": "Kalshi",
        "market_id": market.get("ticker"),
        "k_event_ticker": market.get("event_ticker"),
        "question": market.get("title"),
        "volume_usd": market.get("volume", 0),
        "k_expiration_time": market.get("expiration_time"),
        "trading_close_time": market.get("close_time"),
        "is_closed": status in ("closed", "finalized", "settled"),
        "k_status": status,
        "k_last_price": market.get("last_price"),
        "winning_outcome": winning_outcome,
        "political_category": None,  # Set by pipeline_classify_categories.py
    }


def process_polymarket_market_native(market: dict) -> dict:
    """Convert native Polymarket Gamma API market to pipeline CSV format."""
    # Parse token IDs from clobTokenIds JSON string
    pm_token_yes = None
    pm_token_no = None
    clob_tokens = market.get("clobTokenIds")
    if clob_tokens:
        try:
            tokens = json.loads(clob_tokens) if isinstance(clob_tokens, str) else clob_tokens
            if len(tokens) >= 2:
                pm_token_yes = tokens[0]
                pm_token_no = tokens[1]
            elif len(tokens) == 1:
                pm_token_yes = tokens[0]
        except (json.JSONDecodeError, TypeError):
            pass

    # Check category/tags for electoral auto-assignment
    category = market.get("category", "")
    is_electoral = category in ELECTORAL_TAGS

    return {
        "platform": "Polymarket",
        "market_id": market.get("slug"),
        "pm_condition_id": market.get("conditionId"),
        "pm_token_id_yes": pm_token_yes,
        "pm_token_id_no": pm_token_no,
        "question": market.get("question"),
        "tags": json.dumps([category] if category else []),
        "volume_usd": market.get("volumeNum") or market.get("volume") or 0,
        "scheduled_end_time": market.get("endDate"),
        "trading_close_time": market.get("closedTime"),
        "is_closed": bool(market.get("closed")),
        "pm_closed": bool(market.get("closed")),
        "winning_outcome": None,  # Not available in Gamma API list response
        "political_category": "1. ELECTORAL" if is_electoral else None,
    }


def load_market_index() -> dict:
    """Load existing market ID index for deduplication."""
    if not INDEX_FILE.exists():
        return {"polymarket": [], "kalshi": []}
    try:
        with open(INDEX_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {"polymarket": [], "kalshi": []}


def filter_new_markets(markets: list, index: dict) -> list:
    """Filter out markets already in the index."""
    pm_ids = set(str(x) for x in index.get("polymarket", []))
    kalshi_ids = set(str(x) for x in index.get("kalshi", []))

    new_markets = []
    for m in markets:
        if m["platform"] == "Polymarket":
            cid = str(m.get("pm_condition_id", ""))
            mid = str(m.get("market_id", ""))
            if cid not in pm_ids and mid not in pm_ids:
                new_markets.append(m)
        else:
            mid = str(m.get("market_id", ""))
            if mid not in kalshi_ids:
                new_markets.append(m)
    return new_markets


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main function to fetch all data from native APIs."""
    active_only = "--active-only" in sys.argv

    # Parse --sample argument
    sample_size = None
    for arg in sys.argv:
        if arg.startswith("--sample="):
            sample_size = int(arg.split("=")[1])
        elif arg == "--sample" and sys.argv.index(arg) + 1 < len(sys.argv):
            try:
                sample_size = int(sys.argv[sys.argv.index(arg) + 1])
            except ValueError:
                pass

    print("\n" + "=" * 70)
    print("PIPELINE V2: DISCOVER MARKETS (NATIVE APIs)")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {'ACTIVE ONLY' if active_only else 'ALL MARKETS'}")
    if sample_size:
        print(f"Sample size: {sample_size} per platform")
    print("=" * 70 + "\n")

    # =========================================================================
    # LOAD INDEX UPFRONT FOR EARLY DEDUP
    # =========================================================================

    index = load_market_index()
    kalshi_existing = set(str(x) for x in index.get("kalshi", []))
    pm_existing = set(str(x) for x in index.get("polymarket", []))
    log(f"Loaded index: {len(kalshi_existing)} Kalshi, {len(pm_existing)} Polymarket existing IDs")

    # =========================================================================
    # KALSHI + POLYMARKET (fetched in parallel)
    # =========================================================================

    log("\n" + "=" * 50)
    log("FETCHING KALSHI + POLYMARKET IN PARALLEL")
    log("=" * 50)

    kalshi_status = "open" if active_only else None

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=2) as executor:
        kalshi_future = executor.submit(
            fetch_kalshi_political_markets,
            status=kalshi_status,
            existing_ids=kalshi_existing,
        )
        pm_future = executor.submit(
            fetch_polymarket_political_markets,
            existing_ids=pm_existing,
        )

        kalshi_result = kalshi_future.result()
        pm_markets = pm_future.result()

    # Markets are already processed to CSV format by the fetch functions
    kalshi_markets = kalshi_result["markets"]

    if sample_size and len(kalshi_markets) > sample_size:
        kalshi_markets = kalshi_markets[:sample_size]
        log(f"  Kalshi sampled to {sample_size} markets")

    if sample_size and len(pm_markets) > sample_size:
        pm_markets = pm_markets[:sample_size]
        log(f"  Polymarket sampled to {sample_size} markets")

    log(f"\nKalshi: {len(kalshi_markets)} new political markets "
        f"(from {kalshi_result['total_markets_scanned']} scanned)")
    log(f"Polymarket: {len(pm_markets)} new political markets (via tags)")

    # =========================================================================
    # FINAL DEDUP + SAVE CSV
    # =========================================================================

    log("\n" + "=" * 50)
    log("GENERATING new_markets_discovered.csv")
    log("=" * 50)

    all_new = kalshi_markets + pm_markets

    # Final dedup pass (belt-and-suspenders against the early dedup)
    new_markets = filter_new_markets(all_new, index)

    log(f"After final dedup: {len(new_markets)} new markets "
        f"({len(all_new) - len(new_markets)} filtered)")

    # Save CSV
    if new_markets:
        df_new = pd.DataFrame(new_markets)
        df_new.to_csv(NEW_MARKETS_CSV, index=False)
        log(f"Saved: {NEW_MARKETS_CSV} ({len(new_markets)} markets)")
    else:
        # Write empty CSV with headers so downstream scripts don't break
        pd.DataFrame(columns=[
            "platform", "market_id", "pm_condition_id", "pm_token_id_yes",
            "pm_token_id_no", "k_event_ticker", "question", "tags",
            "volume_usd", "scheduled_end_time", "trading_close_time",
            "is_closed", "pm_closed", "k_status", "k_last_price",
            "k_expiration_time", "winning_outcome", "political_category",
        ]).to_csv(NEW_MARKETS_CSV, index=False)
        log(f"Saved: {NEW_MARKETS_CSV} (empty — no new markets)")

    # =========================================================================
    # SUMMARY
    # =========================================================================

    print("\n" + "=" * 70)
    print("DISCOVERY V2 COMPLETE")
    print("=" * 70)
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nOutput: {NEW_MARKETS_CSV} ({len(new_markets)} new markets)")
    print(f"\nTotals:")
    print(f"  Kalshi: {len(kalshi_markets)} political markets "
          f"(from {kalshi_result['total_markets_scanned']} scanned, "
          f"{kalshi_result['political_event_tickers']} political event tickers)")
    print(f"  Polymarket: {len(pm_markets)} political markets (via tags)")
    print(f"  Combined: {len(kalshi_markets) + len(pm_markets)} political markets")
    print("=" * 70 + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
