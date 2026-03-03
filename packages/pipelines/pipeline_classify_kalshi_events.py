#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Classify Kalshi Political Event Tickers
================================================================================

Part of the Bellwether V2 Pipeline

This script mirrors the Polymarket tag classification approach:
1. Fetches all SERIES from the Kalshi Series API (single page, ~8K series)
2. Filters by political categories (Elections, Politics, Economics, World)
3. Fetches events for political series
4. Saves political event_tickers to kalshi_political_event_tickers.json

The Kalshi hierarchy is: Series -> Events -> Markets
Categories live on Series, so filtering at the series level is most efficient.

Usage:
    python pipeline_classify_kalshi_events.py [--full-refresh]

Options:
    --full-refresh  Reclassify all (ignore existing classifications)

Output:
    - data/kalshi_political_event_tickers.json

================================================================================
"""

import json
import sys
import time
import requests
from datetime import datetime

from config import DATA_DIR

# =============================================================================
# CONFIGURATION
# =============================================================================

KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
EVENT_TICKERS_FILE = DATA_DIR / "kalshi_political_event_tickers.json"

# Categories we consider political (from Kalshi's own category system)
POLITICAL_CATEGORIES = {"Elections", "Politics", "Economics", "World"}

MAX_RETRIES = 3
RATE_LIMIT = 0.1


def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# =============================================================================
# SERIES FETCHING
# =============================================================================

def fetch_all_series() -> list:
    """
    Fetch all series from Kalshi Series API.

    The series endpoint returns all results in a single page (~8K series).
    Each series has: ticker, title, category, tags, etc.
    """
    log("Fetching all Kalshi series...")

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                f"{KALSHI_API_BASE}/series",
                params={"limit": 10000},
                headers={"Accept": "application/json"},
                timeout=60,
            )

            if response.status_code == 200:
                data = response.json()
                series = data.get("series", [])
                log(f"  Fetched {len(series):,} series")
                return series

            elif response.status_code == 429:
                wait = 10 * (2 ** attempt)
                log(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                log(f"  Error {response.status_code}: {response.text[:200]}")
                if attempt == MAX_RETRIES - 1:
                    return []
                time.sleep(5)

        except Exception as e:
            log(f"  Exception: {e}")
            if attempt == MAX_RETRIES - 1:
                return []
            time.sleep(5)

    return []


# =============================================================================
# EVENT FETCHING (for political series only)
# =============================================================================

def fetch_events_for_series(series_ticker: str) -> list:
    """Fetch all events for a given series ticker."""
    all_events = []
    cursor = None

    while True:
        params = {"limit": 200, "series_ticker": series_ticker}
        if cursor:
            params["cursor"] = cursor

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    f"{KALSHI_API_BASE}/events",
                    params=params,
                    timeout=30,
                )

                if response.status_code == 200:
                    data = response.json()
                    events = data.get("events", [])

                    if not events:
                        return all_events

                    all_events.extend(events)
                    cursor = data.get("cursor")

                    if not cursor or len(events) < 200:
                        return all_events

                    time.sleep(RATE_LIMIT)
                    break

                elif response.status_code == 429:
                    wait = 10 * (2 ** attempt)
                    time.sleep(wait)
                else:
                    if attempt == MAX_RETRIES - 1:
                        return all_events
                    time.sleep(2)

            except Exception:
                if attempt == MAX_RETRIES - 1:
                    return all_events
                time.sleep(2)

    return all_events


# =============================================================================
# MAIN
# =============================================================================

def main():
    full_refresh = "--full-refresh" in sys.argv
    test_mode = "--test" in sys.argv

    print("\n" + "=" * 70)
    print("PIPELINE: CLASSIFY KALSHI POLITICAL EVENT TICKERS")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Political categories: {', '.join(sorted(POLITICAL_CATEGORIES))}")
    if full_refresh:
        print("MODE: FULL REFRESH")
    if test_mode:
        print("MODE: TEST (10 series only)")
    print("=" * 70 + "\n")

    # ------------------------------------------------------------------
    # Step 1: Fetch all series (single API call)
    # ------------------------------------------------------------------
    all_series = fetch_all_series()

    if not all_series:
        log("ERROR: No series fetched. Aborting.")
        return 0

    # Category breakdown
    from collections import Counter
    categories = Counter(s.get("category", "None") for s in all_series)
    log("Series by category:")
    for cat, count in categories.most_common():
        marker = " <-- POLITICAL" if cat in POLITICAL_CATEGORIES else ""
        log(f"  {count:5,}  {cat}{marker}")

    # ------------------------------------------------------------------
    # Step 2: Filter to political series
    # ------------------------------------------------------------------
    political_series = [
        s for s in all_series
        if s.get("category") in POLITICAL_CATEGORIES
    ]
    log(f"\nPolitical series: {len(political_series):,}")

    if test_mode:
        political_series = political_series[:10]
        log(f"  TEST MODE: limited to {len(political_series)} series")

    # ------------------------------------------------------------------
    # Step 3: Load existing classifications
    # ------------------------------------------------------------------
    existing = {}
    if EVENT_TICKERS_FILE.exists() and not full_refresh:
        with open(EVENT_TICKERS_FILE, 'r') as f:
            existing = json.load(f)
        log(f"Loaded existing classifications: {len(existing):,}")

    # ------------------------------------------------------------------
    # Step 4: Fetch events for political series (to get event_tickers)
    # ------------------------------------------------------------------
    log("\nFetching events for political series...")

    new_political = 0
    total_events = 0
    series_checked = 0

    # Build set of non-political series tickers (for marking non-political)
    non_political_series_tickers = {
        s.get("ticker") for s in all_series
        if s.get("category") not in POLITICAL_CATEGORIES and s.get("ticker")
    }

    now = datetime.now().isoformat()

    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    lock = threading.Lock()

    def fetch_series_events(series):
        series_ticker = series.get("ticker", "")
        if not series_ticker:
            return []
        events = fetch_events_for_series(series_ticker)
        time.sleep(RATE_LIMIT)
        return [(series, e) for e in events]

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(fetch_series_events, s): s for s in political_series}
        for future in as_completed(futures):
            pairs = future.result()
            with lock:
                series_checked += 1
                total_events += len(pairs)
                for series, event in pairs:
                    event_ticker = event.get("event_ticker", "")
                    if not event_ticker:
                        continue
                    if event_ticker not in existing:
                        existing[event_ticker] = {
                            "is_political": True,
                            "sample_title": event.get("title", ""),
                            "category": series.get("category", ""),
                            "series_ticker": series.get("ticker", ""),
                            "series_title": series.get("title", ""),
                            "classified_at": now,
                            "source": "category",
                            "votes": 3,
                        }
                        new_political += 1
                    else:
                        entry = existing[event_ticker]
                        if not entry.get("is_political"):
                            entry["is_political"] = True
                            entry["source"] = "category"
                            entry["classified_at"] = now
                            new_political += 1

                if series_checked % 100 == 0:
                    log(f"  Checked {series_checked:,}/{len(political_series):,} series "
                        f"({total_events:,} events, {new_political:,} new)")

    log(f"  Done: {series_checked:,} series, {total_events:,} events, {new_political:,} new political")

    # ------------------------------------------------------------------
    # Step 5: Mark non-political entries for events we already know about
    # ------------------------------------------------------------------
    # Any existing entry whose series_ticker is in non-political categories
    # gets marked as non-political
    reverted = 0
    for ticker, entry in existing.items():
        st = entry.get("series_ticker", "")
        if st and st in non_political_series_tickers and entry.get("is_political"):
            entry["is_political"] = False
            entry["source"] = "category-reverted"
            reverted += 1

    if reverted:
        log(f"  Reverted {reverted} entries (series moved to non-political category)")

    # ------------------------------------------------------------------
    # Step 6: Save
    # ------------------------------------------------------------------
    with open(EVENT_TICKERS_FILE, 'w') as f:
        json.dump(existing, f, indent=2)
    log(f"Saved: {EVENT_TICKERS_FILE}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    political_count = sum(1 for v in existing.values() if v.get("is_political"))
    non_political_count = sum(1 for v in existing.values() if not v.get("is_political"))

    print("\n" + "=" * 70)
    print("CLASSIFICATION COMPLETE")
    print("=" * 70)
    print(f"Total series:            {len(all_series):,}")
    print(f"Political series:        {len(political_series):,}")
    print(f"Events fetched:          {total_events:,}")
    print(f"New political tickers:   {new_political:,}")
    print(f"Total political:         {political_count:,}")
    print(f"Total non-political:     {non_political_count:,}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    return political_count


if __name__ == "__main__":
    main()
