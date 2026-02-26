#!/usr/bin/env python3
"""
Incremental Price Fetcher for Polymarket (Native CLOB API)

Part of the NEW Bellwether Pipeline (January 2026+)

Fetches only NEW price data since the last update using the Polymarket
CLOB prices-history endpoint (no Dome API dependency).

Writes directly to CORRECTED.json (v1.json preserved as raw backup).
Used by pipeline_daily_refresh.py for incremental updates.

Usage:
    python pull_polymarket_prices.py [--full-refresh]

Options:
    --full-refresh  Fetch ALL markets regardless of close date (for initial run)
"""

import pandas as pd
import requests
import json
import time
import os
import sys
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import BASE_DIR, DATA_DIR, atomic_write_json
MASTER_FILE = str(DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv")
PRICES_FILE = str(DATA_DIR / "polymarket_all_political_prices_CORRECTED.json")

# Polymarket CLOB API
CLOB_API_URL = "https://clob.polymarket.com/prices-history"

# Rate limiting & parallelism
NUM_WORKERS = 10
RATE_LIMIT_DELAY = 0.12  # ~80 req/sec across all workers
MAX_RETRIES = 3
RETRY_DELAY = 5

# Thread-safe state
_lock = threading.Lock()
_results = {}  # token_id -> new prices list
_counters = {"updated": 0, "errors": 0, "empty": 0}


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


class RateLimiter:
    """Thread-safe rate limiter."""
    def __init__(self, delay):
        self._delay = delay
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            if elapsed < self._delay:
                time.sleep(self._delay - elapsed)
            self._last = time.monotonic()


_rate_limiter = RateLimiter(RATE_LIMIT_DELAY)


def fetch_market_prices(token_id):
    """Fetch price history for a single Polymarket token via CLOB API."""
    for attempt in range(MAX_RETRIES):
        try:
            _rate_limiter.wait()

            response = requests.get(
                CLOB_API_URL,
                params={
                    'market': token_id,
                    'interval': 'max',
                    'fidelity': 1440  # Daily candles
                },
                timeout=15
            )

            if response.status_code == 200:
                data = response.json()
                history = data.get('history', [])

                prices = []
                for point in history:
                    if isinstance(point, dict) and 't' in point and 'p' in point:
                        prices.append({
                            't': point['t'],
                            'p': float(point['p']) if isinstance(point['p'], str) else point['p']
                        })
                return prices

            elif response.status_code == 429:
                wait_time = 10 * (2 ** attempt)
                time.sleep(wait_time)
                continue
            elif response.status_code == 404:
                return []
            else:
                return None

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            return None

    return None


def get_last_price_ts(existing_prices, token_id):
    """Get the most recent price timestamp for a token, or None if no prices."""
    if token_id in existing_prices and existing_prices[token_id]:
        return max(p['t'] for p in existing_prices[token_id])
    return None


def parse_close_time(close_time_val):
    """Parse trading_close_time to a timezone-naive datetime, or None."""
    if pd.isna(close_time_val):
        return None
    try:
        dt = pd.to_datetime(close_time_val, errors='coerce')
        if pd.isna(dt):
            return None
        if dt.tzinfo is not None:
            dt = dt.tz_localize(None)
        return dt
    except:
        return None


def process_market(token_id, existing_prices, is_closed, close_time, now_ts, full_history_start_ts, full_refresh=False):
    """Process a single market: decide whether to fetch and do it."""
    DAILY_BUFFER = 86400

    last_price_ts = get_last_price_ts(existing_prices, token_id)

    if not full_refresh:
        if is_closed:
            if close_time:
                close_ts = int(close_time.timestamp())
                if last_price_ts and last_price_ts >= close_ts - DAILY_BUFFER:
                    return token_id, "skip_complete", None
            else:
                if last_price_ts and last_price_ts >= now_ts - DAILY_BUFFER:
                    return token_id, "skip_uptodate", None
        else:
            if last_price_ts and last_price_ts >= now_ts - DAILY_BUFFER:
                return token_id, "skip_uptodate", None

    prices = fetch_market_prices(token_id)

    if prices:
        # Filter to only new prices (after last known timestamp)
        if last_price_ts:
            prices = [p for p in prices if p['t'] > last_price_ts]

        if prices:
            return token_id, "updated", prices
        else:
            return token_id, "skip_uptodate", None
    elif prices is None:
        return token_id, "error", None
    else:
        return token_id, "empty", None


def main():
    full_refresh = "--full-refresh" in sys.argv

    log("=" * 60)
    log("POLYMARKET PRICE FETCH (CLOB API)")
    log(f"Mode: {'FULL REFRESH' if full_refresh else 'INCREMENTAL'}")
    log(f"Workers: {NUM_WORKERS}")
    log("=" * 60)

    now_ts = int(datetime.now().timestamp())
    one_year_ago = datetime.now() - timedelta(days=364)
    full_history_start_ts = int(one_year_ago.timestamp())

    # Load master data
    log("Loading master data...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    pm_markets = df[
        (df['platform'] == 'Polymarket') &
        (df['pm_condition_id'].notna()) &
        (df['pm_token_id_yes'].notna())
    ].copy()

    log(f"Found {len(pm_markets)} Polymarket markets with token IDs")

    # Load existing prices
    existing_prices = {}
    if os.path.exists(PRICES_FILE):
        with open(PRICES_FILE, 'r') as f:
            existing_prices = json.load(f)
        log(f"Loaded {len(existing_prices)} existing price records")

    # Counters
    updated = 0
    errors = 0
    skipped_complete = 0
    skipped_up_to_date = 0

    # Analyze markets
    closed_markets = pm_markets[pm_markets['is_closed'] == True]
    open_markets = pm_markets[pm_markets['is_closed'] == False]
    log(f"Closed markets: {len(closed_markets)}, Open markets: {len(open_markets)}")

    # Build work items
    work_items = []
    for idx, row in pm_markets.iterrows():
        token_id = str(row['pm_token_id_yes'])
        is_closed = row.get('is_closed', False)
        close_time = parse_close_time(row.get('trading_close_time'))
        work_items.append((token_id, existing_prices, is_closed, close_time, now_ts, full_history_start_ts, full_refresh))

    log(f"Submitting {len(work_items)} markets to {NUM_WORKERS} workers...")

    # Process in parallel
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {
            executor.submit(process_market, *item): item[0]
            for item in work_items
        }

        completed = 0
        for future in as_completed(futures):
            token_id, status, prices = future.result()

            if status == "updated" and prices:
                with _lock:
                    if token_id in existing_prices:
                        existing_prices[token_id].extend(prices)
                        existing_prices[token_id].sort(key=lambda x: x['t'])
                    else:
                        existing_prices[token_id] = prices
                    updated += 1
            elif status == "error":
                errors += 1
            elif status == "skip_complete":
                skipped_complete += 1
            elif status == "skip_uptodate":
                skipped_up_to_date += 1

            completed += 1
            if completed % 200 == 0:
                log(f"Progress: {completed}/{len(work_items)} "
                    f"({updated} updated, {errors} errors, "
                    f"{skipped_complete + skipped_up_to_date} skipped)")

    # Save updated prices
    atomic_write_json(PRICES_FILE, existing_prices)

    log("=" * 60)
    log("COMPLETE")
    log(f"  Updated: {updated}")
    log(f"  Errors: {errors}")
    log(f"  Skipped (complete history): {skipped_complete}")
    log(f"  Skipped (already up-to-date): {skipped_up_to_date}")
    log(f"  Total price records: {len(existing_prices)}")
    log("=" * 60)

if __name__ == "__main__":
    main()
