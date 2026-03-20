#!/usr/bin/env python3
"""
Incremental Price Fetcher for Kalshi (Native Candlestick API)

Part of the NEW Bellwether Pipeline (January 2026+)

Fetches Kalshi price data via the native series candlestick endpoint
(no Dome API dependency). Falls back to the Kalshi /markets/trades
endpoint for markets where candlesticks are unavailable.

Writes directly to CORRECTED_v3.json.

Usage:
    python pull_kalshi_prices.py [--full-refresh]

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
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import BASE_DIR, DATA_DIR, atomic_write_json
MASTER_FILE = str(DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv")
PRICES_FILE = str(DATA_DIR / "kalshi_all_political_prices_CORRECTED_v3.json")

# Kalshi Native API
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Rate limiting & parallelism
NUM_WORKERS = 10
RATE_LIMIT_DELAY = 0.15  # ~65 req/sec across all workers (conservative for Kalshi)
MAX_RETRIES = 3
RETRY_DELAY = 5

# Thread-safe state
_lock = threading.Lock()
_fallback_count = 0


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


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


def derive_series_ticker(ticker):
    """Derive series ticker from market ticker (everything before last hyphen).

    e.g. PRES-2024-DT -> PRES-2024, KXSENATE-26-GA-R -> KXSENATE-26-GA
    """
    parts = ticker.split('-')
    if len(parts) > 1:
        return '-'.join(parts[:-1])
    return ticker


def fetch_kalshi_candlesticks(ticker, start_ts, end_ts):
    """Fetch candlestick data from Kalshi native series endpoint."""
    series_ticker = derive_series_ticker(ticker)

    for attempt in range(MAX_RETRIES):
        try:
            _rate_limiter.wait()

            response = requests.get(
                f"{KALSHI_API_BASE}/series/{series_ticker}/markets/{ticker}/candlesticks",
                params={
                    'period_interval': 1440,  # Daily candles
                    'start_ts': start_ts,
                    'end_ts': end_ts
                },
                timeout=15
            )

            if response.status_code == 200:
                data = response.json()
                return data.get('candlesticks', [])
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


def fetch_kalshi_trades_direct(ticker, start_ts, end_ts):
    """
    Fetch trades directly from Kalshi API and convert to daily candlesticks.
    Fallback when the candlestick endpoint returns no data.
    """
    global _fallback_count

    all_trades = []
    cursor = None

    for attempt in range(MAX_RETRIES):
        try:
            while True:
                _rate_limiter.wait()

                params = {
                    'ticker': ticker,
                    'min_ts': start_ts,
                    'max_ts': end_ts,
                    'limit': 1000
                }
                if cursor:
                    params['cursor'] = cursor

                response = requests.get(
                    f"{KALSHI_API_BASE}/markets/trades",
                    params=params,
                    timeout=30
                )

                if response.status_code == 200:
                    data = response.json()
                    trades = data.get('trades', [])
                    all_trades.extend(trades)

                    cursor = data.get('cursor')
                    if not cursor or not trades:
                        break
                elif response.status_code == 429:
                    time.sleep(10 * (2 ** attempt))
                    break
                else:
                    return None

            if all_trades:
                break

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            return None

    if not all_trades:
        return []

    with _lock:
        _fallback_count += 1

    # Convert trades to daily candlesticks
    daily_trades = defaultdict(list)

    for trade in all_trades:
        created_time = trade.get('created_time', '')
        if not created_time:
            continue
        try:
            dt = datetime.fromisoformat(created_time.replace('Z', '+00:00'))
            day_key = dt.strftime('%Y-%m-%d')
            daily_trades[day_key].append(trade)
        except:
            continue

    candlesticks = []
    for day, trades in sorted(daily_trades.items()):
        if not trades:
            continue

        prices = [t.get('yes_price', 0) for t in trades if t.get('yes_price') is not None]
        volumes = [t.get('count', 0) for t in trades]

        if not prices:
            continue

        day_dt = datetime.strptime(day, '%Y-%m-%d')
        end_period_ts = int((day_dt + timedelta(days=1)).timestamp())

        candlestick = {
            'end_period_ts': end_period_ts,
            'price': {
                'open': prices[0],
                'high': max(prices),
                'low': min(prices),
                'close': prices[-1],
            },
            'volume': sum(volumes),
            '_source': 'kalshi_direct'
        }
        candlesticks.append(candlestick)

    return candlesticks


def process_market(ticker, existing_prices, start_ts, end_ts, full_history_start_ts, full_refresh=False):
    """Process a single Kalshi market: fetch candlesticks, fall back to trades if empty."""
    # Determine actual start timestamp based on existing data
    if not full_refresh and ticker in existing_prices and existing_prices[ticker]:
        last_ts = max(p.get('end_period_ts', p.get('t', 0)) for p in existing_prices[ticker])
        actual_start = last_ts + 1
    else:
        actual_start = full_history_start_ts

    # Try native candlestick endpoint first
    prices = fetch_kalshi_candlesticks(ticker, actual_start, end_ts)

    # If candlesticks returned nothing, try trades fallback
    if prices is not None and len(prices) == 0:
        fallback = fetch_kalshi_trades_direct(ticker, actual_start, end_ts)
        if fallback:
            prices = fallback

    if prices:
        return ticker, "updated", prices
    elif prices is None:
        return ticker, "error", None
    else:
        return ticker, "empty", None


def main():
    global _fallback_count
    full_refresh = "--full-refresh" in sys.argv

    log("=" * 60)
    log("KALSHI PRICE FETCH (NATIVE API)")
    log(f"Mode: {'FULL REFRESH' if full_refresh else 'INCREMENTAL'}")
    log(f"Workers: {NUM_WORKERS}")
    log("=" * 60)

    # Get date range
    if full_refresh:
        default_start = '2020-01-01'
    else:
        default_start = '2024-11-10'

    start_date = os.environ.get('FETCH_START_DATE', default_start)
    end_date = os.environ.get('FETCH_END_DATE', datetime.now().strftime('%Y-%m-%d'))

    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    default_start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())
    full_history_start_ts = int(datetime(2020, 1, 1).timestamp())

    log(f"Date range: {start_date} to {end_date}")

    # Load master data
    log("Loading master data...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    kalshi_markets = df[
        (df['platform'] == 'Kalshi') &
        (df['market_id'].notna())
    ].copy()

    log(f"Found {len(kalshi_markets)} Kalshi markets")

    # Load existing prices
    existing_prices = {}
    if os.path.exists(PRICES_FILE):
        with open(PRICES_FILE, 'r') as f:
            existing_prices = json.load(f)
        log(f"Loaded {len(existing_prices)} existing price records")

    # Filter out markets that don't need fetching
    skipped = 0
    skipped_uptodate = 0
    work_items = []

    for idx, row in kalshi_markets.iterrows():
        ticker = str(row['market_id'])

        if not full_refresh:
            # Skip markets that closed long ago
            close_time = pd.to_datetime(row.get('k_expiration_time'), errors='coerce')
            if pd.notna(close_time):
                close_time_naive = close_time.tz_localize(None) if close_time.tzinfo else close_time
                if close_time_naive < start_dt - timedelta(days=7):
                    skipped += 1
                    continue

            # Skip closed/settled markets that already have prices
            k_status = str(row.get('k_status', '')).lower()
            if k_status in ('closed', 'settled', 'finalized') and ticker in existing_prices and existing_prices[ticker]:
                skipped_uptodate += 1
                continue

            # Skip open markets whose last price is already recent (within 2 days)
            if ticker in existing_prices and existing_prices[ticker]:
                last_ts = max(p.get('end_period_ts', p.get('t', 0)) for p in existing_prices[ticker])
                if end_ts - last_ts < 2 * 86400:
                    skipped_uptodate += 1
                    continue

        work_items.append((ticker, existing_prices, default_start_ts, end_ts, full_history_start_ts, full_refresh))

    log(f"Skipped {skipped} old closed markets")
    log(f"Submitting {len(work_items)} markets to {NUM_WORKERS} workers...")

    # Process in parallel
    updated = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {
            executor.submit(process_market, *item): item[0]
            for item in work_items
        }

        completed = 0
        for future in as_completed(futures):
            ticker, status, prices = future.result()

            if status == "updated" and prices:
                with _lock:
                    if ticker in existing_prices:
                        existing_prices[ticker].extend(prices)
                        existing_prices[ticker].sort(key=lambda x: x.get('end_period_ts', x.get('t', 0)))
                    else:
                        existing_prices[ticker] = prices
                    updated += 1
            elif status == "error":
                errors += 1

            completed += 1
            if completed % 200 == 0:
                log(f"Progress: {completed}/{len(work_items)} "
                    f"({updated} updated, {errors} errors)")

    # Save
    atomic_write_json(PRICES_FILE, existing_prices)

    log("=" * 60)
    log(f"COMPLETE: {updated} updated, {errors} errors, {skipped} skipped")
    log(f"Total price records: {len(existing_prices)}")
    if _fallback_count > 0:
        log(f"Used Kalshi trades API fallback for {_fallback_count} markets")
    log("=" * 60)

if __name__ == "__main__":
    main()
