#!/usr/bin/env python3
"""
Pull Trade-Level Data for VWAP Computation

Fetches trade data from the public Polymarket Data API for all resolved
Polymarket political markets. Used for "Researcher Degrees of Freedom"
paper VWAP analysis.

Data needed per trade: price, size, timestamp, token_id
We pull trades for a 24-hour window around the truncation point for each market.

Usage:
    python pull_trades_for_vwap.py [--max-workers 50] [--cutoff-date 2026-02-10]
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from threading import Lock

import pandas as pd
import requests

# Add scripts dir to path
sys.path.insert(0, str(Path(__file__).parent))
from config import BASE_DIR, DATA_DIR

# Constants
DATA_API_BASE = "https://data-api.polymarket.com"
OUTPUT_FILE = DATA_DIR / "trades_for_vwap.json"
CHECKPOINT_FILE = DATA_DIR / "trades_for_vwap_checkpoint.json"
MASTER_CSV = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
ELECTION_DATES_CSV = DATA_DIR / "election_dates_lookup.csv"

# VWAP window: pull trades from 24 hours before truncation to truncation time
VWAP_WINDOW_HOURS = 24

# Rate limiting
RATE_LIMIT_DELAY = 0.1  # 100ms between requests
MAX_RETRIES = 3

# Progress tracking
progress_lock = Lock()
completed_count = 0
error_count = 0
total_trades = 0


class RateLimiter:
    """Thread-safe rate limiter."""
    def __init__(self, delay):
        self._delay = delay
        self._lock = Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            if elapsed < self._delay:
                time.sleep(self._delay - elapsed)
            self._last = time.monotonic()


_rate_limiter = RateLimiter(RATE_LIMIT_DELAY)


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get_truncation_timestamp(row, election_dates_lookup):
    """
    Determine the truncation timestamp for a market.

    - Electoral markets: 00:00 UTC on election day
    - Non-electoral: trading_close_time - 24 hours

    Returns Unix timestamp or None.
    """
    category = str(row.get('political_category', ''))

    # Electoral markets: use election date
    if category.startswith('1. ELECTORAL'):
        key = (
            str(row.get('country', '')).strip(),
            str(row.get('office', '')).strip(),
            str(row.get('location', '')).strip(),
            int(row.get('election_year')) if pd.notna(row.get('election_year')) else None
        )
        if key in election_dates_lookup:
            return int(election_dates_lookup[key].timestamp())

    # Non-electoral: use trading_close_time - 24 hours
    close_time = row.get('trading_close_time')
    if pd.notna(close_time):
        try:
            dt = pd.to_datetime(close_time)
            if dt.tzinfo is None:
                dt = dt.tz_localize('UTC')
            # Subtract 24 hours to get pre-event price
            truncation_dt = dt - timedelta(hours=24)
            return int(truncation_dt.timestamp())
        except:
            pass

    return None


def fetch_trades_for_condition(condition_id, start_ts, end_ts):
    """
    Fetch all trades for a condition ID within a time window from the Data API.

    Returns list of trades with: price, shares (size), timestamp, token_id (asset), side
    """
    all_trades = []
    offset = 0
    limit = 1000  # Data API caps at 1000 per page
    max_offset = 10000  # Data API max offset

    while offset <= max_offset:
        for retry in range(MAX_RETRIES):
            try:
                _rate_limiter.wait()
                resp = requests.get(
                    f"{DATA_API_BASE}/trades",
                    params={
                        "market": condition_id,
                        "limit": limit,
                        "offset": offset
                    },
                    timeout=60
                )

                if resp.status_code == 429:
                    wait_time = 2 ** (retry + 1)
                    time.sleep(wait_time)
                    continue

                resp.raise_for_status()
                trades = resp.json()

                if not isinstance(trades, list):
                    trades = trades.get('data', [])

                if not trades:
                    return all_trades

                # Filter to trades within our time window and map fields
                for trade in trades:
                    ts = trade.get('timestamp')
                    # Parse timestamp - Data API returns ISO string or unix
                    if isinstance(ts, str):
                        try:
                            ts = int(datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp())
                        except:
                            continue
                    elif ts is None:
                        continue

                    if start_ts <= ts <= end_ts:
                        all_trades.append({
                            'price': trade.get('price'),
                            'shares': trade.get('size', 0),           # size -> shares
                            'timestamp': ts,
                            'token_id': trade.get('asset', ''),       # asset -> token_id
                            'side': trade.get('side', '')
                        })

                if len(trades) < limit:
                    return all_trades

                offset += len(trades)
                break

            except requests.exceptions.RequestException as e:
                if retry < MAX_RETRIES - 1:
                    time.sleep(2 ** retry)
                else:
                    return all_trades

    return all_trades


def process_market(market_row, truncation_ts, worker_id):
    """Process a single market: fetch trades around truncation point."""
    global completed_count, error_count, total_trades

    condition_id = market_row['pm_condition_id']
    market_id = market_row['market_id']

    # Time window: 24 hours before truncation to truncation
    start_ts = truncation_ts - (VWAP_WINDOW_HOURS * 3600)
    end_ts = truncation_ts

    try:
        trades = fetch_trades_for_condition(condition_id, start_ts, end_ts)

        with progress_lock:
            completed_count += 1
            total_trades += len(trades)
            if completed_count % 100 == 0:
                log(f"Progress: {completed_count} markets, {total_trades} trades")

        return {
            'market_id': str(market_id),
            'condition_id': condition_id,
            'truncation_ts': truncation_ts,
            'window_start': start_ts,
            'window_end': end_ts,
            'trades': trades,
            'trade_count': len(trades)
        }

    except Exception as e:
        with progress_lock:
            error_count += 1
        return {
            'market_id': str(market_id),
            'condition_id': condition_id,
            'error': str(e),
            'trades': [],
            'trade_count': 0
        }


def load_checkpoint():
    """Load checkpoint of already-processed markets."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {'processed_markets': set(), 'results': []}


def save_checkpoint(processed_ids, results):
    """Save checkpoint."""
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump({
            'processed_markets': list(processed_ids),
            'results_count': len(results),
            'last_update': datetime.now().isoformat()
        }, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description='Pull trade data for VWAP computation')
    parser.add_argument('--max-workers', type=int, default=50, help='Number of parallel workers')
    parser.add_argument('--cutoff-date', type=str, default='2026-02-10', help='Only include markets closed before this date')
    parser.add_argument('--resume', action='store_true', help='Resume from checkpoint')
    args = parser.parse_args()

    cutoff_dt = pd.to_datetime(args.cutoff_date).tz_localize('UTC')

    log("=" * 70)
    log("FETCHING TRADE DATA FOR VWAP COMPUTATION (Polymarket Data API)")
    log(f"Max workers: {args.max_workers}")
    log(f"Cutoff date: {args.cutoff_date}")
    log("=" * 70)

    # Load election dates
    log("Loading election dates...")
    election_dates_df = pd.read_csv(ELECTION_DATES_CSV)
    election_dates_lookup = {}
    for _, row in election_dates_df.iterrows():
        key = (
            str(row['country']).strip(),
            str(row['office']).strip(),
            str(row['location']).strip(),
            int(row['election_year']) if pd.notna(row['election_year']) else None
        )
        dt = pd.to_datetime(row['election_date'])
        election_dates_lookup[key] = dt.replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)
    log(f"Loaded {len(election_dates_lookup)} election dates")

    # Load master data
    log("Loading master data...")
    df = pd.read_csv(MASTER_CSV, low_memory=False)

    # Filter to Polymarket markets with condition IDs, resolved, before cutoff
    pm_markets = df[
        (df['platform'] == 'Polymarket') &
        (df['pm_condition_id'].notna()) &
        (df['pm_condition_id'] != '') &
        (df['resolution_outcome'].notna())  # Only resolved markets
    ].copy()

    # Apply cutoff date filter
    pm_markets['close_dt'] = pd.to_datetime(pm_markets['trading_close_time'], errors='coerce')
    pm_markets = pm_markets[pm_markets['close_dt'] < cutoff_dt].copy()

    log(f"Found {len(pm_markets)} resolved Polymarket markets before {args.cutoff_date}")

    # Compute truncation timestamps
    log("Computing truncation timestamps...")
    truncation_data = []
    for _, row in pm_markets.iterrows():
        trunc_ts = get_truncation_timestamp(row, election_dates_lookup)
        if trunc_ts:
            truncation_data.append({
                'row': row,
                'truncation_ts': trunc_ts
            })

    log(f"Computed truncation timestamps for {len(truncation_data)} markets")

    # Load checkpoint if resuming
    processed_ids = set()
    all_results = []
    if args.resume and CHECKPOINT_FILE.exists():
        checkpoint = load_checkpoint()
        processed_ids = set(checkpoint.get('processed_markets', []))
        log(f"Resuming from checkpoint: {len(processed_ids)} markets already processed")

        # Load existing results
        if OUTPUT_FILE.exists():
            with open(OUTPUT_FILE, 'r') as f:
                existing = json.load(f)
                all_results = existing.get('markets', [])

    # Filter to unprocessed markets
    to_process = [
        item for item in truncation_data
        if str(item['row']['market_id']) not in processed_ids
    ]

    log(f"Markets to process: {len(to_process)}")

    if not to_process:
        log("Nothing to process!")
        return

    # Process in parallel
    log(f"\nStarting parallel fetch with {args.max_workers} workers...")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {}
        for i, item in enumerate(to_process):
            future = executor.submit(
                process_market,
                item['row'],
                item['truncation_ts'],
                i % args.max_workers
            )
            futures[future] = item['row']['market_id']

        # Collect results
        for future in as_completed(futures):
            try:
                result = future.result()
                all_results.append(result)
                processed_ids.add(str(result['market_id']))

                # Save checkpoint every 500 markets
                if len(processed_ids) % 500 == 0:
                    save_checkpoint(processed_ids, all_results)

            except Exception as e:
                log(f"Error processing market: {e}")

    elapsed = time.time() - start_time

    # Final stats
    log("\n" + "=" * 70)
    log("COMPLETE")
    log(f"Processed: {completed_count} markets")
    log(f"Total trades: {total_trades}")
    log(f"Errors: {error_count}")
    log(f"Time: {elapsed:.1f}s ({completed_count/elapsed:.1f} markets/sec)" if elapsed > 0 else "Time: 0s")
    log("=" * 70)

    # Save final output
    output = {
        'metadata': {
            'cutoff_date': args.cutoff_date,
            'vwap_window_hours': VWAP_WINDOW_HOURS,
            'total_markets': len(all_results),
            'total_trades': sum(r.get('trade_count', 0) for r in all_results),
            'generated_at': datetime.now().isoformat()
        },
        'markets': all_results
    }

    log(f"Saving to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f)

    log(f"Saved {len(all_results)} markets with trade data")

    # Cleanup checkpoint
    if CHECKPOINT_FILE.exists():
        os.remove(CHECKPOINT_FILE)
        log("Removed checkpoint file")


if __name__ == '__main__':
    main()
