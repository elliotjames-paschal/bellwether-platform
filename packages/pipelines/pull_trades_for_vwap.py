#!/usr/bin/env python3
"""
Pull Trade-Level Data for VWAP Computation

Fetches trade data from Dome API for all Polymarket political markets.
Used for "Researcher Degrees of Freedom" paper VWAP analysis.

Data needed per trade: price, size, timestamp, token_id
We pull trades for a 24-hour window around the truncation point for each market.

Usage:
    python pull_trades_for_vwap.py [--max-workers 50] [--cutoff-date 2026-02-10]

Dev tier: 100 queries/sec → use 50 parallel workers with small delays
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
from config import BASE_DIR, DATA_DIR, get_dome_api_key

# Constants
DOME_API_BASE = "https://api.domeapi.io/v1"
OUTPUT_FILE = DATA_DIR / "trades_for_vwap.json"
CHECKPOINT_FILE = DATA_DIR / "trades_for_vwap_checkpoint.json"
MASTER_CSV = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
ELECTION_DATES_CSV = DATA_DIR / "election_dates_lookup.csv"

# VWAP window: pull trades from 24 hours before truncation to truncation time
VWAP_WINDOW_HOURS = 24

# Rate limiting for dev tier (100 req/sec)
# With 50 workers, each worker needs ~0.5s between requests
WORKER_DELAY = 0.5
MAX_RETRIES = 3

# Progress tracking
progress_lock = Lock()
completed_count = 0
error_count = 0
total_trades = 0


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


def fetch_trades_for_condition(condition_id, start_ts, end_ts, api_key):
    """
    Fetch all trades for a condition ID within a time window.

    Returns list of trades with: price, shares_normalized, timestamp, token_id
    """
    headers = {"Authorization": api_key}
    all_trades = []
    offset = 0
    limit = 100
    max_pages = 500  # Safety limit

    for page in range(max_pages):
        url = f"{DOME_API_BASE}/polymarket/orders"
        params = {
            "condition_id": condition_id,
            "limit": limit,
            "offset": offset
        }

        for retry in range(MAX_RETRIES):
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=60)

                if resp.status_code == 429:
                    # Rate limited - wait and retry
                    wait_time = 2 ** (retry + 1)
                    time.sleep(wait_time)
                    continue

                resp.raise_for_status()
                data = resp.json()

                orders = data if isinstance(data, list) else data.get('orders', data.get('data', []))

                # Filter to trades within our time window
                window_trades = []
                for order in orders:
                    ts = order.get('timestamp')
                    if ts and start_ts <= ts <= end_ts:
                        window_trades.append({
                            'price': order.get('price'),
                            'shares': order.get('shares_normalized', order.get('shares', 0)),
                            'timestamp': ts,
                            'token_id': order.get('token_id'),
                            'side': order.get('side')
                        })
                    elif ts and ts < start_ts:
                        # Orders are returned newest-first, so if we hit timestamps before our window,
                        # we've passed it and can stop
                        return all_trades

                all_trades.extend(window_trades)

                if len(orders) < limit:
                    # Reached end of data
                    return all_trades

                offset += len(orders)
                break

            except requests.exceptions.RequestException as e:
                if retry < MAX_RETRIES - 1:
                    time.sleep(2 ** retry)
                else:
                    return all_trades

        time.sleep(0.02)  # Small delay between pages

    return all_trades


def process_market(market_row, truncation_ts, api_key, worker_id):
    """Process a single market: fetch trades around truncation point."""
    global completed_count, error_count, total_trades

    condition_id = market_row['pm_condition_id']
    market_id = market_row['market_id']

    # Time window: 24 hours before truncation to truncation
    start_ts = truncation_ts - (VWAP_WINDOW_HOURS * 3600)
    end_ts = truncation_ts

    try:
        trades = fetch_trades_for_condition(condition_id, start_ts, end_ts, api_key)

        with progress_lock:
            completed_count += 1
            total_trades += len(trades)
            if completed_count % 100 == 0:
                log(f"Progress: {completed_count} markets, {total_trades} trades")

        time.sleep(WORKER_DELAY)  # Rate limit

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
    with open(CHECKPOINT_FILE, 'w') as f:
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
    log("FETCHING TRADE DATA FOR VWAP COMPUTATION")
    log(f"Max workers: {args.max_workers}")
    log(f"Cutoff date: {args.cutoff_date}")
    log("=" * 70)

    # Get API key
    api_key = get_dome_api_key()
    log("✓ API key loaded")

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
    log(f"✓ Loaded {len(election_dates_lookup)} election dates")

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

    log(f"✓ Found {len(pm_markets)} resolved Polymarket markets before {args.cutoff_date}")

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

    log(f"✓ Computed truncation timestamps for {len(truncation_data)} markets")

    # Load checkpoint if resuming
    processed_ids = set()
    all_results = []
    if args.resume and CHECKPOINT_FILE.exists():
        checkpoint = load_checkpoint()
        processed_ids = set(checkpoint.get('processed_markets', []))
        log(f"✓ Resuming from checkpoint: {len(processed_ids)} markets already processed")

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
                api_key,
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
    log(f"Time: {elapsed:.1f}s ({completed_count/elapsed:.1f} markets/sec)")
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
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f)

    log(f"✓ Saved {len(all_results)} markets with trade data")

    # Cleanup checkpoint
    if CHECKPOINT_FILE.exists():
        os.remove(CHECKPOINT_FILE)
        log("✓ Removed checkpoint file")


if __name__ == '__main__':
    main()
