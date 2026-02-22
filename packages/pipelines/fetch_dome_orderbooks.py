#!/usr/bin/env python3
"""
Fetch Orderbook History from Dome API (Parallel Version)

Fetches historical orderbook snapshots for all eligible political markets
(resolved after Oct 2025) from Dome API's orderbook endpoints.

Uses parallel fetching with 10 workers for ~5x speedup.

Output:
    data/orderbook_history_polymarket.json
    data/orderbook_history_kalshi.json
"""

import pandas as pd
import requests
import json
import time
import os
import sys
import threading
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import BASE_DIR, DATA_DIR, get_dome_api_key

MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
PM_OUTPUT_FILE = DATA_DIR / "orderbook_history_polymarket.json"
KALSHI_OUTPUT_FILE = DATA_DIR / "orderbook_history_kalshi.json"
CHECKPOINT_FILE = DATA_DIR / "orderbook_fetch_checkpoint.json"

# Dome API Configuration
DOME_API_BASE = "https://api.domeapi.io/v1"
DOME_API_KEY = get_dome_api_key()

# Parallelism settings
NUM_WORKERS = 10  # 10 parallel workers
MAX_RETRIES = 3
CHECKPOINT_INTERVAL = 100  # Save checkpoint every N markets

# Rate limiting (Dev tier: 100 req/sec, we'll use 80 to be safe)
# With 10 workers, each worker waits 0.125s between requests = 80 req/sec total
RATE_LIMIT_DELAY = 0.125

# Data availability dates (in ms)
PM_DATA_START = int(datetime(2025, 10, 14).timestamp() * 1000)
KALSHI_DATA_START = int(datetime(2025, 10, 29).timestamp() * 1000)

# Thread-safe counters and data
_lock = threading.Lock()
_request_times = []


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def rate_limit():
    """Simple rate limiting - just sleep a small amount per request.
    With 10 workers each sleeping 0.1s, we get ~100 req/sec total.
    """
    time.sleep(0.1)


def fetch_orderbook_snapshots(platform, identifier, start_ts, end_ts, sample_rate=1):
    """
    Fetch all orderbook snapshots for a market within a time range.

    Polymarket: Uses cursor-based pagination (pagination_key)
    Kalshi: Uses timestamp-based pagination (advance start_time)

    Args:
        sample_rate: Keep every Nth snapshot (1 = all, 10 = every 10th)
    """
    all_snapshots = []

    if platform == 'polymarket':
        endpoint = f"{DOME_API_BASE}/polymarket/orderbooks"
        id_param = 'token_id'
    else:
        endpoint = f"{DOME_API_BASE}/kalshi/orderbooks"
        id_param = 'ticker'

    # For Kalshi, use timestamp-based pagination
    # For Polymarket, use cursor-based pagination
    cursor = None
    current_start = start_ts
    snapshot_count = 0  # For sampling

    for page in range(500):  # Max 500 pages = 100,000 snapshots
        params = {
            id_param: identifier,
            'start_time': current_start,
            'end_time': end_ts,
            'limit': 200
        }

        # Only use cursor for Polymarket
        if platform == 'polymarket' and cursor:
            params['pagination_key'] = cursor

        for attempt in range(MAX_RETRIES):
            try:
                rate_limit()

                response = requests.get(
                    endpoint,
                    headers={"Authorization": DOME_API_KEY},
                    params=params,
                    timeout=30
                )

                if response.status_code == 200:
                    data = response.json()
                    snapshots = data.get('snapshots', [])

                    if not snapshots:
                        return all_snapshots

                    # Apply sampling if sample_rate > 1
                    if sample_rate > 1:
                        for s in snapshots:
                            if snapshot_count % sample_rate == 0:
                                all_snapshots.append(s)
                            snapshot_count += 1
                    else:
                        all_snapshots.extend(snapshots)

                    if platform == 'polymarket':
                        # Polymarket: use cursor-based pagination
                        pagination = data.get('pagination', {})
                        cursor = pagination.get('pagination_key') or pagination.get('paginationKey')
                        has_more = pagination.get('has_more', False)

                        if not cursor or not has_more:
                            return all_snapshots
                    else:
                        # Kalshi: use timestamp-based pagination
                        # Move start_time to after the last snapshot
                        last_ts = snapshots[-1].get('timestamp', 0)
                        if last_ts <= current_start:
                            # No progress, stop
                            return all_snapshots
                        current_start = last_ts + 1

                        if current_start >= end_ts:
                            return all_snapshots

                    break

                elif response.status_code == 429:
                    wait_time = 10 * (2 ** attempt)
                    time.sleep(wait_time)
                    continue

                elif response.status_code == 404:
                    return all_snapshots

                else:
                    time.sleep(2)
                    continue

            except Exception as e:
                time.sleep(2)
                continue
        else:
            break  # All retries failed

    return all_snapshots


def extract_metrics_from_snapshot(snapshot, platform='polymarket'):
    """Extract key metrics from an orderbook snapshot."""
    timestamp = snapshot.get('timestamp', 0)

    if platform == 'kalshi':
        orderbook = snapshot.get('orderbook', {})
        yes_bids = orderbook.get('yes', [])
        no_bids = orderbook.get('no', [])

        if yes_bids:
            best_bid = max(y[0] for y in yes_bids) / 100.0
            bid_depth = sum(y[1] for y in yes_bids)
            best_bid_size = next((y[1] for y in yes_bids if y[0] == max(y[0] for y in yes_bids)), 0)
        else:
            best_bid = None
            bid_depth = 0
            best_bid_size = 0

        if no_bids:
            best_no_bid = max(n[0] for n in no_bids)
            best_ask = 1.0 - (best_no_bid / 100.0)
            ask_depth = sum(n[1] for n in no_bids)
            best_ask_size = next((n[1] for n in no_bids if n[0] == best_no_bid), 0)
        else:
            best_ask = None
            ask_depth = 0
            best_ask_size = 0

        n_bid_levels = len(yes_bids)
        n_ask_levels = len(no_bids)

    else:
        bids = snapshot.get('bids', [])
        asks = snapshot.get('asks', [])

        if bids:
            bid_prices = [(float(b['price']), float(b['size'])) for b in bids]
            best_bid_price, best_bid_size = max(bid_prices, key=lambda x: x[0])
            best_bid = best_bid_price
            bid_depth = sum(p[1] for p in bid_prices)
        else:
            best_bid = None
            best_bid_size = 0
            bid_depth = 0

        if asks:
            ask_prices = [(float(a['price']), float(a['size'])) for a in asks]
            best_ask_price, best_ask_size = min(ask_prices, key=lambda x: x[0])
            best_ask = best_ask_price
            ask_depth = sum(p[1] for p in ask_prices)
        else:
            best_ask = None
            best_ask_size = 0
            ask_depth = 0

        n_bid_levels = len(bids)
        n_ask_levels = len(asks)

    if best_bid is not None and best_ask is not None:
        spread = best_ask - best_bid
        midpoint = (best_ask + best_bid) / 2
        relative_spread = spread / midpoint if midpoint > 0 else None
    else:
        spread = None
        midpoint = None
        relative_spread = None

    total_depth = bid_depth + ask_depth

    return {
        'timestamp': timestamp,
        'best_bid': best_bid,
        'best_ask': best_ask,
        'spread': spread,
        'midpoint': midpoint,
        'relative_spread': relative_spread,
        'bid_depth': bid_depth,
        'ask_depth': ask_depth,
        'total_depth': total_depth,
        'best_bid_size': best_bid_size,
        'best_ask_size': best_ask_size,
        'n_bid_levels': n_bid_levels,
        'n_ask_levels': n_ask_levels
    }


def process_market(row, platform):
    """Process a single market - fetch and extract metrics."""
    market_id = str(row['market_id'])

    if platform == 'polymarket':
        identifier = str(row['pm_token_id_yes'])
        data_start = PM_DATA_START
        sample_rate = 1  # Keep all snapshots for Polymarket
    else:
        identifier = market_id  # For Kalshi, market_id is the ticker
        data_start = KALSHI_DATA_START
        sample_rate = 10  # Sample every 10th for Kalshi (~30 min intervals)

    close_ts = int(row['trading_close_time'].timestamp() * 1000)
    start_ts = max(data_start, close_ts - (90 * 24 * 3600 * 1000))
    end_ts = close_ts

    snapshots = fetch_orderbook_snapshots(platform, identifier, start_ts, end_ts, sample_rate=sample_rate)

    if snapshots:
        metrics = [extract_metrics_from_snapshot(s, platform=platform) for s in snapshots]
        return market_id, {
            'token_id' if platform == 'polymarket' else 'ticker': identifier,
            'question': row['question'][:100] if pd.notna(row.get('question')) else '',
            'category': row.get('political_category', ''),
            'volume_usd': row.get('volume_usd', 0),
            'trading_close_time': str(row['trading_close_time']),
            'n_snapshots': len(snapshots),
            'metrics': metrics
        }

    return market_id, None


def save_checkpoint(processed_with_data, processed_no_data):
    """Save lightweight checkpoint to allow resuming.

    Only stores IDs, not data. Data is stored in output files.
    - processed_with_data: Markets that returned snapshots (don't retry)
    - processed_no_data: Markets that returned 0 snapshots (retry on next run)
    """
    checkpoint = {
        'processed_with_data': list(processed_with_data),
        'processed_no_data': list(processed_no_data),
        'timestamp': datetime.now().isoformat()
    }
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f)


def load_checkpoint():
    """Load checkpoint if exists.

    Returns (processed_with_data, processed_no_data) sets.
    Data is NOT stored in checkpoint - it's loaded from output files.
    """
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE) as f:
                checkpoint = json.load(f)

            # New format: just ID sets
            processed_with_data = set(checkpoint.get('processed_with_data', []))
            processed_no_data = set(checkpoint.get('processed_no_data', []))

            # Backwards compatibility: old format stored data in checkpoint
            # Extract IDs from old format if present
            if 'pm_data' in checkpoint:
                processed_with_data.update(checkpoint['pm_data'].keys())
            if 'kalshi_data' in checkpoint:
                processed_with_data.update(checkpoint['kalshi_data'].keys())
            if 'processed_ids' in checkpoint:
                # Old processed_ids - assume they had data
                processed_with_data.update(checkpoint['processed_ids'])

            return processed_with_data, processed_no_data
        except:
            pass
    return set(), set()


def main():
    log("=" * 60)
    log("FETCHING ORDERBOOK HISTORY FROM DOME API (PARALLEL)")
    log(f"Using {NUM_WORKERS} parallel workers")
    log("=" * 60)

    # Load master data
    log("\n1. Loading eligible markets...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    df['trading_close_time'] = pd.to_datetime(
        df['trading_close_time'], format='mixed', utc=True, errors='coerce'
    )

    # Filter to resolved markets after Oct 14, 2025
    oct_14 = pd.Timestamp('2025-10-14', tz='UTC')
    resolved = df[df['winning_outcome'].isin(['Yes', 'No'])]
    eligible = resolved[resolved['trading_close_time'] >= oct_14].copy()

    # Split by platform
    pm_markets = eligible[
        (eligible['platform'] == 'Polymarket') &
        (eligible['pm_token_id_yes'].notna())
    ].copy()

    kalshi_markets = eligible[
        (eligible['platform'] == 'Kalshi') &
        (eligible['market_id'].notna())
    ].copy()

    log(f"   Eligible markets: {len(eligible):,}")
    log(f"   - Polymarket: {len(pm_markets):,}")
    log(f"   - Kalshi: {len(kalshi_markets):,}")

    # Load checkpoint (lightweight - just IDs)
    processed_with_data, processed_no_data = load_checkpoint()

    # Always load actual data from output files
    pm_data = {}
    kalshi_data = {}

    if PM_OUTPUT_FILE.exists():
        try:
            with open(PM_OUTPUT_FILE) as f:
                pm_data = json.load(f)
            processed_with_data.update(pm_data.keys())
            log(f"\n   Loaded {len(pm_data):,} existing Polymarket markets")
        except:
            pass

    if KALSHI_OUTPUT_FILE.exists():
        try:
            with open(KALSHI_OUTPUT_FILE) as f:
                kalshi_data = json.load(f)
            processed_with_data.update(kalshi_data.keys())
            log(f"   Loaded {len(kalshi_data):,} existing Kalshi markets")
        except:
            pass

    if processed_with_data:
        log(f"\n   Skipping {len(processed_with_data):,} markets already with data")
    if processed_no_data:
        log(f"   Retrying {len(processed_no_data):,} markets that previously had no data")

    # Process Polymarket markets in parallel
    log("\n2. Fetching Polymarket orderbooks...")
    pm_success = 0
    pm_empty = 0
    pm_to_process = [(_, row) for _, row in pm_markets.iterrows()
                      if str(row['market_id']) not in processed_with_data]

    log(f"   {len(pm_to_process)} markets to fetch")

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {
            executor.submit(process_market, row, 'polymarket'): row
            for _, row in pm_to_process
        }

        completed = 0
        for future in as_completed(futures):
            market_id, result = future.result()

            with _lock:
                if result:
                    pm_data[market_id] = result
                    pm_success += 1
                    processed_with_data.add(market_id)
                    processed_no_data.discard(market_id)  # Remove from no-data set if present
                else:
                    pm_empty += 1
                    processed_no_data.add(market_id)

                completed += 1

                if completed % CHECKPOINT_INTERVAL == 0:
                    log(f"   Processed {completed}/{len(pm_to_process)} PM "
                        f"(success: {pm_success}, empty: {pm_empty})")
                    save_checkpoint(processed_with_data, processed_no_data)
                    # Also save data to output file periodically
                    with open(PM_OUTPUT_FILE, 'w') as f:
                        json.dump(pm_data, f)

    log(f"   Polymarket complete: {pm_success} with data, {pm_empty} empty")
    save_checkpoint(processed_with_data, processed_no_data)

    # Process Kalshi markets in parallel
    log("\n3. Fetching Kalshi orderbooks...")
    k_success = 0
    k_empty = 0
    kalshi_to_process = [(_, row) for _, row in kalshi_markets.iterrows()
                          if str(row['market_id']) not in processed_with_data]

    log(f"   {len(kalshi_to_process)} markets to fetch")

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {
            executor.submit(process_market, row, 'kalshi'): row
            for _, row in kalshi_to_process
        }

        completed = 0
        for future in as_completed(futures):
            market_id, result = future.result()

            with _lock:
                if result:
                    kalshi_data[market_id] = result
                    k_success += 1
                    processed_with_data.add(market_id)
                    processed_no_data.discard(market_id)  # Remove from no-data set if present
                else:
                    k_empty += 1
                    processed_no_data.add(market_id)

                completed += 1

                if completed % CHECKPOINT_INTERVAL == 0:
                    log(f"   Processed {completed}/{len(kalshi_to_process)} Kalshi "
                        f"(success: {k_success}, empty: {k_empty})")
                    save_checkpoint(processed_with_data, processed_no_data)
                    # Also save data to output file periodically
                    with open(KALSHI_OUTPUT_FILE, 'w') as f:
                        json.dump(kalshi_data, f)

    log(f"   Kalshi complete: {k_success} with data, {k_empty} empty")

    # Save final results
    log("\n4. Saving results...")

    with open(PM_OUTPUT_FILE, 'w') as f:
        json.dump(pm_data, f)
    log(f"   Saved {len(pm_data):,} Polymarket markets to {PM_OUTPUT_FILE.name}")

    with open(KALSHI_OUTPUT_FILE, 'w') as f:
        json.dump(kalshi_data, f)
    log(f"   Saved {len(kalshi_data):,} Kalshi markets to {KALSHI_OUTPUT_FILE.name}")

    # Save final checkpoint (keep track of no-data markets for retry)
    save_checkpoint(processed_with_data, processed_no_data)
    if processed_no_data:
        log(f"\n   Checkpoint saved with {len(processed_no_data):,} markets to retry")

    # Summary
    log("\n" + "=" * 60)
    log("SUMMARY")
    log("=" * 60)
    log(f"Markets with orderbook data:")
    log(f"  Polymarket: {len(pm_data):,}")
    log(f"  Kalshi: {len(kalshi_data):,}")
    log(f"  Total: {len(pm_data) + len(kalshi_data):,}")
    if processed_no_data:
        log(f"\nMarkets with no data (will retry next run): {len(processed_no_data):,}")

    if pm_data:
        total_snapshots = sum(m['n_snapshots'] for m in pm_data.values())
        log(f"\nPolymarket snapshots: {total_snapshots:,} total")
        log(f"  Avg per market: {total_snapshots / len(pm_data):.1f}")

    if kalshi_data:
        total_snapshots = sum(m['n_snapshots'] for m in kalshi_data.values())
        log(f"\nKalshi snapshots: {total_snapshots:,} total")
        log(f"  Avg per market: {total_snapshots / len(kalshi_data):.1f}")

    log("\nDone!")


if __name__ == "__main__":
    main()
