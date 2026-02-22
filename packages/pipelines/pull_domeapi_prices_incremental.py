#!/usr/bin/env python3
"""
Incremental Dome API Price Fetcher for Polymarket

Part of the NEW Bellwether Pipeline (January 2026+)

Fetches only NEW price data since the last update.
Writes directly to CORRECTED.json (v1.json preserved as raw backup).
Used by pipeline_daily_refresh.py for incremental updates.

Usage:
    python pull_domeapi_prices_incremental.py [--full-refresh]

Options:
    --full-refresh  Fetch ALL markets regardless of close date (for initial run)
"""

import pandas as pd
import requests
import json
import time
import os
import sys
from datetime import datetime, timedelta

# Paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import BASE_DIR, DATA_DIR, get_dome_api_key
MASTER_FILE = str(DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv")
PRICES_FILE = str(DATA_DIR / "polymarket_all_political_prices_DOMEAPI_CORRECTED.json")

# Dome API Configuration (Dev tier: 100 queries/sec)
DOME_API_BASE = "https://api.domeapi.io/v1/polymarket"
DOME_API_KEY = get_dome_api_key()

# Rate limit: 0.01s = 100 req/sec (dev tier)
RATE_LIMIT_DELAY = float(os.environ.get('DOME_RATE_LIMIT', '0.01'))
MAX_RETRIES = 3

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def fetch_market_prices(condition_id, token_id_yes, start_time, end_time):
    """Fetch price data for a single market from Dome API."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                f"{DOME_API_BASE}/candlesticks/{condition_id}",
                headers={"Authorization": DOME_API_KEY},
                params={
                    'start_time': start_time,
                    'end_time': end_time,
                    'interval': 1440  # Daily candles
                },
                timeout=15
            )

            if response.status_code == 200:
                data = response.json()
                candlesticks = data.get('candlesticks', [])

                for token_data in candlesticks:
                    if len(token_data) != 2:
                        continue

                    candle_array = token_data[0]
                    token_info = token_data[1]
                    this_token_id = str(token_info.get('token_id', ''))

                    if this_token_id == str(token_id_yes):
                        prices = []
                        for candle in candle_array:
                            timestamp = candle.get('end_period_ts')
                            price_cents = candle.get('price', {}).get('close', 0)
                            prices.append({
                                't': timestamp,
                                'p': price_cents / 100.0
                            })
                        return prices

                return []

            elif response.status_code == 429:
                wait_time = 10 * (2 ** attempt)
                time.sleep(wait_time)
                continue
            else:
                return None

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(5)
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
        # Make timezone-naive for consistent comparison
        if dt.tzinfo is not None:
            dt = dt.tz_localize(None)
        return dt
    except:
        return None


def main():
    full_refresh = "--full-refresh" in sys.argv

    log("="*60)
    log("POLYMARKET PRICE FETCH (OPTIMIZED)")
    log(f"Mode: {'FULL REFRESH' if full_refresh else 'INCREMENTAL'}")
    log("="*60)

    # Timestamps
    now_ts = int(datetime.now().timestamp())
    # DOME API limit: max 365 days per request for daily candles
    # For new markets, start from 1 year ago (incremental updates fill in daily)
    one_year_ago = datetime.now() - timedelta(days=364)
    full_history_start_ts = int(one_year_ago.timestamp())
    # Buffer for daily candle granularity (1 day in seconds)
    DAILY_BUFFER = 86400

    # Load master data
    log("Loading master data...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    pm_markets = df[
        (df['platform'] == 'Polymarket') &
        (df['pm_condition_id'].notna()) &
        (df['pm_token_id_yes'].notna())
    ].copy()

    log(f"Found {len(pm_markets)} Polymarket markets with condition IDs")

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

    for idx, row in pm_markets.iterrows():
        token_id = str(row['pm_token_id_yes'])
        condition_id = row['pm_condition_id']
        is_closed = row.get('is_closed', False)
        close_time = parse_close_time(row.get('trading_close_time'))

        last_price_ts = get_last_price_ts(existing_prices, token_id)

        # Determine if we need to fetch and what range
        if is_closed:
            # CLOSED MARKET
            if close_time:
                close_ts = int(close_time.timestamp())

                # Check if we already have complete history (with daily buffer for granularity)
                if last_price_ts and last_price_ts >= close_ts - DAILY_BUFFER:
                    skipped_complete += 1
                    continue  # History complete, skip forever

                # Need to fetch up to close_time
                start_ts = (last_price_ts + 1) if last_price_ts else full_history_start_ts
                end_ts = close_ts
            else:
                # Edge case: closed but no close_time (only 6 markets)
                # Fetch up to now as fallback, will eventually complete
                if last_price_ts and last_price_ts >= now_ts - DAILY_BUFFER:
                    skipped_up_to_date += 1
                    continue
                start_ts = (last_price_ts + 1) if last_price_ts else full_history_start_ts
                end_ts = now_ts
                log(f"  WARN: Closed market {token_id[:20]}... missing close_time")
        else:
            # OPEN MARKET
            # Skip if we already have recent prices (within last day)
            if last_price_ts and last_price_ts >= now_ts - DAILY_BUFFER:
                skipped_up_to_date += 1
                continue

            start_ts = (last_price_ts + 1) if last_price_ts else full_history_start_ts
            end_ts = now_ts

        # Log new markets
        if not last_price_ts:
            log(f"  New market {token_id[:20]}... - fetching full history")

        # Fetch prices
        prices = fetch_market_prices(condition_id, token_id, start_ts, end_ts)

        if prices:
            if token_id in existing_prices:
                existing_prices[token_id].extend(prices)
                existing_prices[token_id].sort(key=lambda x: x['t'])
            else:
                existing_prices[token_id] = prices
            updated += 1
        elif prices is None:
            errors += 1

        if (updated + errors) % 50 == 0 and (updated + errors) > 0:
            log(f"Progress: {updated} updated, {errors} errors, {skipped_complete + skipped_up_to_date} skipped")

        time.sleep(RATE_LIMIT_DELAY)

    # Save updated prices
    with open(PRICES_FILE, 'w') as f:
        json.dump(existing_prices, f)

    log("="*60)
    log("COMPLETE")
    log(f"  Updated: {updated}")
    log(f"  Errors: {errors}")
    log(f"  Skipped (complete history): {skipped_complete}")
    log(f"  Skipped (already up-to-date): {skipped_up_to_date}")
    log(f"  Total price records: {len(existing_prices)}")
    log("="*60)

if __name__ == "__main__":
    main()
