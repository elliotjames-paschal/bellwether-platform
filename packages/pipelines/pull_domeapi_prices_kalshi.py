#!/usr/bin/env python3
"""
Dome API Price Fetcher for Kalshi

Part of the NEW Bellwether Pipeline (January 2026+)

Fetches Kalshi price data via Dome API.
Writes directly to CORRECTED_v3.json.

Usage:
    python pull_domeapi_prices_kalshi.py [--full-refresh]

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
PRICES_FILE = str(DATA_DIR / "kalshi_all_political_prices_CORRECTED_v3.json")

# Dome API Configuration (Dev tier: 100 queries/sec)
DOME_API_BASE = "https://api.domeapi.io/v1/kalshi"
DOME_API_KEY = get_dome_api_key()

# Kalshi Direct API (fallback when Dome is stale)
KALSHI_DIRECT_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Rate limit: 0.01s = 100 req/sec (dev tier)
RATE_LIMIT_DELAY = float(os.environ.get('DOME_RATE_LIMIT', '0.01'))
MAX_RETRIES = 3

# Track fallback usage
_kalshi_fallback_count = 0

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def fetch_kalshi_trades_direct(ticker, start_ts, end_ts):
    """
    Fetch trades directly from Kalshi API and convert to daily candlesticks.
    This is a fallback when Dome API data is stale.
    """
    global _kalshi_fallback_count

    all_trades = []
    cursor = None

    for attempt in range(MAX_RETRIES):
        try:
            # Paginate through all trades
            while True:
                params = {
                    'ticker': ticker,
                    'min_ts': start_ts,
                    'max_ts': end_ts,
                    'limit': 1000
                }
                if cursor:
                    params['cursor'] = cursor

                response = requests.get(
                    f"{KALSHI_DIRECT_BASE}/markets/trades",
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

                    time.sleep(0.05)  # Rate limit
                elif response.status_code == 429:
                    time.sleep(10 * (2 ** attempt))
                    break  # Will retry outer loop
                else:
                    return None

            if all_trades:
                break

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(5)
                continue
            return None

    if not all_trades:
        return []

    _kalshi_fallback_count += 1

    # Convert trades to daily candlesticks
    from collections import defaultdict
    daily_trades = defaultdict(list)

    for trade in all_trades:
        created_time = trade.get('created_time', '')
        if not created_time:
            continue

        # Parse ISO timestamp and get day
        try:
            dt = datetime.fromisoformat(created_time.replace('Z', '+00:00'))
            day_key = dt.strftime('%Y-%m-%d')
            daily_trades[day_key].append(trade)
        except:
            continue

    # Build candlesticks from daily trades
    candlesticks = []
    for day, trades in sorted(daily_trades.items()):
        if not trades:
            continue

        # Extract prices (yes_price is in cents)
        prices = [t.get('yes_price', 0) for t in trades if t.get('yes_price') is not None]
        volumes = [t.get('count', 0) for t in trades]

        if not prices:
            continue

        # Calculate end_period_ts (end of day UTC)
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
            '_source': 'kalshi_direct'  # Mark as from fallback
        }
        candlesticks.append(candlestick)

    return candlesticks


def is_dome_data_stale(candlesticks, close_time_ts):
    """
    Check if Dome data is stale for a market that should have recent data.
    Returns True if market closed recently but Dome has no data after Nov 2025.
    """
    if not candlesticks:
        return True

    # Find latest timestamp in Dome data
    latest_ts = max(c.get('end_period_ts', 0) for c in candlesticks)

    # If market closed after Dec 1, 2025 but Dome data ends before then, it's stale
    dec_2025_ts = int(datetime(2025, 12, 1).timestamp())

    if close_time_ts > dec_2025_ts and latest_ts < dec_2025_ts:
        return True

    return False

def fetch_kalshi_prices(ticker, start_time, end_time):
    """Fetch price data for a Kalshi market from Dome API."""
    for attempt in range(MAX_RETRIES):
        try:
            # TODO: Confirm exact endpoint structure
            response = requests.get(
                f"{DOME_API_BASE}/candlesticks/{ticker}",
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
                # Return the full candlestick data (includes yes_bid, yes_ask, price, etc.)
                return candlesticks

            elif response.status_code == 429:
                wait_time = 10 * (2 ** attempt)
                time.sleep(wait_time)
                continue
            elif response.status_code == 404:
                # Endpoint might not exist yet
                return []
            else:
                log(f"  API error: HTTP {response.status_code} for {ticker}")
                return None

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(5)
                continue
            log(f"  Exception fetching {ticker}: {str(e)[:100]}")
            return None

    return None

def main():
    full_refresh = "--full-refresh" in sys.argv

    log("="*60)
    log("KALSHI PRICE FETCH VIA DOME API")
    log(f"Mode: {'FULL REFRESH' if full_refresh else 'INCREMENTAL'}")
    log("="*60)

    # Get date range
    # For full refresh: fetch ALL historical prices (from 2020)
    # For incremental: fetch from recent date
    if full_refresh:
        default_start = '2020-01-01'  # Fetch all historical data
    else:
        default_start = '2024-11-10'  # Recent data only

    start_date = os.environ.get('FETCH_START_DATE', default_start)
    end_date = os.environ.get('FETCH_END_DATE', datetime.now().strftime('%Y-%m-%d'))

    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    default_start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())

    log(f"Date range: {start_date} to {end_date}")
    log(f"Full refresh will fetch complete price history for new markets")

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

    # Fetch prices
    updated = 0
    errors = 0
    skipped = 0

    # Early start timestamp for fetching full history of new markets
    full_history_start_ts = int(datetime(2020, 1, 1).timestamp())

    for idx, row in kalshi_markets.iterrows():
        ticker = str(row['market_id'])

        # Skip old closed markets unless --full-refresh is set
        if not full_refresh:
            close_time = pd.to_datetime(row.get('k_expiration_time'), errors='coerce')
            if pd.notna(close_time):
                # Make comparison timezone-naive to avoid comparison errors
                close_time_naive = close_time.tz_localize(None) if close_time.tzinfo else close_time
                if close_time_naive < start_dt - timedelta(days=7):
                    skipped += 1
                    continue

        # Determine start timestamp:
        # - If we already have prices: fetch from last known timestamp (truly incremental)
        # - If this is a new market: fetch full history from 2020
        if ticker in existing_prices and existing_prices[ticker]:
            # Find the most recent timestamp we have (Kalshi uses 'end_period_ts' not 't')
            last_ts = max(p.get('end_period_ts', p.get('t', 0)) for p in existing_prices[ticker])
            start_ts = last_ts + 1  # Start from 1 second after our last data point
        else:
            start_ts = full_history_start_ts
            log(f"  New market {ticker} - fetching full history")

        # Get close time for staleness check
        close_time = pd.to_datetime(row.get('trading_close_time'), errors='coerce')
        close_time_ts = int(close_time.timestamp()) if pd.notna(close_time) else 0

        prices = fetch_kalshi_prices(ticker, start_ts, end_ts)

        # Check if Dome data is stale and we need to use fallback
        use_fallback = False
        if prices is not None:
            existing_data = existing_prices.get(ticker, [])
            combined_data = existing_data + prices
            if is_dome_data_stale(combined_data, close_time_ts):
                use_fallback = True

        if use_fallback:
            # Try Kalshi direct API fallback
            fallback_prices = fetch_kalshi_trades_direct(ticker, full_history_start_ts, end_ts)
            if fallback_prices:
                prices = fallback_prices

        if prices:
            # Append new prices (no dedup needed since we fetched from after our last timestamp)
            if ticker in existing_prices:
                existing_prices[ticker].extend(prices)
                # Sort just in case (Kalshi uses 'end_period_ts' not 't')
                existing_prices[ticker].sort(key=lambda x: x.get('end_period_ts', x.get('t', 0)))
            else:
                existing_prices[ticker] = prices
            updated += 1
        elif prices is None:
            errors += 1

        if (updated + errors) % 50 == 0:
            log(f"Progress: {updated} updated, {errors} errors, {skipped} skipped")

        time.sleep(RATE_LIMIT_DELAY)

    # Save
    with open(PRICES_FILE, 'w') as f:
        json.dump(existing_prices, f)

    log("="*60)
    log(f"COMPLETE: {updated} updated, {errors} errors, {skipped} skipped")
    log(f"Total price records: {len(existing_prices)}")
    if _kalshi_fallback_count > 0:
        log(f"Used Kalshi direct API fallback for {_kalshi_fallback_count} stale Dome responses")
    log("="*60)

if __name__ == "__main__":
    main()
