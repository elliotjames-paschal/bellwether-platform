#!/usr/bin/env python3
"""
Pull Kalshi price history for new markets and append to existing JSON
"""

import pandas as pd
import requests
import json
import time
from datetime import datetime

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
PRICE_FILE = f"{DATA_DIR}/kalshi_all_political_prices_CORRECTED_v3.json"

# API Configuration
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
RATE_LIMIT_DELAY = 0.3
MAX_RETRIES = 3
RETRY_DELAY = 5

print("=" * 80)
print("PULLING KALSHI PRICE HISTORY FOR NEW MARKETS")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load master file
print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

master_df = pd.read_csv(MASTER_FILE, low_memory=False)
print(f"✓ Loaded master file: {len(master_df):,} rows")

# Load existing price data
with open(PRICE_FILE, 'r') as f:
    price_data = json.load(f)
print(f"✓ Loaded existing price data: {len(price_data):,} tickers")

# Find Kalshi markets missing price data
kalshi_markets = master_df[master_df['platform'] == 'Kalshi'].copy()
existing_tickers = set(price_data.keys())
kalshi_tickers = set(kalshi_markets['market_id'].astype(str))
missing_tickers = kalshi_tickers - existing_tickers

print(f"\n✓ Kalshi markets in master: {len(kalshi_markets):,}")
print(f"✓ Markets with price data: {len(existing_tickers):,}")
print(f"✓ Markets missing price data: {len(missing_tickers):,}")

if len(missing_tickers) == 0:
    print("\n✓ All Kalshi markets already have price data!")
    exit(0)

# Get market details for missing tickers
missing_markets = kalshi_markets[kalshi_markets['market_id'].astype(str).isin(missing_tickers)].copy()

print(f"\n{'=' * 80}")
print("FETCHING PRICE HISTORY FROM KALSHI API")
print(f"{'=' * 80}")

def get_candlesticks(ticker):
    """Fetch candlestick price history for a Kalshi market"""
    for attempt in range(MAX_RETRIES):
        try:
            # Get market data first to check close_time
            market_url = f"{KALSHI_API_BASE}/markets/{ticker}"
            market_response = requests.get(market_url, timeout=10)

            if market_response.status_code != 200:
                if attempt == MAX_RETRIES - 1:
                    print(f"  ✗ Market not found: {ticker}")
                    return None
                time.sleep(RETRY_DELAY)
                continue

            market_data = market_response.json().get('market', {})
            close_time = market_data.get('close_time')

            # Extract series ticker from ticker (part before last hyphen)
            parts = ticker.split('-')
            if len(parts) > 1:
                series_ticker = '-'.join(parts[:-1])
            else:
                series_ticker = ticker

            # Get candlestick history using series endpoint
            # Set start_ts to 2020 and end_ts to now to get all available data
            from datetime import datetime
            start_ts = int(datetime(2020, 1, 1).timestamp())
            end_ts = int(datetime.now().timestamp())

            history_url = f"{KALSHI_API_BASE}/series/{series_ticker}/markets/{ticker}/candlesticks"
            history_response = requests.get(
                history_url,
                params={'period_interval': 1440, 'start_ts': start_ts, 'end_ts': end_ts},
                timeout=10
            )

            if history_response.status_code == 200:
                history = history_response.json()
                candlesticks = history.get('candlesticks', [])

                # Truncate at close_time if available
                if close_time and candlesticks:
                    # Parse close_time to timestamp
                    from dateutil import parser
                    close_dt = parser.parse(close_time)
                    close_ts = int(close_dt.timestamp())

                    # Filter candlesticks
                    candlesticks = [
                        c for c in candlesticks
                        if c.get('end_period_ts', 0) <= close_ts
                    ]

                return candlesticks

            elif history_response.status_code == 404:
                # No history available
                if attempt == MAX_RETRIES - 1:
                    return []
            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                return None

        except Exception as e:
            print(f"  ✗ Error fetching {ticker}: {str(e)[:50]}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            return None

    return None

# Process markets
success_count = 0
empty_count = 0
error_count = 0

for idx, row in missing_markets.iterrows():
    ticker = str(row['market_id'])

    print(f"\n[{success_count + empty_count + error_count + 1}/{len(missing_markets)}] {ticker}")
    print(f"  {row['question'][:70]}...")

    candlesticks = get_candlesticks(ticker)
    time.sleep(RATE_LIMIT_DELAY)

    if candlesticks is not None:
        if len(candlesticks) > 0:
            price_data[ticker] = candlesticks
            success_count += 1
            print(f"  ✓ Got {len(candlesticks)} candlesticks")
        else:
            price_data[ticker] = []
            empty_count += 1
            print(f"  ⚠ No price history available")
    else:
        error_count += 1
        print(f"  ✗ Failed to fetch")

# Save updated price data
print(f"\n{'=' * 80}")
print("SAVING UPDATED PRICE DATA")
print(f"{'=' * 80}")

# Backup original file first
backup_file = PRICE_FILE.replace('.json', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
with open(PRICE_FILE, 'r') as f:
    original_data = json.load(f)
with open(backup_file, 'w') as f:
    json.dump(original_data, f)
print(f"✓ Backed up original to: {backup_file}")

# Save updated data
with open(PRICE_FILE, 'w') as f:
    json.dump(price_data, f)
print(f"✓ Saved updated price data: {len(price_data):,} tickers")

# Summary
print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")

print(f"\nMarkets processed: {len(missing_markets):,}")
print(f"  Successful: {success_count:,}")
print(f"  Empty (no history): {empty_count:,}")
print(f"  Errors: {error_count:,}")

print(f"\nPrice file updated:")
print(f"  Before: {len(original_data):,} tickers")
print(f"  After: {len(price_data):,} tickers")
print(f"  Added: {len(price_data) - len(original_data):,}")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
