#!/usr/bin/env python3
"""
Test DomeAPI's Candlesticks endpoint for markets with no price history

For the 733 markets with empty price arrays, check if DomeAPI has candlestick data
using their condition_id.
"""

import pandas as pd
import requests
import time
from datetime import datetime, timedelta

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
EMPTY_MARKETS_FILE = f"{DATA_DIR}/empty_markets_to_test.csv"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"

# DomeAPI Configuration
DOME_API_BASE = "https://api.domeapi.io/v1/polymarket"
DOME_API_KEY = "Bearer d2d4d3b2cd3b0424bd2145a57d6f34a3661050e2"
RATE_LIMIT_DELAY = 0.25
MAX_RETRIES = 3
RETRY_DELAY = 2

print("=" * 80)
print("TESTING DOMEAPI CANDLESTICKS FOR EMPTY MARKETS")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load empty markets list
print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

empty_markets_df = pd.read_csv(EMPTY_MARKETS_FILE)
master_df = pd.read_csv(MASTER_FILE, low_memory=False)

# Convert market_id to string for merge
empty_markets_df['market_id'] = empty_markets_df['market_id'].astype(str)
master_df['market_id'] = master_df['market_id'].astype(str)

# Merge to get trading_close_time
empty_markets_df = empty_markets_df.merge(
    master_df[['market_id', 'trading_close_time']],
    on='market_id',
    how='left'
)

# Filter to only markets with trading_close_time
empty_with_close = empty_markets_df[empty_markets_df['trading_close_time'].notna()].copy()
print(f"✓ Total empty markets: {len(empty_markets_df):,}")
print(f"✓ Markets with trading_close_time: {len(empty_with_close):,}")
print(f"✓ Markets without trading_close_time: {len(empty_markets_df) - len(empty_with_close):,}")

empty_markets = empty_with_close.to_dict('records')
print(f"✓ Testing {len(empty_markets):,} markets with close times")

# Test DomeAPI
print(f"\n{'=' * 80}")
print("TESTING DOMEAPI CANDLESTICKS ENDPOINT")
print(f"{'=' * 80}")
print()

found_markets = []
not_found_markets = []
error_markets = []

total = len(empty_markets)

for i, market in enumerate(empty_markets, 1):
    market_id = str(market['market_id'])
    condition_id = market['condition_id']

    # Calculate time range: 1 year before trading_close_time to trading_close_time
    try:
        close_time = pd.to_datetime(market['trading_close_time'], utc=True).replace(tzinfo=None)
        end_time = int(close_time.timestamp())
        start_time = int((close_time - timedelta(days=365)).timestamp())
    except:
        # Skip if can't parse date
        error_markets.append({**market, 'error': 'Invalid trading_close_time'})
        continue

    # Try to get candlestick data from DomeAPI
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                f"{DOME_API_BASE}/candlesticks/{condition_id}",
                headers={"Authorization": DOME_API_KEY},
                params={
                    'start_time': start_time,
                    'end_time': end_time,
                    'interval': 1440
                },
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                # Check if we got meaningful data
                if data and len(data) > 0:
                    found_markets.append({
                        'market_id': market_id,
                        'question': market['question'],
                        'condition_id': condition_id,
                        'political_category': market['political_category'],
                        'candlesticks_count': len(data)
                    })
                    status = "✓ FOUND"
                else:
                    not_found_markets.append(market)
                    status = "✗ Empty"
                break

            elif response.status_code == 404:
                not_found_markets.append(market)
                status = "✗ 404"
                break

            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    error_markets.append({**market, 'error': f'Status {response.status_code}'})
                    status = f"✗ Error {response.status_code}"

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            else:
                error_markets.append({**market, 'error': str(e)})
                status = f"✗ Error"

    # Print live progress
    found_count = len(found_markets)
    processed = i
    percent_found = (found_count / processed) * 100 if processed > 0 else 0

    print(f"[{processed}/{total}] {status} | Market: {market_id[:20]}...")
    print(f"  Found on DomeAPI: {found_count}/{processed} ({percent_found:.1f}%)")
    print()

    time.sleep(RATE_LIMIT_DELAY)

# Print summary
print("=" * 80)
print("RESULTS")
print("=" * 80)
print()
print(f"Total tested: {total}")
print(f"Found on DomeAPI: {len(found_markets)} ({len(found_markets)/total*100:.1f}%)")
print(f"Not found: {len(not_found_markets)} ({len(not_found_markets)/total*100:.1f}%)")
print(f"Errors: {len(error_markets)} ({len(error_markets)/total*100:.1f}%)")

# Save results
if found_markets:
    found_df = pd.DataFrame(found_markets)
    output_file = f"{DATA_DIR}/empty_markets_found_on_domeapi_candlesticks.csv"
    found_df.to_csv(output_file, index=False)
    print(f"\n✓ Saved found markets to: {output_file}")

    print(f"\nFound markets by category:")
    category_counts = found_df['political_category'].value_counts()
    for cat, count in category_counts.items():
        print(f"  {cat}: {count}")

if error_markets:
    error_df = pd.DataFrame(error_markets)
    error_file = f"{DATA_DIR}/empty_markets_domeapi_errors.csv"
    error_df.to_csv(error_file, index=False)
    print(f"\n✓ Saved error markets to: {error_file}")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
