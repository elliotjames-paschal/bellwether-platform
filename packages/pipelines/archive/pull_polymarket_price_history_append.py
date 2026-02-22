#!/usr/bin/env python3
"""
Pull Polymarket price history for new markets and append to existing JSON
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
PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_CORRECTED_v3.json"

# API Configuration
CLOB_API_URL = "https://clob.polymarket.com/prices-history"
RATE_LIMIT_DELAY = 0.15
MAX_RETRIES = 3
RETRY_DELAY = 5

print("=" * 80)
print("PULLING POLYMARKET PRICE HISTORY FOR NEW MARKETS")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load data
print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

master_df = pd.read_csv(MASTER_FILE, low_memory=False)
print(f"✓ Loaded master file: {len(master_df):,} rows")

# Get all Polymarket markets
pm_markets = master_df[master_df['platform'] == 'Polymarket'].copy()
print(f"✓ Polymarket markets in master: {len(pm_markets):,}")

# Load existing price data
with open(PRICE_FILE, 'r') as f:
    price_data = json.load(f)
print(f"✓ Loaded existing price data: {len(price_data):,} tokens")

# Find markets with tokens but missing or empty price data
markets_with_tokens = pm_markets[pm_markets['pm_token_id_yes'].notna()].copy()
print(f"✓ Markets with token IDs: {len(markets_with_tokens):,}")

# Apply date filters
from datetime import datetime as dt_module
import pytz

cutoff_date = dt_module(2025, 11, 10, 23, 59, 59, tzinfo=pytz.UTC)
markets_with_tokens['end_dt'] = pd.to_datetime(markets_with_tokens['scheduled_end_time'], errors='coerce')

# Check for missing or insufficient price data (≤1 points) AND date filters
missing_markets = []
for idx, row in markets_with_tokens.iterrows():
    token_yes = str(row['pm_token_id_yes'])
    token_no = str(row['pm_token_id_no']) if pd.notna(row['pm_token_id_no']) else None

    # Check if yes token is missing or has ≤1 price points
    needs_data = False
    if token_yes not in price_data:
        needs_data = True
    elif len(price_data.get(token_yes, [])) <= 1:
        needs_data = True

    if needs_data:
        # Apply date filters: scheduled_end_time <= Nov 10, 2025 OR election_year <= 2025
        passes_filter = False

        # Filter 1: scheduled_end_time <= Nov 10, 2025
        if pd.notna(row['end_dt']) and row['end_dt'] <= cutoff_date:
            passes_filter = True

        # Filter 2: election_year <= 2025
        if pd.notna(row.get('election_year')) and row.get('election_year') <= 2025:
            passes_filter = True

        if passes_filter:
            missing_markets.append(row)

missing_df = pd.DataFrame(missing_markets)

print(f"✓ Markets with ≤1 price points matching date filters: {len(missing_df):,}")

if len(missing_df) == 0:
    print("\n✓ All markets already have price data!")
    exit(0)

print(f"\n{'=' * 80}")
print("FETCHING PRICE HISTORY FROM POLYMARKET API")
print(f"{'=' * 80}")

def get_price_history(token_id):
    """Fetch price history for a Polymarket token"""
    for attempt in range(MAX_RETRIES):
        try:
            # Use correct CLOB API endpoint with token_id as 'market' parameter
            # fidelity: 1440 = daily (24h) resolution
            params = {
                'market': token_id,
                'interval': 'max',
                'fidelity': 1440
            }

            response = requests.get(CLOB_API_URL, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()

                # Response is dict with 'history' key: {'history': [{'t': timestamp, 'p': price}, ...]}
                if isinstance(data, dict) and 'history' in data:
                    history = data['history']
                    # Ensure prices are strings
                    price_list = []
                    for point in history:
                        if isinstance(point, dict) and 't' in point and 'p' in point:
                            price_list.append({'t': point['t'], 'p': str(point['p'])})
                    return price_list

                return []

            elif response.status_code == 404:
                # No history available
                if attempt == MAX_RETRIES - 1:
                    return []
            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                return None

        except Exception as e:
            print(f"    ✗ Error: {str(e)[:50]}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            return None

    return None

# Process markets
success_count = 0
empty_count = 0
error_count = 0

for idx, row in missing_df.iterrows():
    market_id = str(row['market_id'])
    token_yes = str(row['pm_token_id_yes'])
    token_no = str(row['pm_token_id_no']) if pd.notna(row['pm_token_id_no']) else None

    print(f"\n[{success_count + empty_count + error_count + 1}/{len(missing_df)}] Market {market_id}")
    print(f"  Token: {token_yes}")
    print(f"  {row['question'][:70]}...")

    # Get price history
    price_list = get_price_history(token_yes)
    time.sleep(RATE_LIMIT_DELAY)

    if price_list is not None:
        if len(price_list) > 0:
            # Add for yes token
            price_data[token_yes] = price_list

            # For no token, invert prices (1 - yes_price)
            if token_no:
                inverted_prices = [
                    {'t': p['t'], 'p': str(1.0 - float(p['p']))}
                    for p in price_list
                ]
                price_data[token_no] = inverted_prices

            success_count += 1
            print(f"  ✓ Got {len(price_list)} price points")
        else:
            # Empty history
            price_data[token_yes] = []
            if token_no:
                price_data[token_no] = []
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
print(f"✓ Saved updated price data: {len(price_data):,} tokens")

# Summary
print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")

print(f"\nMarkets processed: {len(missing_df):,}")
print(f"  Successful: {success_count:,}")
print(f"  Empty (no history): {empty_count:,}")
print(f"  Errors: {error_count:,}")

print(f"\nPrice file updated:")
print(f"  Before: {len(original_data):,} tokens")
print(f"  After: {len(price_data):,} tokens")
print(f"  Added: {len(price_data) - len(original_data):,}")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
