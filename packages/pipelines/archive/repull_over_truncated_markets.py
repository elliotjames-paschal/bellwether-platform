#!/usr/bin/env python3
"""
Re-pull Over-Truncated Markets from Polymarket API

Reads the list of over-truncated markets and re-pulls their full price history from Polymarket API.
"""

import pandas as pd
import requests
import json
import time
from datetime import datetime

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MARKETS_FILE = f"{DATA_DIR}/all_markets_needing_repull.csv"
PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_CORRECTED_v3.json"

# API Configuration
CLOB_API_URL = "https://clob.polymarket.com/prices-history"
RATE_LIMIT_DELAY = 0.15
MAX_RETRIES = 3
RETRY_DELAY = 5

print("=" * 80)
print("RE-PULLING OVER-TRUNCATED MARKETS FROM POLYMARKET API")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load markets needing re-pull
print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

markets_df = pd.read_csv(MARKETS_FILE)
print(f"✓ Markets needing re-pull: {len(markets_df):,}")

# Load existing price data
with open(PRICE_FILE, 'r') as f:
    price_data = json.load(f)
print(f"✓ Loaded existing price data: {len(price_data):,} tokens")

# Get unique tokens to pull
tokens_to_pull = []
for _, row in markets_df.iterrows():
    token_yes = str(row['pm_token_id_yes'])
    token_no = str(row['pm_token_id_no']) if pd.notna(row['pm_token_id_no']) and row['pm_token_id_no'] != '' else None

    tokens_to_pull.append({'market_id': row['market_id'], 'token': token_yes, 'type': 'yes'})
    if token_no:
        tokens_to_pull.append({'market_id': row['market_id'], 'token': token_no, 'type': 'no'})

print(f"✓ Total tokens to pull: {len(tokens_to_pull):,}")

def get_price_history(token_id):
    """Fetch price history for a Polymarket token"""
    for attempt in range(MAX_RETRIES):
        try:
            params = {
                'market': token_id,
                'interval': 'max',
                'fidelity': 1440  # Daily resolution
            }

            response = requests.get(CLOB_API_URL, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()

                if isinstance(data, dict) and 'history' in data:
                    history = data['history']
                    price_list = []
                    for point in history:
                        if isinstance(point, dict) and 't' in point and 'p' in point:
                            price_list.append({'t': point['t'], 'p': str(point['p'])})
                    return price_list

                return []

            elif response.status_code == 404:
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

# Process tokens
print(f"\n{'=' * 80}")
print("FETCHING PRICE HISTORY FROM POLYMARKET API")
print(f"{'=' * 80}")

success_count = 0
empty_count = 0
error_count = 0

for idx, token_info in enumerate(tokens_to_pull, 1):
    market_id = token_info['market_id']
    token = token_info['token']
    token_type = token_info['type']

    if idx % 50 == 0:
        print(f"\n[{idx}/{len(tokens_to_pull)}] Progress checkpoint...")

    # Get price history
    price_list = get_price_history(token)
    time.sleep(RATE_LIMIT_DELAY)

    if price_list is not None:
        if len(price_list) > 0:
            price_data[token] = price_list
            success_count += 1
        else:
            price_data[token] = []
            empty_count += 1
    else:
        error_count += 1

# Save updated price data
print(f"\n{'=' * 80}")
print("SAVING UPDATED PRICE DATA")
print(f"{'=' * 80}")

# Backup original file first
backup_file = PRICE_FILE.replace('.json', f'_backup_repull_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
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

print(f"\nTokens processed: {len(tokens_to_pull):,}")
print(f"  Successful: {success_count:,}")
print(f"  Empty (no history): {empty_count:,}")
print(f"  Errors: {error_count:,}")

print(f"\nPrice file updated:")
print(f"  Before: {len(original_data):,} tokens")
print(f"  After: {len(price_data):,} tokens")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
