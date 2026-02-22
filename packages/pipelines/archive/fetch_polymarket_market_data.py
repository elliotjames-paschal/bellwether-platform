#!/usr/bin/env python3
"""
B4: Fetch Polymarket market data via API
Gets token IDs and market metadata for all 394 political markets
"""

import pandas as pd
import requests
import json
import time
from datetime import datetime
import os

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
INPUT_FILE = f"{DATA_DIR}/polymarket_untagged_electoral_details.csv"
OUTPUT_FILE = f"{DATA_DIR}/polymarket_untagged_with_api_data.csv"
CHECKPOINT_FILE = f"{DATA_DIR}/polymarket_api_fetch_checkpoint.json"

# API Configuration
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
RATE_LIMIT_DELAY = 0.15  # seconds between requests
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

print("=" * 80)
print("FETCHING POLYMARKET MARKET DATA FROM API")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load markets
print(f"\n{'=' * 80}")
print("LOADING MARKETS")
print(f"{'=' * 80}")

df = pd.read_csv(INPUT_FILE)
print(f"✓ Loaded {len(df):,} markets")

# Load checkpoint if exists
processed_ids = set()
market_data = {}

if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint = json.load(f)
        processed_ids = set(checkpoint.get('processed_ids', []))
        market_data = checkpoint.get('market_data', {})
    print(f"✓ Loaded checkpoint: {len(processed_ids):,} markets processed")

remaining = df[~df['id'].isin(processed_ids)]
print(f"✓ Markets remaining to fetch: {len(remaining):,}")

# Fetch market data
print(f"\n{'=' * 80}")
print("FETCHING MARKET DATA FROM POLYMARKET API")
print(f"{'=' * 80}")

start_time = time.time()
success_count = 0
not_found_count = 0
error_count = 0

for idx, row in remaining.iterrows():
    market_id = str(row['id'])

    # Fetch market data directly by ID
    success = False
    for attempt in range(MAX_RETRIES):
        try:
            # Call direct market endpoint
            response = requests.get(
                f"{GAMMA_API_BASE}/markets/{market_id}",
                timeout=10
            )

            if response.status_code == 200:
                market = response.json()

                # Parse clobTokenIds - it's a JSON string
                clob_tokens_str = market.get('clobTokenIds', '[]')
                try:
                    clob_tokens = json.loads(clob_tokens_str)
                except:
                    clob_tokens = []

                # Parse outcomePrices - also a JSON string
                outcome_prices_str = market.get('outcomePrices', '[]')
                try:
                    outcome_prices = json.loads(outcome_prices_str)
                except:
                    outcome_prices = []

                # Parse umaResolutionStatuses
                uma_statuses = market.get('umaResolutionStatuses', [])
                uma_status = uma_statuses[0] if len(uma_statuses) > 0 else None

                market_data[market_id] = {
                    'pm_token_id_yes': clob_tokens[0] if len(clob_tokens) > 0 else None,
                    'pm_token_id_no': clob_tokens[1] if len(clob_tokens) > 1 else None,
                    'pm_outcome_prices': json.dumps(outcome_prices),
                    'pm_closed': market.get('closed', False),
                    'pm_volume': market.get('volume', None),
                    'pm_uma_resolution_status': uma_status
                }
                processed_ids.add(int(market_id))
                success_count += 1
                success = True

                if success_count % 10 == 0:
                    print(f"✓ [{success_count}/{len(remaining)}] Fetched market {market_id}")

                break

            elif response.status_code == 404:
                # Market not found
                if attempt == MAX_RETRIES - 1:
                    not_found_count += 1
                    processed_ids.add(int(market_id))
                    print(f"✗ [{success_count + not_found_count}/{len(remaining)}] Market {market_id} not found")
                    success = True  # Mark as processed even if not found
            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    error_count += 1
                    print(f"✗ API error {response.status_code} for market {market_id}")

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            else:
                error_count += 1
                print(f"✗ Error fetching market {market_id}: {str(e)[:50]}")

    # Save checkpoint every 50 markets
    if len(processed_ids) % 50 == 0:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                'processed_ids': list(processed_ids),
                'market_data': market_data,
                'last_updated': datetime.now().isoformat()
            }, f)

        elapsed = time.time() - start_time
        rate = len(processed_ids) / elapsed if elapsed > 0 else 0
        remaining_count = len(df) - len(processed_ids)
        eta = (remaining_count / rate / 60) if rate > 0 else 0

        print(f"\n  [Checkpoint: {len(processed_ids):,}/{len(df):,} | Rate: {rate:.1f}/sec | ETA: {eta:.1f}min]\n")

    time.sleep(RATE_LIMIT_DELAY)

# Final checkpoint
with open(CHECKPOINT_FILE, 'w') as f:
    json.dump({
        'processed_ids': list(processed_ids),
        'market_data': market_data,
        'last_updated': datetime.now().isoformat()
    }, f)

elapsed = time.time() - start_time

print(f"\n{'=' * 80}")
print("API FETCH SUMMARY")
print(f"{'=' * 80}")
print(f"\nTotal markets attempted: {len(remaining):,}")
print(f"Successfully fetched: {success_count:,}")
print(f"Not found: {not_found_count:,}")
print(f"Errors: {error_count:,}")
print(f"Time: {elapsed/60:.1f} minutes")

# Merge data back to dataframe
print(f"\n{'=' * 80}")
print("MERGING DATA TO DATAFRAME")
print(f"{'=' * 80}")

for col in ['pm_token_id_yes', 'pm_token_id_no', 'pm_outcome_prices', 'pm_closed', 'pm_volume']:
    df[col] = None

for market_id, data in market_data.items():
    mask = df['id'] == int(market_id)
    for key, value in data.items():
        df.loc[mask, key] = value

# Save results
df.to_csv(OUTPUT_FILE, index=False)
print(f"\n✓ Saved {len(df):,} markets to:")
print(f"  {OUTPUT_FILE}")
print(f"\n✓ {len(market_data)} markets have API data")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
