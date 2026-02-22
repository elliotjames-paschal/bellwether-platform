#!/usr/bin/env python3
"""
Pull condition_id for all Polymarket markets from gamma-api

Fetches the conditionId field for all Polymarket markets and adds it to the master file.
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
CHECKPOINT_FILE = f"{DATA_DIR}/condition_id_fetch_checkpoint.json"

# API Configuration
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
RATE_LIMIT_DELAY = 0.15
MAX_RETRIES = 3
RETRY_DELAY = 5

print("=" * 80)
print("FETCHING CONDITION IDS FROM POLYMARKET API")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load master file
print(f"\n{'=' * 80}")
print("LOADING MASTER FILE")
print(f"{'=' * 80}")

master_df = pd.read_csv(MASTER_FILE, low_memory=False)
pm_markets = master_df[master_df['platform'] == 'Polymarket'].copy()
print(f"✓ Total Polymarket markets: {len(pm_markets):,}")

# Load checkpoint if exists
condition_ids = {}
processed_ids = set()

try:
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint = json.load(f)
        condition_ids = checkpoint.get('condition_ids', {})
        processed_ids = set(checkpoint.get('processed_ids', []))
    print(f"✓ Loaded checkpoint: {len(processed_ids):,} markets processed")
except FileNotFoundError:
    print(f"✓ No checkpoint found, starting fresh")

remaining = pm_markets[~pm_markets['market_id'].astype(str).isin(processed_ids)]
print(f"✓ Markets remaining to fetch: {len(remaining):,}")

# Fetch condition_ids
print(f"\n{'=' * 80}")
print("FETCHING CONDITION IDS")
print(f"{'=' * 80}")

success_count = 0
not_found_count = 0
error_count = 0

for idx, row in remaining.iterrows():
    market_id = str(row['market_id'])

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                f"{GAMMA_API_BASE}/markets/{market_id}",
                timeout=10
            )

            if response.status_code == 200:
                market = response.json()
                condition_id = market.get('conditionId')

                if condition_id:
                    condition_ids[market_id] = condition_id
                    success_count += 1
                else:
                    not_found_count += 1

                processed_ids.add(market_id)

                if success_count % 50 == 0:
                    print(f"  [{success_count + not_found_count}/{len(remaining)}] Processed...")

                break

            elif response.status_code == 404:
                not_found_count += 1
                processed_ids.add(market_id)
                break
            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    error_count += 1

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            else:
                error_count += 1
                print(f"  ✗ Error fetching market {market_id}: {str(e)[:50]}")

    # Save checkpoint every 100 markets
    if len(processed_ids) % 100 == 0:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                'condition_ids': condition_ids,
                'processed_ids': list(processed_ids),
                'last_updated': datetime.now().isoformat()
            }, f)

    time.sleep(RATE_LIMIT_DELAY)

# Final checkpoint
with open(CHECKPOINT_FILE, 'w') as f:
    json.dump({
        'condition_ids': condition_ids,
        'processed_ids': list(processed_ids),
        'last_updated': datetime.now().isoformat()
    }, f)

print(f"\n{'=' * 80}")
print("FETCH SUMMARY")
print(f"{'=' * 80}")
print(f"\nTotal markets: {len(pm_markets):,}")
print(f"Successfully fetched condition_id: {success_count:,}")
print(f"No condition_id: {not_found_count:,}")
print(f"Errors: {error_count:,}")

# Add condition_ids to master file
print(f"\n{'=' * 80}")
print("UPDATING MASTER FILE")
print(f"{'=' * 80}")

master_df['pm_condition_id'] = None

for market_id, condition_id in condition_ids.items():
    mask = (master_df['platform'] == 'Polymarket') & (master_df['market_id'].astype(str) == market_id)
    master_df.loc[mask, 'pm_condition_id'] = condition_id

# Backup original
backup_file = MASTER_FILE.replace('.csv', f'_backup_condition_ids_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
master_df_original = pd.read_csv(MASTER_FILE, low_memory=False)
master_df_original.to_csv(backup_file, index=False)
print(f"✓ Backed up original to: {backup_file}")

# Save updated master
master_df.to_csv(MASTER_FILE, index=False)
print(f"✓ Saved updated master file")
print(f"  Markets with condition_id: {master_df['pm_condition_id'].notna().sum():,}")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
