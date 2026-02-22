#!/usr/bin/env python3
"""
Populate missing trading_close_time values in master CSV

Calls Polymarket gamma-api to get closedTime field for all Polymarket markets
that are missing trading_close_time in the master file.
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
CHECKPOINT_FILE = f"{DATA_DIR}/populate_close_time_checkpoint.json"

# API Configuration
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
RATE_LIMIT_DELAY = 0.15
MAX_RETRIES = 3
RETRY_DELAY = 5

print("=" * 80)
print("POPULATING MISSING TRADING_CLOSE_TIME VALUES")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load master file
print(f"\n{'=' * 80}")
print("LOADING MASTER FILE")
print(f"{'=' * 80}")

master_df = pd.read_csv(MASTER_FILE, low_memory=False)
pm_markets = master_df[master_df['platform'] == 'Polymarket'].copy()
missing_close_time = pm_markets[pm_markets['trading_close_time'].isna()].copy()

print(f"✓ Total Polymarket markets: {len(pm_markets):,}")
print(f"✓ Missing trading_close_time: {len(missing_close_time):,}")

# Load checkpoint if exists
close_times = {}
processed_ids = set()

try:
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint = json.load(f)
        close_times = checkpoint.get('close_times', {})
        processed_ids = set(checkpoint.get('processed_ids', []))
    print(f"✓ Loaded checkpoint: {len(processed_ids):,} markets processed")
except FileNotFoundError:
    print(f"✓ No checkpoint found, starting fresh")

remaining = missing_close_time[~missing_close_time['market_id'].astype(str).isin(processed_ids)]
print(f"✓ Markets remaining to fetch: {len(remaining):,}")

# Fetch closedTime values
print(f"\n{'=' * 80}")
print("FETCHING CLOSED TIME FROM POLYMARKET API")
print(f"{'=' * 80}")
print()

success_count = 0
not_found_count = 0
error_count = 0
no_close_time_count = 0

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
                closed_time = market.get('closedTime')

                if closed_time:
                    close_times[market_id] = closed_time
                    success_count += 1
                else:
                    no_close_time_count += 1

                processed_ids.add(market_id)

                if (success_count + no_close_time_count) % 50 == 0:
                    print(f"  [{success_count + no_close_time_count + not_found_count}/{len(remaining)}] Processed...")

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
                    print(f"  ✗ Error for market {market_id}: Status {response.status_code}")

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
                'close_times': close_times,
                'processed_ids': list(processed_ids),
                'last_updated': datetime.now().isoformat()
            }, f)

    time.sleep(RATE_LIMIT_DELAY)

# Final checkpoint
with open(CHECKPOINT_FILE, 'w') as f:
    json.dump({
        'close_times': close_times,
        'processed_ids': list(processed_ids),
        'last_updated': datetime.now().isoformat()
    }, f)

print(f"\n{'=' * 80}")
print("FETCH SUMMARY")
print(f"{'=' * 80}")
print(f"\nTotal markets: {len(missing_close_time):,}")
print(f"Successfully fetched closedTime: {success_count:,}")
print(f"No closedTime in response: {no_close_time_count:,}")
print(f"Market not found (404): {not_found_count:,}")
print(f"Errors: {error_count:,}")

# Update master file with closedTime values
print(f"\n{'=' * 80}")
print("UPDATING MASTER FILE")
print(f"{'=' * 80}")

# Create trading_close_time column if it doesn't exist
if 'trading_close_time' not in master_df.columns:
    master_df['trading_close_time'] = None

# Update values
for market_id, closed_time in close_times.items():
    mask = (master_df['platform'] == 'Polymarket') & (master_df['market_id'].astype(str) == market_id)
    master_df.loc[mask, 'trading_close_time'] = closed_time

# Backup original
backup_file = MASTER_FILE.replace('.csv', f'_backup_populate_close_time_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
master_df_original = pd.read_csv(MASTER_FILE, low_memory=False)
master_df_original.to_csv(backup_file, index=False)
print(f"✓ Backed up original to: {backup_file}")

# Save updated master
master_df.to_csv(MASTER_FILE, index=False)
print(f"✓ Saved updated master file")

# Count how many Polymarket markets now have trading_close_time
pm_markets_updated = master_df[master_df['platform'] == 'Polymarket']
has_close_time = pm_markets_updated['trading_close_time'].notna().sum()
print(f"  Polymarket markets with trading_close_time: {has_close_time:,}/{len(pm_markets_updated):,}")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
