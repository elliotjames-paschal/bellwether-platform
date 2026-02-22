#!/usr/bin/env python3
"""
Create Properly Truncated Price Files

Takes the original unadjusted price files and truncates them at the
trading_close_time from the master file, ensuring we have full price
history up to the actual close time for Brier score calculations.
"""

import pandas as pd
import json
from datetime import datetime
import os

print("=" * 80)
print("CREATING PROPERLY TRUNCATED PRICE FILES")
print("=" * 80)

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
BACKUP_DIR = f"{DATA_DIR}/Old:Backups"

# Input files
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details.csv"
PM_ELECTION_PRICES = f"{BACKUP_DIR}/polymarket_election_prices_20251113_164348.json"
PM_NON_ELECTION_PRICES = f"{BACKUP_DIR}/polymarket_non_election_prices_20251114_051342.json"
KALSHI_ELECTION_PRICES = f"{BACKUP_DIR}/kalshi_election_prices_20251113_164348.json"
KALSHI_NON_ELECTION_PRICES = f"{BACKUP_DIR}/kalshi_non_election_prices_20251114_051342.json"

# Output files
PM_OUTPUT = f"{DATA_DIR}/polymarket_all_political_prices_CORRECTED_v3.json"
KALSHI_OUTPUT = f"{DATA_DIR}/kalshi_all_political_prices_CORRECTED_v3.json"

# ============================================================================
# Load Master File and Create Close Time Mappings
# ============================================================================

print("\nLoading master file...")
df = pd.read_csv(MASTER_FILE, low_memory=False)

# Get markets with close times
markets_with_close = df[df['trading_close_time'].notna()].copy()
markets_with_close['trading_close_time'] = pd.to_datetime(
    markets_with_close['trading_close_time'],
    format='ISO8601',
    utc=True
)

print(f"Markets with trading_close_time: {len(markets_with_close):,}")
print(f"  Polymarket: {len(markets_with_close[markets_with_close['platform']=='Polymarket']):,}")
print(f"  Kalshi: {len(markets_with_close[markets_with_close['platform']=='Kalshi']):,}")

# Create close time mappings
print("\nCreating close time mappings...")

# Polymarket: map token_id -> close_time
pm_close_times = {}
for idx, row in markets_with_close[markets_with_close['platform']=='Polymarket'].iterrows():
    token_id = row['pm_token_id_yes']
    if token_id and not pd.isna(token_id):
        # Convert to Unix timestamp for comparison
        close_timestamp = int(row['trading_close_time'].timestamp())
        pm_close_times[str(token_id)] = close_timestamp

# Kalshi: map ticker -> close_time
kalshi_close_times = {}
for idx, row in markets_with_close[markets_with_close['platform']=='Kalshi'].iterrows():
    close_timestamp = int(row['trading_close_time'].timestamp())
    kalshi_close_times[row['market_id']] = close_timestamp

print(f"  Polymarket tokens with close times: {len(pm_close_times):,}")
print(f"  Kalshi tickers with close times: {len(kalshi_close_times):,}")

# ============================================================================
# Process Polymarket Prices
# ============================================================================

print("\n" + "=" * 80)
print("PROCESSING POLYMARKET PRICES")
print("=" * 80)

pm_all_prices = {}

# Load election prices
if os.path.exists(PM_ELECTION_PRICES):
    print(f"\nLoading: {PM_ELECTION_PRICES}")
    with open(PM_ELECTION_PRICES, 'r') as f:
        pm_election = json.load(f)
    print(f"  Loaded {len(pm_election):,} markets")
    pm_all_prices.update(pm_election)

# Load non-election prices
if os.path.exists(PM_NON_ELECTION_PRICES):
    print(f"\nLoading: {PM_NON_ELECTION_PRICES}")
    with open(PM_NON_ELECTION_PRICES, 'r') as f:
        pm_non_election = json.load(f)
    print(f"  Loaded {len(pm_non_election):,} markets")
    pm_all_prices.update(pm_non_election)

print(f"\nTotal Polymarket markets: {len(pm_all_prices):,}")

# Truncate prices
print("\nTruncating Polymarket prices...")
pm_truncated = {}
markets_truncated = 0
prices_removed = 0

for token_id, price_data in pm_all_prices.items():
    if token_id in pm_close_times:
        close_timestamp = pm_close_times[token_id]

        # Keep only prices at or before close time
        truncated_prices = [
            p for p in price_data
            if p['t'] <= close_timestamp
        ]

        if len(truncated_prices) < len(price_data):
            markets_truncated += 1
            prices_removed += len(price_data) - len(truncated_prices)

        pm_truncated[token_id] = truncated_prices
    else:
        # No close time, keep all prices
        pm_truncated[token_id] = price_data

print(f"  Markets truncated: {markets_truncated:,}")
print(f"  Price points removed: {prices_removed:,}")

# Save
print(f"\nSaving to: {PM_OUTPUT}")
with open(PM_OUTPUT, 'w') as f:
    json.dump(pm_truncated, f)
print(f"✓ Saved {len(pm_truncated):,} Polymarket markets")

# ============================================================================
# Process Kalshi Prices
# ============================================================================

print("\n" + "=" * 80)
print("PROCESSING KALSHI PRICES")
print("=" * 80)

kalshi_all_prices = {}

# Load election prices
if os.path.exists(KALSHI_ELECTION_PRICES):
    print(f"\nLoading: {KALSHI_ELECTION_PRICES}")
    with open(KALSHI_ELECTION_PRICES, 'r') as f:
        kalshi_election = json.load(f)
    print(f"  Loaded {len(kalshi_election):,} markets")
    kalshi_all_prices.update(kalshi_election)

# Load non-election prices
if os.path.exists(KALSHI_NON_ELECTION_PRICES):
    print(f"\nLoading: {KALSHI_NON_ELECTION_PRICES}")
    with open(KALSHI_NON_ELECTION_PRICES, 'r') as f:
        kalshi_non_election = json.load(f)
    print(f"  Loaded {len(kalshi_non_election):,} markets")
    kalshi_all_prices.update(kalshi_non_election)

print(f"\nTotal Kalshi markets: {len(kalshi_all_prices):,}")

# Truncate prices
print("\nTruncating Kalshi prices...")
kalshi_truncated = {}
markets_truncated = 0
prices_removed = 0

for ticker, price_data in kalshi_all_prices.items():
    if ticker in kalshi_close_times:
        close_timestamp = kalshi_close_times[ticker]

        # Keep only prices at or before close time
        # Kalshi uses 'end_period_ts' instead of 't'
        truncated_prices = [
            p for p in price_data
            if p.get('end_period_ts', p.get('t', 0)) <= close_timestamp
        ]

        if len(truncated_prices) < len(price_data):
            markets_truncated += 1
            prices_removed += len(price_data) - len(truncated_prices)

        kalshi_truncated[ticker] = truncated_prices
    else:
        # No close time, keep all prices
        kalshi_truncated[ticker] = price_data

print(f"  Markets truncated: {markets_truncated:,}")
print(f"  Price points removed: {prices_removed:,}")

# Save
print(f"\nSaving to: {KALSHI_OUTPUT}")
with open(KALSHI_OUTPUT, 'w') as f:
    json.dump(kalshi_truncated, f)
print(f"✓ Saved {len(kalshi_truncated):,} Kalshi markets")

# ============================================================================
# Summary
# ============================================================================

print("\n" + "=" * 80)
print("✓ COMPLETE")
print("=" * 80)
print(f"\nCreated properly truncated price files:")
print(f"  {PM_OUTPUT}")
print(f"  {KALSHI_OUTPUT}")
print(f"\nThese files contain full price history up to trading_close_time,")
print(f"enabling proper calculation of prices 24 hours before closing.")
