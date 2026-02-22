#!/usr/bin/env python3
"""
Identify ALL Over-Truncated Markets (Complete Check)

Checks ALL closed Polymarket markets for over-truncation (last price >24h before trading_close_time).
This includes:
1. Non-electoral markets (any category except ELECTORAL)
2. Electoral markets where Country != United States

Output: CSV of ALL markets needing re-pull from API
"""

import pandas as pd
import json
from datetime import datetime, timedelta

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_CORRECTED_v3.json"
OUTPUT_FILE = f"{DATA_DIR}/all_markets_needing_repull.csv"

print("=" * 80)
print("IDENTIFYING ALL OVER-TRUNCATED MARKETS (COMPREHENSIVE CHECK)")
print("=" * 80)

# Load master
print("\nLoading master file...")
master_df = pd.read_csv(MASTER_FILE, low_memory=False)
print(f"✓ Loaded {len(master_df):,} markets")

# Load price data
print("\nLoading price data...")
with open(PRICE_FILE, 'r') as f:
    pm_prices = json.load(f)
print(f"✓ Loaded {len(pm_prices):,} tokens")

# Get ALL closed Polymarket markets
pm_closed = master_df[
    (master_df['platform'] == 'Polymarket') &
    (master_df['is_closed'] == True)
].copy()

print(f"\n✓ Total closed Polymarket markets: {len(pm_closed):,}")

# Filter to markets we care about:
# 1. Non-electoral (any political_category except ELECTORAL)
# 2. Electoral where Country != United States

pm_closed['political_category'] = pm_closed['political_category'].fillna('')
pm_closed['country'] = pm_closed['country'].fillna('')

markets_to_check = pm_closed[
    (pm_closed['political_category'] != '1. ELECTORAL') |  # Non-electoral
    ((pm_closed['political_category'] == '1. ELECTORAL') & (pm_closed['country'] != 'United States'))  # Non-US electoral
]

print(f"✓ Markets to check for over-truncation: {len(markets_to_check):,}")
print(f"  - Non-electoral: {len(markets_to_check[markets_to_check['political_category'] != '1. ELECTORAL']):,}")
print(f"  - Electoral (non-US): {len(markets_to_check[(markets_to_check['political_category'] == '1. ELECTORAL') & (markets_to_check['country'] != 'United States')]):,}")

# Check each market for over-truncation
print("\nChecking for over-truncation...")
over_truncated = []
no_price_data = []
no_close_time = []

for idx, market in markets_to_check.iterrows():
    market_id = str(market['market_id'])
    token_yes = str(market['pm_token_id_yes']) if pd.notna(market.get('pm_token_id_yes')) else None
    token_no = str(market['pm_token_id_no']) if pd.notna(market.get('pm_token_id_no')) else None

    # Check if we have price data
    if not token_yes or token_yes not in pm_prices:
        no_price_data.append(market_id)
        continue

    price_history = pm_prices[token_yes]
    if len(price_history) == 0:
        no_price_data.append(market_id)
        continue

    # Check if we have close_time
    close_time_val = market['trading_close_time']
    if pd.isna(close_time_val):
        no_close_time.append(market_id)
        continue

    try:
        close_time = pd.to_datetime(close_time_val, utc=True).replace(tzinfo=None)
    except:
        no_close_time.append(market_id)
        continue

    # Get last price timestamp
    last_price = max(price_history, key=lambda x: x['t'])
    last_date = datetime.fromtimestamp(last_price['t'])

    # Check if over-truncated: last price more than 24h before close_time
    diff_hours = (close_time - last_date).total_seconds() / 3600

    if diff_hours > 24:
        is_electoral = market['political_category'] == '1. ELECTORAL'

        over_truncated.append({
            'market_id': market_id,
            'question': market['question'],
            'political_category': market.get('political_category', ''),
            'is_electoral': is_electoral,
            'country': market.get('country', ''),
            'office': market.get('office', '') if is_electoral else '',
            'location': market.get('location', '') if is_electoral else '',
            'election_year': market.get('election_year', '') if is_electoral else '',
            'pm_token_id_yes': token_yes,
            'pm_token_id_no': token_no if pd.notna(token_no) else '',
            'trading_close_time': close_time_val,
            'last_price_date': last_date.isoformat(),
            'hours_before_close': diff_hours
        })

# Save results
print(f"\n{'='*80}")
print("RESULTS")
print(f"{'='*80}")

print(f"\n✓ Found {len(over_truncated)} over-truncated markets")
print(f"  - Markets with no price data: {len(no_price_data)}")
print(f"  - Markets with no close_time: {len(no_close_time)}")

if len(over_truncated) > 0:
    over_truncated_df = pd.DataFrame(over_truncated)
    over_truncated_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✓ Saved to: {OUTPUT_FILE}")

    # Summary by type
    print(f"\n{'='*80}")
    print("BREAKDOWN:")
    print(f"{'='*80}")

    electoral_count = len(over_truncated_df[over_truncated_df['is_electoral'] == True])
    non_electoral_count = len(over_truncated_df[over_truncated_df['is_electoral'] == False])

    print(f"\n  Total over-truncated: {len(over_truncated)}")
    print(f"    - Electoral (non-US): {electoral_count}")
    print(f"    - Non-electoral: {non_electoral_count}")

    # By category
    if non_electoral_count > 0:
        print(f"\n  Non-electoral by category:")
        non_electoral = over_truncated_df[over_truncated_df['is_electoral'] == False]
        category_counts = non_electoral['political_category'].value_counts()
        for cat, count in category_counts.head(10).items():
            print(f"    {cat}: {count}")

    # Electoral by country
    if electoral_count > 0:
        print(f"\n  Electoral (non-US) by country:")
        electoral = over_truncated_df[over_truncated_df['is_electoral'] == True]
        country_counts = electoral['country'].value_counts()
        for country, count in country_counts.head(10).items():
            print(f"    {country}: {count}")

    # Average/max hours
    avg_hours = over_truncated_df['hours_before_close'].mean()
    max_hours = over_truncated_df['hours_before_close'].max()
    print(f"\n  Average hours before close: {avg_hours:.1f}")
    print(f"  Max hours before close: {max_hours:.1f}")
else:
    print("\n✓ No over-truncated markets found!")

print(f"\n{'='*80}")
print("COMPLETE")
print(f"{'='*80}")
