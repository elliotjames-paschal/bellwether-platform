#!/usr/bin/env python3
"""
Identify Over-Truncated Markets

Finds markets where the last price point is more than 24 hours before trading_close_time.
This includes:
- Non-electoral markets (from old pipeline)
- Non-US electoral markets (from old pipeline)

Output: CSV of markets needing re-pull from API
"""

import pandas as pd
import json
from datetime import datetime, timedelta

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_CORRECTED_v3.json"
OUTPUT_FILE = f"{DATA_DIR}/markets_needing_repull.csv"

print("=" * 80)
print("IDENTIFYING OVER-TRUNCATED MARKETS")
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

# Load old pipeline data to identify truncated markets
print("\nLoading old pipeline data...")
with open('/Users/paschal/Desktop/Polymarket:Kalshi/event_dates_chatgpt.json', 'r') as f:
    old_event_dates = json.load(f)

old_df = pd.read_csv('/Users/paschal/Desktop/Polymarket:Kalshi/polymarket_political_categorized_20250920_153926 copy.csv', low_memory=False)

# Get truncated market IDs from old pipeline
old_truncated_ids = set(old_event_dates.keys())
print(f"✓ Old pipeline truncated: {len(old_truncated_ids)} markets")

# Get markets that were truncated by old pipeline
pm_closed = master_df[
    (master_df['platform'] == 'Polymarket') &
    (master_df['is_closed'] == True)
].copy()

pm_closed['market_id_str'] = pm_closed['market_id'].astype(str)
truncated_by_old_pipeline = pm_closed[pm_closed['market_id_str'].isin(old_truncated_ids)]

# Filter to non-electoral + non-US electoral
print("\nFiltering to markets we care about...")
# Get categories from old pipeline
old_categories = dict(zip(old_df['id'].astype(str), old_df['political_category']))
truncated_by_old_pipeline['old_category'] = truncated_by_old_pipeline['market_id_str'].map(old_categories)

# Non-electoral OR (electoral AND non-US)
markets_to_check = truncated_by_old_pipeline[
    (truncated_by_old_pipeline['old_category'] != '1. ELECTORAL') |
    ((truncated_by_old_pipeline['old_category'] == '1. ELECTORAL') & (truncated_by_old_pipeline['country'] != 'United States'))
]

print(f"✓ Checking {len(markets_to_check):,} markets (non-electoral + non-US electoral)")

# Check each market for over-truncation
print("\nChecking for over-truncation...")
over_truncated = []

for idx, market in markets_to_check.iterrows():
    market_id = str(market['market_id'])
    token_yes = str(market['pm_token_id_yes']) if pd.notna(market.get('pm_token_id_yes')) else None
    token_no = str(market['pm_token_id_no']) if pd.notna(market.get('pm_token_id_no')) else None

    if not token_yes or token_yes not in pm_prices:
        continue

    price_history = pm_prices[token_yes]
    if len(price_history) == 0:
        continue

    close_time_val = market['trading_close_time']
    if pd.isna(close_time_val):
        continue

    try:
        close_time = pd.to_datetime(close_time_val, utc=True).replace(tzinfo=None)
    except:
        continue

    # Get last price timestamp
    last_price = max(price_history, key=lambda x: x['t'])
    last_date = datetime.fromtimestamp(last_price['t'])

    # Check if over-truncated: last price more than 24h before close_time
    diff_hours = (close_time - last_date).total_seconds() / 3600

    if diff_hours > 24:
        over_truncated.append({
            'market_id': market_id,
            'question': market['question'],
            'political_category': market.get('old_category', ''),
            'country': market.get('country', ''),
            'pm_token_id_yes': token_yes,
            'pm_token_id_no': token_no if pd.notna(token_no) else '',
            'trading_close_time': close_time_val,
            'last_price_date': last_date.isoformat(),
            'hours_before_close': diff_hours
        })

# Save results
print(f"\n✓ Found {len(over_truncated)} over-truncated markets")

if len(over_truncated) > 0:
    over_truncated_df = pd.DataFrame(over_truncated)
    over_truncated_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✓ Saved to: {OUTPUT_FILE}")

    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY:")
    print(f"  Total over-truncated: {len(over_truncated)}")

    # By category
    category_counts = over_truncated_df['political_category'].value_counts()
    print(f"\n  By category:")
    for cat, count in category_counts.head(10).items():
        print(f"    {cat}: {count}")

    # Average hours
    avg_hours = over_truncated_df['hours_before_close'].mean()
    max_hours = over_truncated_df['hours_before_close'].max()
    print(f"\n  Average hours before close: {avg_hours:.1f}")
    print(f"  Max hours before close: {max_hours:.1f}")
else:
    print("\n✓ No over-truncated markets found!")

print(f"\n{'='*80}")
print("COMPLETE")
print(f"{'='*80}")
