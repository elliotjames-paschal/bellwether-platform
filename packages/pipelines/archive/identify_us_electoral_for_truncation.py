#!/usr/bin/env python3
"""
Identify US Electoral Markets Needing Truncation via ChatGPT

Finds closed US electoral Polymarket markets that are NOT over-truncated
(i.e., they have full price data extending to or past trading_close_time).

These need to be sent to ChatGPT for election dates and truncated to day before election.

Output:
1. CSV of markets needing truncation
2. CSV of unique elections to send to ChatGPT
"""

import pandas as pd
import json
from datetime import datetime

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_CORRECTED_v3.json"
OUTPUT_MARKETS_FILE = f"{DATA_DIR}/us_electoral_markets_for_truncation.csv"
OUTPUT_ELECTIONS_FILE = f"{DATA_DIR}/us_elections_for_chatgpt.csv"

print("=" * 80)
print("IDENTIFYING US ELECTORAL MARKETS FOR TRUNCATION")
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

# Load old pipeline truncation data to exclude already truncated markets
print("\nLoading previous truncation records...")
with open('/Users/paschal/Desktop/Polymarket:Kalshi/event_dates_chatgpt.json', 'r') as f:
    old_truncated = json.load(f)
previously_truncated_ids = set(old_truncated.keys())
print(f"✓ Old pipeline truncated: {len(previously_truncated_ids)} markets")

# Note: Markets truncated by the election script (Dec 2024) will also show as
# "already_truncated" in the over-truncation check (>24h before close_time)

# Get closed US electoral Polymarket markets
us_electoral = master_df[
    (master_df['platform'] == 'Polymarket') &
    (master_df['is_closed'] == True) &
    (master_df['political_category'] == '1. ELECTORAL') &
    (master_df['country'] == 'United States')
].copy()

print(f"\n✓ Total closed US electoral Polymarket markets: {len(us_electoral):,}")

# Check each market to see if it needs truncation
# A market needs truncation if its last price is NOT >24h before trading_close_time
# (i.e., it has full data)

markets_needing_truncation = []
no_price_data = []
no_close_time = []
already_truncated = []
previously_truncated_by_script = []

for idx, market in us_electoral.iterrows():
    market_id = str(market['market_id'])

    # Skip if already truncated by previous scripts
    if market_id in previously_truncated_ids:
        previously_truncated_by_script.append(market_id)
        continue
    token_yes = str(market['pm_token_id_yes']) if pd.notna(market.get('pm_token_id_yes')) else None

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

    # Check if needs truncation: last price is NOT >24h before close_time
    diff_hours = (close_time - last_date).total_seconds() / 3600

    if diff_hours <= 24:
        # This market has full data and needs truncation
        markets_needing_truncation.append({
            'market_id': market_id,
            'question': market['question'],
            'country': market['country'],
            'office': market.get('office', ''),
            'location': market.get('location', ''),
            'election_year': market.get('election_year', ''),
            'is_primary': market.get('is_primary', False),
            'pm_token_id_yes': token_yes,
            'pm_token_id_no': str(market['pm_token_id_no']) if pd.notna(market.get('pm_token_id_no')) else '',
            'trading_close_time': close_time_val,
            'last_price_date': last_date.isoformat(),
            'hours_before_close': diff_hours
        })
    else:
        # Already truncated (>24h before close)
        already_truncated.append(market_id)

# Save markets CSV
print(f"\n{'='*80}")
print("RESULTS")
print(f"{'='*80}")

print(f"\n✓ Markets needing truncation: {len(markets_needing_truncation)}")
print(f"  - Previously truncated by script: {len(previously_truncated_by_script)}")
print(f"  - Already truncated (over-truncated check): {len(already_truncated)}")
print(f"  - No price data: {len(no_price_data)}")
print(f"  - No close_time: {len(no_close_time)}")

if len(markets_needing_truncation) > 0:
    markets_df = pd.DataFrame(markets_needing_truncation)
    markets_df.to_csv(OUTPUT_MARKETS_FILE, index=False)
    print(f"\n✓ Saved markets to: {OUTPUT_MARKETS_FILE}")

    # Group by unique elections
    print(f"\n{'='*80}")
    print("GROUPING BY UNIQUE ELECTIONS")
    print(f"{'='*80}")

    # Group by (country, office, location, election_year, is_primary)
    election_groups = markets_df.groupby(
        ['country', 'office', 'location', 'election_year', 'is_primary'],
        dropna=False
    ).agg({
        'market_id': 'count',  # Count markets per election
        'question': lambda x: list(x)[:3]  # Sample questions (max 3)
    }).reset_index()

    election_groups.rename(columns={'market_id': 'market_count'}, inplace=True)

    # Convert sample questions to string
    election_groups['sample_questions'] = election_groups['question'].apply(lambda x: ' | '.join(x))
    election_groups.drop('question', axis=1, inplace=True)

    # Sort by market count (most markets first)
    election_groups = election_groups.sort_values('market_count', ascending=False)

    print(f"\n✓ Unique elections: {len(election_groups)}")
    print(f"  Total markets: {election_groups['market_count'].sum()}")

    # Show breakdown
    print(f"\n  By office:")
    office_counts = election_groups.groupby('office')['market_count'].agg(['count', 'sum'])
    for office, row in office_counts.iterrows():
        print(f"    {office}: {row['count']} elections, {row['sum']} markets")

    # Save elections CSV
    election_groups.to_csv(OUTPUT_ELECTIONS_FILE, index=False)
    print(f"\n✓ Saved elections to: {OUTPUT_ELECTIONS_FILE}")

    # Show sample
    print(f"\n{'='*80}")
    print("SAMPLE ELECTIONS (Top 10 by market count):")
    print(f"{'='*80}")
    for idx, row in election_groups.head(10).iterrows():
        print(f"\n{row['election_year']} {row['office']} - {row['location']}")
        print(f"  Primary: {row['is_primary']}")
        print(f"  Markets: {row['market_count']}")
        print(f"  Sample: {row['sample_questions'][:150]}...")

else:
    print("\n✓ No markets need truncation - all already truncated!")

print(f"\n{'='*80}")
print("COMPLETE")
print(f"{'='*80}")
