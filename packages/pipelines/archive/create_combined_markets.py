#!/usr/bin/env python3
"""
Create Combined Political Markets Dataset

Combines Polymarket and Kalshi political markets into a single standardized CSV.
- Deduplicates Polymarket (2 rows per market → 1 row)
- Standardizes common fields (platform, market_id, question, volume_usd, etc.)
- Preserves all platform-specific fields with pm_ and k_ prefixes
"""

import pandas as pd
import json
import os
from datetime import datetime

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
POLYMARKET_FILE = f"{DATA_DIR}/market_categories_with_outcomes.csv"
KALSHI_FILE = f"{DATA_DIR}/kalshi_all_political_with_categories.json"
OUTPUT_FILE = f"{DATA_DIR}/combined_political_markets.csv"

print("=" * 80)
print("CREATE COMBINED POLITICAL MARKETS DATASET")
print("=" * 80)

# ============================================================================
# Load Data
# ============================================================================

print(f"\n📊 Loading Polymarket data from: {POLYMARKET_FILE}")
df_pm = pd.read_csv(POLYMARKET_FILE)
print(f"✓ Loaded {len(df_pm):,} Polymarket rows")

print(f"\n📊 Loading Kalshi data from: {KALSHI_FILE}")
with open(KALSHI_FILE, 'r') as f:
    kalshi_data = json.load(f)
df_kalshi = pd.DataFrame(kalshi_data)
print(f"✓ Loaded {len(df_kalshi):,} Kalshi markets")

# ============================================================================
# Process Polymarket Data
# ============================================================================

print(f"\n{'=' * 80}")
print("PROCESSING POLYMARKET DATA")
print(f"{'=' * 80}")

# Deduplicate to one row per market (keep first row, typically Yes/index 0)
print(f"\nDeduplicating Polymarket markets...")
print(f"  Before: {len(df_pm):,} rows ({df_pm['market_id'].nunique():,} unique markets)")

# Group by market_id to get both outcomes
pm_markets = []

for market_id, group in df_pm.groupby('market_id'):
    # Take the first row as base
    row = group.iloc[0].copy()

    # Parse outcome from outcomePrices
    outcome_prices_str = row.get('outcomePrices', '')
    winning_outcome = None
    resolution_outcome = None

    if pd.notna(outcome_prices_str) and outcome_prices_str:
        try:
            outcome_prices = json.loads(outcome_prices_str.replace("'", '"'))

            # Find which outcome won (price = "1")
            if "1" in outcome_prices:
                winning_index = outcome_prices.index("1")

                # Map to outcome name
                outcome_row = group[group['outcome_index'] == winning_index]
                if len(outcome_row) > 0:
                    winning_outcome = outcome_row.iloc[0]['outcome_name']
                    resolution_outcome = winning_outcome
        except:
            pass

    # Get token IDs for both outcomes
    yes_row = group[group['outcome_name'] == 'Yes']
    no_row = group[group['outcome_name'] == 'No']

    token_id_yes = yes_row.iloc[0]['token_id'] if len(yes_row) > 0 else None
    token_id_no = no_row.iloc[0]['token_id'] if len(no_row) > 0 else None

    # Create standardized row
    pm_row = {
        # Standardized fields
        'platform': 'Polymarket',
        'market_id': str(market_id),
        'question': row['question'],
        'political_category': row['political_category'],
        'election_type': row.get('election_type'),
        'party_affiliation': row.get('party_affiliation'),
        'is_closed': bool(row['closed']),
        'resolution_outcome': resolution_outcome,
        'winning_outcome': winning_outcome,
        'volume_usd': float(row['volume']),
        'trading_close_time': row.get('closedTime'),
        'scheduled_end_time': row.get('endDate'),

        # Election data
        'democrat_vote_share': row.get('democrat_vote_share'),
        'republican_vote_share': row.get('republican_vote_share'),
        'vote_share_source': row.get('vote_share_source'),

        # Polymarket-specific fields (with pm_ prefix)
        'pm_outcome_prices': outcome_prices_str,
        'pm_token_id_yes': token_id_yes,
        'pm_token_id_no': token_id_no,
        'pm_has_price_data': row.get('has_price_data'),
        'pm_uma_resolution_status': row.get('umaResolutionStatus'),
        'pm_closed': row.get('closed'),
    }

    pm_markets.append(pm_row)

df_pm_processed = pd.DataFrame(pm_markets)
print(f"  After: {len(df_pm_processed):,} rows (1 per market)")

# ============================================================================
# Process Kalshi Data
# ============================================================================

print(f"\n{'=' * 80}")
print("PROCESSING KALSHI DATA")
print(f"{'=' * 80}")

kalshi_markets = []

for idx, row in df_kalshi.iterrows():
    # Determine if market is closed
    status = row.get('status', '')
    is_closed = status in ['closed', 'settled', 'finalized']

    # Get resolution outcome
    result = row.get('result', '')
    winning_outcome = None
    if result == 'yes':
        winning_outcome = 'Yes'
    elif result == 'no':
        winning_outcome = 'No'

    # Volume is already in USD (contracts at $1 each)
    volume_usd = float(row.get('volume', 0))

    # Create standardized row
    k_row = {
        # Standardized fields
        'platform': 'Kalshi',
        'market_id': row['ticker'],
        'question': row['title'],
        'political_category': row['political_category'],
        'election_type': row.get('election_type'),
        'party_affiliation': row.get('party_affiliation'),
        'is_closed': is_closed,
        'resolution_outcome': result if result else None,
        'winning_outcome': winning_outcome,
        'volume_usd': volume_usd,
        'trading_close_time': row.get('close_time'),
        'scheduled_end_time': row.get('expected_expiration_time'),

        # Election data (Kalshi doesn't have this)
        'democrat_vote_share': None,
        'republican_vote_share': None,
        'vote_share_source': None,

        # Polymarket fields (NULL for Kalshi)
        'pm_outcome_prices': None,
        'pm_token_id_yes': None,
        'pm_token_id_no': None,
        'pm_has_price_data': None,
        'pm_uma_resolution_status': None,
        'pm_closed': None,

        # Kalshi-specific fields (with k_ prefix)
        'k_event_ticker': row.get('event_ticker'),
        'k_market_type': row.get('market_type'),
        'k_subtitle': row.get('subtitle'),
        'k_yes_sub_title': row.get('yes_sub_title'),
        'k_no_sub_title': row.get('no_sub_title'),
        'k_open_time': row.get('open_time'),
        'k_expiration_time': row.get('expiration_time'),
        'k_latest_expiration_time': row.get('latest_expiration_time'),
        'k_settlement_timer_seconds': row.get('settlement_timer_seconds'),
        'k_status': status,
        'k_settlement_value': row.get('settlement_value'),
        'k_settlement_value_dollars': row.get('settlement_value_dollars'),
        'k_expiration_value': row.get('expiration_value'),
        'k_notional_value': row.get('notional_value'),
        'k_notional_value_dollars': row.get('notional_value_dollars'),
        'k_volume_contracts': row.get('volume'),
        'k_volume_24h': row.get('volume_24h'),
        'k_liquidity': row.get('liquidity'),
        'k_liquidity_dollars': row.get('liquidity_dollars'),
        'k_open_interest': row.get('open_interest'),
        'k_risk_limit_cents': row.get('risk_limit_cents'),
        'k_yes_bid': row.get('yes_bid'),
        'k_yes_ask': row.get('yes_ask'),
        'k_no_bid': row.get('no_bid'),
        'k_no_ask': row.get('no_ask'),
        'k_last_price': row.get('last_price'),
        'k_yes_bid_dollars': row.get('yes_bid_dollars'),
        'k_yes_ask_dollars': row.get('yes_ask_dollars'),
        'k_no_bid_dollars': row.get('no_bid_dollars'),
        'k_no_ask_dollars': row.get('no_ask_dollars'),
        'k_last_price_dollars': row.get('last_price_dollars'),
        'k_previous_yes_bid': row.get('previous_yes_bid'),
        'k_previous_yes_ask': row.get('previous_yes_ask'),
        'k_previous_price': row.get('previous_price'),
        'k_previous_yes_bid_dollars': row.get('previous_yes_bid_dollars'),
        'k_previous_yes_ask_dollars': row.get('previous_yes_ask_dollars'),
        'k_previous_price_dollars': row.get('previous_price_dollars'),
        'k_response_price_units': row.get('response_price_units'),
        'k_rules_primary': row.get('rules_primary'),
        'k_rules_secondary': row.get('rules_secondary'),
        'k_can_close_early': row.get('can_close_early'),
        'k_early_close_condition': row.get('early_close_condition'),
        'k_tick_size': row.get('tick_size'),
        'k_strike_type': row.get('strike_type'),
        'k_custom_strike': str(row.get('custom_strike')) if pd.notna(row.get('custom_strike')) else None,
        'k_floor_strike': row.get('floor_strike'),
        'k_cap_strike': row.get('cap_strike'),
        'k_category': row.get('category'),
        'k_ai_classified_political': row.get('ai_classified_political'),
    }

    kalshi_markets.append(k_row)

df_kalshi_processed = pd.DataFrame(kalshi_markets)
print(f"✓ Processed {len(df_kalshi_processed):,} Kalshi markets")

# ============================================================================
# Combine Datasets
# ============================================================================

print(f"\n{'=' * 80}")
print("COMBINING DATASETS")
print(f"{'=' * 80}")

# Ensure same columns
all_columns = sorted(set(df_pm_processed.columns) | set(df_kalshi_processed.columns))

# Add missing columns to each dataframe
for col in all_columns:
    if col not in df_pm_processed.columns:
        df_pm_processed[col] = None
    if col not in df_kalshi_processed.columns:
        df_kalshi_processed[col] = None

# Reorder columns: standardized first, then pm_, then k_
standard_cols = [c for c in all_columns if not c.startswith('pm_') and not c.startswith('k_')]
pm_cols = [c for c in all_columns if c.startswith('pm_')]
k_cols = [c for c in all_columns if c.startswith('k_')]
column_order = standard_cols + pm_cols + k_cols

df_pm_processed = df_pm_processed[column_order]
df_kalshi_processed = df_kalshi_processed[column_order]

# Combine
df_combined = pd.concat([df_pm_processed, df_kalshi_processed], ignore_index=True)

print(f"\n✓ Combined dataset:")
print(f"  Polymarket markets: {len(df_pm_processed):,}")
print(f"  Kalshi markets: {len(df_kalshi_processed):,}")
print(f"  Total: {len(df_combined):,}")
print(f"  Columns: {len(df_combined.columns)}")

# ============================================================================
# Save Output
# ============================================================================

print(f"\n{'=' * 80}")
print("SAVING OUTPUT")
print(f"{'=' * 80}")

df_combined.to_csv(OUTPUT_FILE, index=False)
print(f"\n✓ Saved combined dataset to: {OUTPUT_FILE}")

# Summary statistics
print(f"\n{'=' * 80}")
print("SUMMARY STATISTICS")
print(f"{'=' * 80}")

print(f"\nMarkets by platform:")
print(df_combined['platform'].value_counts())

print(f"\nMarkets by political category (top 10):")
print(df_combined['political_category'].value_counts().head(10))

print(f"\nMarkets by election type (top 10):")
print(df_combined['election_type'].value_counts().head(10))

print(f"\nClosed vs Open markets:")
print(df_combined['is_closed'].value_counts())

print(f"\nTotal volume by platform:")
vol_by_platform = df_combined.groupby('platform')['volume_usd'].sum()
for platform, vol in vol_by_platform.items():
    print(f"  {platform}: ${vol:,.0f}")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nOutput file: {OUTPUT_FILE}")
print(f"Total markets: {len(df_combined):,}")
print(f"Total columns: {len(df_combined.columns)}")
