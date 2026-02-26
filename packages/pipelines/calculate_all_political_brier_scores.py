#!/usr/bin/env python3
"""
Calculate Brier Scores for ALL Political Markets Using Historical Price Data

This script processes the historical price JSONs for ALL political markets
(elections + non-elections) to calculate Brier scores at multiple time horizons
(60, 30, 14, 7, 3, 1 days before resolution).

Combines data from both election and non-election price datasets.
"""

import pandas as pd
import json
import numpy as np
from datetime import datetime, timedelta, timezone
import sys, os

# Paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import BASE_DIR, DATA_DIR, get_market_anchor_time

# Time horizons to analyze (days before event/reference date)
TIME_HORIZONS = [60, 30, 20, 14, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1, 0]

print("=" * 80)
print("CALCULATING BRIER SCORES FOR ALL POLITICAL MARKETS")
print("=" * 80)

# ============================================================================
# 1. Load Election Dates Lookup
# ============================================================================

print("\n1. Loading election dates lookup...")

election_dates_df = pd.read_csv(DATA_DIR / "election_dates_lookup.csv")
print(f"   ✓ {len(election_dates_df):,} election date records")

# Create lookup dictionary: (country, office, location, year) -> election_date (UTC)
# IMPORTANT: Store as midnight UTC on election day to match election eve price convention
election_dates_lookup = {}
for _, row in election_dates_df.iterrows():
    key = (
        str(row['country']).strip(),
        str(row['office']).strip(),
        str(row['location']).strip(),
        int(row['election_year']) if pd.notna(row['election_year']) else None
    )
    # Parse as midnight UTC on election day
    dt = pd.to_datetime(row['election_date'])
    election_dates_lookup[key] = dt.replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)

print(f"   ✓ Created lookup with {len(election_dates_lookup):,} unique election date entries")


def get_election_date(market_row):
    """
    Look up the actual election date for an electoral market.

    Args:
        market_row: Row from master CSV with country, office, location, election_year

    Returns:
        datetime object for election date, or None if not found
    """
    country = str(market_row.get('country', '')).strip() if pd.notna(market_row.get('country')) else ''
    office = str(market_row.get('office', '')).strip() if pd.notna(market_row.get('office')) else ''
    location = str(market_row.get('location', '')).strip() if pd.notna(market_row.get('location')) else ''
    year = int(market_row.get('election_year')) if pd.notna(market_row.get('election_year')) else None

    key = (country, office, location, year)
    return election_dates_lookup.get(key)


# ============================================================================
# 2. Load Market Metadata
# ============================================================================

print("\n2. Loading market metadata from master file...")

# Load master file (market-level, combined Polymarket + Kalshi)
master_df = pd.read_csv(DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv", low_memory=False)
print(f"   ✓ {len(master_df):,} total market records")

# Separate by platform
pm_markets = master_df[master_df['platform'] == 'Polymarket'].copy()
kalshi_markets_df = master_df[master_df['platform'] == 'Kalshi'].copy()

print(f"   ✓ {len(pm_markets):,} Polymarket markets")
print(f"   ✓ {len(kalshi_markets_df):,} Kalshi markets")

# ============================================================================
# 3. Load Historical Price Data (CORRECTED - with contamination adjustments)
# ============================================================================

print("\n3. Loading CORRECTED historical price data (contamination adjustments applied)...")

# Polymarket - load both price files and merge
# CORRECTED is preferred when non-empty, fall back to v3 for empty/missing entries
with open(DATA_DIR / "polymarket_all_political_prices_CORRECTED.json", 'r') as f:
    pm_prices_main = json.load(f)
with open(DATA_DIR / "polymarket_all_political_prices_CORRECTED_v3.json", 'r') as f:
    pm_prices_v3 = json.load(f)

# Merge: use CORRECTED data if non-empty, otherwise use v3
pm_prices = {}
all_tokens = set(pm_prices_main.keys()) | set(pm_prices_v3.keys())
for token in all_tokens:
    main_data = pm_prices_main.get(token, [])
    v3_data = pm_prices_v3.get(token, [])
    # Prefer CORRECTED if it has data, otherwise use v3
    pm_prices[token] = main_data if main_data else v3_data

main_only = sum(1 for t in pm_prices if pm_prices[t] == pm_prices_main.get(t, []) and pm_prices_main.get(t))
v3_fallback = sum(1 for t in pm_prices if pm_prices[t] == pm_prices_v3.get(t, []) and not pm_prices_main.get(t))
print(f"   ✓ Loaded {len(pm_prices):,} Polymarket tokens (merged CORRECTED + v3)")
print(f"     - {main_only:,} from CORRECTED, {v3_fallback:,} from v3 fallback")

# Kalshi - load corrected all-political prices (v3: truncated at actual trading_close_time)
with open(DATA_DIR / "kalshi_all_political_prices_CORRECTED_v3.json", 'r') as f:
    kalshi_prices = json.load(f)
print(f"   ✓ Loaded {len(kalshi_prices):,} Kalshi markets (CORRECTED v3)")

# ============================================================================
# 4. Helper Functions
# ============================================================================

def get_polymarket_last_price(price_history):
    """
    Get the last available price from price history

    Args:
        price_history: List of {'t': timestamp, 'p': price} objects

    Returns:
        Price (float) or None if no valid price exists
    """
    if not price_history:
        return None

    # Get the latest price
    latest = max(price_history, key=lambda x: x['t'])
    return float(latest['p'])


def get_polymarket_price_at_time_horizon(price_history, resolution_date, days_before):
    """
    Get the price N days before resolution

    Args:
        price_history: List of {'t': timestamp, 'p': price} objects
        resolution_date: datetime object for market resolution
        days_before: Number of days before resolution

    Returns:
        Price (float) or None if no valid price exists
    """
    if not price_history:
        return None

    target_date = resolution_date - timedelta(days=days_before)
    target_timestamp = int(target_date.timestamp())

    # Filter prices before or at target timestamp
    valid_prices = [p for p in price_history if p['t'] <= target_timestamp]

    if not valid_prices:
        return None

    # Get the closest price (latest before target)
    closest = max(valid_prices, key=lambda x: x['t'])
    return float(closest['p'])


def extract_kalshi_price(candle):
    """Extract price from candlestick, trying multiple fallbacks"""
    price_obj = candle.get('price', {})

    # Try close price first
    close_price = price_obj.get('close')
    if close_price is not None:
        return close_price / 100.0

    # Fallback to previous price
    previous_price = price_obj.get('previous')
    if previous_price is not None:
        return previous_price / 100.0

    # Fallback to midpoint of bid/ask
    yes_ask = candle.get('yes_ask', {}).get('close')
    yes_bid = candle.get('yes_bid', {}).get('close')
    if yes_ask is not None and yes_bid is not None:
        return (yes_ask + yes_bid) / 2 / 100.0

    # Fallback to just ask or just bid
    if yes_ask is not None:
        return yes_ask / 100.0
    if yes_bid is not None:
        return yes_bid / 100.0

    return None


def get_kalshi_last_price(candlesticks):
    """
    Get the last available price from Kalshi candlestick data

    Args:
        candlesticks: List of candlestick objects with 'end_period_ts' and 'price'

    Returns:
        Price (float) or None if no valid price exists
    """
    if not candlesticks:
        return None

    # Get the latest candlestick
    latest = max(candlesticks, key=lambda x: x['end_period_ts'])
    return extract_kalshi_price(latest)


def get_kalshi_price_at_time_horizon(candlesticks, resolution_date, days_before):
    """
    Get the price N days before resolution from Kalshi candlestick data
    Uses time-based lookup mirroring Polymarket's approach

    Args:
        candlesticks: List of candlestick objects with 'end_period_ts' and 'price'
        resolution_date: datetime object for market resolution
        days_before: Number of days before resolution

    Returns:
        Price (float) or None if no valid price exists
    """
    if not candlesticks:
        return None

    # Use time-based lookup for all horizons (mirrors Polymarket approach)
    target_date = resolution_date - timedelta(days=days_before)
    target_timestamp = int(target_date.timestamp())

    # Filter candlesticks before or at target timestamp
    valid_candles = [c for c in candlesticks if c['end_period_ts'] <= target_timestamp]

    if not valid_candles:
        return None

    # Get the closest candlestick (latest before target)
    closest = max(valid_candles, key=lambda x: x['end_period_ts'])

    return extract_kalshi_price(closest)


def count_price_points(price_history):
    """Count unique price points in history"""
    if not price_history:
        return 0
    return len(price_history)


# ============================================================================
# 5. Process Polymarket Markets
# ============================================================================

print("\n4. Processing Polymarket markets...")

pm_results = []
pm_election_date_found = 0
pm_election_date_missing = 0

for _, market in pm_markets.iterrows():
    market_id = market['market_id']

    # Check if this is an election market using political_category
    category = str(market.get('political_category', ''))
    is_election = category.startswith('1.') or 'ELECTORAL' in category.upper()

    # Determine winning outcome (Yes or No)
    winning_outcome = market.get('winning_outcome', '')

    # Skip unresolved markets (winning_outcome must be 'Yes' or 'No')
    if winning_outcome not in ['Yes', 'No']:
        continue

    # Process both Yes and No tokens for this market
    # Token 1: Yes outcome
    token_id_yes = str(market['pm_token_id_yes']) if pd.notna(market.get('pm_token_id_yes')) else None
    # Token 2: No outcome
    token_id_no = str(market['pm_token_id_no']) if pd.notna(market.get('pm_token_id_no')) else None

    # Get the reference date using shared anchor logic
    reference_date = get_market_anchor_time(market, is_election, get_election_date)

    if reference_date is None:
        continue

    # Track election date lookup success/failure
    if is_election:
        election_date = get_election_date(market)
        if election_date is not None:
            pm_election_date_found += 1
        else:
            pm_election_date_missing += 1

    # Process Yes token
    if token_id_yes and token_id_yes in pm_prices:
        # Yes token wins if winning_outcome == 'Yes'
        actual_outcome_yes = 1 if winning_outcome == 'Yes' else 0
        price_history_yes = pm_prices[token_id_yes]
        price_count_yes = count_price_points(price_history_yes)

        for days_before in TIME_HORIZONS:
            # Get price at N days before reference date
            price = get_polymarket_price_at_time_horizon(price_history_yes, reference_date, days_before)

            if price is None:
                continue

            prediction_error = price - actual_outcome_yes
            brier_score = prediction_error ** 2

            pm_results.append({
                'market_id': market_id,
                'token_id': token_id_yes,
                'title': market.get('question', ''),
                'category': market.get('political_category', ''),
                'election_type': market.get('election_type', 'NA'),
                'party_affiliation': market.get('party_affiliation', ''),
                'outcome_name': 'Yes',
                'trading_close_time': str(market.get('trading_close_time', '')),
                'reference_date': reference_date.isoformat(),
                'is_election': is_election,
                'days_before_event': days_before,
                'prediction_price': price,
                'actual_outcome': actual_outcome_yes,
                'prediction_error': prediction_error,
                'brier_score': brier_score,
                'year': reference_date.year,
                'price_count': price_count_yes,
            })

    # Process No token
    if token_id_no and token_id_no in pm_prices:
        # No token wins if winning_outcome == 'No'
        actual_outcome_no = 1 if winning_outcome == 'No' else 0
        price_history_no = pm_prices[token_id_no]
        price_count_no = count_price_points(price_history_no)

        for days_before in TIME_HORIZONS:
            # Get price at N days before reference date
            price = get_polymarket_price_at_time_horizon(price_history_no, reference_date, days_before)

            if price is None:
                continue

            prediction_error = price - actual_outcome_no
            brier_score = prediction_error ** 2

            pm_results.append({
                'market_id': market_id,
                'token_id': token_id_no,
                'title': market.get('question', ''),
                'category': market.get('political_category', ''),
                'election_type': market.get('election_type', 'NA'),
                'party_affiliation': market.get('party_affiliation', ''),
                'outcome_name': 'No',
                'trading_close_time': str(market.get('trading_close_time', '')),
                'reference_date': reference_date.isoformat(),
                'is_election': is_election,
                'days_before_event': days_before,
                'prediction_price': price,
                'actual_outcome': actual_outcome_no,
                'prediction_error': prediction_error,
                'brier_score': brier_score,
                'year': reference_date.year,
                'price_count': price_count_no,
            })

pm_df = pd.DataFrame(pm_results)
print(f"   ✓ Generated {len(pm_df):,} prediction records across all time horizons")
print(f"   ✓ Covering {pm_df['market_id'].nunique():,} unique markets")
print(f"   ✓ Election date lookup: {pm_election_date_found} found, {pm_election_date_missing} missing")

# ============================================================================
# 6. Process Kalshi Markets
# ============================================================================

print("\n5. Processing Kalshi markets...")

kalshi_results = []
kalshi_election_date_found = 0
kalshi_election_date_missing = 0

for _, market in kalshi_markets_df.iterrows():
    ticker = market['market_id']  # In master file, market_id is the ticker

    # Check if this is an election market using political_category
    category = str(market.get('political_category', ''))
    is_election = category.startswith('1.') or 'ELECTORAL' in category.upper()

    # Skip if no price data
    if ticker not in kalshi_prices:
        continue

    # Get candlestick data
    candlesticks = kalshi_prices[ticker]
    price_count = len(candlesticks) if candlesticks else 0

    # Get the reference date using shared anchor logic
    reference_date = get_market_anchor_time(market, is_election, get_election_date)

    if reference_date is None:
        continue

    # Track election date lookup success/failure
    if is_election:
        election_date = get_election_date(market)
        if election_date is not None:
            kalshi_election_date_found += 1
        else:
            kalshi_election_date_missing += 1

    # Get actual outcome from winning_outcome (set by current pipeline)
    result = market.get('winning_outcome', '')
    if result in ['yes', 'Yes']:
        actual_outcome = 1
    elif result in ['no', 'No']:
        actual_outcome = 0
    else:
        # Skip if no explicit result
        continue

    # Calculate predictions at each time horizon
    for days_before in TIME_HORIZONS:
        # Get price at N days before reference date
        price = get_kalshi_price_at_time_horizon(candlesticks, reference_date, days_before)

        if price is None:
            continue

        # Calculate metrics
        prediction_error = price - actual_outcome
        brier_score = prediction_error ** 2

        kalshi_results.append({
            'ticker': ticker,
            'title': market.get('question', ''),
            'category': market.get('political_category', ''),
            'election_type': market.get('election_type', 'NA'),
            'party_affiliation': market.get('party_affiliation', ''),
            'trading_close_time': str(market.get('trading_close_time', '')),
            'reference_date': reference_date.isoformat(),
            'is_election': is_election,
            'days_before_event': days_before,
            'prediction_price': price,
            'actual_outcome': actual_outcome,
            'prediction_error': prediction_error,
            'brier_score': brier_score,
            'year': reference_date.year,
            'price_count': price_count,
        })

kalshi_df = pd.DataFrame(kalshi_results)
print(f"   ✓ Generated {len(kalshi_df):,} prediction records across all time horizons")
if len(kalshi_df) > 0:
    print(f"   ✓ Covering {len(kalshi_df['ticker'].unique()):,} unique markets")
    print(f"   ✓ Election date lookup: {kalshi_election_date_found} found, {kalshi_election_date_missing} missing")

# ============================================================================
# 7. Prepare Final DataFrames (no hardcoded date filter)
# ============================================================================

print("\n6. Preparing final dataframes...")

# Polymarket
pm_df['reference_datetime'] = pd.to_datetime(pm_df['reference_date'], format='ISO8601')
pm_df_filtered = pm_df.copy()
print(f"   ✓ Polymarket: {len(pm_df_filtered):,} prediction records")
print(f"   ✓ {pm_df_filtered['market_id'].nunique():,} unique markets")

# Kalshi
if len(kalshi_df) > 0:
    kalshi_df['reference_datetime'] = pd.to_datetime(kalshi_df['reference_date'], format='ISO8601')
    kalshi_df_filtered = kalshi_df.copy()
    print(f"   ✓ Kalshi: {len(kalshi_df_filtered):,} prediction records")
    print(f"   ✓ {len(kalshi_df_filtered['ticker'].unique()):,} unique markets")
else:
    kalshi_df_filtered = pd.DataFrame()
    print(f"   ⚠ Kalshi: No records")

# ============================================================================
# 8. Save Results
# ============================================================================

print("\n7. Saving results...")

# Save to fixed filenames (overwrite each run - no timestamped accumulation)
pm_output = DATA_DIR / "polymarket_prediction_accuracy_all_political.csv"
pm_df_filtered.to_csv(pm_output, index=False)
print(f"   ✓ Polymarket: {pm_output}")

kalshi_output = DATA_DIR / "kalshi_prediction_accuracy_all_political.csv"
kalshi_df_filtered.to_csv(kalshi_output, index=False)
print(f"   ✓ Kalshi: {kalshi_output}")

# ============================================================================
# 9. Summary Statistics
# ============================================================================

print("\n" + "=" * 80)
print("SUMMARY STATISTICS")
print("=" * 80)

print("\nMETHODOLOGY:")
print("  - Electoral markets: Midnight UTC on election day as anchor")
print("  - Non-electoral markets: trading_close_time as anchor (no platform offset)")

print("\nPolymarket:")
for days in TIME_HORIZONS:
    subset = pm_df_filtered[pm_df_filtered['days_before_event'] == days]
    if len(subset) > 0:
        print(f"  {days} days before event: {len(subset):,} predictions, "
              f"Brier={subset['brier_score'].mean():.4f}, "
              f"MAE={subset['prediction_error'].abs().mean():.4f}")

print("\nKalshi:")
for days in TIME_HORIZONS:
    subset = kalshi_df_filtered[kalshi_df_filtered['days_before_event'] == days]
    if len(subset) > 0:
        print(f"  {days} days before event: {len(subset):,} predictions, "
              f"Brier={subset['brier_score'].mean():.4f}, "
              f"MAE={subset['prediction_error'].abs().mean():.4f}")

# Show breakdown by category
print("\nBreakdown by Political Category (7-day before event cohort):")
cohort_7d = pd.concat([
    pm_df_filtered[pm_df_filtered['days_before_event'] == 7],
    kalshi_df_filtered[kalshi_df_filtered['days_before_event'] == 7]
])

category_stats = cohort_7d.groupby('category').agg({
    'brier_score': ['count', 'mean'],
    'prediction_error': lambda x: x.abs().mean()
}).round(4)
category_stats.columns = ['n_predictions', 'mean_brier', 'mean_abs_error']
category_stats = category_stats.sort_values('n_predictions', ascending=False)
print(category_stats.to_string())

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
