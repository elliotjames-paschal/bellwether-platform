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
from config import BASE_DIR, DATA_DIR

# Time horizons to analyze (days before event/reference date)
TIME_HORIZONS = [60, 30, 20, 14, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1, 0]

# Platform-specific offsets for non-electoral markets (days before trading_close_time)
# These account for post-event trading windows:
# - Polymarket: Markets close 24-48 hours after event outcome is known
# - Kalshi: Markets settle 3-12 hours after outcome is known
POLYMARKET_NONELECTION_OFFSET = 2  # Use price 2 days before trading_close_time
KALSHI_NONELECTION_OFFSET = 1      # Use price 1 day before trading_close_time

# Ultra-short market threshold: markets with this many days or less of price history
# will NOT have truncation applied (since there's no "pre-event" period to capture)
ULTRA_SHORT_THRESHOLD_DAYS = 2

# Categories that should skip truncation (inherently ultra-short events)
NO_TRUNCATION_CATEGORIES = ['15. POLITICAL_SPEECH']

# Electoral markets: use 8am UTC on election day as reference
# (polls don't close until late evening US time, so 8am UTC is still pre-outcome)
ELECTION_DAY_HOUR_UTC = 8

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
# IMPORTANT: Store as UTC to match price timestamps which are in UTC
# Use 8am UTC on election day (polls don't close until late evening US time)
election_dates_lookup = {}
for _, row in election_dates_df.iterrows():
    key = (
        str(row['country']).strip(),
        str(row['office']).strip(),
        str(row['location']).strip(),
        int(row['election_year']) if pd.notna(row['election_year']) else None
    )
    # Parse as UTC and set to 8am UTC on election day
    dt = pd.to_datetime(row['election_date'])
    election_dates_lookup[key] = dt.replace(hour=ELECTION_DAY_HOUR_UTC, tzinfo=timezone.utc)

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


def get_first_price_date(price_history):
    """Get the datetime of the first price point in the history (UTC)."""
    if not price_history:
        return None
    first = min(price_history, key=lambda x: x['t'])
    return datetime.fromtimestamp(first['t'], tz=timezone.utc)


def get_price_duration_days(price_history):
    """Calculate the duration of price history in days."""
    if not price_history or len(price_history) < 2:
        return 0
    first_ts = min(p['t'] for p in price_history)
    last_ts = max(p['t'] for p in price_history)
    return (last_ts - first_ts) / 86400  # seconds to days


def get_kalshi_price_duration_days(candlesticks):
    """Calculate the duration of Kalshi candlestick history in days."""
    if not candlesticks or len(candlesticks) < 2:
        return 0
    first_ts = min(c.get('end_period_ts', 0) for c in candlesticks)
    last_ts = max(c.get('end_period_ts', 0) for c in candlesticks)
    return (last_ts - first_ts) / 86400  # seconds to days


def get_fallback_reference_date(market_row, platform):
    """
    Get a fallback reference date based on trading_close_time.
    Used when election date lookup returns a date before price data starts.
    """
    if pd.notna(market_row.get('trading_close_time')):
        try:
            trading_close_time = pd.to_datetime(market_row['trading_close_time'], utc=True)

            # Apply platform-specific offset (same as non-electoral logic)
            if platform == 'Polymarket':
                return trading_close_time - timedelta(days=POLYMARKET_NONELECTION_OFFSET)
            else:
                return trading_close_time - timedelta(days=KALSHI_NONELECTION_OFFSET)
        except:
            pass
    return None


def get_first_kalshi_price_date(candlesticks):
    """Get the datetime of the first candlestick in the history (UTC)."""
    if not candlesticks:
        return None
    first = min(candlesticks, key=lambda x: x.get('end_period_ts', 0))
    ts = first.get('end_period_ts', 0)
    if ts:
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    return None


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


def get_reference_date(market_row, is_election, platform, price_duration_days=None, category=None):
    """
    Determine the reference date for calculating time horizons.

    METHODOLOGY:
    - Electoral markets: Use 8am UTC on election day from election_dates_lookup.csv
    - Non-electoral Polymarket: Use trading_close_time - 2 days (accounts for 24-48hr post-event window)
    - Non-electoral Kalshi: Use trading_close_time - 1 day (accounts for 3-12hr post-event window)
    - Ultra-short markets (≤2 days of price history): No truncation offset applied
    - POLITICAL_SPEECH and similar categories: No truncation offset applied

    Args:
        market_row: Row from master CSV
        is_election: Boolean indicating if this is an electoral market
        platform: 'Polymarket' or 'Kalshi'
        price_duration_days: Duration of price history in days (optional, used for ultra-short detection)
        category: Political category string (optional, used for category-based truncation skip)

    Returns:
        datetime object for reference date, or None if cannot be determined
    """
    if is_election:
        # For electoral markets, use the actual election date (8am UTC)
        election_date = get_election_date(market_row)
        if election_date is not None:
            return election_date
        # Fall back to trading_close_time if election date not found
        # (This shouldn't happen for properly tagged electoral markets)

    # Parse trading_close_time (keep as UTC for consistent timestamp conversion)
    trading_close_time = None
    if pd.notna(market_row.get('trading_close_time')):
        try:
            trading_close_time = pd.to_datetime(market_row['trading_close_time'], utc=True)
        except:
            pass

    if trading_close_time is None:
        return None

    # For categories that skip truncation (e.g., POLITICAL_SPEECH), use trading_close_time directly
    if category and category in NO_TRUNCATION_CATEGORIES:
        return trading_close_time

    # For ultra-short markets, don't apply truncation offset
    # (there's no pre-event period to capture)
    if price_duration_days is not None and price_duration_days <= ULTRA_SHORT_THRESHOLD_DAYS:
        return trading_close_time

    # For non-electoral markets with sufficient history, apply platform-specific offset
    if platform == 'Kalshi':
        return trading_close_time - timedelta(days=KALSHI_NONELECTION_OFFSET)
    else:  # Polymarket
        return trading_close_time - timedelta(days=POLYMARKET_NONELECTION_OFFSET)


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

pm_ultra_short_count = 0

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

    # Calculate price duration from available token data (for ultra-short detection)
    price_duration = None
    if token_id_yes and token_id_yes in pm_prices and pm_prices[token_id_yes]:
        price_duration = get_price_duration_days(pm_prices[token_id_yes])
    elif token_id_no and token_id_no in pm_prices and pm_prices[token_id_no]:
        price_duration = get_price_duration_days(pm_prices[token_id_no])

    # Track ultra-short markets
    if price_duration is not None and price_duration <= ULTRA_SHORT_THRESHOLD_DAYS:
        pm_ultra_short_count += 1

    # Get the reference date (with ultra-short market and category handling)
    reference_date = get_reference_date(market, is_election, 'Polymarket', price_duration, category)

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

        # Check if reference_date is before first price point - if so, use fallback
        effective_reference_date = reference_date
        first_price_date = get_first_price_date(price_history_yes)
        if first_price_date and reference_date < first_price_date:
            fallback = get_fallback_reference_date(market, 'Polymarket')
            if fallback:
                effective_reference_date = fallback

        for days_before in TIME_HORIZONS:
            # Get price at N days before reference date
            price = get_polymarket_price_at_time_horizon(price_history_yes, effective_reference_date, days_before)

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
                'reference_date': effective_reference_date.isoformat(),
                'is_election': is_election,
                'days_before_event': days_before,
                'prediction_price': price,
                'actual_outcome': actual_outcome_yes,
                'prediction_error': prediction_error,
                'brier_score': brier_score,
                'year': effective_reference_date.year,
                'price_count': price_count_yes,
                'price_duration_days': price_duration,
                'is_ultra_short': price_duration is not None and price_duration <= ULTRA_SHORT_THRESHOLD_DAYS
            })

    # Process No token
    if token_id_no and token_id_no in pm_prices:
        # No token wins if winning_outcome == 'No'
        actual_outcome_no = 1 if winning_outcome == 'No' else 0
        price_history_no = pm_prices[token_id_no]
        price_count_no = count_price_points(price_history_no)

        # Check if reference_date is before first price point - if so, use fallback
        effective_reference_date_no = reference_date
        first_price_date_no = get_first_price_date(price_history_no)
        if first_price_date_no and reference_date < first_price_date_no:
            fallback = get_fallback_reference_date(market, 'Polymarket')
            if fallback:
                effective_reference_date_no = fallback

        for days_before in TIME_HORIZONS:
            # Get price at N days before reference date
            price = get_polymarket_price_at_time_horizon(price_history_no, effective_reference_date_no, days_before)

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
                'reference_date': effective_reference_date_no.isoformat(),
                'is_election': is_election,
                'days_before_event': days_before,
                'prediction_price': price,
                'actual_outcome': actual_outcome_no,
                'prediction_error': prediction_error,
                'brier_score': brier_score,
                'year': effective_reference_date_no.year,
                'price_count': price_count_no,
                'price_duration_days': price_duration,
                'is_ultra_short': price_duration is not None and price_duration <= ULTRA_SHORT_THRESHOLD_DAYS
            })

pm_df = pd.DataFrame(pm_results)
print(f"   ✓ Generated {len(pm_df):,} prediction records across all time horizons")
print(f"   ✓ Covering {pm_df['market_id'].nunique():,} unique markets")
print(f"   ✓ Election date lookup: {pm_election_date_found} found, {pm_election_date_missing} missing")
print(f"   ✓ Ultra-short markets (≤{ULTRA_SHORT_THRESHOLD_DAYS} days, no truncation): {pm_ultra_short_count:,}")

# ============================================================================
# 6. Process Kalshi Markets
# ============================================================================

print("\n5. Processing Kalshi markets...")

kalshi_results = []
kalshi_election_date_found = 0
kalshi_election_date_missing = 0
kalshi_ultra_short_count = 0

for _, market in kalshi_markets_df.iterrows():
    ticker = market['market_id']  # In master file, market_id is the ticker

    # Check if this is an election market using political_category
    category = str(market.get('political_category', ''))
    is_election = category.startswith('1.') or 'ELECTORAL' in category.upper()

    # Skip if no price data
    if ticker not in kalshi_prices:
        continue

    # Get candlestick data and calculate price duration
    candlesticks = kalshi_prices[ticker]
    price_count = len(candlesticks) if candlesticks else 0
    price_duration = get_kalshi_price_duration_days(candlesticks)

    # Track ultra-short markets
    if price_duration <= ULTRA_SHORT_THRESHOLD_DAYS:
        kalshi_ultra_short_count += 1

    # Get the reference date (with ultra-short market and category handling)
    reference_date = get_reference_date(market, is_election, 'Kalshi', price_duration, category)

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

    # Check if reference_date is before first price point - if so, use fallback
    effective_reference_date = reference_date
    first_price_date = get_first_kalshi_price_date(candlesticks)
    if first_price_date and reference_date < first_price_date:
        fallback = get_fallback_reference_date(market, 'Kalshi')
        if fallback:
            effective_reference_date = fallback

    # Calculate predictions at each time horizon
    for days_before in TIME_HORIZONS:
        # Get price at N days before reference date
        price = get_kalshi_price_at_time_horizon(candlesticks, effective_reference_date, days_before)

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
            'reference_date': effective_reference_date.isoformat(),
            'is_election': is_election,
            'days_before_event': days_before,
            'prediction_price': price,
            'actual_outcome': actual_outcome,
            'prediction_error': prediction_error,
            'brier_score': brier_score,
            'year': effective_reference_date.year,
            'price_count': price_count,
            'price_duration_days': price_duration,
            'is_ultra_short': price_duration <= ULTRA_SHORT_THRESHOLD_DAYS
        })

kalshi_df = pd.DataFrame(kalshi_results)
print(f"   ✓ Generated {len(kalshi_df):,} prediction records across all time horizons")
if len(kalshi_df) > 0:
    print(f"   ✓ Covering {len(kalshi_df['ticker'].unique()):,} unique markets")
    print(f"   ✓ Election date lookup: {kalshi_election_date_found} found, {kalshi_election_date_missing} missing")
    print(f"   ✓ Ultra-short markets (≤{ULTRA_SHORT_THRESHOLD_DAYS} days, no truncation): {kalshi_ultra_short_count:,}")

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

print("\nNEW METHODOLOGY (as of 2026-01-25):")
print("  - Electoral markets: Use actual election date as reference")
print("  - Non-electoral Polymarket: Use trading_close_time - 2 days")
print("  - Non-electoral Kalshi: Use trading_close_time - 1 day")

print("\nPolymarket:")
for days in TIME_HORIZONS:
    subset = pm_df_filtered[pm_df_filtered['days_before_event'] == days]
    if len(subset) > 0:
        print(f"  {days} days before event: {len(subset):,} predictions, "
              f"Brier={subset['brier_score'].mean():.4f}, "
              f"MAE={subset['prediction_error'].abs().mean():.4f}")

# Report ultra-short markets
if 'is_ultra_short' in pm_df_filtered.columns:
    ultra_short_pm = pm_df_filtered[pm_df_filtered['is_ultra_short'] == True]
    if len(ultra_short_pm) > 0:
        print(f"\n  Ultra-short markets (single price point): {len(ultra_short_pm):,} predictions")

print("\nKalshi:")
for days in TIME_HORIZONS:
    subset = kalshi_df_filtered[kalshi_df_filtered['days_before_event'] == days]
    if len(subset) > 0:
        print(f"  {days} days before event: {len(subset):,} predictions, "
              f"Brier={subset['brier_score'].mean():.4f}, "
              f"MAE={subset['prediction_error'].abs().mean():.4f}")

# Report ultra-short markets for Kalshi
if 'is_ultra_short' in kalshi_df_filtered.columns:
    ultra_short_k = kalshi_df_filtered[kalshi_df_filtered['is_ultra_short'] == True]
    if len(ultra_short_k) > 0:
        print(f"\n  Ultra-short markets (single price point): {len(ultra_short_k):,} predictions")

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
