#!/usr/bin/env python3
"""
Truncate Polymarket Price Data

Part of the NEW Bellwether Pipeline (January 2026+)

This script truncates the Polymarket price data appropriately:
- For elections: Truncate at end of election day (23:59:59)
- For non-elections: Truncate at trading_close_time - 24 hours (final prediction before event)

Input: polymarket_all_political_prices_CORRECTED.json (in-place truncation)
Output: polymarket_all_political_prices_CORRECTED.json

NOTE: Polymarket's trading_close_time is typically event + 24 hours, so we subtract 24 hours
to get the final prediction price before the event actually occurs.
"""

import pandas as pd
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Paths
import sys
sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, atomic_write_json

# Input/Output files - IMPORTANT: Read from CORRECTED to preserve repulled data
INPUT_PRICES = DATA_DIR / "polymarket_all_political_prices_CORRECTED.json"
OUTPUT_PRICES = DATA_DIR / "polymarket_all_political_prices_CORRECTED.json"
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
ELECTION_DATES = DATA_DIR / "election_dates_lookup.csv"

import shutil
import sys

# Add scripts dir to path for config import
sys.path.insert(0, str(Path(__file__).parent))
from config import rotate_backups

print("=" * 80)
print("TRUNCATING DOME API PRICE DATA")
print("=" * 80)

# ============================================================================
# 0. Create Backup (Safety Step)
# ============================================================================

print("\n0. Creating backup before truncation...")
BACKUP_DIR = DATA_DIR / "backups"
BACKUP_DIR.mkdir(exist_ok=True)
backup_file = BACKUP_DIR / f"pm_prices_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

if INPUT_PRICES.exists():
    shutil.copy2(INPUT_PRICES, backup_file)
    print(f"   Created backup: {backup_file.name}")

    # Rotate old backups (keep last 5)
    deleted = rotate_backups("pm_prices_backup_*.json")
    if deleted > 0:
        print(f"   Rotated {deleted} old backup(s)")
else:
    print("   WARNING: Input file does not exist, skipping backup")

# ============================================================================
# 1. Load Data
# ============================================================================

print("\n1. Loading data...")

# Load Dome API prices
with open(INPUT_PRICES, 'r') as f:
    dome_prices = json.load(f)
print(f"   Loaded {len(dome_prices):,} token price histories from Dome API")

# Load master file
master_df = pd.read_csv(MASTER_FILE, low_memory=False)
pm_markets = master_df[master_df['platform'] == 'Polymarket'].copy()
print(f"   Loaded {len(pm_markets):,} Polymarket markets from master file")

# Load election dates lookup
election_dates_df = pd.read_csv(ELECTION_DATES)
print(f"   Loaded {len(election_dates_df):,} election date records")

# ============================================================================
# 2. Build Token-to-Market Mapping
# ============================================================================

print("\n2. Building token-to-market mapping...")

# Create mapping from token_id to market info
token_to_market = {}

for _, row in pm_markets.iterrows():
    # Get token IDs
    token_yes = str(row['pm_token_id_yes']) if pd.notna(row.get('pm_token_id_yes')) else None
    token_no = str(row['pm_token_id_no']) if pd.notna(row.get('pm_token_id_no')) else None

    market_info = {
        'market_id': row['market_id'],
        'trading_close_time': row.get('trading_close_time'),
        'political_category': row.get('political_category'),
        'country': row.get('country'),
        'office': row.get('office'),
        'location': row.get('location'),
        'election_year': row.get('election_year'),
        'question': row.get('question', '')
    }

    if token_yes and token_yes != 'nan':
        token_to_market[token_yes] = market_info
    if token_no and token_no != 'nan':
        token_to_market[token_no] = market_info

print(f"   Mapped {len(token_to_market):,} tokens to markets")

# ============================================================================
# 3. Build Election Date Lookup
# ============================================================================

print("\n3. Building election date lookup...")

# Create lookup: (country, office, location, election_year) -> election_date
election_lookup = {}
for _, row in election_dates_df.iterrows():
    # Include country in key (default to 'United States' for backwards compatibility)
    country = row.get('country', 'United States') if 'country' in election_dates_df.columns else 'United States'
    key = (country, row['office'], row['location'], int(row['election_year']))
    election_date = pd.to_datetime(row['election_date']).date()
    election_lookup[key] = election_date

print(f"   Built lookup with {len(election_lookup):,} election dates")

# ============================================================================
# 4. Truncate Price Histories
# ============================================================================

print("\n4. Truncating price histories...")

truncated_prices = {}
stats = {
    'total_tokens': 0,
    'tokens_with_market_info': 0,
    'tokens_truncated_by_election_date': 0,
    'tokens_truncated_by_trading_close': 0,
    'tokens_unchanged': 0,
    'tokens_no_market_info': 0,
    'prices_removed_total': 0,
}

for token_id, price_history in dome_prices.items():
    stats['total_tokens'] += 1

    if not price_history:
        truncated_prices[token_id] = price_history
        stats['tokens_unchanged'] += 1
        continue

    # Get market info for this token
    market_info = token_to_market.get(token_id)

    if market_info is None:
        # No market info found - keep prices as-is (no truncation)
        truncated_prices[token_id] = price_history
        stats['tokens_unchanged'] += 1
        stats['tokens_no_market_info'] += 1
        continue

    stats['tokens_with_market_info'] += 1

    # Determine if this is an election market using political_category
    # This correctly handles non-partisan, international, and all election types
    category = str(market_info.get('political_category', ''))
    is_election = category.startswith('1.') or 'ELECTORAL' in category.upper()

    # Determine market-specific cutoff
    market_cutoff_ts = None
    cutoff_reason = None

    if is_election:
        # Look up election date
        country = market_info.get('country', 'United States')
        office = market_info.get('office')
        location = market_info.get('location')
        election_year = market_info.get('election_year')

        if pd.notna(office) and pd.notna(location) and pd.notna(election_year):
            key = (country, office, location, int(election_year))
            election_date = election_lookup.get(key)

            if election_date:
                # Truncate at end of election day (23:59:59)
                election_end = datetime(
                    election_date.year,
                    election_date.month,
                    election_date.day,
                    23, 59, 59,
                    tzinfo=timezone.utc
                )
                market_cutoff_ts = int(election_end.timestamp())
                cutoff_reason = 'election_date'

    # If no election date found or not an election, use trading_close_time - 24 hours
    # This gives us the "final prediction" price before the event
    # (Polymarket's trading_close_time is typically event + 24 hours)
    if market_cutoff_ts is None:
        trading_close = market_info.get('trading_close_time')
        if pd.notna(trading_close):
            try:
                close_dt = pd.to_datetime(trading_close)
                if close_dt.tzinfo is None:
                    close_dt = close_dt.tz_localize('UTC')
                # Subtract 24 hours to get final pre-event prediction
                cutoff_dt = close_dt - timedelta(hours=24)
                market_cutoff_ts = int(cutoff_dt.timestamp())
                cutoff_reason = 'trading_close_minus_24h'
            except:
                pass

    # Apply cutoff if we have one
    if market_cutoff_ts is not None:
        # Truncate prices
        original_count = len(price_history)
        truncated = [p for p in price_history if p['t'] <= market_cutoff_ts]
        truncated_prices[token_id] = truncated

        prices_removed = original_count - len(truncated)
        stats['prices_removed_total'] += prices_removed

        if prices_removed > 0:
            if cutoff_reason == 'election_date':
                stats['tokens_truncated_by_election_date'] += 1
            elif cutoff_reason == 'trading_close_minus_24h':
                stats['tokens_truncated_by_trading_close'] += 1
        else:
            stats['tokens_unchanged'] += 1
    else:
        # No cutoff - keep prices as-is
        truncated_prices[token_id] = price_history
        stats['tokens_unchanged'] += 1

print(f"   Truncation statistics:")
print(f"      Total tokens processed: {stats['total_tokens']:,}")
print(f"      Tokens with market info: {stats['tokens_with_market_info']:,}")
print(f"      Tokens truncated by election date: {stats['tokens_truncated_by_election_date']:,}")
print(f"      Tokens truncated by trading_close: {stats['tokens_truncated_by_trading_close']:,}")
print(f"      Tokens unchanged: {stats['tokens_unchanged']:,}")
print(f"      Tokens without market info: {stats['tokens_no_market_info']:,}")
print(f"      Total price points removed: {stats['prices_removed_total']:,}")

# ============================================================================
# 5. Save Corrected Prices
# ============================================================================

print("\n5. Saving corrected prices...")

atomic_write_json(OUTPUT_PRICES, truncated_prices)

print(f"   Saved to: {OUTPUT_PRICES}")

# ============================================================================
# 6. Summary
# ============================================================================

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"\nInput:  {INPUT_PRICES}")
print(f"Output: {OUTPUT_PRICES}")
print(f"\nTruncation logic:")
print(f"  - Election markets: Truncated at end of election day (23:59:59)")
print(f"  - Non-election markets: Truncated at trading_close_time - 24 hours")
print(f"\nResults:")
print(f"  - {stats['tokens_truncated_by_election_date']:,} tokens truncated by election date")
print(f"  - {stats['tokens_truncated_by_trading_close']:,} tokens truncated by trading close")
print(f"  - {stats['prices_removed_total']:,} total price points removed")
print(f"\nDONE")
