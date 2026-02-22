#!/usr/bin/env python3
"""
Truncate Kalshi Price Data

Part of the NEW Bellwether Pipeline (January 2026+)

This script truncates Kalshi price data appropriately:
- For elections: Truncate at end of election day (23:59:59)
- For non-elections: Truncate at trading_close_time - 12 hours (final prediction before event)

Input: kalshi_all_political_prices_CORRECTED_v3.json (in-place truncation)
Output: kalshi_all_political_prices_CORRECTED_v3.json

NOTE: Kalshi's trading_close_time is typically event + 12 hours, so we subtract 12 hours
to get the final prediction price before the event actually occurs.
"""

import pandas as pd
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Paths
BASE_DIR = Path("/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi")
DATA_DIR = BASE_DIR / "data"

# Input files - read from CURRENT file as primary source
# This ensures we don't lose newly-pulled markets
CURRENT_PRICES_FILE = DATA_DIR / "kalshi_all_political_prices_CORRECTED_v3.json"
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
ELECTION_DATES = DATA_DIR / "election_dates_lookup.csv"

# Output file (same as input - will re-truncate in place)
OUTPUT_PRICES = DATA_DIR / "kalshi_all_political_prices_CORRECTED_v3.json"

import shutil
import sys

# Add scripts dir to path for config import
sys.path.insert(0, str(Path(__file__).parent))
from config import rotate_backups

print("=" * 80)
print("TRUNCATING KALSHI PRICE DATA")
print("=" * 80)

# ============================================================================
# 0. Create Backup (Safety Step)
# ============================================================================

print("\n0. Creating backup before truncation...")
BACKUP_DIR = DATA_DIR / "backups"
BACKUP_DIR.mkdir(exist_ok=True)
backup_file = BACKUP_DIR / f"kalshi_prices_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

if CURRENT_PRICES_FILE.exists():
    shutil.copy2(CURRENT_PRICES_FILE, backup_file)
    print(f"   Created backup: {backup_file.name}")

    # Rotate old backups (keep last 5)
    deleted = rotate_backups("kalshi_prices_backup_*.json")
    if deleted > 0:
        print(f"   Rotated {deleted} old backup(s)")
else:
    print("   WARNING: Input file does not exist, skipping backup")

# ============================================================================
# 1. Load Data
# ============================================================================

print("\n1. Loading data...")

# Load Kalshi prices from current file
# We read from the current file to ensure we don't lose any newly-pulled markets
kalshi_all_prices = {}

if CURRENT_PRICES_FILE.exists():
    with open(CURRENT_PRICES_FILE, 'r') as f:
        kalshi_all_prices = json.load(f)
    print(f"   Loaded {len(kalshi_all_prices):,} markets from current file")
else:
    print("   ERROR: Current prices file not found!")
    exit(1)

print(f"   Total Kalshi markets: {len(kalshi_all_prices):,}")

# Load master file
master_df = pd.read_csv(MASTER_FILE, low_memory=False)
kalshi_markets = master_df[master_df['platform'] == 'Kalshi'].copy()
print(f"   Loaded {len(kalshi_markets):,} Kalshi markets from master file")

# Load election dates lookup
election_dates_df = pd.read_csv(ELECTION_DATES)
print(f"   Loaded {len(election_dates_df):,} election date records")

# ============================================================================
# 2. Build Ticker-to-Market Mapping
# ============================================================================

print("\n2. Building ticker-to-market mapping...")

ticker_to_market = {}
for _, row in kalshi_markets.iterrows():
    ticker = row['market_id']
    ticker_to_market[ticker] = {
        'trading_close_time': row.get('trading_close_time'),
        'political_category': row.get('political_category'),
        'country': row.get('country'),
        'office': row.get('office'),
        'location': row.get('location'),
        'election_year': row.get('election_year'),
        'question': row.get('question', '')
    }

print(f"   Mapped {len(ticker_to_market):,} tickers to markets")

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
    'total_tickers': 0,
    'tickers_with_market_info': 0,
    'tickers_truncated_by_election_date': 0,
    'tickers_truncated_by_trading_close': 0,
    'tickers_unchanged': 0,
    'tickers_no_market_info': 0,
    'prices_removed_total': 0,
}

for ticker, candlesticks in kalshi_all_prices.items():
    stats['total_tickers'] += 1

    if not candlesticks:
        truncated_prices[ticker] = candlesticks
        stats['tickers_unchanged'] += 1
        continue

    # Get market info for this ticker
    market_info = ticker_to_market.get(ticker)

    if market_info is None:
        # No market info found - keep prices as-is (no truncation)
        truncated_prices[ticker] = candlesticks
        stats['tickers_unchanged'] += 1
        stats['tickers_no_market_info'] += 1
        continue

    stats['tickers_with_market_info'] += 1

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

    # If no election date found or not an election, use trading_close_time - 12 hours
    # This gives us the "final prediction" price before the event
    # (Kalshi's trading_close_time is typically event + 12 hours)
    if market_cutoff_ts is None:
        trading_close = market_info.get('trading_close_time')
        if pd.notna(trading_close):
            try:
                close_dt = pd.to_datetime(trading_close)
                if close_dt.tzinfo is None:
                    close_dt = close_dt.tz_localize('UTC')
                # Subtract 12 hours to get final pre-event prediction
                cutoff_dt = close_dt - timedelta(hours=12)
                market_cutoff_ts = int(cutoff_dt.timestamp())
                cutoff_reason = 'trading_close_minus_12h'
            except:
                pass

    # Apply cutoff if we have one
    if market_cutoff_ts is not None:
        # Truncate prices (handles both 'end_period_ts' and 't' key formats)
        original_count = len(candlesticks)
        truncated = [c for c in candlesticks if c.get('end_period_ts', c.get('t', 0)) <= market_cutoff_ts]
        truncated_prices[ticker] = truncated

        prices_removed = original_count - len(truncated)
        stats['prices_removed_total'] += prices_removed

        if prices_removed > 0:
            if cutoff_reason == 'election_date':
                stats['tickers_truncated_by_election_date'] += 1
            elif cutoff_reason == 'trading_close_minus_12h':
                stats['tickers_truncated_by_trading_close'] += 1
        else:
            stats['tickers_unchanged'] += 1
    else:
        # No cutoff - keep prices as-is
        truncated_prices[ticker] = candlesticks
        stats['tickers_unchanged'] += 1

print(f"   Truncation statistics:")
print(f"      Total tickers processed: {stats['total_tickers']:,}")
print(f"      Tickers with market info: {stats['tickers_with_market_info']:,}")
print(f"      Tickers truncated by election date: {stats['tickers_truncated_by_election_date']:,}")
print(f"      Tickers truncated by trading_close: {stats['tickers_truncated_by_trading_close']:,}")
print(f"      Tickers unchanged: {stats['tickers_unchanged']:,}")
print(f"      Tickers without market info: {stats['tickers_no_market_info']:,}")
print(f"      Total price points removed: {stats['prices_removed_total']:,}")

# ============================================================================
# 5. Verify Problematic Markets Fixed
# ============================================================================

print("\n5. Verifying problematic markets were truncated correctly...")

problematic = ['HOUSECA27-24-R', 'KXWISCOTUS-25-BS', 'KXMAYORATLANTA-25-AD',
               'KXMAYORDETROIT-25-MS', 'KXMAYORNOLA-25-FJAN']

for ticker in problematic:
    if ticker in truncated_prices:
        candles = truncated_prices[ticker]
        if candles:
            last_ts = max(c.get('end_period_ts', c.get('t', 0)) for c in candles)
            last_dt = datetime.fromtimestamp(last_ts, tz=timezone.utc)
            last_candle = max(candles, key=lambda x: x.get('end_period_ts', x.get('t', 0)))
            price_data = last_candle.get('price', {})
            if isinstance(price_data, dict):
                last_price = price_data.get('close')
                if last_price is not None:
                    last_price = last_price / 100
                else:
                    last_price = 'N/A'
            else:
                last_price = 'N/A'
            print(f"   {ticker}: last price {last_dt.strftime('%Y-%m-%d')} = {last_price}")
        else:
            print(f"   {ticker}: no candles")
    else:
        print(f"   {ticker}: not found")

# ============================================================================
# 6. Save Corrected Prices
# ============================================================================

print("\n6. Saving corrected prices...")

with open(OUTPUT_PRICES, 'w') as f:
    json.dump(truncated_prices, f)

print(f"   Saved to: {OUTPUT_PRICES}")

# ============================================================================
# 7. Summary
# ============================================================================

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"\nOutput: {OUTPUT_PRICES}")
print(f"\nTruncation logic:")
print(f"  - Election markets: Truncated at end of election day (23:59:59)")
print(f"  - Non-election markets: Truncated at trading_close_time - 12 hours")
print(f"\nResults:")
print(f"  - {stats['tickers_truncated_by_election_date']:,} tickers truncated by election date")
print(f"  - {stats['tickers_truncated_by_trading_close']:,} tickers truncated by trading close - 12h")
print(f"  - {stats['prices_removed_total']:,} total price points removed")
print(f"\nDONE")
