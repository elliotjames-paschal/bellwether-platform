#!/usr/bin/env python3
"""
Fix Missing Polymarket Market Data

This script identifies PM markets with missing data (no token_id, 0 price points,
no resolution_outcome) and fetches the data from:
1. Gamma API (CLOB API) - for market metadata (token_id, condition_id, resolution)
2. Dome API - for price history

Issues this script addresses:
- Markets with no pm_token_id_yes
- Markets with 0 price points
- Markets missing resolution_outcome

Usage:
    python scripts/fix_missing_pm_market_data.py
"""

import pandas as pd
import requests
import json
import time
import os
from datetime import datetime, timedelta

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"

# Input files
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_DOMEAPI_CORRECTED.json"

# Output files
OUTPUT_MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
OUTPUT_PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_DOMEAPI_CORRECTED.json"
BACKUP_MASTER = f"{DATA_DIR}/combined_political_markets_BACKUP_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
BACKUP_PRICE = f"{DATA_DIR}/polymarket_prices_BACKUP_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
LOG_FILE = f"{DATA_DIR}/fix_missing_pm_data_log.csv"

# API Configuration
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
DOME_API_BASE = "https://api.domeapi.io/v1/polymarket"
DOME_API_KEY = "Bearer d2d4d3b2cd3b0424bd2145a57d6f34a3661050e2"

# Timing
GAMMA_RATE_LIMIT = 0.15  # 150ms between requests
DOME_RATE_LIMIT = 1.0    # 1 second (free tier)
MAX_RETRIES = 3
RETRY_DELAY = 5
DOME_TIMEOUT = 60        # Higher timeout for Dome API

# CRITICAL: Cutoff date for resolution updates
# Only update resolution_outcome for markets that closed BEFORE this date
# This prevents incorrectly marking newer markets as resolved
RESOLUTION_CUTOFF_DATE = pd.Timestamp('2025-11-10', tz='UTC')

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def fetch_gamma_market_data(market_id):
    """Fetch market data from Gamma API (CLOB API)."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                f"{GAMMA_API_BASE}/markets/{market_id}",
                timeout=15
            )

            if response.status_code == 200:
                market = response.json()

                # Parse clobTokenIds
                clob_tokens_str = market.get('clobTokenIds', '[]')
                try:
                    clob_tokens = json.loads(clob_tokens_str)
                except:
                    clob_tokens = []

                # Parse outcomePrices
                outcome_prices_str = market.get('outcomePrices', '[]')
                try:
                    outcome_prices = json.loads(outcome_prices_str)
                except:
                    outcome_prices = []

                # Get resolution status
                resolution_source = market.get('resolutionSource', '')
                resolved = market.get('resolved', False)
                closed = market.get('closed', False)

                # Determine resolution outcome from outcome prices if resolved
                resolution_outcome = None
                if resolved and outcome_prices:
                    try:
                        # If first outcome (Yes) price is 1.0, resolution is yes
                        # If first outcome (Yes) price is 0.0, resolution is no
                        yes_price = float(outcome_prices[0]) if outcome_prices else None
                        if yes_price is not None:
                            if yes_price >= 0.99:
                                resolution_outcome = "yes"
                            elif yes_price <= 0.01:
                                resolution_outcome = "no"
                    except:
                        pass

                return {
                    'success': True,
                    'token_id_yes': clob_tokens[0] if len(clob_tokens) > 0 else None,
                    'token_id_no': clob_tokens[1] if len(clob_tokens) > 1 else None,
                    'condition_id': market.get('conditionId'),
                    'outcome_prices': outcome_prices,
                    'resolved': resolved,
                    'closed': closed,
                    'resolution_outcome': resolution_outcome,
                    'end_date_iso': market.get('endDateIso'),
                    'volume': market.get('volume')
                }

            elif response.status_code == 404:
                return {'success': False, 'error': 'Market not found (404)'}
            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                return {'success': False, 'error': f'API error: {response.status_code}'}

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            return {'success': False, 'error': str(e)}

    return {'success': False, 'error': 'Max retries exceeded'}


def fetch_dome_price_data(condition_id, end_time_dt):
    """Fetch price history from Dome API."""
    try:
        close_time = end_time_dt.replace(tzinfo=None)
        end_time = int(close_time.timestamp())
        start_time = int((close_time - timedelta(days=365)).timestamp())
    except Exception as e:
        return {'success': False, 'error': f'Date parse error: {e}', 'prices': []}

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                f"{DOME_API_BASE}/candlesticks/{condition_id}",
                headers={"Authorization": DOME_API_KEY},
                params={
                    'start_time': start_time,
                    'end_time': end_time,
                    'interval': 1440  # Daily candles
                },
                timeout=DOME_TIMEOUT
            )

            if response.status_code == 200:
                data = response.json()
                candlesticks = data.get('candlesticks', [])

                if candlesticks and len(candlesticks) > 0:
                    return {
                        'success': True,
                        'candlesticks': candlesticks,
                        'error': None
                    }
                else:
                    return {
                        'success': False,
                        'error': 'Empty candlesticks',
                        'candlesticks': []
                    }

            elif response.status_code == 429:
                wait_time = RETRY_DELAY * (2 ** attempt)
                print(f"        Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                return {
                    'success': False,
                    'error': f'API error: {response.status_code}',
                    'candlesticks': []
                }

        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (2 ** attempt))
                continue
            return {'success': False, 'error': 'Timeout', 'candlesticks': []}

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            return {'success': False, 'error': str(e), 'candlesticks': []}

    return {'success': False, 'error': 'Max retries exceeded', 'candlesticks': []}


def process_candlesticks(candlesticks, token_id_yes):
    """Convert Dome API candlesticks to price format."""
    converted_prices = []
    token_id_str = str(token_id_yes)

    for token_data in candlesticks:
        if len(token_data) != 2:
            continue

        candle_array = token_data[0]
        token_info = token_data[1]
        this_token_id = str(token_info.get('token_id', ''))

        if this_token_id == token_id_str:
            for candle in candle_array:
                timestamp = candle.get('end_period_ts')
                price_cents = candle.get('price', {}).get('close', 0)
                if timestamp and price_cents is not None:
                    converted_prices.append({
                        't': timestamp,
                        'p': price_cents / 100.0  # Convert cents to decimal
                    })
            break

    return converted_prices


# =============================================================================
# MAIN SCRIPT
# =============================================================================

print("=" * 80)
print("FIX MISSING POLYMARKET MARKET DATA")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# 1. Load data
print("\n1. Loading data...")
master_df = pd.read_csv(MASTER_FILE, low_memory=False)
master_df['market_id'] = master_df['market_id'].astype(str)

with open(PRICE_FILE, 'r') as f:
    price_data = json.load(f)

print(f"   Master file: {len(master_df):,} markets")
print(f"   Price file: {len(price_data):,} tokens")

# 2. Identify PM markets with issues
print("\n2. Identifying PM markets with issues...")
pm_all = master_df[master_df['platform'] == 'Polymarket'].copy()
print(f"   Total PM markets: {len(pm_all):,}")

# Filter to election winner markets (2024-2025) - these are what we need for Brier analysis
pm_markets = pm_all[
    (pm_all['political_category'] == '1. ELECTORAL') &
    (pm_all['office'].notna()) &
    (pm_all['election_year'].notna()) &
    (pm_all['election_year'].isin([2024, 2025, 2024.0, 2025.0]))
].copy()
print(f"   PM election winner markets (2024-2025): {len(pm_markets):,}")

# Issue 1: No token_id
no_token_id = pm_markets[pm_markets['pm_token_id_yes'].isna()].copy()
print(f"   - Missing token_id: {len(no_token_id):,}")

# Issue 2: 0 price points (check price file)
# NOTE: Token IDs are very large integers stored as strings - do NOT convert via float (loses precision)
def has_price_data(token_id):
    if pd.isna(token_id):
        return False
    token_str = str(token_id).strip()
    return token_str in price_data and len(price_data.get(token_str, [])) > 0

pm_markets['has_prices'] = pm_markets['pm_token_id_yes'].apply(has_price_data)
no_prices = pm_markets[~pm_markets['has_prices'] & pm_markets['pm_token_id_yes'].notna()].copy()
print(f"   - Has token_id but 0 prices: {len(no_prices):,}")

# Issue 3: Missing resolution (for closed markets)
# Check markets that are closed but missing resolution_outcome
resolved_no_outcome = pm_markets[
    (pm_markets['pm_closed'] == True) &
    (pm_markets['resolution_outcome'].isna() | (pm_markets['resolution_outcome'] == ''))
].copy()
print(f"   - Closed but no outcome: {len(resolved_no_outcome):,}")

# Combine all markets with issues (deduplicate by market_id)
all_problem_ids = set(no_token_id['market_id'].tolist() +
                      no_prices['market_id'].tolist() +
                      resolved_no_outcome['market_id'].tolist())

problem_markets = pm_markets[pm_markets['market_id'].isin(all_problem_ids)].copy()
print(f"\n   Total unique markets with issues: {len(problem_markets):,}")

if len(problem_markets) == 0:
    print("\n   No markets with issues found. Exiting.")
    exit(0)

# Show sample of problem markets
print("\n   Sample of problem markets:")
for _, row in problem_markets.head(10).iterrows():
    has_token = "YES" if pd.notna(row.get('pm_token_id_yes')) else "NO"
    token_id = str(row['pm_token_id_yes']).strip() if pd.notna(row.get('pm_token_id_yes')) else None
    has_price = "YES" if token_id and token_id in price_data and len(price_data.get(token_id, [])) > 0 else "NO"
    has_res = "YES" if pd.notna(row.get('resolution_outcome')) and row.get('resolution_outcome') != '' else "NO"
    print(f"      {row['market_id']}: token={has_token}, prices={has_price}, resolution={has_res} - {str(row.get('question', ''))[:40]}...")

# 3. Create backups
print("\n3. Creating backups...")
master_df.to_csv(BACKUP_MASTER, index=False)
with open(BACKUP_PRICE, 'w') as f:
    json.dump(price_data, f)
print(f"   Master backup: {os.path.basename(BACKUP_MASTER)}")
print(f"   Price backup: {os.path.basename(BACKUP_PRICE)}")

# 4. Process markets
print("\n4. Processing markets...")
print("-" * 80)

log_entries = []
updated_count = 0
price_updated_count = 0
resolution_updated_count = 0
error_count = 0

for idx, (_, row) in enumerate(problem_markets.iterrows()):
    market_id = row['market_id']
    question = str(row.get('question', ''))[:50]

    print(f"\n   [{idx + 1}/{len(problem_markets)}] {market_id}: {question}...")

    log_entry = {
        'market_id': market_id,
        'question': question,
        'status': 'pending',
        'token_id_updated': False,
        'condition_id_updated': False,
        'resolution_updated': False,
        'prices_updated': False,
        'price_count': 0,
        'error': ''
    }

    # Step 1: Fetch from Gamma API
    print(f"      Fetching from Gamma API...")
    gamma_result = fetch_gamma_market_data(market_id)
    time.sleep(GAMMA_RATE_LIMIT)

    if not gamma_result['success']:
        print(f"      ERROR: {gamma_result['error']}")
        log_entry['status'] = 'gamma_error'
        log_entry['error'] = gamma_result['error']
        log_entries.append(log_entry)
        error_count += 1
        continue

    # Update master dataframe with Gamma data
    mask = master_df['market_id'] == market_id

    # Update token_id if missing
    if gamma_result['token_id_yes'] and pd.isna(master_df.loc[mask, 'pm_token_id_yes'].values[0]):
        master_df.loc[mask, 'pm_token_id_yes'] = gamma_result['token_id_yes']
        log_entry['token_id_updated'] = True
        print(f"      Updated token_id_yes: {gamma_result['token_id_yes'][:20]}...")

    if gamma_result['token_id_no'] and pd.isna(master_df.loc[mask, 'pm_token_id_no'].values[0]):
        master_df.loc[mask, 'pm_token_id_no'] = gamma_result['token_id_no']

    # Update condition_id if missing
    if gamma_result['condition_id'] and pd.isna(master_df.loc[mask, 'pm_condition_id'].values[0]):
        master_df.loc[mask, 'pm_condition_id'] = gamma_result['condition_id']
        log_entry['condition_id_updated'] = True
        print(f"      Updated condition_id: {gamma_result['condition_id'][:20]}...")

    # Update resolution_outcome if missing but resolved
    # CRITICAL: Only update if market closed BEFORE the cutoff date (Nov 10, 2025)
    # to avoid incorrectly marking newer markets as resolved
    current_resolution = master_df.loc[mask, 'resolution_outcome'].values[0]
    trading_close = master_df.loc[mask, 'trading_close_time'].values[0]

    should_update_resolution = False
    if gamma_result['resolution_outcome'] and (pd.isna(current_resolution) or current_resolution == ''):
        try:
            close_dt = pd.to_datetime(trading_close, utc=True)
            if close_dt <= RESOLUTION_CUTOFF_DATE:
                should_update_resolution = True
            else:
                print(f"      Skipping resolution update (closed {close_dt.strftime('%Y-%m-%d')} > cutoff {RESOLUTION_CUTOFF_DATE.strftime('%Y-%m-%d')})")
        except:
            # If we can't parse the date, skip resolution update to be safe
            print(f"      Skipping resolution update (couldn't parse trading_close_time)")

    if should_update_resolution:
        master_df.loc[mask, 'resolution_outcome'] = gamma_result['resolution_outcome']
        log_entry['resolution_updated'] = True
        resolution_updated_count += 1
        print(f"      Updated resolution_outcome: {gamma_result['resolution_outcome']}")

    updated_count += 1

    # Step 2: Fetch price data from Dome API if needed
    token_id_yes = gamma_result['token_id_yes'] or str(row.get('pm_token_id_yes', ''))
    condition_id = gamma_result['condition_id'] or row.get('pm_condition_id')

    if not token_id_yes or not condition_id:
        print(f"      Skipping Dome API (no token_id or condition_id)")
        log_entry['status'] = 'partial_success'
        log_entries.append(log_entry)
        continue

    # Check if we already have price data
    # NOTE: Do NOT convert via float - large integers lose precision
    token_id_str = str(token_id_yes).strip() if token_id_yes else None
    if token_id_str and token_id_str in price_data and len(price_data.get(token_id_str, [])) > 0:
        print(f"      Price data already exists ({len(price_data[token_id_str])} points)")
        log_entry['status'] = 'success'
        log_entry['price_count'] = len(price_data[token_id_str])
        log_entries.append(log_entry)
        continue

    # Get end time for price fetch
    end_time_str = gamma_result.get('end_date_iso') or row.get('trading_close_time')
    if not end_time_str or pd.isna(end_time_str):
        print(f"      Skipping Dome API (no end_time)")
        log_entry['status'] = 'partial_success'
        log_entries.append(log_entry)
        continue

    try:
        end_time_dt = pd.to_datetime(end_time_str, utc=True)
    except:
        print(f"      Skipping Dome API (invalid end_time)")
        log_entry['status'] = 'partial_success'
        log_entries.append(log_entry)
        continue

    print(f"      Fetching from Dome API...")
    dome_result = fetch_dome_price_data(condition_id, end_time_dt)
    time.sleep(DOME_RATE_LIMIT)

    if not dome_result['success']:
        print(f"      Dome API issue: {dome_result['error']}")
        log_entry['status'] = 'dome_error'
        log_entry['error'] = dome_result['error']
        log_entries.append(log_entry)
        continue

    # Process candlesticks
    converted_prices = process_candlesticks(dome_result['candlesticks'], token_id_yes)

    if converted_prices:
        price_data[token_id_str] = converted_prices
        log_entry['prices_updated'] = True
        log_entry['price_count'] = len(converted_prices)
        price_updated_count += 1
        print(f"      Added {len(converted_prices)} price points")
    else:
        print(f"      No matching token in candlesticks")
        log_entry['error'] = 'No matching token in candlesticks'

    log_entry['status'] = 'success' if converted_prices else 'partial_success'
    log_entries.append(log_entry)

# 5. Save results
print("\n" + "=" * 80)
print("5. Saving results...")

master_df.to_csv(OUTPUT_MASTER_FILE, index=False)
print(f"   Master file saved: {os.path.basename(OUTPUT_MASTER_FILE)}")

with open(OUTPUT_PRICE_FILE, 'w') as f:
    json.dump(price_data, f)
print(f"   Price file saved: {os.path.basename(OUTPUT_PRICE_FILE)}")

# Save log
log_df = pd.DataFrame(log_entries)
log_df.to_csv(LOG_FILE, index=False)
print(f"   Log file saved: {os.path.basename(LOG_FILE)}")

# Summary
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"\n   Total markets processed: {len(problem_markets)}")
print(f"   Markets updated (Gamma API): {updated_count}")
print(f"   Resolution outcomes added: {resolution_updated_count}")
print(f"   Price data added (Dome API): {price_updated_count}")
print(f"   Errors: {error_count}")

# Show status breakdown
if log_entries:
    status_counts = pd.DataFrame(log_entries)['status'].value_counts()
    print(f"\n   Status breakdown:")
    for status, count in status_counts.items():
        print(f"      {status}: {count}")

print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
