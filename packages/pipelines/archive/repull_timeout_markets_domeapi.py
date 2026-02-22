#!/usr/bin/env python3
"""
Re-pull Timed Out Markets from Dome API

This script re-pulls the 37 markets that timed out during the initial Dome API migration.
These are high-volume markets (including Trump/Harris Presidential) that need a longer timeout.

Usage:
    python scripts/repull_timeout_markets_domeapi.py
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
ERROR_FILE = f"{DATA_DIR}/domeapi_full_migration_errors.csv"
PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_DOMEAPI_CORRECTED.json"

# Output files
OUTPUT_PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_DOMEAPI_CORRECTED.json"
BACKUP_FILE = f"{DATA_DIR}/polymarket_all_political_prices_DOMEAPI_CORRECTED_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
REPULL_LOG_FILE = f"{DATA_DIR}/domeapi_repull_timeout_log.csv"

# Dome API Configuration
DOME_API_BASE = "https://api.domeapi.io/v1/polymarket"
DOME_API_KEY = "Bearer d2d4d3b2cd3b0424bd2145a57d6f34a3661050e2"  # Update if you have dev tier key

# Timing configuration - ADJUST FOR DEV TIER
TIMEOUT_SECONDS = 120  # Increased from 15 to 120 seconds
RATE_LIMIT_DELAY = 1.0  # Adjust based on dev tier limits (free tier = 1 req/sec)
MAX_RETRIES = 3
RETRY_DELAY_BASE = 30  # Base delay for retries (will use exponential backoff)

# =============================================================================
# MAIN SCRIPT
# =============================================================================

print("=" * 80)
print("RE-PULL TIMED OUT MARKETS FROM DOME API")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load error file and filter to timeout errors
print("\n1. Loading timed out markets...")
errors_df = pd.read_csv(ERROR_FILE)
timeout_errors = errors_df[errors_df['error'].str.contains('timed out', case=False, na=False)].copy()
print(f"   Found {len(timeout_errors)} markets that timed out")

# Load master data for additional info
master_df = pd.read_csv(MASTER_FILE, low_memory=False)
master_df['market_id'] = master_df['market_id'].astype(str)

# Merge to get token_id and trading_close_time
timeout_errors['market_id'] = timeout_errors['market_id'].astype(str)
markets_to_pull = timeout_errors.merge(
    master_df[['market_id', 'pm_token_id_yes', 'trading_close_time', 'volume_usd']],
    on='market_id',
    how='left'
)
markets_to_pull = markets_to_pull.sort_values('volume_usd', ascending=False)

print(f"\n   Markets to re-pull:")
for _, row in markets_to_pull.head(10).iterrows():
    vol = f"${row['volume_usd']:,.0f}" if pd.notna(row['volume_usd']) else "N/A"
    print(f"      {row['market_id']}: {vol} - {row['question'][:50]}...")
if len(markets_to_pull) > 10:
    print(f"      ... and {len(markets_to_pull) - 10} more")

# Load existing price data
print("\n2. Loading existing price data...")
with open(PRICE_FILE, 'r') as f:
    price_data = json.load(f)
print(f"   Existing tokens: {len(price_data):,}")

# Backup existing data
print(f"\n3. Creating backup: {os.path.basename(BACKUP_FILE)}")
with open(BACKUP_FILE, 'w') as f:
    json.dump(price_data, f)
print(f"   Backup saved")

# Pull data from Dome API
print(f"\n4. Pulling data from Dome API...")
print(f"   Timeout: {TIMEOUT_SECONDS}s | Rate limit delay: {RATE_LIMIT_DELAY}s")
print("-" * 80)

success_count = 0
error_count = 0
log_entries = []

for idx, row in markets_to_pull.iterrows():
    market_id = row['market_id']
    condition_id = row['condition_id']
    token_id_yes = str(row['pm_token_id_yes'])
    question = row['question'][:60]

    print(f"\n   [{success_count + error_count + 1}/{len(markets_to_pull)}] {market_id}: {question}...")

    # Calculate time range (1 year before close_time)
    try:
        close_time = pd.to_datetime(row['trading_close_time'], utc=True).replace(tzinfo=None)
        end_time = int(close_time.timestamp())
        start_time = int((close_time - timedelta(days=365)).timestamp())
    except Exception as e:
        print(f"      ✗ Date parse error: {e}")
        log_entries.append({
            'market_id': market_id,
            'status': 'error',
            'error': f'Date parse error: {e}',
            'candlesticks': 0
        })
        error_count += 1
        continue

    # Call Dome API with retry logic
    success = False
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
                timeout=TIMEOUT_SECONDS
            )

            if response.status_code == 200:
                data = response.json()
                candlesticks = data.get('candlesticks', [])

                if candlesticks and len(candlesticks) > 0:
                    # Process candlestick data - find matching token
                    converted_prices = []
                    found_token = False

                    for token_data in candlesticks:
                        if len(token_data) != 2:
                            continue

                        candle_array = token_data[0]
                        token_info = token_data[1]
                        this_token_id = str(token_info.get('token_id', ''))

                        if this_token_id == token_id_yes:
                            found_token = True
                            for candle in candle_array:
                                timestamp = candle.get('end_period_ts')
                                price_cents = candle.get('price', {}).get('close', 0)
                                if timestamp and price_cents is not None:
                                    converted_prices.append({
                                        't': timestamp,
                                        'p': price_cents / 100.0  # Convert cents to decimal
                                    })
                            break

                    if found_token and len(converted_prices) > 0:
                        # Add to price data
                        price_data[token_id_yes] = converted_prices
                        print(f"      ✓ Success: {len(converted_prices)} candlesticks")
                        log_entries.append({
                            'market_id': market_id,
                            'status': 'success',
                            'error': '',
                            'candlesticks': len(converted_prices)
                        })
                        success_count += 1
                        success = True
                        break
                    else:
                        error_msg = "No matching token_id in response"
                        print(f"      ✗ {error_msg}")
                        log_entries.append({
                            'market_id': market_id,
                            'status': 'error',
                            'error': error_msg,
                            'candlesticks': 0
                        })
                        error_count += 1
                        success = True  # Don't retry - data issue not timeout
                        break
                else:
                    error_msg = "Empty candlesticks array"
                    print(f"      ✗ {error_msg}")
                    log_entries.append({
                        'market_id': market_id,
                        'status': 'error',
                        'error': error_msg,
                        'candlesticks': 0
                    })
                    error_count += 1
                    success = True
                    break

            elif response.status_code == 429:
                # Rate limited - wait and retry
                wait_time = RETRY_DELAY_BASE * (2 ** attempt)
                print(f"      ⚠ Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                continue

            else:
                error_msg = f"Status {response.status_code}: {response.text[:100]}"
                print(f"      ✗ {error_msg}")
                log_entries.append({
                    'market_id': market_id,
                    'status': 'error',
                    'error': error_msg,
                    'candlesticks': 0
                })
                error_count += 1
                success = True
                break

        except requests.exceptions.Timeout:
            wait_time = RETRY_DELAY_BASE * (2 ** attempt)
            print(f"      ⚠ Timeout (attempt {attempt + 1}/{MAX_RETRIES}), retrying in {wait_time}s...")
            time.sleep(wait_time)
            continue

        except Exception as e:
            error_msg = str(e)
            print(f"      ✗ Error: {error_msg}")
            log_entries.append({
                'market_id': market_id,
                'status': 'error',
                'error': error_msg,
                'candlesticks': 0
            })
            error_count += 1
            success = True
            break

    if not success:
        # All retries exhausted
        log_entries.append({
            'market_id': market_id,
            'status': 'error',
            'error': f'All {MAX_RETRIES} retries exhausted (timeout)',
            'candlesticks': 0
        })
        error_count += 1
        print(f"      ✗ All retries exhausted")

    # Rate limit delay
    time.sleep(RATE_LIMIT_DELAY)

# Save updated price data
print(f"\n5. Saving updated price data...")
with open(OUTPUT_PRICE_FILE, 'w') as f:
    json.dump(price_data, f)
print(f"   Saved {len(price_data):,} tokens to {os.path.basename(OUTPUT_PRICE_FILE)}")

# Save log
log_df = pd.DataFrame(log_entries)
log_df.to_csv(REPULL_LOG_FILE, index=False)
print(f"   Saved log to {os.path.basename(REPULL_LOG_FILE)}")

# Summary
print(f"\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"   Total markets: {len(markets_to_pull)}")
print(f"   Successful: {success_count}")
print(f"   Errors: {error_count}")
print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
