#!/usr/bin/env python3
"""
Full DomeAPI Migration for All Polymarket Markets

Creates a complete parallel DomeAPI dataset for all 14,736 Polymarket markets.
Keeps existing CLOB data untouched and saves to new file.
"""

import pandas as pd
import requests
import json
import time
import os
from datetime import datetime, timedelta

# TEST MODE - Set to True to test with 100 markets
TEST_MODE = False  # Change to False for full migration
TEST_SAMPLE_SIZE = 100

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
CHECKPOINT_FILE = f"{DATA_DIR}/domeapi_full_migration_checkpoint.json"

# Output files (NEW)
NEW_PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_DOMEAPI_v1.json"
TRACKING_FILE = f"{DATA_DIR}/domeapi_full_migration_results.json"
ERROR_FILE = f"{DATA_DIR}/domeapi_full_migration_errors.csv"

# DomeAPI Configuration - FREE TIER (1 req/sec)
DOME_API_BASE = "https://api.domeapi.io/v1/polymarket"
DOME_API_KEY = "Bearer d2d4d3b2cd3b0424bd2145a57d6f34a3661050e2"
RATE_LIMIT_DELAY = 1.0  # 1 second (free tier limit)
MAX_RETRIES = 3
RETRY_DELAY = 10  # Exponential backoff: 10s, 20s, 40s

print("=" * 80)
if TEST_MODE:
    print(f"TESTING DOMEAPI MIGRATION ({TEST_SAMPLE_SIZE} MARKETS)")
else:
    print("FULL DOMEAPI MIGRATION FOR ALL POLYMARKET MARKETS")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
if TEST_MODE:
    print(f"TEST MODE: Processing {TEST_SAMPLE_SIZE} markets (~{TEST_SAMPLE_SIZE * RATE_LIMIT_DELAY / 60:.0f} minutes)")
else:
    print(f"Expected runtime: ~4 hours at 1 req/sec")

# Load data
print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

master_df = pd.read_csv(MASTER_FILE, low_memory=False)
master_df['market_id'] = master_df['market_id'].astype(str)

# Filter to Polymarket markets WITH trading_close_time
pm_markets = master_df[
    (master_df['platform'] == 'Polymarket') &
    (master_df['trading_close_time'].notna())
].copy()

print(f"✓ Total Polymarket markets: {len(master_df[master_df['platform'] == 'Polymarket']):,}")
print(f"✓ PM markets with trading_close_time: {len(pm_markets):,}")
print(f"✓ PM markets to process: {len(pm_markets):,}")

# Load checkpoint if exists
processed_markets = set()
new_price_data = {}
success_list = []
error_list = []

if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint = json.load(f)
            processed_markets = set(checkpoint.get('processed_markets', []))
            new_price_data = checkpoint.get('new_price_data', {})
            success_list = checkpoint.get('success_list', [])
            error_list = checkpoint.get('error_list', [])
        print(f"✓ Loaded checkpoint: {len(processed_markets):,} markets processed")
        print(f"  - Successful: {len(success_list):,}")
        print(f"  - Errors: {len(error_list):,}")
    except Exception as e:
        print(f"✗ Error loading checkpoint: {e}")
        print(f"✓ Starting fresh")
else:
    print(f"✓ No checkpoint found, starting fresh")

remaining = pm_markets[~pm_markets['market_id'].isin(processed_markets)]

# TEST MODE: Limit to sample size
if TEST_MODE:
    remaining = remaining.head(TEST_SAMPLE_SIZE)
    print(f"✓ TEST MODE: Limited to {len(remaining):,} markets")
else:
    print(f"✓ Markets remaining to fetch: {len(remaining):,}")

# Pull price data from DomeAPI
print(f"\n{'=' * 80}")
print("FETCHING PRICE DATA FROM DOMEAPI")
print(f"{'=' * 80}")
print(f"Rate limit: {RATE_LIMIT_DELAY}s between requests")
print(f"Checkpoints: Every 100 markets (~{100 * RATE_LIMIT_DELAY / 60:.0f} minutes)")
print()

success_count = len(success_list)
error_count = len(error_list)
total_markets = len(remaining) if TEST_MODE else len(pm_markets)

for idx, row in remaining.iterrows():
    market_id = str(row['market_id'])
    condition_id = row['pm_condition_id']
    token_id_yes = str(row['pm_token_id_yes'])
    question = row.get('question', 'N/A')

    # Calculate time range (1 year before close_time)
    try:
        close_time = pd.to_datetime(row['trading_close_time'], utc=True).replace(tzinfo=None)
        end_time = int(close_time.timestamp())
        start_time = int((close_time - timedelta(days=365)).timestamp())
    except Exception as e:
        error_list.append({
            'market_id': market_id,
            'condition_id': condition_id,
            'question': question,
            'error': f'Date parse error: {str(e)}'
        })
        processed_markets.add(market_id)
        error_count += 1
        continue

    # Call DomeAPI with retry logic
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
                timeout=15
            )

            if response.status_code == 200:
                data = response.json()
                candlesticks = data.get('candlesticks', [])

                if candlesticks and len(candlesticks) > 0:
                    # Process candlestick data
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
                                price_decimal = price_cents / 100.0

                                converted_prices.append({
                                    't': timestamp,
                                    'p': price_decimal
                                })
                            break

                    if found_token and converted_prices:
                        # Add to new price data
                        new_price_data[token_id_yes] = converted_prices
                        success_count += 1

                        # Track this market
                        success_list.append({
                            'market_id': market_id,
                            'token_ids': [token_id_yes],
                            'source': 'DomeAPI',
                            'pulled_at': datetime.now().isoformat(),
                            'candlesticks_count': len(converted_prices)
                        })

                        status = "✓ FOUND"
                    else:
                        error_list.append({
                            'market_id': market_id,
                            'condition_id': condition_id,
                            'question': question,
                            'error': 'No matching token_id in response'
                        })
                        error_count += 1
                        status = "✗ No match"
                else:
                    error_list.append({
                        'market_id': market_id,
                        'condition_id': condition_id,
                        'question': question,
                        'error': 'Empty candlesticks array'
                    })
                    error_count += 1
                    status = "✗ Empty"

                processed_markets.add(market_id)
                break

            elif response.status_code == 400:
                error_list.append({
                    'market_id': market_id,
                    'condition_id': condition_id,
                    'question': question,
                    'error': 'Status 400'
                })
                processed_markets.add(market_id)
                error_count += 1
                status = "✗ 400"
                break

            elif response.status_code == 429:
                if attempt < MAX_RETRIES - 1:
                    # Exponential backoff for rate limits
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    print(f"  Rate limited, waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    error_list.append({
                        'market_id': market_id,
                        'condition_id': condition_id,
                        'question': question,
                        'error': '429 Rate Limit'
                    })
                    processed_markets.add(market_id)
                    error_count += 1
                    status = "✗ 429"
                    break

            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    error_list.append({
                        'market_id': market_id,
                        'condition_id': condition_id,
                        'question': question,
                        'error': f'Status {response.status_code}'
                    })
                    processed_markets.add(market_id)
                    error_count += 1
                    status = f"✗ {response.status_code}"
                    break

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            else:
                error_list.append({
                    'market_id': market_id,
                    'condition_id': condition_id,
                    'question': question,
                    'error': str(e)[:100]
                })
                processed_markets.add(market_id)
                error_count += 1
                status = "✗ Error"
                break

    # Print progress every 50 markets
    processed = len(processed_markets)
    if processed % 50 == 0 or processed == total_markets:
        percent_success = (success_count / processed) * 100 if processed > 0 else 0
        elapsed = (datetime.now() - datetime.strptime(str(datetime.now().date()), '%Y-%m-%d')).seconds
        remaining_markets = total_markets - processed
        est_remaining = remaining_markets * RATE_LIMIT_DELAY / 3600

        print(f"[{processed}/{total_markets}] {status} | Market: {market_id[:20]}...")
        print(f"  Success: {success_count}/{processed} ({percent_success:.1f}%)")
        print(f"  Errors: {error_count}/{processed}")
        print(f"  Est. time remaining: {est_remaining:.1f} hours")
        print()

    # Checkpoint every 100 markets (or at end of test)
    if len(processed_markets) % 100 == 0 or (TEST_MODE and processed == total_markets):
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                'processed_markets': list(processed_markets),
                'new_price_data': new_price_data,
                'success_list': success_list,
                'error_list': error_list,
                'test_mode': TEST_MODE,
                'last_updated': datetime.now().isoformat()
            }, f)
        print(f"  ✓ Checkpoint saved at {processed} markets")
        print()

    # Rate limit delay
    time.sleep(RATE_LIMIT_DELAY)

# Final checkpoint
with open(CHECKPOINT_FILE, 'w') as f:
    json.dump({
        'processed_markets': list(processed_markets),
        'new_price_data': new_price_data,
        'success_list': success_list,
        'error_list': error_list,
        'last_updated': datetime.now().isoformat()
    }, f)

# Save results
print(f"\n{'=' * 80}")
print("SAVING RESULTS")
print(f"{'=' * 80}")

# Save new price data
with open(NEW_PRICE_FILE, 'w') as f:
    json.dump(new_price_data, f)
print(f"✓ Saved new price file: {len(new_price_data):,} tokens")
file_size_mb = os.path.getsize(NEW_PRICE_FILE) / 1024 / 1024
print(f"  File size: {file_size_mb:.1f} MB")

# Save tracking file
with open(TRACKING_FILE, 'w') as f:
    json.dump({
        'markets': success_list,
        'created_at': datetime.now().isoformat(),
        'total_markets': len(success_list),
        'total_candlesticks': sum(m['candlesticks_count'] for m in success_list),
        'avg_candlesticks': sum(m['candlesticks_count'] for m in success_list) / len(success_list) if success_list else 0
    }, f, indent=2)
print(f"✓ Saved tracking file: {len(success_list):,} successful markets")

# Save error file
if error_list:
    error_df = pd.DataFrame(error_list)
    error_df.to_csv(ERROR_FILE, index=False)
    print(f"✓ Saved error file: {len(error_list):,} errors")

# Summary
print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")
print(f"\nTotal PM markets: {len(pm_markets):,}")
print(f"Successfully pulled: {success_count:,} ({success_count/len(pm_markets)*100:.1f}%)")
print(f"Failed: {error_count:,} ({error_count/len(pm_markets)*100:.1f}%)")

if success_list:
    avg_candlesticks = sum(m['candlesticks_count'] for m in success_list) / len(success_list)
    print(f"\nAverage candlesticks per market: {avg_candlesticks:.1f}")

if error_list:
    error_df = pd.DataFrame(error_list)
    error_counts = error_df['error'].value_counts()
    print(f"\nError breakdown:")
    for error, count in error_counts.items():
        print(f"  {error}: {count}")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"\nOutput files:")
print(f"  - {NEW_PRICE_FILE}")
print(f"  - {TRACKING_FILE}")
print(f"  - {ERROR_FILE}")
