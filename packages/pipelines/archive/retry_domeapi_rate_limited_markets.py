#!/usr/bin/env python3
"""
Retry DomeAPI pulls for rate-limited markets

Retries the 164 markets that failed with 429 Rate Limit errors,
using slower rate limiting to avoid hitting limits again.
"""

import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_CORRECTED_v3.json"
ERROR_FILE = f"{DATA_DIR}/domeapi_pull_errors.csv"
TRACKING_FILE = f"{DATA_DIR}/domeapi_price_sources.json"
CHECKPOINT_FILE = f"{DATA_DIR}/domeapi_retry_checkpoint.json"

# DomeAPI Configuration - SLOWER rate limiting
# Free tier limits: 1 query/second, 10 queries/10 seconds
DOME_API_BASE = "https://api.domeapi.io/v1/polymarket"
DOME_API_KEY = "Bearer d2d4d3b2cd3b0424bd2145a57d6f34a3661050e2"
RATE_LIMIT_DELAY = 1.0  # 1 second to respect free tier limit (1 query/sec)
MAX_RETRIES = 3
RETRY_DELAY = 10  # Increased to 10 seconds for rate limit backoff

print("=" * 80)
print("RETRYING RATE-LIMITED DOMEAPI MARKETS")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load data
print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

master_df = pd.read_csv(MASTER_FILE, low_memory=False)
master_df['market_id'] = master_df['market_id'].astype(str)

errors_df = pd.read_csv(ERROR_FILE)
errors_deduped = errors_df.drop_duplicates(subset=['market_id'], keep='first')

# Filter to only 429 errors
rate_limited = errors_deduped[errors_deduped['error'] == '429 Rate Limit'].copy()
print(f"✓ Rate-limited markets to retry: {len(rate_limited)}")

# Load price data
with open(PRICE_FILE, 'r') as f:
    price_data = json.load(f)
print(f"✓ Loaded price data: {len(price_data):,} tokens")

# Load existing tracking
with open(TRACKING_FILE, 'r') as f:
    tracking = json.load(f)
existing_domeapi_markets = tracking['markets']
print(f"✓ Existing DomeAPI markets: {len(existing_domeapi_markets)}")

# Load checkpoint if exists
processed_markets = set()
new_domeapi_markets = []
new_error_markets = []

try:
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint = json.load(f)
        processed_markets = set(checkpoint.get('processed_markets', []))
        new_domeapi_markets = checkpoint.get('new_domeapi_markets', [])
        new_error_markets = checkpoint.get('new_error_markets', [])
    print(f"✓ Loaded checkpoint: {len(processed_markets)} markets processed")
except FileNotFoundError:
    print(f"✓ No checkpoint found, starting fresh")

# Merge rate_limited with master to get all needed fields
# Ensure consistent types for merge
rate_limited['market_id'] = rate_limited['market_id'].astype(str)
rate_limited = rate_limited.merge(
    master_df[['market_id', 'pm_condition_id', 'pm_token_id_yes', 'trading_close_time', 'political_category']],
    left_on='market_id',
    right_on='market_id',
    how='left'
)

# Filter to unprocessed
remaining = rate_limited[~rate_limited['market_id'].isin(processed_markets)]
print(f"✓ Markets remaining to retry: {len(remaining)}")

# Retry markets
print(f"\n{'=' * 80}")
print("RETRYING RATE-LIMITED MARKETS")
print(f"{'=' * 80}")
print(f"\nUsing slower rate limit: {RATE_LIMIT_DELAY}s delay between requests")
print()

success_count = 0
error_count = 0

for idx, row in remaining.iterrows():
    market_id = str(row['market_id'])
    condition_id = row['pm_condition_id']
    token_id_yes = str(row['pm_token_id_yes'])
    question = row['question']

    # Calculate time range
    try:
        close_time = pd.to_datetime(row['trading_close_time'], utc=True).replace(tzinfo=None)
        end_time = int(close_time.timestamp())
        start_time = int((close_time - timedelta(days=365)).timestamp())
    except Exception as e:
        new_error_markets.append({
            'market_id': market_id,
            'question': question,
            'error': f'Date parse error: {str(e)}'
        })
        processed_markets.add(market_id)
        continue

    # Call DomeAPI with retries
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                f"{DOME_API_BASE}/candlesticks/{condition_id}",
                headers={"Authorization": DOME_API_KEY},
                params={
                    'start_time': start_time,
                    'end_time': end_time,
                    'interval': 1440
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
                        # Update price data
                        price_data[token_id_yes] = converted_prices
                        success_count += 1

                        # Track this market
                        new_domeapi_markets.append({
                            'market_id': market_id,
                            'token_ids': [token_id_yes],
                            'source': 'DomeAPI',
                            'pulled_at': datetime.now().isoformat(),
                            'candlesticks_count': len(converted_prices)
                        })

                        status = "✓ FOUND"
                    else:
                        new_error_markets.append({
                            'market_id': market_id,
                            'question': question,
                            'error': 'No matching token_id in response'
                        })
                        status = "✗ No match"
                else:
                    new_error_markets.append({
                        'market_id': market_id,
                        'question': question,
                        'error': 'Empty candlesticks array'
                    })
                    status = "✗ Empty"

                processed_markets.add(market_id)
                break

            elif response.status_code == 404:
                new_error_markets.append({
                    'market_id': market_id,
                    'question': question,
                    'error': '404 Not Found'
                })
                processed_markets.add(market_id)
                status = "✗ 404"
                break

            elif response.status_code == 429:
                if attempt < MAX_RETRIES - 1:
                    # Exponential backoff for rate limits
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    print(f"  Rate limited, waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    new_error_markets.append({
                        'market_id': market_id,
                        'question': question,
                        'error': '429 Rate Limit (retry failed)'
                    })
                    processed_markets.add(market_id)
                    status = "✗ 429 Still"
                    break
            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    new_error_markets.append({
                        'market_id': market_id,
                        'question': question,
                        'error': f'Status {response.status_code}'
                    })
                    processed_markets.add(market_id)
                    status = f"✗ {response.status_code}"
                    break

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            else:
                new_error_markets.append({
                    'market_id': market_id,
                    'question': question,
                    'error': str(e)[:100]
                })
                processed_markets.add(market_id)
                status = "✗ Error"
                break

    # Print progress
    processed = len(processed_markets)
    percent_found = (success_count / processed) * 100 if processed > 0 else 0

    print(f"[{processed}/{len(rate_limited)}] {status} | Market: {market_id[:20]}...")
    print(f"  Found on retry: {success_count}/{processed} ({percent_found:.1f}%)")
    print()

    # Checkpoint every 25 markets
    if len(processed_markets) % 25 == 0:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                'processed_markets': list(processed_markets),
                'new_domeapi_markets': new_domeapi_markets,
                'new_error_markets': new_error_markets,
                'last_updated': datetime.now().isoformat()
            }, f)

    time.sleep(RATE_LIMIT_DELAY)

# Final checkpoint
with open(CHECKPOINT_FILE, 'w') as f:
    json.dump({
        'processed_markets': list(processed_markets),
        'new_domeapi_markets': new_domeapi_markets,
        'new_error_markets': new_error_markets,
        'last_updated': datetime.now().isoformat()
    }, f)

# Save results
print(f"\n{'=' * 80}")
print("SAVING RESULTS")
print(f"{'=' * 80}")

# Backup original price file
backup_file = PRICE_FILE.replace('.json', f'_backup_retry_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
with open(PRICE_FILE, 'r') as f:
    original_data = json.load(f)
with open(backup_file, 'w') as f:
    json.dump(original_data, f)
print(f"✓ Backed up original price file")

# Save updated price data
with open(PRICE_FILE, 'w') as f:
    json.dump(price_data, f)
print(f"✓ Saved updated price data: {len(price_data):,} tokens")

# Update tracking file
all_domeapi_markets = existing_domeapi_markets + new_domeapi_markets
with open(TRACKING_FILE, 'w') as f:
    json.dump({
        'markets': all_domeapi_markets,
        'created_at': tracking.get('created_at'),
        'updated_at': datetime.now().isoformat(),
        'total_markets': len(all_domeapi_markets)
    }, f, indent=2)
print(f"✓ Updated tracking file: {len(all_domeapi_markets):,} total DomeAPI markets")

# Update error file (remove successful retries)
if success_count > 0:
    successful_ids = [m['market_id'] for m in new_domeapi_markets]
    updated_errors = errors_df[~errors_df['market_id'].astype(str).isin(successful_ids)]

    # Add new errors
    if new_error_markets:
        new_errors_df = pd.DataFrame(new_error_markets)
        updated_errors = pd.concat([updated_errors, new_errors_df], ignore_index=True)

    updated_errors.to_csv(ERROR_FILE, index=False)
    print(f"✓ Updated error file: {len(updated_errors):,} total errors")

# Summary
print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")
print(f"\nRate-limited markets retried: {len(rate_limited)}")
print(f"Successfully pulled on retry: {success_count} ({success_count/len(rate_limited)*100:.1f}%)")
print(f"Still failed: {len(new_error_markets)}")

if new_error_markets:
    retry_error_types = pd.DataFrame(new_error_markets)['error'].value_counts()
    print(f"\nRetry failure breakdown:")
    for error_type, count in retry_error_types.items():
        print(f"  {error_type}: {count}")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
