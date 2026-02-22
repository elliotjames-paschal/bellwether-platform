#!/usr/bin/env python3
"""
Pull price data from DomeAPI for empty markets

Fetches candlestick data from DomeAPI for Polymarket markets that have empty
price arrays, converts to existing format, and updates the price file.
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
EMPTY_MARKETS_FILE = f"{DATA_DIR}/empty_markets_to_test.csv"
CHECKPOINT_FILE = f"{DATA_DIR}/domeapi_pull_checkpoint.json"
TRACKING_FILE = f"{DATA_DIR}/domeapi_price_sources.json"

# DomeAPI Configuration
DOME_API_BASE = "https://api.domeapi.io/v1/polymarket"
DOME_API_KEY = "Bearer d2d4d3b2cd3b0424bd2145a57d6f34a3661050e2"
RATE_LIMIT_DELAY = 0.25
MAX_RETRIES = 3
RETRY_DELAY = 2

print("=" * 80)
print("PULLING DOMEAPI PRICE DATA FOR EMPTY MARKETS")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load data
print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

empty_df = pd.read_csv(EMPTY_MARKETS_FILE)
master_df = pd.read_csv(MASTER_FILE, low_memory=False)

# Merge to get trading_close_time
empty_df['market_id'] = empty_df['market_id'].astype(str)
master_df['market_id'] = master_df['market_id'].astype(str)

empty_df = empty_df.merge(
    master_df[['market_id', 'trading_close_time']],
    on='market_id',
    how='left'
)

# Filter to markets WITH trading_close_time
empty_with_close = empty_df[empty_df['trading_close_time'].notna()].copy()
print(f"✓ Total empty markets: {len(empty_df):,}")
print(f"✓ Markets with trading_close_time: {len(empty_with_close):,}")
print(f"✓ Markets without trading_close_time: {len(empty_df) - len(empty_with_close):,}")

# Load price data
with open(PRICE_FILE, 'r') as f:
    price_data = json.load(f)
print(f"✓ Loaded price data: {len(price_data):,} tokens")

# Load checkpoint if exists
processed_markets = set()
domeapi_markets = []
error_markets = []

try:
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint = json.load(f)
        processed_markets = set(checkpoint.get('processed_markets', []))
        domeapi_markets = checkpoint.get('domeapi_markets', [])
        error_markets = checkpoint.get('error_markets', [])
    print(f"✓ Loaded checkpoint: {len(processed_markets):,} markets processed")
except FileNotFoundError:
    print(f"✓ No checkpoint found, starting fresh")

remaining = empty_with_close[~empty_with_close['market_id'].isin(processed_markets)]
print(f"✓ Markets remaining to fetch: {len(remaining):,}")

# Pull price data from DomeAPI
print(f"\n{'=' * 80}")
print("FETCHING PRICE DATA FROM DOMEAPI")
print(f"{'=' * 80}")
print()

success_count = 0
error_count = 0

markets_list = remaining.to_dict('records')

for i, market in enumerate(markets_list, 1):
    market_id = str(market['market_id'])
    condition_id = market['condition_id']
    token_id_yes = str(market['token_id'])

    # Calculate time range
    try:
        close_time = pd.to_datetime(market['trading_close_time'], utc=True).replace(tzinfo=None)
        end_time = int(close_time.timestamp())
        start_time = int((close_time - timedelta(days=365)).timestamp())
    except Exception as e:
        error_markets.append({
            'market_id': market_id,
            'question': market['question'],
            'error': f'Date parse error: {str(e)}'
        })
        processed_markets.add(market_id)
        continue

    # Call DomeAPI
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
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                candlesticks = data.get('candlesticks', [])

                if candlesticks and len(candlesticks) > 0:
                    # Process candlestick data
                    # Structure: [ [[candle1, candle2, ...], {token_id}], [[...], {token_id}] ]

                    converted_prices = []
                    found_token = False

                    for token_data in candlesticks:
                        if len(token_data) != 2:
                            continue

                        candle_array = token_data[0]
                        token_info = token_data[1]
                        this_token_id = str(token_info.get('token_id', ''))

                        # Match token_id_yes
                        if this_token_id == token_id_yes:
                            found_token = True
                            # Convert candlesticks to our format
                            for candle in candle_array:
                                timestamp = candle.get('end_period_ts')
                                price_cents = candle.get('price', {}).get('close', 0)
                                price_decimal = price_cents / 100.0  # Convert cents to 0-1 range

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
                        domeapi_markets.append({
                            'market_id': market_id,
                            'token_ids': [token_id_yes],
                            'source': 'DomeAPI',
                            'pulled_at': datetime.now().isoformat(),
                            'candlesticks_count': len(converted_prices)
                        })

                        status = "✓ FOUND"
                    else:
                        error_markets.append({
                            'market_id': market_id,
                            'question': market['question'],
                            'error': 'No matching token_id in response'
                        })
                        status = "✗ No match"
                else:
                    error_markets.append({
                        'market_id': market_id,
                        'question': market['question'],
                        'error': 'Empty candlesticks array'
                    })
                    status = "✗ Empty"

                processed_markets.add(market_id)
                break

            elif response.status_code == 404:
                error_markets.append({
                    'market_id': market_id,
                    'question': market['question'],
                    'error': '404 Not Found'
                })
                processed_markets.add(market_id)
                status = "✗ 404"
                break

            elif response.status_code == 429:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * 2)  # Extra delay for rate limit
                    continue
                else:
                    error_markets.append({
                        'market_id': market_id,
                        'question': market['question'],
                        'error': '429 Rate Limit'
                    })
                    processed_markets.add(market_id)
                    status = "✗ 429"
                    break
            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    error_markets.append({
                        'market_id': market_id,
                        'question': market['question'],
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
                error_markets.append({
                    'market_id': market_id,
                    'question': market['question'],
                    'error': str(e)[:100]
                })
                processed_markets.add(market_id)
                status = "✗ Error"
                break

    # Print progress
    processed = len(processed_markets)
    percent_found = (success_count / processed) * 100 if processed > 0 else 0

    print(f"[{processed}/{len(empty_with_close)}] {status} | Market: {market_id[:20]}...")
    print(f"  Found on DomeAPI: {success_count}/{processed} ({percent_found:.1f}%)")
    print()

    # Checkpoint every 50 markets
    if len(processed_markets) % 50 == 0:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                'processed_markets': list(processed_markets),
                'domeapi_markets': domeapi_markets,
                'error_markets': error_markets,
                'last_updated': datetime.now().isoformat()
            }, f)

    time.sleep(RATE_LIMIT_DELAY)

# Final checkpoint
with open(CHECKPOINT_FILE, 'w') as f:
    json.dump({
        'processed_markets': list(processed_markets),
        'domeapi_markets': domeapi_markets,
        'error_markets': error_markets,
        'last_updated': datetime.now().isoformat()
    }, f)

# Save results
print(f"\n{'=' * 80}")
print("SAVING RESULTS")
print(f"{'=' * 80}")

# Backup original price file
backup_file = PRICE_FILE.replace('.json', f'_backup_domeapi_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
with open(PRICE_FILE, 'r') as f:
    original_data = json.load(f)
with open(backup_file, 'w') as f:
    json.dump(original_data, f)
print(f"✓ Backed up original price file to: {backup_file}")

# Save updated price data
with open(PRICE_FILE, 'w') as f:
    json.dump(price_data, f)
print(f"✓ Saved updated price data: {len(price_data):,} tokens")

# Save DomeAPI tracking file
with open(TRACKING_FILE, 'w') as f:
    json.dump({
        'markets': domeapi_markets,
        'created_at': datetime.now().isoformat(),
        'total_markets': len(domeapi_markets)
    }, f, indent=2)
print(f"✓ Saved DomeAPI tracking file: {len(domeapi_markets):,} markets")

# Save error log
if error_markets:
    error_df = pd.DataFrame(error_markets)
    error_file = f"{DATA_DIR}/domeapi_pull_errors.csv"
    error_df.to_csv(error_file, index=False)
    print(f"✓ Saved error log: {len(error_markets):,} errors")

# Summary
print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")
print(f"\nTotal markets processed: {len(processed_markets):,}")
print(f"Successfully pulled from DomeAPI: {success_count:,} ({success_count/len(processed_markets)*100:.1f}%)")
print(f"Errors: {len(error_markets):,}")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
