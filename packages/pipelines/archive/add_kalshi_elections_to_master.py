#!/usr/bin/env python3
"""
Add Kalshi elections to master dataset
Handles both:
- Uppercase tickers: Single markets via /markets/{ticker}
- Lowercase tickers: Events with multiple markets via /events/{ticker}
"""

import pandas as pd
import requests
import json
import time
from datetime import datetime
import os
from openai import OpenAI

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
KALSHI_OFFICIAL_FILE = f"{DATA_DIR}/kalshi_official_with_party_winner_tickers.csv"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
CHECKPOINT_FILE = f"{DATA_DIR}/kalshi_master_addition_checkpoint.json"

# API Configuration
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
RATE_LIMIT_DELAY = 0.3  # seconds between requests
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# OpenAI Configuration
OPENAI_BATCH_SIZE = 50
OPENAI_MODEL = "gpt-4o-mini"
OPENAI_TEMPERATURE = 0

# Election type categories (20 types)
ELECTION_TYPES = [
    "Presidential", "Presidential Primary",
    "Senate", "Senate Primary",
    "House", "House Primary",
    "VP Nomination",
    "Gubernatorial", "Gubernatorial Primary",
    "Mayoral", "Mayoral Primary",
    "Democratic Primary", "Republican Primary",
    "Parliamentary", "Prime Minister", "General Election",
    "European Parliament", "Regional Election",
    "Chancellor", "National Election", "Provincial"
]

print("=" * 80)
print("ADDING KALSHI ELECTIONS TO MASTER DATASET")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load kalshi_official file
print(f"\n{'=' * 80}")
print("LOADING KALSHI OFFICIAL FILE")
print(f"{'=' * 80}")

kalshi_df = pd.read_csv(KALSHI_OFFICIAL_FILE)
print(f"✓ Loaded {len(kalshi_df):,} rows from kalshi_official file")

# Filter for US elections with year <= 2025 that have tickers
filtered = kalshi_df[
    (kalshi_df['country'] == 'United States') &
    (kalshi_df['election_year'] <= 2025) &
    (kalshi_df['ticker'].notna()) &
    (kalshi_df['ticker'] != '')
].copy()

print(f"✓ Filtered to {len(filtered):,} US elections (year <= 2025) with tickers")

# Separate uppercase and lowercase tickers
uppercase_tickers = filtered[filtered['ticker'].str[0].str.isupper()].copy()
lowercase_tickers = filtered[filtered['ticker'].str[0].str.islower()].copy()

print(f"\n  Uppercase tickers (markets): {len(uppercase_tickers):,}")
print(f"  Lowercase tickers (events): {len(lowercase_tickers):,}")

# Load master file
print(f"\n{'=' * 80}")
print("LOADING MASTER FILE")
print(f"{'=' * 80}")

master_df = pd.read_csv(MASTER_FILE, low_memory=False)
print(f"✓ Loaded master file: {len(master_df):,} rows")

# Get existing Kalshi market IDs
existing_kalshi = master_df[master_df['platform'] == 'Kalshi']
existing_market_ids = set(existing_kalshi['market_id'].astype(str).str.lower())
print(f"✓ Found {len(existing_market_ids):,} existing Kalshi markets in master")

# Load checkpoint if exists
processed_tickers = set()
new_markets = []

if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint = json.load(f)
        processed_tickers = set(checkpoint.get('processed_tickers', []))
        new_markets = checkpoint.get('new_markets', [])
    print(f"✓ Loaded checkpoint: {len(processed_tickers):,} tickers processed, {len(new_markets):,} new markets found")

# Function to call Kalshi API with retries
def call_kalshi_api(endpoint, ticker):
    """Call Kalshi API with retry logic"""
    for attempt in range(MAX_RETRIES):
        try:
            url = f"{KALSHI_API_BASE}/{endpoint}/{ticker}"
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
            else:
                print(f"  ⚠ API error {response.status_code} for {ticker}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                return None

        except Exception as e:
            print(f"  ❌ Error calling API for {ticker}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return None

    return None

# Function to extract market data
def extract_market_data(market, source_row):
    """Extract market data into master file format"""
    data = {
        'platform': 'Kalshi',
        'market_id': market.get('ticker'),
        'question': market.get('title'),
        'is_closed': market.get('status') in ['settled', 'finalized', 'closed'],
        'political_category': '1. ELECTORAL',

        # Electoral details from source file
        'country': source_row['country'],
        'office': source_row['office'],
        'location': source_row['location'],
        'election_year': source_row['election_year'],
        'is_primary': source_row['is_primary'],

        # Volume (already in USD)
        'volume_usd': float(market.get('volume', 0)) if market.get('volume') else None,

        # Kalshi-specific fields (k_ prefix)
        'k_event_ticker': market.get('event_ticker'),
        'k_market_type': market.get('market_type'),
        'k_status': market.get('status'),
        'k_yes_bid': market.get('yes_bid'),
        'k_yes_ask': market.get('yes_ask'),
        'k_no_bid': market.get('no_bid'),
        'k_no_ask': market.get('no_ask'),
        'k_last_price': market.get('last_price'),
        'k_volume_contracts': market.get('volume'),
        'k_liquidity': market.get('liquidity'),
        'k_open_interest': market.get('open_interest'),
        'k_settlement_value': market.get('settlement_value'),
        'k_result': market.get('result'),
        'k_close_time': market.get('close_time'),
        'k_expiration_time': market.get('expiration_time'),
        'k_created_time': market.get('created_time'),
        'k_open_time': market.get('open_time'),
        'trading_close_time': market.get('close_time'),
    }

    return data

# Process markets
print(f"\n{'=' * 80}")
print("FETCHING MARKET DATA FROM KALSHI API")
print(f"{'=' * 80}")

total_tickers = len(uppercase_tickers) + len(lowercase_tickers)
processed_count = 0
new_count = 0
duplicate_count = 0
error_count = 0

start_time = time.time()

# Process uppercase tickers (single markets)
print(f"\nProcessing uppercase tickers (markets)...")
for idx, row in uppercase_tickers.iterrows():
    ticker = row['ticker']

    # Skip if already processed
    if ticker in processed_tickers:
        processed_count += 1
        continue

    # Check if already in master
    if ticker.lower() in existing_market_ids:
        duplicate_count += 1
        processed_tickers.add(ticker)
        processed_count += 1
        continue

    # Call API
    data = call_kalshi_api('markets', ticker)
    time.sleep(RATE_LIMIT_DELAY)

    if data and 'market' in data:
        market = data['market']
        market_data = extract_market_data(market, row)
        new_markets.append(market_data)
        new_count += 1

        print(f"\n✓ [{processed_count + 1}/{total_tickers}] {ticker}")
        print(f"  {market.get('title', '')[:70]}")

    else:
        error_count += 1
        print(f"\n✗ [{processed_count + 1}/{total_tickers}] {ticker} - NOT FOUND")

    processed_tickers.add(ticker)
    processed_count += 1

    # Save checkpoint every 10 tickers
    if processed_count % 10 == 0:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                'processed_tickers': list(processed_tickers),
                'new_markets': new_markets,
                'last_updated': datetime.now().isoformat()
            }, f)
        print(f"\n  [Checkpoint saved: {processed_count}/{total_tickers} processed]")

# Process lowercase tickers (events with multiple markets)
print(f"\n\nProcessing lowercase tickers (events)...")
for idx, row in lowercase_tickers.iterrows():
    ticker = row['ticker']
    ticker_upper = ticker.upper()

    # Skip if already processed
    if ticker in processed_tickers:
        processed_count += 1
        continue

    # Call API with uppercase version
    data = call_kalshi_api('events', ticker_upper)
    time.sleep(RATE_LIMIT_DELAY)

    if data and 'markets' in data:
        markets = data['markets']
        event_title = data.get('event', {}).get('title', '')

        print(f"\n✓ [{processed_count + 1}/{total_tickers}] {ticker} → {ticker_upper}")
        print(f"  Event: {event_title[:70]}")
        print(f"  Markets in event: {len(markets)}")

        # Process each market in the event
        for market in markets:
            market_ticker = market.get('ticker')

            # Check if already in master
            if market_ticker.lower() in existing_market_ids:
                duplicate_count += 1
                continue

            market_data = extract_market_data(market, row)
            new_markets.append(market_data)
            new_count += 1

            print(f"    + {market_ticker}: {market.get('title', '')[:60]}")

    else:
        error_count += 1
        print(f"\n✗ [{processed_count + 1}/{total_tickers}] {ticker} → {ticker_upper} - NOT FOUND")

    processed_tickers.add(ticker)
    processed_count += 1

    # Save checkpoint every 10 tickers
    if processed_count % 10 == 0:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                'processed_tickers': list(processed_tickers),
                'new_markets': new_markets,
                'last_updated': datetime.now().isoformat()
            }, f)
        print(f"\n  [Checkpoint saved: {processed_count}/{total_tickers} processed]")

# Final checkpoint save
with open(CHECKPOINT_FILE, 'w') as f:
    json.dump({
        'processed_tickers': list(processed_tickers),
        'new_markets': new_markets,
        'last_updated': datetime.now().isoformat()
    }, f)

elapsed = time.time() - start_time
print(f"\n{'=' * 80}")
print("API FETCH SUMMARY")
print(f"{'=' * 80}")
print(f"\nTotal tickers processed: {processed_count}/{total_tickers}")
print(f"New markets found: {new_count}")
print(f"Duplicates skipped: {duplicate_count}")
print(f"Errors/Not found: {error_count}")
print(f"Time elapsed: {elapsed/60:.1f} minutes")

# Label election_type using ChatGPT
if new_markets:
    print(f"\n{'=' * 80}")
    print("LABELING ELECTION TYPES WITH CHATGPT")
    print(f"{'=' * 80}")

    # Load OpenAI API key
    api_key_path = f"{BASE_DIR}/openai_api_key.txt"
    with open(api_key_path, 'r') as f:
        api_key = f.read().strip()

    client = OpenAI(api_key=api_key)

    # Prepare questions for batching
    questions = [m['question'] for m in new_markets]
    total_batches = (len(questions) + OPENAI_BATCH_SIZE - 1) // OPENAI_BATCH_SIZE

    print(f"\nTotal markets to categorize: {len(questions)}")
    print(f"Batch size: {OPENAI_BATCH_SIZE}")
    print(f"Total batches: {total_batches}")

    all_election_types = []

    for batch_num in range(total_batches):
        start_idx = batch_num * OPENAI_BATCH_SIZE
        end_idx = min(start_idx + OPENAI_BATCH_SIZE, len(questions))
        batch_questions = questions[start_idx:end_idx]

        # Create prompt
        prompt = f"""You are categorizing political prediction markets into election types.

For each market question below, determine which election type it belongs to from this list:
{', '.join(ELECTION_TYPES)}

Return a JSON object with a "categories" array containing exactly one election type for each question, in the same order.

Questions:
"""
        for i, q in enumerate(batch_questions, 1):
            prompt += f"{i}. {q}\n"

        prompt += f"\nReturn JSON format: {{\"categories\": [\"{ELECTION_TYPES[0]}\", ...]}}"

        try:
            response = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=OPENAI_TEMPERATURE,
                response_format={"type": "json_object"}
            )

            result = json.loads(response.choices[0].message.content)
            categories = result.get('categories', [])

            if len(categories) != len(batch_questions):
                print(f"\n⚠ Batch {batch_num + 1}: Expected {len(batch_questions)} categories, got {len(categories)}")
                # Fill with None for missing
                categories.extend([None] * (len(batch_questions) - len(categories)))

            all_election_types.extend(categories)
            print(f"✓ Batch {batch_num + 1}/{total_batches} completed ({len(batch_questions)} markets)")

        except Exception as e:
            print(f"❌ Error in batch {batch_num + 1}: {e}")
            # Fill with None for this batch
            all_election_types.extend([None] * len(batch_questions))

        time.sleep(1)  # Rate limiting

    # Add election types to new markets
    for i, market in enumerate(new_markets):
        if i < len(all_election_types):
            market['election_type'] = all_election_types[i]

    print(f"\n✓ Labeled {len(all_election_types)} markets with election types")

# Add new markets to master
if new_markets:
    print(f"\n{'=' * 80}")
    print("ADDING NEW MARKETS TO MASTER FILE")
    print(f"{'=' * 80}")

    new_df = pd.DataFrame(new_markets)

    # Backup master file
    backup_file = MASTER_FILE.replace('.csv', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    master_df.to_csv(backup_file, index=False)
    print(f"✓ Backed up master file to: {backup_file}")

    # Append new markets
    updated_df = pd.concat([master_df, new_df], ignore_index=True)
    updated_df.to_csv(MASTER_FILE, index=False)

    print(f"✓ Added {len(new_df)} new markets to master")
    print(f"✓ Updated master file: {len(updated_df):,} total rows")
    print(f"✓ Saved to: {MASTER_FILE}")
else:
    print(f"\n⚠ No new markets to add to master file")

# Summary
print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")

print(f"\nFinal Summary:")
print(f"  Tickers processed: {processed_count}/{total_tickers}")
print(f"  New markets added: {new_count}")
print(f"  Duplicates skipped: {duplicate_count}")
print(f"  Errors: {error_count}")

if new_markets:
    print(f"\nNext steps:")
    print(f"  1. Add vote share data for these elections (manual)")
    print(f"  2. Run party_affiliation population script")
    print(f"  3. Verify close_time accuracy")
    print(f"  4. Pull price history data")

print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
