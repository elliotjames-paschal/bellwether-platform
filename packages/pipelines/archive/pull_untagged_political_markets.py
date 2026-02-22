#!/usr/bin/env python3
"""
Pull untagged political markets from Polymarket API
Finds political markets that were missed by tag-based filtering
"""

import pandas as pd
import requests
import json
import time
from datetime import datetime
import os

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
OUTPUT_FILE = f"{DATA_DIR}/polymarket_untagged_political_markets.csv"
CHECKPOINT_FILE = f"{DATA_DIR}/untagged_political_checkpoint.json"
EXISTING_MARKETS_FILE = f"{DATA_DIR}/market_categories_with_outcomes.csv"

# Configuration
RATE_LIMIT_DELAY = 0.15  # seconds between requests
MAX_OFFSET = 100000  # Maximum pagination depth
BATCH_SIZE = 100  # Markets per request

# Comprehensive political keywords
POLITICAL_KEYWORDS = [
    # Offices
    'president', 'presidential', 'vice president', 'vp',
    'governor', 'gubernatorial',
    'senate', 'senator', 'senatorial',
    'house', 'congress', 'congressional', 'representative',
    'mayor', 'mayoral',
    'attorney general',
    'secretary of state',
    'lieutenant governor',

    # Election terms
    'election', 'primary', 'caucus', 'nomination', 'nominee',
    'ballot', 'vote', 'voting', 'midterm',

    # Parties
    'democrat', 'democratic', 'republican', 'gop',
    'party', 'nomination',

    # Common phrases
    'win the', 'elected', 'campaign',
    'swing state', 'battleground',
]

print("=" * 80)
print("PULLING UNTAGGED POLITICAL MARKETS FROM POLYMARKET")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load existing markets to avoid duplicates
print(f"\n{'=' * 80}")
print("LOADING EXISTING MARKETS")
print(f"{'=' * 80}")

existing_market_ids = set()
if os.path.exists(EXISTING_MARKETS_FILE):
    existing_df = pd.read_csv(EXISTING_MARKETS_FILE)
    # Handle both 'id' and 'market_id' column names
    id_col = 'id' if 'id' in existing_df.columns else 'market_id'
    if id_col in existing_df.columns:
        existing_market_ids = set(existing_df[id_col].astype(str))
    print(f"✓ Loaded {len(existing_market_ids):,} existing market IDs")
else:
    print("⚠ No existing markets file found, will pull all untagged political markets")

# Load checkpoint
processed_offsets = set()
untagged_markets = []

if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint = json.load(f)
        processed_offsets = set(checkpoint.get('processed_offsets', []))
        untagged_markets = checkpoint.get('untagged_markets', [])
    print(f"✓ Loaded checkpoint: {len(processed_offsets):,} batches processed, {len(untagged_markets):,} untagged markets found")

# Calculate progress
total_batches = MAX_OFFSET // BATCH_SIZE
remaining_batches = total_batches - len(processed_offsets)

print(f"\nTotal batches: {total_batches:,}")
print(f"Already processed: {len(processed_offsets):,}")
print(f"Remaining: {remaining_batches:,}")

# Process markets
print(f"\n{'=' * 80}")
print("SEARCHING FOR UNTAGGED POLITICAL MARKETS")
print(f"{'=' * 80}")

offset = 0
markets_checked = 0
political_found = 0
untagged_political_found = 0
new_markets_found = 0

start_time = time.time()

while offset < MAX_OFFSET:
    # Skip if already processed
    if offset in processed_offsets:
        offset += BATCH_SIZE
        continue

    try:
        # Fetch batch
        response = requests.get(
            "https://gamma-api.polymarket.com/markets",
            params={'closed': 'true', 'limit': BATCH_SIZE, 'offset': offset},
            timeout=10
        )

        if response.status_code != 200:
            print(f"\n⚠ API error {response.status_code} at offset {offset}")
            offset += BATCH_SIZE
            time.sleep(RATE_LIMIT_DELAY * 3)  # Back off on error
            continue

        markets = response.json()

        if not markets:
            print(f"\nNo more markets at offset {offset}")
            break

        markets_checked += len(markets)

        # Check each market
        for market in markets:
            question = market.get('question', '').lower()
            market_id = str(market.get('id', ''))
            tags = market.get('tags', [])

            # Check if political by keyword
            is_political = any(keyword in question for keyword in POLITICAL_KEYWORDS)

            if is_political:
                political_found += 1

                # Check if it has no tags
                has_tags = len(tags) > 0

                if not has_tags:
                    untagged_political_found += 1

                    # Check if it's new (not in existing dataset)
                    if market_id not in existing_market_ids:
                        new_markets_found += 1

                        untagged_markets.append({
                            'id': market_id,
                            'question': market.get('question'),
                            'closed': market.get('closed'),
                            'restricted': market.get('restricted'),
                            'end_date': market.get('endDateIso'),
                            'created_at': market.get('createdAt'),
                            'category': market.get('category'),
                            'volume': market.get('volume'),
                            'offset_found': offset
                        })

                        print(f"\n✓ NEW untagged political market at offset {offset}")
                        print(f"  ID: {market_id}")
                        print(f"  Q: {market.get('question')[:70]}...")

        # Mark offset as processed
        processed_offsets.add(offset)

        # Save checkpoint every 10 batches
        if len(processed_offsets) % 10 == 0:
            with open(CHECKPOINT_FILE, 'w') as f:
                json.dump({
                    'processed_offsets': list(processed_offsets),
                    'untagged_markets': untagged_markets,
                    'last_updated': datetime.now().isoformat()
                }, f)

        # Progress update every 1000 markets
        if markets_checked % 1000 == 0:
            elapsed = time.time() - start_time
            rate = markets_checked / elapsed if elapsed > 0 else 0
            remaining_markets = MAX_OFFSET - offset
            est_time_remaining = (remaining_markets / rate / 60) if rate > 0 else 0

            print(f"\n[Progress] Offset {offset:,}/{MAX_OFFSET:,}")
            print(f"  Checked: {markets_checked:,} markets | Rate: {rate:.1f} markets/sec")
            print(f"  Political found: {political_found:,} | Untagged: {untagged_political_found:,} | NEW: {new_markets_found:,}")
            print(f"  Est. time remaining: {est_time_remaining:.1f} minutes")

        offset += BATCH_SIZE
        time.sleep(RATE_LIMIT_DELAY)

    except Exception as e:
        print(f"\n❌ Error at offset {offset}: {e}")
        time.sleep(RATE_LIMIT_DELAY * 3)  # Back off on error
        offset += BATCH_SIZE

# Final checkpoint save
with open(CHECKPOINT_FILE, 'w') as f:
    json.dump({
        'processed_offsets': list(processed_offsets),
        'untagged_markets': untagged_markets,
        'last_updated': datetime.now().isoformat()
    }, f)

# Save results
print(f"\n{'=' * 80}")
print("SAVING RESULTS")
print(f"{'=' * 80}")

if untagged_markets:
    df = pd.DataFrame(untagged_markets)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✓ Saved {len(untagged_markets):,} untagged political markets to:")
    print(f"  {OUTPUT_FILE}")
else:
    print("\n⚠ No untagged political markets found")

# Summary
print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")

elapsed_total = time.time() - start_time
print(f"\nTotal runtime: {elapsed_total/60:.1f} minutes")
print(f"Markets checked: {markets_checked:,}")
print(f"Political markets found: {political_found:,}")
print(f"Untagged political markets: {untagged_political_found:,}")
print(f"NEW markets (not in existing dataset): {new_markets_found:,}")

if untagged_markets:
    print(f"\nBreakdown by restricted status:")
    restricted_count = sum(1 for m in untagged_markets if m['restricted'])
    print(f"  restricted=True: {restricted_count}")
    print(f"  restricted=False: {len(untagged_markets) - restricted_count}")

    print(f"\nSample untagged markets:")
    for market in untagged_markets[:5]:
        print(f"  {market['id']}: {market['question'][:70]}...")

print(f"\n✓ COMPLETE")
print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
