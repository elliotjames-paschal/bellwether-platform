#!/usr/bin/env python3
"""
B1: Categorize untagged Polymarket markets into 15 political categories
Filters out NOT_POLITICAL markets
"""

import pandas as pd
import numpy as np
import json
import time
from datetime import datetime
from openai import OpenAI
import os

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
INPUT_FILE = f"{DATA_DIR}/polymarket_untagged_political_markets_filtered.csv"
OUTPUT_FILE = f"{DATA_DIR}/polymarket_untagged_political_markets_categorized.csv"
CHECKPOINT_FILE = f"{DATA_DIR}/polymarket_categorization_checkpoint.json"

# OpenAI Configuration
BATCH_SIZE = 20
MODEL = "gpt-4o"
TEMPERATURE = 0

# Political categories (15 types)
POLITICAL_CATEGORIES = [
    "1. ELECTORAL",
    "2. MONETARY_POLICY",
    "3. LEGISLATIVE",
    "4. APPOINTMENTS",
    "5. REGULATORY",
    "6. INTERNATIONAL",
    "7. JUDICIAL",
    "8. MILITARY_SECURITY",
    "9. CRISIS_EMERGENCY",
    "10. GOVERNMENT_OPERATIONS",
    "11. PARTY_POLITICS",
    "12. STATE_LOCAL",
    "13. TIMING_EVENTS",
    "14. POLLING_APPROVAL",
    "15. POLITICAL_SPEECH"
]

print("=" * 80)
print("CATEGORIZING UNTAGGED POLYMARKET MARKETS")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load OpenAI API key
api_key_path = f"{BASE_DIR}/openai_api_key.txt"
with open(api_key_path, 'r') as f:
    api_key = f.read().strip()

client = OpenAI(api_key=api_key)

# Load untagged markets
print(f"\n{'=' * 80}")
print("LOADING UNTAGGED MARKETS")
print(f"{'=' * 80}")

df = pd.read_csv(INPUT_FILE)
print(f"✓ Loaded {len(df):,} untagged markets")

# Load checkpoint if exists
processed_offset = 0
categorized_markets = []

if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint = json.load(f)
        processed_offset = checkpoint.get('processed_offset', 0)
        categorized_markets = checkpoint.get('categorized_markets', [])
    print(f"✓ Loaded checkpoint: {processed_offset:,} markets processed")

# Prepare questions
questions = df['question'].tolist()
market_ids = df['id'].tolist()
total_batches = (len(questions) + BATCH_SIZE - 1) // BATCH_SIZE

print(f"\nTotal markets: {len(questions):,}")
print(f"Batch size: {BATCH_SIZE}")
print(f"Total batches: {total_batches:,}")
print(f"Starting from offset: {processed_offset:,}")

print(f"\n{'=' * 80}")
print("CATEGORIZING WITH CHATGPT")
print(f"{'=' * 80}")

start_time = time.time()
successful_batches = 0
failed_batches = 0
not_political_count = 0

for batch_num in range(processed_offset // BATCH_SIZE, total_batches):
    start_idx = batch_num * BATCH_SIZE
    end_idx = min(start_idx + BATCH_SIZE, len(questions))
    batch_questions = questions[start_idx:end_idx]
    batch_ids = market_ids[start_idx:end_idx]

    # Create batch prompt - matches original pipeline format
    markets_text = ""
    for i, q in enumerate(batch_questions, 1):
        markets_text += f"""
Market {i}:
Question: "{q}"
"""

    # System prompt (sets behavior/role)
    system_prompt = """You are an expert political scientist and data categorization specialist. Your task is to categorize political prediction markets with perfect accuracy. You must assign each market to exactly one category. You follow instructions precisely and never deviate from the required format."""

    # User prompt (the actual task) - using original pipeline definitions + NOT_POLITICAL
    prompt = f"""
You must categorize each political market using EXACTLY one of these numbered categories:

1. ELECTORAL - Election outcomes, candidate performance, voting results, primaries, campaigns (domestic & international)
2. MONETARY_POLICY - Fed decisions, interest rates, inflation, economic policy, central bank actions
3. LEGISLATIVE - Congressional actions, bill passage, votes, committee decisions, legislation
4. APPOINTMENTS - Government nominations, confirmations, cabinet picks, judicial appointments (domestic & international)
5. REGULATORY - Agency decisions (SEC, FDA, EPA), regulatory approvals, government oversight
6. INTERNATIONAL - Foreign policy, sanctions, trade deals, diplomatic outcomes, treaties, wars
7. JUDICIAL - Court decisions, legal rulings, Supreme Court cases, legal proceedings
8. MILITARY_SECURITY - Military actions, defense decisions, conflicts, intelligence, cybersecurity
9. CRISIS_EMERGENCY - Disaster response, emergencies, pandemic response, crisis management
10. GOVERNMENT_OPERATIONS - Budget decisions, shutdowns, debt ceiling, government contracts
11. PARTY_POLITICS - Internal party decisions, leadership changes, scandals, investigations
12. STATE_LOCAL - State/local politics, governors, mayors, state legislation, ballot initiatives
13. TIMING_EVENTS - Political timing, announcement timing, scheduling decisions, "when will X"
14. POLLING_APPROVAL - Opinion polls, approval ratings, public opinion surveys
15. POLITICAL_SPEECH - What politicians will say, speech content, word usage in political events

IMPORTANT: If a market is NOT political (sports, entertainment, crypto prices, tech products, gaming, weather, science not related to policy), respond with "NOT_POLITICAL" for that market.

{markets_text}

CRITICAL RULES:
- You MUST choose a number 1-15 for each political market
- If market is non-political, respond with "NOT_POLITICAL"
- International political events use the same categories as domestic ones
- Pick the closest fit when uncertain between political categories

Respond with exactly {len(batch_questions)} lines:
Market 1: [NUMBER]. [CATEGORY NAME] or NOT_POLITICAL
Market 2: [NUMBER]. [CATEGORY NAME] or NOT_POLITICAL
...and so on.
"""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            temperature=TEMPERATURE
        )

        result = response.choices[0].message.content.strip()

        # Parse text-based response (line by line)
        lines = result.split('\n')
        categories = []

        for i, line in enumerate(lines):
            if f"Market {i+1}:" in line:
                # Extract category after the colon
                category = line.split(":", 1)[1].strip() if ":" in line else "NOT_POLITICAL"
                categories.append(category)

        # If we didn't get enough results, pad with NOT_POLITICAL
        while len(categories) < len(batch_questions):
            categories.append("NOT_POLITICAL")

        # Handle mismatched counts
        if len(categories) != len(batch_questions):
            print(f"\n⚠ Batch {batch_num + 1}/{total_batches}: Expected {len(batch_questions)}, got {len(categories)}")
            # Pad or truncate to match
            if len(categories) < len(batch_questions):
                categories.extend(["NOT_POLITICAL"] * (len(batch_questions) - len(categories)))
                print(f"  → Padded with NOT_POLITICAL to match")
            else:
                categories = categories[:len(batch_questions)]
                print(f"  → Truncated to match")

        # Store categorized markets
        batch_not_political = 0
        for i, (market_id, question, category) in enumerate(zip(batch_ids, batch_questions, categories)):
            if category == "NOT_POLITICAL":
                batch_not_political += 1
                not_political_count += 1
            else:
                # Find the original row
                original_row = df[df['id'] == market_id].iloc[0]
                categorized_markets.append({
                    'id': int(market_id),
                    'question': str(question),
                    'political_category': str(category),
                    'closed': bool(original_row['closed']) if pd.notna(original_row['closed']) else None,
                    'end_date': str(original_row['end_date']) if pd.notna(original_row['end_date']) else None,
                    'volume': float(original_row['volume']) if pd.notna(original_row['volume']) else None
                })

        successful_batches += 1
        print(f"\n✓ Batch {batch_num + 1}/{total_batches} ({len(batch_questions)} markets)")
        print(f"  Political: {len(batch_questions) - batch_not_political} | NOT_POLITICAL: {batch_not_political}")
        print(f"  Sample: {batch_questions[0][:60]}... → {categories[0]}")

        # Save checkpoint every 10 batches
        if (batch_num + 1) % 10 == 0:
            with open(CHECKPOINT_FILE, 'w') as f:
                json.dump({
                    'processed_offset': end_idx,
                    'categorized_markets': categorized_markets,
                    'last_updated': datetime.now().isoformat()
                }, f)

            elapsed = time.time() - start_time
            rate = end_idx / elapsed if elapsed > 0 else 0
            remaining = len(questions) - end_idx
            eta_minutes = (remaining / rate / 60) if rate > 0 else 0

            print(f"\n  [Checkpoint saved: {end_idx:,}/{len(questions):,} processed]")
            print(f"  [Rate: {rate:.1f} markets/sec | ETA: {eta_minutes:.1f} minutes]")

    except Exception as e:
        print(f"\n✗ Batch {batch_num + 1}/{total_batches}: ERROR - {str(e)[:100]}")
        failed_batches += 1

    time.sleep(1.5)  # Rate limiting

# Final checkpoint
with open(CHECKPOINT_FILE, 'w') as f:
    json.dump({
        'processed_offset': len(questions),
        'categorized_markets': categorized_markets,
        'last_updated': datetime.now().isoformat()
    }, f)

elapsed_total = time.time() - start_time

print(f"\n{'=' * 80}")
print("CATEGORIZATION SUMMARY")
print(f"{'=' * 80}")
print(f"\nTotal markets processed: {len(questions):,}")
print(f"Political markets: {len(categorized_markets):,}")
print(f"NOT_POLITICAL (filtered): {not_political_count:,}")
print(f"Successful batches: {successful_batches}/{total_batches}")
print(f"Failed batches: {failed_batches}")
print(f"Total time: {elapsed_total/60:.1f} minutes")

# Category distribution
if categorized_markets:
    from collections import Counter
    cat_counts = Counter([m['political_category'] for m in categorized_markets])
    print(f"\nCategory distribution:")
    for cat in POLITICAL_CATEGORIES:
        count = cat_counts.get(cat, 0)
        if count > 0:
            print(f"  {cat}: {count:,}")

# Save results
print(f"\n{'=' * 80}")
print("SAVING RESULTS")
print(f"{'=' * 80}")

if categorized_markets:
    output_df = pd.DataFrame(categorized_markets)
    output_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✓ Saved {len(categorized_markets):,} political markets to:")
    print(f"  {OUTPUT_FILE}")
else:
    print("\n⚠ No political markets found")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
