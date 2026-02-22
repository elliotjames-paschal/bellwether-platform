#!/usr/bin/env python3
"""
B3: Label election_type for all ELECTORAL Polymarket markets
Labels all 261 markets (US and non-US) with 20 election type categories
"""

import pandas as pd
import json
import time
from datetime import datetime
from openai import OpenAI

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
INPUT_FILE = f"{DATA_DIR}/polymarket_untagged_political_markets_categorized.csv"
OUTPUT_FILE = f"{DATA_DIR}/polymarket_untagged_with_election_types.csv"

# OpenAI Configuration
BATCH_SIZE = 50
MODEL = "gpt-4o"
TEMPERATURE = 0

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
print("LABELING ELECTION TYPES FOR POLYMARKET ELECTORAL MARKETS")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load OpenAI API key
api_key_path = f"{BASE_DIR}/openai_api_key.txt"
with open(api_key_path, 'r') as f:
    api_key = f.read().strip()

client = OpenAI(api_key=api_key)

# Load categorized markets
print(f"\n{'=' * 80}")
print("LOADING CATEGORIZED MARKETS")
print(f"{'=' * 80}")

df = pd.read_csv(INPUT_FILE)
print(f"✓ Loaded {len(df):,} categorized markets")

# Filter for ELECTORAL markets only
electoral_df = df[df['political_category'] == '1. ELECTORAL'].copy()
print(f"✓ Found {len(electoral_df):,} ELECTORAL markets to label")

# Prepare questions
questions = electoral_df['question'].tolist()
market_ids = electoral_df['id'].tolist()
total_batches = (len(questions) + BATCH_SIZE - 1) // BATCH_SIZE

print(f"\nBatch size: {BATCH_SIZE}")
print(f"Total batches: {total_batches}")

print(f"\n{'=' * 80}")
print("LABELING WITH CHATGPT")
print(f"{'=' * 80}")

all_election_types = []
start_time = time.time()

for batch_num in range(total_batches):
    start_idx = batch_num * BATCH_SIZE
    end_idx = min(start_idx + BATCH_SIZE, len(questions))
    batch_questions = questions[start_idx:end_idx]

    # Create prompt
    prompt = f"""You are categorizing election prediction markets into election types.

For each market question below, determine which ONE election type it belongs to from this list:
{', '.join(ELECTION_TYPES)}

IMPORTANT:
- Return EXACTLY one category for each question
- Categories are case-sensitive - use exact spelling from the list
- For US elections:
  - Presidential elections → "Presidential"
  - Presidential primaries → "Presidential Primary"
  - Senate elections → "Senate"
  - Senate primaries → "Senate Primary"
  - House elections → "House"
  - House primaries → "House Primary"
  - Governor elections → "Gubernatorial"
  - Governor primaries → "Gubernatorial Primary"
  - Mayor elections → "Mayoral"
  - Mayor primaries → "Mayoral Primary"
  - VP nominations → "VP Nomination"
  - Party-specific primaries → "Democratic Primary" or "Republican Primary"

- For non-US elections:
  - Parliamentary elections → "Parliamentary"
  - Prime Minister elections → "Prime Minister"
  - General national elections → "General Election"
  - European Parliament → "European Parliament"
  - Regional/state elections → "Regional Election"
  - Chancellor elections → "Chancellor"
  - National elections (generic) → "National Election"
  - Provincial elections → "Provincial"

Return valid JSON only, no markdown.

Questions:
"""
    for i, q in enumerate(batch_questions, 1):
        prompt += f"{i}. {q}\n"

    prompt += f'\nReturn JSON format: {{"categories": ["{ELECTION_TYPES[0]}", "{ELECTION_TYPES[1]}", ...]}}'

    try:
        print(f"\nBatch {batch_num + 1}/{total_batches} ({len(batch_questions)} markets)...", end=" ")

        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=TEMPERATURE,
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)
        categories = result.get('categories', [])

        # Handle mismatched counts
        if len(categories) != len(batch_questions):
            print(f"⚠ Expected {len(batch_questions)}, got {len(categories)}")
            # Pad or truncate
            if len(categories) < len(batch_questions):
                categories.extend([None] * (len(batch_questions) - len(categories)))
            else:
                categories = categories[:len(batch_questions)]

        # Validate categories
        invalid = [c for c in categories if c not in ELECTION_TYPES and c is not None]
        if invalid:
            print(f"⚠ Invalid: {invalid}")

        all_election_types.extend(categories)
        print(f"✓")

        # Show sample
        if len(batch_questions) > 0 and len(categories) > 0:
            print(f"  Sample: {batch_questions[0][:60]}... → {categories[0]}")

    except Exception as e:
        print(f"✗ ERROR: {str(e)[:100]}")
        all_election_types.extend([None] * len(batch_questions))

    time.sleep(1)  # Rate limiting

elapsed = time.time() - start_time

print(f"\n{'=' * 80}")
print("LABELING SUMMARY")
print(f"{'=' * 80}")
print(f"\nTotal markets: {len(questions):,}")
print(f"Successfully labeled: {sum(1 for x in all_election_types if x is not None):,}")
print(f"Failed: {sum(1 for x in all_election_types if x is None):,}")
print(f"Time: {elapsed/60:.1f} minutes")

# Show distribution
if all_election_types:
    from collections import Counter
    counts = Counter([x for x in all_election_types if x is not None])
    print(f"\nElection type distribution:")
    for etype in ELECTION_TYPES:
        count = counts.get(etype, 0)
        if count > 0:
            print(f"  {etype}: {count}")

# Add election_type to electoral_df
electoral_df['election_type'] = all_election_types

# Merge back with full categorized df
df_with_types = df.merge(
    electoral_df[['id', 'election_type']],
    on='id',
    how='left'
)

# Save results
print(f"\n{'=' * 80}")
print("SAVING RESULTS")
print(f"{'=' * 80}")

df_with_types.to_csv(OUTPUT_FILE, index=False)
print(f"\n✓ Saved {len(df_with_types):,} markets to:")
print(f"  {OUTPUT_FILE}")
print(f"\n✓ {len(electoral_df)} ELECTORAL markets now have election_type")
print(f"✓ {len(df) - len(electoral_df)} non-electoral markets have election_type = NaN")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
