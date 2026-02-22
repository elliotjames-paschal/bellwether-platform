#!/usr/bin/env python3
"""
Relabel election_type for new Kalshi markets using ChatGPT
Uses smaller batch size and better error handling
"""

import pandas as pd
import json
import time
from openai import OpenAI

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"

# OpenAI Configuration
BATCH_SIZE = 20  # Smaller batches for reliability
MODEL = "gpt-4o-mini"
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
print("RELABELING KALSHI ELECTION TYPES")
print("=" * 80)

# Load OpenAI API key
api_key_path = f"{BASE_DIR}/openai_api_key.txt"
with open(api_key_path, 'r') as f:
    api_key = f.read().strip()

client = OpenAI(api_key=api_key)

# Load master file
print(f"\nLoading master file...")
master_df = pd.read_csv(MASTER_FILE, low_memory=False)
print(f"✓ Loaded {len(master_df):,} rows")

# Get last 113 Kalshi markets (the ones just added)
kalshi_markets = master_df[master_df['platform'] == 'Kalshi'].tail(113).copy()
print(f"✓ Found {len(kalshi_markets)} new Kalshi markets to label")

# Get their indices in the main dataframe
kalshi_indices = kalshi_markets.index.tolist()

# Prepare questions
questions = kalshi_markets['question'].tolist()
total_batches = (len(questions) + BATCH_SIZE - 1) // BATCH_SIZE

print(f"\nBatch size: {BATCH_SIZE}")
print(f"Total batches: {total_batches}")

print(f"\n{'=' * 80}")
print("LABELING WITH CHATGPT")
print(f"{'=' * 80}")

all_election_types = []
failed_batches = []

for batch_num in range(total_batches):
    start_idx = batch_num * BATCH_SIZE
    end_idx = min(start_idx + BATCH_SIZE, len(questions))
    batch_questions = questions[start_idx:end_idx]

    # Create prompt
    prompt = f"""You are categorizing political prediction markets into election types.

For each market question below, determine which ONE election type it belongs to from this list:
{', '.join(ELECTION_TYPES)}

IMPORTANT:
- Return EXACTLY one category for each question
- Categories are case-sensitive - use exact spelling from the list
- Return valid JSON only, no markdown formatting
- For state/local offices (Comptroller, Public Advocate, Borough President, etc.), use "Mayoral"
- For state supreme courts, use "Regional Election"
- For House of Delegates, use "House"

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

        result_text = response.choices[0].message.content

        # Parse JSON
        result = json.loads(result_text)
        categories = result.get('categories', [])

        # Validate
        if len(categories) != len(batch_questions):
            print(f"❌ FAILED")
            print(f"  Expected {len(batch_questions)} categories, got {len(categories)}")
            failed_batches.append(batch_num)
            # Fill with None
            all_election_types.extend([None] * len(batch_questions))
            continue

        # Validate each category is in allowed list
        invalid = [c for c in categories if c not in ELECTION_TYPES and c is not None]
        if invalid:
            print(f"⚠ WARNING - Invalid categories: {invalid}")

        all_election_types.extend(categories)
        print(f"✓ SUCCESS")

        # Show sample
        if len(batch_questions) > 0:
            print(f"  Sample: {batch_questions[0][:60]}... → {categories[0]}")

    except json.JSONDecodeError as e:
        print(f"❌ JSON ERROR: {str(e)[:100]}")
        failed_batches.append(batch_num)
        all_election_types.extend([None] * len(batch_questions))

    except Exception as e:
        print(f"❌ ERROR: {str(e)[:100]}")
        failed_batches.append(batch_num)
        all_election_types.extend([None] * len(batch_questions))

    time.sleep(1)  # Rate limiting

# Summary
print(f"\n{'=' * 80}")
print("LABELING SUMMARY")
print(f"{'=' * 80}")

print(f"\nTotal markets: {len(questions)}")
print(f"Successfully labeled: {sum(1 for x in all_election_types if x is not None)}")
print(f"Failed: {sum(1 for x in all_election_types if x is None)}")
print(f"Failed batches: {failed_batches if failed_batches else 'None'}")

# Show distribution
if all_election_types:
    from collections import Counter
    counts = Counter([x for x in all_election_types if x is not None])
    print(f"\nElection type distribution:")
    for etype, count in counts.most_common():
        print(f"  {etype}: {count}")

# Update master file
print(f"\n{'=' * 80}")
print("UPDATING MASTER FILE")
print(f"{'=' * 80}")

# Backup first
from datetime import datetime
backup_file = MASTER_FILE.replace('.csv', f'_backup_relabel_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
master_df.to_csv(backup_file, index=False)
print(f"✓ Backed up to: {backup_file}")

# Update election_type for the new Kalshi markets
for i, idx in enumerate(kalshi_indices):
    if i < len(all_election_types):
        master_df.at[idx, 'election_type'] = all_election_types[i]

# Save
master_df.to_csv(MASTER_FILE, index=False)
print(f"✓ Updated {len(kalshi_indices)} markets in master file")
print(f"✓ Saved to: {MASTER_FILE}")

# Verify
updated = master_df.loc[kalshi_indices]
labeled_count = updated['election_type'].notna().sum()
print(f"\n✓ Verification: {labeled_count}/{len(kalshi_indices)} markets have election_type")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
